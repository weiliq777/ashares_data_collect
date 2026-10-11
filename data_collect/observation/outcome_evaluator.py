"""Incremental evaluator for frozen Market Anomaly Outcome (市场异常后验结果)."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
import json
from typing import Any, Mapping, Protocol, Sequence

from data_collect.data_access.context import RunContext
from data_collect.observation.outcome_metrics import OutcomePolicy, compute_outcome


MAX_BATCHES_PER_RUN = 5
MAX_EVENTS_PER_BATCH = 100
MAX_INITIALIZATION_DATES_PER_RUN = 31


class OutcomeRepository(Protocol):
    """Storage seam used by the evaluator; SQL remains in its PostgreSQL adapter."""

    def exclusive(self): ...
    def initialization_watermark(self, policy_hash: str) -> date | None: ...
    def advance_initialization_watermark(self, through_date: date, policy_hash: str) -> None: ...
    def next_frozen_observation_date(self, after_date: date, through_date: date, context: RunContext) -> date | None: ...
    def has_initialization_candidates(self, observation_date: date, policy_hash: str, context: RunContext, *, snapshot_ids: Sequence[str] = ()) -> bool: ...
    def snapshot_dates(self, snapshot_ids: Sequence[str], context: RunContext) -> list[dict[str, Any]]: ...
    def initialization_batch(self, observation_date: date, through_date: date, policy_hash: str, context: RunContext, *, snapshot_ids: Sequence[str] = (), benchmark_code: str, limit: int = MAX_EVENTS_PER_BATCH) -> tuple[list[dict[str, Any]], dict[str, tuple[date, ...]]]: ...
    def due_batch(self, through_date: date, context: RunContext, *, policy_hash: str, benchmark_code: str, snapshot_ids: Sequence[str] = (), limit: int = MAX_EVENTS_PER_BATCH): ...
    def has_due(self, through_date: date, policy_hash: str, context: RunContext, *, snapshot_ids: Sequence[str] = ()) -> bool: ...
    def write_rows(self, rows: Sequence[Mapping[str, Any]], context: RunContext) -> int: ...


@dataclass(frozen=True, slots=True)
class OutcomeRunReport:
    status: str
    through_date: date
    batches: int
    candidate_events: int
    inserted_or_updated: int
    pending_events: int
    matured_events: int
    incomplete_events: int
    invalid_events: int
    initialization_dates_scanned: int
    initialization_watermark: date | None
    explicit_replay: bool
    more_work_pending: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "through_date": self.through_date.isoformat(),
            "batches": self.batches,
            "candidate_events": self.candidate_events,
            "inserted_or_updated": self.inserted_or_updated,
            "pending_events": self.pending_events,
            "matured_events": self.matured_events,
            "incomplete_events": self.incomplete_events,
            "invalid_events": self.invalid_events,
            "initialization_dates_scanned": self.initialization_dates_scanned,
            "initialization_watermark": (
                self.initialization_watermark.isoformat()
                if self.initialization_watermark else None
            ),
            "explicit_replay": self.explicit_replay,
            "more_work_pending": self.more_work_pending,
        }


class OutcomeEvaluator:
    """Small interface for bounded initialization and maturity progression.

    One run handles at most five transactions of at most 100 events each. A
    partial run is reported explicitly and resumes from persisted Outcome rows
    and the frozen-snapshot date watermark; it never scans the full history.
    """

    def __init__(
        self, repository: OutcomeRepository, policy: OutcomePolicy | None = None,
    ) -> None:
        self._repository = repository
        self._policy = policy or OutcomePolicy()

    def run(
        self,
        *,
        through_date: date,
        observation_snapshot_ids: Sequence[str] = (),
        context: RunContext,
    ) -> OutcomeRunReport:
        if not isinstance(through_date, date):
            raise TypeError("through_date must be a date")
        snapshot_ids = tuple(dict.fromkeys(
            str(value).strip() for value in observation_snapshot_ids if str(value).strip()
        ))
        if len(snapshot_ids) > 5:
            raise ValueError("at most 5 explicit frozen snapshot IDs are allowed per run")
        explicit = bool(snapshot_ids)
        batches = candidate_count = written = 0
        pending = matured = incomplete = invalid = 0
        initialization_dates_scanned = 0
        limit_hit = False
        last_initialization_date: date | None = None
        dates_by_snapshot: dict[date, list[str]] = {}

        with self._repository.exclusive():
            if explicit:
                dates_by_snapshot = defaultdict(list)
                snapshot_rows = self._repository.snapshot_dates(snapshot_ids, context)
                for row in snapshot_rows:
                    snapshot_date = self._as_date(row["observation_date_start"])
                    if snapshot_date > through_date:
                        raise ValueError("explicit snapshot date cannot be after through_date")
                    dates_by_snapshot[snapshot_date].append(str(row["snapshot_id"]))
                for snapshot_date in sorted(dates_by_snapshot):
                    initialization_dates_scanned += 1
                    ids = tuple(dates_by_snapshot[snapshot_date])
                    last_initialization_date = snapshot_date
                    while batches < MAX_BATCHES_PER_RUN:
                        events, calendar_paths = self._repository.initialization_batch(
                            snapshot_date, through_date, self._policy.methodology_hash,
                            context, snapshot_ids=ids,
                            benchmark_code=self._policy.benchmark_code,
                            limit=MAX_EVENTS_PER_BATCH,
                        )
                        if not events:
                            break
                        results = self._initialize_rows(
                            events, calendar_paths, through_date, context,
                        )
                        written += self._repository.write_rows(results, context)
                        candidate_count += len(events)
                        batches += 1
                        invalid += sum(row["maturity_status"] == "INVALID_INPUT" for row in results)
                        pending += sum(row["maturity_status"] == "PENDING_MATURITY" for row in results)
                    if batches >= MAX_BATCHES_PER_RUN:
                        break
                if batches >= MAX_BATCHES_PER_RUN:
                    limit_hit = any(
                        self._repository.has_initialization_candidates(
                            snapshot_date, self._policy.methodology_hash, context,
                            snapshot_ids=tuple(ids),
                        )
                        for snapshot_date, ids in dates_by_snapshot.items()
                    )
            else:
                watermark = self._repository.initialization_watermark(
                    self._policy.methodology_hash,
                )
                after_date = watermark or (through_date - timedelta(days=1))
                if after_date > through_date:
                    raise ValueError("through_date precedes persisted Outcome watermark")
                while (
                    batches < MAX_BATCHES_PER_RUN
                    and initialization_dates_scanned < MAX_INITIALIZATION_DATES_PER_RUN
                ):
                    observation_date = self._repository.next_frozen_observation_date(
                        after_date, through_date, context,
                    )
                    if observation_date is None:
                        self._repository.advance_initialization_watermark(
                            through_date, self._policy.methodology_hash,
                        )
                        after_date = through_date
                        break
                    initialization_dates_scanned += 1
                    last_initialization_date = observation_date
                    while batches < MAX_BATCHES_PER_RUN:
                        events, calendar_paths = self._repository.initialization_batch(
                            observation_date, through_date, self._policy.methodology_hash,
                            context, benchmark_code=self._policy.benchmark_code,
                            limit=MAX_EVENTS_PER_BATCH,
                        )
                        if not events:
                            self._repository.advance_initialization_watermark(
                                observation_date, self._policy.methodology_hash,
                            )
                            after_date = observation_date
                            break
                        results = self._initialize_rows(
                            events, calendar_paths, through_date, context,
                        )
                        written += self._repository.write_rows(results, context)
                        candidate_count += len(events)
                        batches += 1
                        invalid += sum(row["maturity_status"] == "INVALID_INPUT" for row in results)
                        pending += sum(row["maturity_status"] == "PENDING_MATURITY" for row in results)
                    if batches >= MAX_BATCHES_PER_RUN:
                        break
                    if after_date < observation_date:
                        # The selected date was fully drained and checkpointed.
                        continue
                watermark = self._repository.initialization_watermark(
                    self._policy.methodology_hash,
                )

                if (
                    initialization_dates_scanned >= MAX_INITIALIZATION_DATES_PER_RUN
                    and batches < MAX_BATCHES_PER_RUN
                ):
                    next_date = self._repository.next_frozen_observation_date(
                        after_date, through_date, context,
                    )
                    if next_date is None:
                        self._repository.advance_initialization_watermark(
                            through_date, self._policy.methodology_hash,
                        )
                    else:
                        limit_hit = True

            if not explicit and batches >= MAX_BATCHES_PER_RUN and last_initialization_date is not None:
                same_date_pending = self._repository.has_initialization_candidates(
                    last_initialization_date, self._policy.methodology_hash, context,
                )
                if same_date_pending:
                    limit_hit = True
                else:
                    self._repository.advance_initialization_watermark(
                        last_initialization_date, self._policy.methodology_hash,
                    )
                    next_date = self._repository.next_frozen_observation_date(
                        last_initialization_date, through_date, context,
                    )
                    if next_date is None:
                        self._repository.advance_initialization_watermark(
                            through_date, self._policy.methodology_hash,
                        )
                    else:
                        limit_hit = True

            while batches < MAX_BATCHES_PER_RUN:
                events, calendar_paths, stock_paths, benchmark_paths = self._repository.due_batch(
                    through_date, context,
                    policy_hash=self._policy.methodology_hash,
                    benchmark_code=self._policy.benchmark_code,
                    snapshot_ids=snapshot_ids if explicit else (),
                    limit=MAX_EVENTS_PER_BATCH,
                )
                if not events:
                    break
                results: list[dict[str, Any]] = []
                for event in events:
                    event_id = str(event["observation_id"])
                    calculation_event = self._restore_anchor_metadata(event)
                    calculation_event["input_manifest_hash"] = event.get(
                        "observation_input_manifest_hash", "",
                    )
                    calculation_event["observation_hash"] = event.get("observation_hash", "")
                    result = compute_outcome(
                        calculation_event,
                        calendar_dates=calendar_paths[event_id],
                        stock_rows=stock_paths[event_id],
                        benchmark_rows=benchmark_paths[event_id],
                        through_date=through_date,
                        policy=self._policy,
                        existing=event,
                    )
                    result["observation_date"] = self._as_date(event["observation_date"])
                    result["ts_code"] = str(event["ts_code"])
                    results.append(result)
                written += self._repository.write_rows(results, context)
                candidate_count += len(events)
                batches += 1
                matured += sum(row["maturity_status"] == "MATURED" for row in results)
                incomplete += sum(row["maturity_status"] == "INCOMPLETE" for row in results)
                invalid += sum(row["maturity_status"] == "INVALID_INPUT" for row in results)
                pending += sum(row["maturity_status"] == "PENDING_MATURITY" for row in results)
            if batches >= MAX_BATCHES_PER_RUN:
                limit_hit = limit_hit or self._repository.has_due(
                    through_date, self._policy.methodology_hash, context,
                    snapshot_ids=snapshot_ids if explicit else (),
                )
                if explicit:
                    limit_hit = limit_hit or any(
                        self._repository.has_initialization_candidates(
                            snapshot_date, self._policy.methodology_hash, context,
                            snapshot_ids=tuple(ids),
                        )
                        for snapshot_date, ids in dates_by_snapshot.items()
                    )

            watermark = (
                self._repository.initialization_watermark(self._policy.methodology_hash)
                if not explicit else None
            )

        return OutcomeRunReport(
            status="INCOMPLETE_RUN" if limit_hit else "COMPLETE",
            through_date=through_date,
            batches=batches,
            candidate_events=candidate_count,
            inserted_or_updated=written,
            pending_events=pending,
            matured_events=matured,
            incomplete_events=incomplete,
            invalid_events=invalid,
            initialization_dates_scanned=initialization_dates_scanned,
            initialization_watermark=watermark,
            explicit_replay=explicit,
            more_work_pending=limit_hit,
        )

    def _initialize_rows(
        self,
        events: Sequence[Mapping[str, Any]],
        calendar_paths: Mapping[str, Sequence[date]],
        through_date: date,
        context: RunContext,
    ) -> list[dict[str, Any]]:
        results = []
        for event in events:
            event_date = self._as_date(event["observation_date"])
            event_id = str(event["observation_id"])
            if event_date != through_date:
                # Historical anchor capture is forbidden unless it was stored
                # with the original frozen Observation. This domain has no such
                # legacy anchor, so use the pure calculator's fail-closed path.
                result = compute_outcome(
                    event,
                    calendar_dates=(event_date,),
                    stock_rows=(), benchmark_rows=(), through_date=event_date,
                    policy=self._policy,
                )
            else:
                result = compute_outcome(
                    event,
                    calendar_dates=calendar_paths[event_id],
                    stock_rows=(), benchmark_rows=(), through_date=through_date,
                    policy=self._policy,
                )
            result["observation_date"] = event_date
            result["ts_code"] = str(event["ts_code"])
            results.append(result)
        return results

    @staticmethod
    def _restore_anchor_metadata(event: Mapping[str, Any]) -> dict[str, Any]:
        """Rehydrate calculation-only metadata from the immutable anchor manifest.

        The durable schema stores the numeric anchor coordinates in dedicated
        columns and the corresponding quality/as-of/version/hash in JSONB.
        Without this join-back, every resumed event would incorrectly become
        INVALID_INPUT after process restart.
        """
        restored = dict(event)
        provenance = restored.get("input_provenance") or {}
        if isinstance(provenance, str):
            try:
                provenance = json.loads(provenance)
            except (TypeError, ValueError):
                provenance = {}
        if not isinstance(provenance, Mapping):
            provenance = {}
        anchor = provenance.get("anchor") or {}
        if not isinstance(anchor, Mapping):
            anchor = {}
        restored.setdefault("anchor_adj_close_base", event.get("anchor_adj_close_base"))
        restored.setdefault("anchor_total_return_price", event.get("anchor_total_return_price"))
        restored.setdefault("anchor_benchmark_close", event.get("anchor_benchmark_close"))
        restored["anchor_quality_status"] = anchor.get(
            "anchor_quality_status", anchor.get("quality_status"),
        )
        restored["anchor_source_asof_date"] = anchor.get(
            "anchor_source_asof_date", anchor.get("source_asof_date"),
        )
        restored["anchor_price_coordinate_contract_version"] = anchor.get(
            "price_coordinate_contract_version",
        )
        restored["anchor_source_hash"] = anchor.get(
            "anchor_source_hash", anchor.get("source_hash", ""),
        )
        if restored.get("anchor_benchmark_close") is None:
            restored["anchor_benchmark_close"] = anchor.get(
                "anchor_benchmark_close", anchor.get("benchmark_close"),
            )
        return restored

    @staticmethod
    def _as_date(value: Any) -> date:
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value)[:10])


__all__ = [
    "MAX_BATCHES_PER_RUN", "MAX_EVENTS_PER_BATCH",
    "MAX_INITIALIZATION_DATES_PER_RUN", "OutcomeEvaluator",
    "OutcomeRunReport", "OutcomeRepository",
]
