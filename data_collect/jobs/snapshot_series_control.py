"""Bounded monitoring, resume and finalization for Research Snapshot series.

The orchestration layer deliberately treats the manifest table as the source
of progress truth.  Progress monitoring never scans materialized snapshot or
lineage rows.  Deep acceptance is delayed until finalization and runs in small
``snapshot_id`` batches so PostgreSQL can use the existing lookup indexes and
commit every lifecycle transition independently.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable, Protocol, Sequence

from data_collect.data_access.snapshot_lifecycle import MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE


KNOWN_STATUSES = frozenset({"DRAFT", "FAILED", "BUILDING", "VALIDATED", "FROZEN"})
PUBLISHABLE_STATUSES = frozenset({"VALIDATED", "FROZEN"})


class MonitorMode(str, Enum):
    PROGRESS = "progress"
    ACCEPTANCE = "acceptance"


class InventoryRepository(Protocol):
    def open_trade_dates(
        self, start: date, end: date, exchange: str, context: Any,
    ) -> tuple[date, ...]: ...

    def series_inventory(
        self,
        baseline_tag: str,
        schema_version: str,
        start: date,
        end: date,
        context: Any,
        *,
        max_rows: int,
    ) -> list[dict]: ...


@dataclass(frozen=True)
class SeriesSelector:
    start: date
    end: date
    baseline_tag: str
    schema_version: str
    expected_snapshots: int
    exchange: str = "SSE"

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("start must not be after end")
        if not self.baseline_tag.strip() or not self.schema_version.strip():
            raise ValueError("baseline_tag and schema_version are required")
        # Monitoring fetches at most expectation + 1 so an overflow is
        # detected without an unbounded read.  Keep that sentinel within the
        # repository/policy ceiling of 100,000 rows.
        if self.expected_snapshots < 1 or self.expected_snapshots > 99_999:
            raise ValueError("expected_snapshots must be between 1 and 99999")

    @property
    def digest(self) -> str:
        raw = "|".join((
            self.start.isoformat(), self.end.isoformat(), self.baseline_tag,
            self.schema_version, str(self.expected_snapshots), self.exchange,
        ))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


@dataclass(frozen=True)
class SnapshotRecord:
    snapshot_id: str
    cutoff: date
    status: str
    input_hash: str | None = None
    quality_hash: str | None = None
    baseline_quality_hash: str | None = None
    baseline_source_hash: str | None = None
    code_version: str | None = None
    git_commit: str | None = None
    git_dirty: bool = False
    diff_hash: str | None = None
    code_archive_path: str | None = None
    built_at: datetime | None = None
    validated_at: datetime | None = None
    frozen_at: datetime | None = None
    failed_at: datetime | None = None
    failure_reason: str | None = None
    lineage_storage_version: str = "v1"

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "SnapshotRecord":
        return cls(
            snapshot_id=str(row["snapshot_id"]),
            cutoff=row["research_cutoff_date"],
            status=str(row["status"]),
            input_hash=row.get("input_manifest_hash"),
            quality_hash=row.get("quality_report_hash"),
            baseline_quality_hash=row.get("baseline_quality_report_hash"),
            baseline_source_hash=row.get("baseline_source_manifest_hash"),
            code_version=row.get("code_version"),
            git_commit=row.get("git_commit"),
            git_dirty=bool(row.get("git_dirty")),
            diff_hash=row.get("diff_hash"),
            code_archive_path=row.get("code_archive_path"),
            built_at=row.get("built_at"),
            validated_at=row.get("validated_at"),
            frozen_at=row.get("frozen_at"),
            failed_at=row.get("failed_at"),
            failure_reason=row.get("failure_reason"),
            lineage_storage_version=str(row.get("lineage_storage_version") or "v1"),
        )


@dataclass(frozen=True)
class SeriesIssue:
    code: str
    severity: str
    message: str
    details: Any = None


@dataclass(frozen=True)
class LogFinding:
    path: str
    category: str
    severity: str
    excerpt: str


@dataclass
class SeriesReport:
    selector: SeriesSelector
    mode: MonitorMode
    expected_dates: tuple[date, ...]
    records: tuple[SnapshotRecord, ...]
    issues: list[SeriesIssue] = field(default_factory=list)
    log_findings: list[LogFinding] = field(default_factory=list)

    @property
    def status_counts(self) -> dict[str, int]:
        counts = Counter(record.status for record in self.records)
        return {key: counts[key] for key in sorted(counts)}

    @property
    def blocking(self) -> bool:
        return (
            any(issue.severity == "ERROR" for issue in self.issues)
            or any(item.severity == "ERROR" for item in self.log_findings)
        )

    def records_with_statuses(self, statuses: Iterable[str]) -> tuple[SnapshotRecord, ...]:
        accepted = frozenset(statuses)
        return tuple(record for record in self.records if record.status in accepted)

    def to_dict(self) -> dict[str, Any]:
        return {
            "selector": _jsonable(asdict(self.selector)),
            "mode": self.mode.value,
            "expected_date_count": len(self.expected_dates),
            "manifest_count": len(self.records),
            "status_counts": self.status_counts,
            "blocking": self.blocking,
            "issues": [_jsonable(asdict(issue)) for issue in self.issues],
            "log_findings": [_jsonable(asdict(item)) for item in self.log_findings],
        }


_LOG_PATTERNS = (
    ("statement_timeout", "ERROR", re.compile(r"statement timeout|canceling statement due to statement timeout", re.I)),
    ("lock_timeout", "ERROR", re.compile(r"lock timeout|canceling statement due to lock timeout", re.I)),
    ("deadlock", "ERROR", re.compile(r"deadlock detected", re.I)),
    # A bare ROLLBACK can be deliberate (smoke-test cleanup or operator
    # recovery).  Surface it without failing an otherwise healthy progress
    # check; surrounding ERROR/Traceback lines are classified separately.
    ("rollback", "WARNING", re.compile(r"\brollback\b|transaction.*rolled back", re.I)),
    ("serialization", "ERROR", re.compile(r"could not serialize|serialization failure", re.I)),
    ("failed_transaction", "ERROR", re.compile(r"InFailedSqlTransaction|current transaction is aborted", re.I)),
    ("traceback", "ERROR", re.compile(r"Traceback \(most recent call last\)", re.I)),
)


def scan_log_tails(paths: Sequence[str | Path], *, tail_bytes: int = 2 * 1024 * 1024) -> list[LogFinding]:
    """Scan only the bounded tail of explicitly selected log files."""
    if tail_bytes < 1 or tail_bytes > 16 * 1024 * 1024:
        raise ValueError("tail_bytes must be between 1 and 16 MiB")
    findings: list[LogFinding] = []
    for raw_path in paths:
        path = Path(raw_path)
        if not path.is_file():
            findings.append(LogFinding(
                str(path), "missing_log", "ERROR", "log file does not exist",
            ))
            continue
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - tail_bytes))
            content = handle.read(tail_bytes).decode("utf-8", "replace")
        lines = content.splitlines()
        for index, line in enumerate(lines):
            for category, severity, pattern in _LOG_PATTERNS:
                if pattern.search(line):
                    start = max(0, index - 1)
                    end = min(len(lines), index + 2)
                    findings.append(LogFinding(
                        str(path), category, severity,
                        " | ".join(lines[start:end])[:2000],
                    ))
                    break
    return findings


class SnapshotSeriesMonitor:
    """Small-table progress inspection with no lifecycle writes."""

    def __init__(self, repository: InventoryRepository, context: Any) -> None:
        self.repository = repository
        self.context = context

    def inspect(
        self,
        selector: SeriesSelector,
        *,
        mode: MonitorMode = MonitorMode.PROGRESS,
        log_paths: Sequence[str | Path] = (),
        stale_building_after: timedelta | None = None,
    ) -> SeriesReport:
        expected_dates = self.repository.open_trade_dates(
            selector.start, selector.end, selector.exchange, self.context,
        )
        # One extra row makes an unexpected manifest count fail explicitly
        # without permitting an unbounded fetch.
        rows = self.repository.series_inventory(
            selector.baseline_tag,
            selector.schema_version,
            selector.start,
            selector.end,
            self.context,
            max_rows=selector.expected_snapshots + 1,
        )
        records = tuple(SnapshotRecord.from_row(row) for row in rows)
        report = SeriesReport(selector, mode, expected_dates, records)
        report.log_findings.extend(scan_log_tails(log_paths))

        if len(expected_dates) != selector.expected_snapshots:
            report.issues.append(SeriesIssue(
                "calendar_count_mismatch", "ERROR",
                "standard trading calendar count differs from the explicit expectation",
                {"calendar": len(expected_dates), "expected": selector.expected_snapshots},
            ))
        if len(records) != selector.expected_snapshots:
            report.issues.append(SeriesIssue(
                "manifest_count_mismatch", "ERROR",
                "snapshot manifest count differs from the explicit expectation",
                {"manifest": len(records), "expected": selector.expected_snapshots},
            ))

        expected_set = set(expected_dates)
        actual_dates = [record.cutoff for record in records]
        actual_set = set(actual_dates)
        duplicates = sorted(day for day, count in Counter(actual_dates).items() if count > 1)
        missing = sorted(expected_set - actual_set)
        unexpected = sorted(actual_set - expected_set)
        if duplicates:
            report.issues.append(SeriesIssue(
                "duplicate_cutoff", "ERROR", "duplicate snapshot dates detected",
                [day.isoformat() for day in duplicates],
            ))
        if missing:
            report.issues.append(SeriesIssue(
                "missing_cutoff", "ERROR", "trading dates have no snapshot manifest",
                [day.isoformat() for day in missing],
            ))
        if unexpected:
            report.issues.append(SeriesIssue(
                "unexpected_cutoff", "ERROR", "snapshot dates are absent from the open calendar",
                [day.isoformat() for day in unexpected],
            ))

        unknown = sorted({record.status for record in records} - KNOWN_STATUSES)
        if unknown:
            report.issues.append(SeriesIssue(
                "unknown_status", "ERROR", "unknown snapshot lifecycle status", unknown,
            ))
        if mode == MonitorMode.ACCEPTANCE:
            pending = [
                {"date": record.cutoff.isoformat(), "status": record.status}
                for record in records if record.status not in PUBLISHABLE_STATUSES
            ]
            if pending:
                report.issues.append(SeriesIssue(
                    "non_publishable_status", "ERROR",
                    "acceptance requires every date to be VALIDATED or FROZEN",
                    pending[:100],
                ))
        else:
            failed = [record for record in records if record.status == "FAILED"]
            if failed:
                report.issues.append(SeriesIssue(
                    "failed_work_queue", "WARNING",
                    "FAILED snapshots remain in the resumable work queue",
                    {
                        "count": len(failed),
                        "samples": [
                            {
                                "date": record.cutoff.isoformat(),
                                "snapshot_id": record.snapshot_id,
                                "reason": (record.failure_reason or "")[:500],
                            }
                            for record in failed[:20]
                        ],
                    },
                ))

        for record in records:
            if record.status in PUBLISHABLE_STATUSES and (
                not record.input_hash or not record.quality_hash
                or not record.baseline_quality_hash or not record.baseline_source_hash
            ):
                report.issues.append(SeriesIssue(
                    "publishable_hash_missing", "ERROR",
                    "publishable snapshot is missing required hash evidence",
                    {"snapshot_id": record.snapshot_id, "date": record.cutoff.isoformat()},
                ))
            if record.status in PUBLISHABLE_STATUSES and (
                not record.code_version or not record.git_commit
            ):
                report.issues.append(SeriesIssue(
                    "publishable_code_identity_missing", "ERROR",
                    "publishable snapshot is missing its build code identity",
                    {"snapshot_id": record.snapshot_id, "date": record.cutoff.isoformat()},
                ))
            if record.git_dirty and not record.diff_hash:
                report.issues.append(SeriesIssue(
                    "dirty_identity_incomplete", "ERROR",
                    "dirty snapshot code identity is missing diff_hash",
                    {"snapshot_id": record.snapshot_id, "date": record.cutoff.isoformat()},
                ))
            lifecycle_problem = None
            if record.status == "BUILDING" and record.built_at is None:
                lifecycle_problem = "BUILDING snapshot has no built_at"
            elif record.status == "VALIDATED" and record.validated_at is None:
                lifecycle_problem = "VALIDATED snapshot has no validated_at"
            elif record.status == "FROZEN" and (
                record.validated_at is None or record.frozen_at is None
            ):
                lifecycle_problem = "FROZEN snapshot lacks validated_at/frozen_at"
            elif record.status == "FROZEN" and record.git_dirty and not record.code_archive_path:
                lifecycle_problem = "dirty FROZEN snapshot has no code archive"
            if lifecycle_problem:
                report.issues.append(SeriesIssue(
                    "lifecycle_evidence_incomplete", "ERROR", lifecycle_problem,
                    {"snapshot_id": record.snapshot_id, "date": record.cutoff.isoformat()},
                ))

        if mode == MonitorMode.ACCEPTANCE:
            build_identities = {
                (
                    record.code_version,
                    record.git_commit,
                    record.git_dirty,
                    record.diff_hash,
                )
                for record in records
                if record.status in PUBLISHABLE_STATUSES
            }
            if len(build_identities) > 1:
                report.issues.append(SeriesIssue(
                    "mixed_build_code_identity", "WARNING",
                    "snapshot series contains multiple recorded build code identities",
                    {"identity_count": len(build_identities)},
                ))

        if stale_building_after is not None:
            if stale_building_after <= timedelta(0):
                raise ValueError("stale_building_after must be positive")
            stale = []
            for record in records:
                built_at = record.built_at
                if record.status != "BUILDING" or built_at is None:
                    continue
                now = (
                    datetime.now()
                    if built_at.tzinfo is None
                    else datetime.now(timezone.utc)
                )
                if now - built_at > stale_building_after:
                    stale.append({"snapshot_id": record.snapshot_id, "date": record.cutoff.isoformat()})
            if stale:
                report.issues.append(SeriesIssue(
                    "stale_building_candidate", "WARNING",
                    "old BUILDING snapshots require operator/process verification; they are not reset automatically",
                    stale[:100],
                ))
        return report


def bounded_batches(
    values: Iterable[Any], batch_size: int, *,
    maximum: int = MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE,
) -> tuple[tuple[Any, ...], ...]:
    if maximum < 1 or maximum > MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE:
        raise ValueError(
            "maximum must be between 1 and "
            f"{MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE}"
        )
    if batch_size < 1 or batch_size > maximum:
        raise ValueError(f"batch_size must be between 1 and {maximum}")
    items = tuple(values)
    return tuple(items[offset:offset + batch_size] for offset in range(0, len(items), batch_size))


def _checkpoint_progress(progress_tracker, run_id: str, job_name: str, scope_key: str, position: int) -> None:
    previous = progress_tracker.load(run_id, job_name, scope_key)
    if previous is None or (not previous.done and position > previous.cursor):
        progress_tracker.mark_progress(run_id, job_name, scope_key, position)


def _checkpoint_batch_done(
    progress_tracker,
    run_id: str,
    job_name: str,
    selector_digest: str,
    phase: str,
    records: Sequence[SnapshotRecord],
) -> None:
    """Persist one independently replayable batch, not a process-local count."""
    if not records:
        return
    identity = "|".join(record.snapshot_id for record in records)
    batch_digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]
    scope_key = (
        f"{selector_digest}:{phase}:"
        f"{records[0].cutoff.isoformat()}:{records[-1].cutoff.isoformat()}:"
        f"{batch_digest}"
    )
    previous = progress_tracker.load(run_id, job_name, scope_key)
    if previous is None or not previous.done:
        progress_tracker.mark_progress(
            run_id, job_name, scope_key, len(records),
        )
        progress_tracker.mark_done(run_id, job_name, scope_key)


def _resolve_run_id(value: str | None, default: str) -> str:
    resolved = value if value is not None else default
    if not resolved.strip() or len(resolved) > 160:
        raise ValueError("run_id must contain 1..160 non-blank characters")
    return resolved


class SnapshotSeriesResume:
    """Validate BUILDING first, then build-and-validate DRAFT/FAILED batches."""

    def __init__(
        self,
        monitor: SnapshotSeriesMonitor,
        progress_tracker,
        build_batch: Callable[[list[str]], None],
        validate_batch: Callable[[list[str]], None],
    ) -> None:
        self.monitor = monitor
        self.progress_tracker = progress_tracker
        self.build_batch = build_batch
        self.validate_batch = validate_batch

    def run(
        self, selector: SeriesSelector, *, batch_size: int = 2, run_id: str | None = None,
    ) -> SeriesReport:
        bounded_batches((), batch_size)
        run_id = _resolve_run_id(
            run_id, f"snapshot-series-resume-{selector.digest}",
        )
        report = self.monitor.inspect(selector, mode=MonitorMode.PROGRESS)
        structural_errors = [issue for issue in report.issues if issue.severity == "ERROR"]
        if structural_errors:
            raise RuntimeError(f"snapshot series resume preflight failed: {structural_errors}")

        building = report.records_with_statuses({"BUILDING"})
        for chunk in bounded_batches(building, batch_size):
            ids = [record.snapshot_id for record in chunk]
            self.validate_batch(ids)
            _checkpoint_batch_done(
                self.progress_tracker, run_id, "snapshot_series_resume",
                selector.digest, "validate-building", chunk,
            )
        self.progress_tracker.mark_done(
            run_id, "snapshot_series_resume", f"{selector.digest}:validate-building",
        )

        report = self.monitor.inspect(selector, mode=MonitorMode.PROGRESS)
        pending = report.records_with_statuses({"DRAFT", "FAILED"})
        for chunk in bounded_batches(pending, batch_size):
            ids = [record.snapshot_id for record in chunk]
            self.build_batch(ids)
            # Validate the same bounded batch immediately.  This prevents a
            # multi-year run from accumulating hundreds of BUILDING rows.
            self.validate_batch(ids)
            _checkpoint_batch_done(
                self.progress_tracker, run_id, "snapshot_series_resume",
                selector.digest, "build-validate", chunk,
            )
        self.progress_tracker.mark_done(
            run_id, "snapshot_series_resume", f"{selector.digest}:build-validate",
        )

        final_report = self.monitor.inspect(selector, mode=MonitorMode.ACCEPTANCE)
        if final_report.blocking:
            raise RuntimeError(f"snapshot series remains incomplete after resume: {final_report.to_dict()}")
        return final_report


class SnapshotSeriesFinalize:
    """Deep-inspect the whole series, then freeze it in bounded batches."""

    def __init__(
        self,
        monitor: SnapshotSeriesMonitor,
        progress_tracker,
        inspect_batch: Callable[[list[str]], dict[str, list[dict]]],
        freeze_batch: Callable[[list[str]], None],
        *,
        code_archive_available: bool = False,
        immediate_inspect_batch: Callable[[list[str]], dict[str, list[dict]]] | None = None,
    ) -> None:
        self.monitor = monitor
        self.progress_tracker = progress_tracker
        self.inspect_batch = inspect_batch
        self.immediate_inspect_batch = immediate_inspect_batch or inspect_batch
        self.freeze_batch = freeze_batch
        self.code_archive_available = code_archive_available

    @staticmethod
    def _assert_resumable_state(
        report: SeriesReport, *, partial_resume_authorized: bool = False,
    ) -> None:
        if report.blocking:
            raise RuntimeError(f"snapshot finalize inventory preflight failed: {report.to_dict()}")
        statuses = [record.status for record in report.records]
        if any(status not in PUBLISHABLE_STATUSES for status in statuses):
            raise RuntimeError("finalize accepts only VALIDATED/FROZEN snapshots")
        if "FROZEN" in statuses and "VALIDATED" in statuses and not partial_resume_authorized:
            raise RuntimeError(
                "partial freeze requires a completed preflight checkpoint from the same finalize run"
            )
        # A resumed finalization may have a chronologically frozen prefix.  A
        # mixed or gapped pattern indicates an unrelated/manual freeze and is
        # deliberately rejected.
        seen_validated = False
        for status in statuses:
            if status == "VALIDATED":
                seen_validated = True
            elif status == "FROZEN" and seen_validated:
                raise RuntimeError("partial freeze is not a contiguous chronological prefix")

    def run(
        self, selector: SeriesSelector, *, batch_size: int = 10, run_id: str | None = None,
    ) -> SeriesReport:
        bounded_batches((), batch_size)
        run_id = _resolve_run_id(
            run_id, f"snapshot-series-finalize-{selector.digest}",
        )
        report = self.monitor.inspect(selector, mode=MonitorMode.ACCEPTANCE)
        preflight_scope = f"{selector.digest}:preflight"
        previous_preflight = self.progress_tracker.load(
            run_id, "snapshot_series_finalize", preflight_scope,
        )
        self._assert_resumable_state(
            report,
            partial_resume_authorized=bool(
                previous_preflight is not None and previous_preflight.done
            ),
        )
        dirty_pending = [
            record for record in report.records
            if record.status == "VALIDATED" and record.git_dirty
        ]
        if dirty_pending and not self.code_archive_available:
            raise RuntimeError(
                "dirty-built VALIDATED snapshots require code_archive_path before finalize"
            )

        # Complete the deep preflight for every date before freezing the first
        # pending batch.  Queries remain per-snapshot and batch-bounded.  The
        # aggregate checkpoint stores only a cursor, not the issue findings.
        # Therefore an incomplete preflight cannot safely resume at its cursor:
        # a process failure after a bad batch could otherwise skip that batch
        # and publish an incomplete audit.  Completed preflights still skip all
        # deep checks on a legitimate restart.
        if previous_preflight is None or not previous_preflight.done:
            inspected = 0
            issue_count = 0
            issue_samples: dict[str, list[dict]] = {}
            for chunk in bounded_batches(report.records[inspected:], batch_size):
                issues = self.inspect_batch([record.snapshot_id for record in chunk])
                issue_count += len(issues)
                for key, value in issues.items():
                    if len(issue_samples) >= 100:
                        break
                    sample_key = key
                    if key == "__batch__":
                        sample_key = (
                            f"batch:{chunk[0].cutoff.isoformat()}:"
                            f"{chunk[-1].cutoff.isoformat()}"
                        )
                    issue_samples[sample_key] = value
                inspected += len(chunk)
                _checkpoint_progress(
                    self.progress_tracker, run_id, "snapshot_series_finalize",
                    preflight_scope, inspected,
                )
                _checkpoint_batch_done(
                    self.progress_tracker, run_id, "snapshot_series_finalize",
                    selector.digest, "preflight-batch", chunk,
                )
            if issue_count:
                raise RuntimeError(
                    "snapshot deep acceptance failed: "
                    f"issue_groups={issue_count}, samples={issue_samples}"
                )
            self.progress_tracker.mark_done(
                run_id, "snapshot_series_finalize", preflight_scope,
            )

        pending = report.records_with_statuses({"VALIDATED"})
        frozen = 0
        for chunk in bounded_batches(pending, batch_size):
            ids = [record.snapshot_id for record in chunk]
            # VALIDATED child rows are not database-immutable yet.  Repeat the
            # same bounded audit immediately before each freeze transition so
            # a change after the all-series preflight cannot be published.
            immediate_issues = self.immediate_inspect_batch(ids)
            if immediate_issues:
                raise RuntimeError(
                    f"snapshot changed after finalize preflight: {immediate_issues}"
                )
            self.freeze_batch(ids)
            frozen += len(chunk)
            _checkpoint_batch_done(
                self.progress_tracker, run_id, "snapshot_series_finalize",
                selector.digest, "freeze-batch", chunk,
            )
            _checkpoint_progress(
                self.progress_tracker, run_id, "snapshot_series_finalize",
                f"{selector.digest}:freeze", frozen,
            )
        self.progress_tracker.mark_done(
            run_id, "snapshot_series_finalize", f"{selector.digest}:freeze",
        )

        final_report = self.monitor.inspect(selector, mode=MonitorMode.ACCEPTANCE)
        non_frozen = [record for record in final_report.records if record.status != "FROZEN"]
        if final_report.blocking or non_frozen:
            raise RuntimeError(f"snapshot finalize did not reach all-FROZEN: {final_report.to_dict()}")
        return final_report


def report_json(report: SeriesReport) -> str:
    return json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True, indent=2)


def _jsonable(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value
