"""Bounded SQL adapter for Market Anomaly Outcome (市场异常后验结果)."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import json
from typing import Any, Iterator, Mapping, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository
from data_collect.observation.outcome_metrics import HORIZONS


MAX_EVENTS_PER_BATCH = 100
MAX_CALENDAR_ROWS_PER_EVENT = max(HORIZONS) + 1
MAX_INITIAL_SNAPSHOTS = 5
BENCHMARK_CODE = "000985.CSI"
OUTCOME_CHECKPOINT_RUN_ID = "market-anomaly-outcome-init-v1"
OUTCOME_CHECKPOINT_SCOPE = "frozen-observation-date-watermark"

_METRIC_COLUMNS = tuple(
    f"{metric}_{horizon}d"
    for horizon in HORIZONS
    for metric in (
        "future_price_return", "future_total_return", "excess_return",
        "max_drawdown", "max_favorable_excursion", "max_adverse_excursion",
        "volatility", "best_return", "worst_return",
    )
)
_WRITE_COLUMNS = (
    "outcome_id", "observation_id", "evaluation_methodology_hash",
    "outcome_revision", "maturity_status", "anchor_adj_close_base",
    "anchor_total_return_price", "anchor_benchmark_close", "next_evaluation_date",
    "horizon_status", "input_provenance", "missing_reason_codes",
    "input_manifest_hash", *_METRIC_COLUMNS, "benchmark_code", "outcome_hash",
    "calculation_run_id", "updated_at",
)


def _date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _event_scope(events: Sequence[Mapping[str, Any]], *, domain: str) -> WorkScope:
    dates = [_date(event["observation_date"]) for event in events]
    codes = tuple(sorted({str(event["ts_code"]) for event in events}))
    snapshot_ids = tuple(sorted({str(event["snapshot_id"]) for event in events}))
    upper = max(dates) + timedelta(days=240)
    return WorkScope(
        date_from=min(dates), date_to=upper, codes=codes,
        partition=f"market-anomaly-outcome:{domain}",
        dimensions={"domain": "market_anomaly_outcome", "snapshot_ids": snapshot_ids},
    )


class MarketAnomalyOutcomeRepository(BaseRepository):
    """Own all set-based reads/writes for the bounded Outcome evaluator."""

    @contextmanager
    def exclusive(self) -> Iterator[None]:
        with self.kernel.lock_manager.session_exclusive(
            "evaluation.market_anomaly_outcome", "incremental-evaluator-v1",
        ):
            yield

    def initialization_watermark(self, policy_hash: str) -> date | None:
        state = self.kernel.checkpoint.load(
            OUTCOME_CHECKPOINT_RUN_ID, "market_anomaly_outcome_evaluator",
            f"{OUTCOME_CHECKPOINT_SCOPE}:{policy_hash}",
        )
        if state is None or state.cursor <= 0:
            return None
        return date.fromordinal(state.cursor)

    def advance_initialization_watermark(self, through_date: date, policy_hash: str) -> None:
        current = self.initialization_watermark(policy_hash)
        if current is not None and through_date < current:
            raise ValueError("Outcome initialization watermark cannot move backwards")
        self.kernel.checkpoint.mark_progress(
            OUTCOME_CHECKPOINT_RUN_ID, "market_anomaly_outcome_evaluator",
            f"{OUTCOME_CHECKPOINT_SCOPE}:{policy_hash}", through_date.toordinal(),
        )

    def next_frozen_observation_date(
        self, after_date: date, through_date: date, context: RunContext,
    ) -> date | None:
        if after_date >= through_date:
            return None
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.snapshot",
            sql="""
                SELECT observation_date_start
                  FROM observation.snapshot
                 WHERE status='FROZEN'
                   AND observation_date_start > %s
                   AND observation_date_start <= %s
                 ORDER BY observation_date_start, snapshot_id
                 LIMIT 1
            """,
            params=(after_date, through_date),
            scope=WorkScope(
                date_from=after_date + timedelta(days=1), date_to=through_date,
                partition="market-anomaly-outcome:next-frozen-date",
            ),
            max_rows=1,
        )
        return _date(rows[0]["observation_date_start"]) if rows else None

    def snapshot_dates(
        self, snapshot_ids: Sequence[str], context: RunContext,
    ) -> list[dict[str, Any]]:
        ids = tuple(dict.fromkeys(str(value).strip() for value in snapshot_ids if str(value).strip()))
        if not ids or len(ids) > MAX_INITIAL_SNAPSHOTS:
            raise ValueError(f"explicit replay requires 1..{MAX_INITIAL_SNAPSHOTS} snapshot IDs")
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.snapshot",
            sql="""
                SELECT snapshot_id, observation_date_start, observation_date_end,
                       status
                  FROM observation.snapshot
                 WHERE snapshot_id=ANY(%s::uuid[])
                 ORDER BY observation_date_start, snapshot_id
                 LIMIT %s
            """,
            params=(list(ids), len(ids) + 1),
            scope=WorkScope(
                partition="market-anomaly-outcome:explicit-snapshots",
                dimensions={"snapshot_ids": ids},
            ),
            max_rows=len(ids) + 1,
        )
        if len(rows) != len(ids):
            raise ValueError("one or more explicit Observation snapshot IDs do not exist")
        if any(str(row.get("status") or "").upper() != "FROZEN" for row in rows):
            raise ValueError("Outcome replay accepts FROZEN Observation snapshots only")
        return [dict(row) for row in rows]

    def _candidate_rows(
        self,
        observation_date: date,
        policy_hash: str,
        context: RunContext,
        *,
        snapshot_ids: Sequence[str] = (),
        limit: int = MAX_EVENTS_PER_BATCH,
    ) -> list[dict[str, Any]]:
        if not 1 <= limit <= MAX_EVENTS_PER_BATCH:
            raise ValueError("Outcome event batch limit must be in 1..100")
        scope = WorkScope(
            date_from=observation_date, date_to=observation_date,
            partition=f"market-anomaly-outcome:init:{observation_date.isoformat()}",
            dimensions={"domain": "market_anomaly_outcome", "snapshot_ids": tuple(snapshot_ids)},
        )
        snapshot_predicate = ""
        params: tuple[Any, ...] = (observation_date, policy_hash, limit + 1)
        if snapshot_ids:
            if len(snapshot_ids) > MAX_INITIAL_SNAPSHOTS:
                raise ValueError(f"at most {MAX_INITIAL_SNAPSHOTS} snapshots may be replayed per run")
            snapshot_predicate = "AND s.snapshot_id=ANY(%s::uuid[])"
            params = (observation_date, list(snapshot_ids), policy_hash, limit + 1)
        sql = f"""
            SELECT s.snapshot_id, s.observation_date_start,
                   s.data_baseline_id, s.input_manifest_hash AS snapshot_input_manifest_hash,
                   s.methodology_hash AS snapshot_methodology_hash,
                   d.observation_id, d.observation_date, d.ts_code,
                   d.methodology_hash, d.data_baseline_id AS observation_baseline_id,
                   d.input_manifest_hash, d.observation_hash
              FROM observation.snapshot s
              JOIN observation.stock_anomaly_daily d
                ON d.snapshot_id=s.snapshot_id
               AND d.observation_date=s.observation_date_start
             WHERE s.status='FROZEN'
               AND s.observation_date_start=%s
               {snapshot_predicate}
               AND d.anomaly_flag IS TRUE
               AND NOT EXISTS (
                   SELECT 1
                     FROM evaluation.market_anomaly_outcome o
                    WHERE o.observation_id=d.observation_id
                      AND o.evaluation_methodology_hash=%s
               )
             ORDER BY s.snapshot_id, d.observation_id
             LIMIT %s
        """
        rows = self.kernel.read._repository_query(
            context=context, source="observation.stock_anomaly_daily", sql=sql,
            params=params, scope=scope, max_rows=limit + 1,
        )
        if len(rows) > limit:
            return [dict(row) for row in rows[:limit]]
        return [dict(row) for row in rows]

    def has_initialization_candidates(
        self,
        observation_date: date,
        policy_hash: str,
        context: RunContext,
        *,
        snapshot_ids: Sequence[str] = (),
    ) -> bool:
        """Cheap bounded probe used only when the five-batch budget is hit."""
        return bool(self._candidate_rows(
            observation_date, policy_hash, context,
            snapshot_ids=snapshot_ids, limit=1,
        ))

    def has_due(
        self, through_date: date, policy_hash: str, context: RunContext,
        *, snapshot_ids: Sequence[str] = (),
    ) -> bool:
        ids = tuple(dict.fromkeys(str(value) for value in snapshot_ids))
        if len(ids) > MAX_INITIAL_SNAPSHOTS:
            raise ValueError(f"at most {MAX_INITIAL_SNAPSHOTS} snapshots may be replayed per run")
        if ids:
            sql = """
                SELECT 1
                  FROM observation.stock_anomaly_daily d
                  JOIN evaluation.market_anomaly_outcome o
                    ON o.observation_id=d.observation_id
                 WHERE d.snapshot_id=ANY(%s::uuid[])
                   AND o.maturity_status='PENDING_MATURITY'
                   AND o.next_evaluation_date IS NOT NULL
                   AND o.next_evaluation_date <= %s
                   AND o.evaluation_methodology_hash=%s
                 ORDER BY o.next_evaluation_date,o.outcome_id
                 LIMIT 1
            """
            params = (list(ids), through_date, policy_hash)
        else:
            sql = """
                SELECT 1
                  FROM evaluation.market_anomaly_outcome
                 WHERE maturity_status='PENDING_MATURITY'
                   AND next_evaluation_date IS NOT NULL
                   AND next_evaluation_date <= %s
                   AND evaluation_methodology_hash=%s
                 ORDER BY next_evaluation_date,outcome_id
                 LIMIT 1
            """
            params = (through_date, policy_hash)
        rows = self.kernel.read._repository_query(
            context=context,
            source="evaluation.market_anomaly_outcome",
            sql=sql,
            params=params,
            scope=WorkScope(
                date_to=through_date, partition="market-anomaly-outcome:due-probe",
                dimensions={"snapshot_ids": ids} if ids else {},
            ),
            max_rows=1,
        )
        return bool(rows)

    def _calendar_paths(
        self, events: Sequence[Mapping[str, Any]], context: RunContext,
    ) -> dict[str, tuple[date, ...]]:
        if not events:
            return {}
        if len(events) > MAX_EVENTS_PER_BATCH:
            raise ValueError("calendar path batch exceeds 100 events")
        scope = _event_scope(events, domain="calendar")
        sql = """
            WITH event_keys AS (
                SELECT *
                  FROM unnest(%s::uuid[], %s::date[])
                       AS e(observation_id, observation_date)
            )
            SELECT e.observation_id, c.trade_date
              FROM event_keys e
              CROSS JOIN LATERAL (
                  SELECT trade_date
                    FROM std.trading_calendar
                   WHERE exchange='SSE' AND is_open IS TRUE
                     AND trade_date >= e.observation_date
                     AND trade_date <= e.observation_date + 240
                   ORDER BY trade_date
                   LIMIT 121
              ) c
             ORDER BY e.observation_id, c.trade_date
        """
        rows = self.kernel.read._repository_query(
            context=context, source="std.trading_calendar", sql=sql,
            params=(
                [str(row["observation_id"]) for row in events],
                [_date(row["observation_date"]) for row in events],
            ), scope=scope,
            max_rows=len(events) * MAX_CALENDAR_ROWS_PER_EVENT,
        )
        paths: dict[str, list[date]] = {str(row["observation_id"]): [] for row in events}
        for row in rows:
            paths[str(row["observation_id"])].append(_date(row["trade_date"]))
        if any(not values for values in paths.values()):
            raise RuntimeError("frozen outcome event is absent from the SSE trading calendar")
        return {key: tuple(values) for key, values in paths.items()}

    def initialization_batch(
        self,
        observation_date: date,
        through_date: date,
        policy_hash: str,
        context: RunContext,
        *,
        snapshot_ids: Sequence[str] = (),
        benchmark_code: str = BENCHMARK_CODE,
        limit: int = MAX_EVENTS_PER_BATCH,
    ) -> tuple[list[dict[str, Any]], dict[str, tuple[date, ...]]]:
        events = self._candidate_rows(
            observation_date, policy_hash, context,
            snapshot_ids=snapshot_ids, limit=limit,
        )
        if not events:
            return [], {}
        paths = self._calendar_paths(events, context)

        # Only the latest quality-gated observation date can capture a new
        # anchor. Older frozen records have no immutable anchor and therefore
        # become INVALID_INPUT rather than borrowing today's revised feature.
        if observation_date != through_date:
            return events, paths
        codes = sorted({str(event["ts_code"]) for event in events})
        price_rows = self.kernel.read._repository_query(
            context=context,
            source="feature.price_daily_v2",
            sql="""
                SELECT ts_code, trade_date, adj_close_base, total_return_price,
                       quality_status, source_asof_date,
                       price_coordinate_contract_version, calculated_at
                  FROM feature.price_daily_v2
                 WHERE trade_date=%s AND ts_code=ANY(%s::text[])
                 ORDER BY ts_code
                 LIMIT %s
            """,
            params=(observation_date, codes, len(codes) + 1),
            scope=WorkScope(
                date_from=observation_date, date_to=observation_date,
                codes=tuple(codes), partition="market-anomaly-outcome:anchor-price",
            ),
            max_rows=len(codes) + 1,
        )
        if len(price_rows) > len(codes):
            raise RuntimeError("duplicate price-coordinate rows for one code/date")
        prices_by_code = {str(row["ts_code"]): row for row in price_rows}
        benchmark_rows = self.kernel.read._repository_query(
            context=context,
            source="std.index_daily_tushare",
            sql="""
                SELECT ts_code, trade_date, close
                  FROM std.index_daily_tushare
                 WHERE ts_code=%s AND trade_date=%s AND source='tushare'
                 ORDER BY ts_code, trade_date
                 LIMIT 2
            """,
            params=(benchmark_code, observation_date),
            scope=WorkScope(
                date_from=observation_date, date_to=observation_date,
                codes=(benchmark_code,), partition="market-anomaly-outcome:anchor-benchmark",
            ),
            max_rows=2,
        )
        benchmark = benchmark_rows[0] if benchmark_rows else None
        if len(benchmark_rows) > 1:
            raise RuntimeError("duplicate benchmark rows for one code/date")
        for event in events:
            source = prices_by_code.get(str(event["ts_code"]))
            event["anchor_adj_close_base"] = source.get("adj_close_base") if source else None
            event["anchor_total_return_price"] = source.get("total_return_price") if source else None
            event["anchor_quality_status"] = source.get("quality_status") if source else None
            event["anchor_source_asof_date"] = source.get("source_asof_date") if source else None
            event["anchor_price_coordinate_contract_version"] = (
                source.get("price_coordinate_contract_version") if source else None
            )
            event["anchor_source_hash"] = self._anchor_hash(source) if source else ""
            event["anchor_benchmark_close"] = benchmark.get("close") if benchmark else None
        return events, paths

    @staticmethod
    def _anchor_hash(row: Mapping[str, Any]) -> str:
        from data_collect.observation.hashing import hash_object
        return hash_object({
            "source_table": "feature.price_daily_v2",
            "ts_code": row.get("ts_code"),
            "trade_date": row.get("trade_date"),
            "adj_close_base": row.get("adj_close_base"),
            "total_return_price": row.get("total_return_price"),
            "quality_status": row.get("quality_status"),
            "source_asof_date": row.get("source_asof_date"),
            "price_coordinate_contract_version": row.get("price_coordinate_contract_version"),
            "calculated_at": row.get("calculated_at"),
        })

    def due_batch(
        self, through_date: date, context: RunContext,
        *, policy_hash: str, benchmark_code: str = BENCHMARK_CODE,
        snapshot_ids: Sequence[str] = (),
        limit: int = MAX_EVENTS_PER_BATCH,
    ) -> tuple[list[dict[str, Any]], dict[str, tuple[date, ...]], dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
        if not 1 <= limit <= MAX_EVENTS_PER_BATCH:
            raise ValueError("Outcome event batch limit must be in 1..100")
        ids = tuple(dict.fromkeys(str(value) for value in snapshot_ids))
        if len(ids) > MAX_INITIAL_SNAPSHOTS:
            raise ValueError(f"at most {MAX_INITIAL_SNAPSHOTS} snapshots may be replayed per run")
        due_scope = WorkScope(
            date_to=through_date, partition="market-anomaly-outcome:due-queue",
            dimensions={
                "domain": "market_anomaly_outcome",
                **({"snapshot_ids": ids} if ids else {}),
            },
        )
        if ids:
            due_sql = """
                SELECT o.*, d.snapshot_id, d.observation_date, d.ts_code,
                       d.methodology_hash,
                       s.data_baseline_id,
                       d.data_baseline_id AS observation_baseline_id,
                       s.data_baseline_id AS snapshot_baseline_id,
                       s.methodology_hash AS snapshot_methodology_hash,
                       s.input_manifest_hash AS snapshot_input_manifest_hash,
                       d.input_manifest_hash AS observation_input_manifest_hash,
                       d.observation_hash
                  FROM observation.stock_anomaly_daily d
                  JOIN evaluation.market_anomaly_outcome o
                    ON o.observation_id=d.observation_id
                  JOIN observation.snapshot s
                    ON s.snapshot_id=d.snapshot_id AND s.status='FROZEN'
                 WHERE d.snapshot_id=ANY(%s::uuid[])
                   AND o.maturity_status='PENDING_MATURITY'
                   AND o.next_evaluation_date IS NOT NULL
                   AND o.next_evaluation_date <= %s
                   AND o.evaluation_methodology_hash=%s
                 ORDER BY o.next_evaluation_date,o.outcome_id
                 LIMIT %s
            """
            due_params = (list(ids), through_date, policy_hash, limit + 1)
        else:
            due_sql = """
                WITH earliest_due AS (
                    SELECT next_evaluation_date
                      FROM evaluation.market_anomaly_outcome
                     WHERE maturity_status='PENDING_MATURITY'
                       AND next_evaluation_date IS NOT NULL
                       AND next_evaluation_date <= %s
                       AND evaluation_methodology_hash=%s
                     ORDER BY next_evaluation_date, outcome_id
                     LIMIT 1
                )
                SELECT o.*, d.snapshot_id, d.observation_date, d.ts_code,
                       d.methodology_hash,
                       s.data_baseline_id,
                       d.data_baseline_id AS observation_baseline_id,
                       s.data_baseline_id AS snapshot_baseline_id,
                       s.methodology_hash AS snapshot_methodology_hash,
                       s.input_manifest_hash AS snapshot_input_manifest_hash,
                       d.input_manifest_hash AS observation_input_manifest_hash,
                       d.observation_hash
                  FROM earliest_due x
                  JOIN evaluation.market_anomaly_outcome o
                    ON o.next_evaluation_date=x.next_evaluation_date
                  JOIN observation.stock_anomaly_daily d
                    ON d.observation_id=o.observation_id
                  JOIN observation.snapshot s
                    ON s.snapshot_id=d.snapshot_id AND s.status='FROZEN'
                 WHERE o.maturity_status='PENDING_MATURITY'
                   AND o.evaluation_methodology_hash=%s
                 ORDER BY o.outcome_id
                 LIMIT %s
            """
            due_params = (through_date, policy_hash, policy_hash, limit + 1)
        rows = self.kernel.read._repository_query(
            context=context,
            source="evaluation.market_anomaly_outcome",
            sql=due_sql, params=due_params, scope=due_scope,
            max_rows=limit + 1,
        )
        events = [dict(row) for row in rows[:limit]]
        if not events:
            return [], {}, {}, {}
        paths = self._calendar_paths(events, context)
        codes = sorted({str(event["ts_code"]) for event in events})
        calendar_dates = sorted({item for values in paths.values() for item in values})
        date_from = min(_date(event["observation_date"]) for event in events)
        date_to = max(calendar_dates)
        path_scope = _event_scope(events, domain="price-path")
        path_rows = self.kernel.read._repository_query(
            context=context,
            source="feature.price_daily_v2",
            sql="""
                WITH event_keys AS (
                    SELECT * FROM unnest(%s::uuid[], %s::text[], %s::date[])
                         AS e(observation_id, ts_code, observation_date)
                ), calendar_rows AS (
                    SELECT e.observation_id, e.ts_code, c.trade_date
                      FROM event_keys e
                      CROSS JOIN LATERAL (
                          SELECT trade_date
                            FROM std.trading_calendar
                           WHERE exchange='SSE' AND is_open IS TRUE
                             AND trade_date >= e.observation_date
                             AND trade_date <= e.observation_date + 240
                           ORDER BY trade_date
                           LIMIT 121
                      ) c
                ), feature_rows AS (
                    SELECT k.observation_id, p.ts_code, p.trade_date,
                           p.adj_close_base, p.total_return_price,
                           p.quality_status, p.source_asof_date,
                           p.price_coordinate_contract_version, p.calculated_at
                      FROM feature.price_daily_v2 p
                      JOIN calendar_rows k
                        ON k.ts_code=p.ts_code AND k.trade_date=p.trade_date
                     WHERE p.trade_date <= %s
                ), benchmark_rows AS (
                    SELECT k.observation_id, i.trade_date, i.close
                      FROM std.index_daily_tushare i
                      JOIN calendar_rows k ON k.trade_date=i.trade_date
                     WHERE i.ts_code=%s AND i.source='tushare'
                       AND i.trade_date <= %s
                )
                SELECT k.observation_id, k.ts_code, k.trade_date,
                       f.adj_close_base, f.total_return_price,
                       f.quality_status, f.source_asof_date,
                       f.price_coordinate_contract_version, f.calculated_at,
                       b.close AS benchmark_close
                  FROM calendar_rows k
                  LEFT JOIN feature_rows f
                    ON f.observation_id=k.observation_id
                   AND f.trade_date=k.trade_date
                  LEFT JOIN benchmark_rows b
                    ON b.observation_id=k.observation_id
                   AND b.trade_date=k.trade_date
                 ORDER BY k.observation_id, k.trade_date
            """,
            params=(
                [str(event["observation_id"]) for event in events],
                [str(event["ts_code"]) for event in events],
                [_date(event["observation_date"]) for event in events],
                through_date, benchmark_code, through_date,
            ),
            scope=path_scope,
            max_rows=len(events) * MAX_CALENDAR_ROWS_PER_EVENT,
        )
        if len(path_rows) > len(events) * MAX_CALENDAR_ROWS_PER_EVENT:
            raise RuntimeError("Outcome market path exceeded the 121-session-per-event ceiling")
        stock_paths: dict[str, list[dict[str, Any]]] = {str(e["observation_id"]): [] for e in events}
        benchmark_paths: dict[str, list[dict[str, Any]]] = {str(e["observation_id"]): [] for e in events}
        for row in path_rows:
            event_id = str(row["observation_id"])
            stock_paths[event_id].append({
                "ts_code": row["ts_code"], "trade_date": row["trade_date"],
                "adj_close_base": row["adj_close_base"],
                "total_return_price": row["total_return_price"],
                "quality_status": row["quality_status"],
                "source_asof_date": row["source_asof_date"],
                "price_coordinate_contract_version": row["price_coordinate_contract_version"],
                "calculated_at": row["calculated_at"],
            })
            benchmark_paths[event_id].append({
                "ts_code": benchmark_code, "trade_date": row["trade_date"],
                "close": row["benchmark_close"], "quality_status": "VALID",
            })
        return events, paths, stock_paths, benchmark_paths

    def write_rows(
        self, rows: Sequence[Mapping[str, Any]], context: RunContext,
    ) -> int:
        if not rows:
            return 0
        grouped: dict[str, list[dict[str, Any]]] = {}
        for source in rows:
            snapshot_id = str(source.get("observation_snapshot_id") or "").strip()
            if not snapshot_id:
                raise ValueError("Outcome writes require observation_snapshot_id for scoped policy")
            projected = {column: source.get(column) for column in _WRITE_COLUMNS}
            for column in ("horizon_status", "input_provenance", "missing_reason_codes"):
                projected[column] = _json(projected[column])
            projected["calculation_run_id"] = context.run_id
            projected["updated_at"] = datetime.now(timezone.utc)
            grouped.setdefault(snapshot_id, []).append(projected)

        written = 0
        for snapshot_id, batch in grouped.items():
            dates = [
                _date(source["observation_date"])
                for source in rows if str(source.get("observation_snapshot_id")) == snapshot_id
            ]
            stock_codes = tuple(sorted({
                str(source["ts_code"]) for source in rows
                if str(source.get("observation_snapshot_id")) == snapshot_id
            }))
            scope = WorkScope(
                date_from=min(dates), date_to=max(dates),
                codes=stock_codes,
                partition="market-anomaly-outcome:write",
                dimensions={"snapshot_id": snapshot_id, "domain": "market_anomaly_outcome"},
            )
            written += self.kernel.write.upsert(
                context=context, scope=scope,
                target="evaluation.market_anomaly_outcome", rows=batch,
                key=("observation_id", "evaluation_methodology_hash", "outcome_revision"),
                update_columns=tuple(
                    column for column in _WRITE_COLUMNS
                    if column not in {
                        "outcome_id", "observation_id", "evaluation_methodology_hash",
                        "outcome_revision",
                    }
                ),
                skip_noop=True,
            )
        return written

    def explain_initialization_queries(
        self, observation_date: date, sample_codes: Sequence[str],
        sample_snapshot_ids: Sequence[str], policy_hash: str, context: RunContext,
    ) -> dict[str, dict[str, Any]]:
        """Inspect bounded source plans without executing their SELECT bodies."""
        codes = tuple(sample_codes[:MAX_EVENTS_PER_BATCH])
        if not codes:
            raise ValueError("EXPLAIN requires at least one representative security")
        snapshots = tuple(sample_snapshot_ids[:MAX_INITIAL_SNAPSHOTS])
        scope = WorkScope(
            date_from=observation_date, date_to=observation_date,
            codes=codes, partition="market-anomaly-outcome:explain",
            dimensions={"snapshot_ids": snapshots},
        )
        candidate_sql = """
            SELECT s.snapshot_id, d.observation_id, d.ts_code
              FROM observation.snapshot s
              JOIN observation.stock_anomaly_daily d
                ON d.snapshot_id=s.snapshot_id
               AND d.observation_date=s.observation_date_start
             WHERE s.status='FROZEN' AND s.observation_date_start=%s
               AND s.snapshot_id=ANY(%s::uuid[])
               AND d.anomaly_flag IS TRUE
               AND NOT EXISTS (
                   SELECT 1 FROM evaluation.market_anomaly_outcome o
                    WHERE o.observation_id=d.observation_id
                      AND o.evaluation_methodology_hash=%s
               )
             ORDER BY s.snapshot_id,d.observation_id LIMIT 101
        """
        daily_candidate_sql = """
            SELECT s.snapshot_id, d.observation_id, d.ts_code
              FROM observation.snapshot s
              JOIN observation.stock_anomaly_daily d
                ON d.snapshot_id=s.snapshot_id
               AND d.observation_date=s.observation_date_start
             WHERE s.status='FROZEN' AND s.observation_date_start=%s
               AND d.anomaly_flag IS TRUE
               AND NOT EXISTS (
                   SELECT 1 FROM evaluation.market_anomaly_outcome o
                    WHERE o.observation_id=d.observation_id
                      AND o.evaluation_methodology_hash=%s
               )
             ORDER BY s.snapshot_id,d.observation_id LIMIT 101
        """
        snapshot_sql = """
            SELECT snapshot_id,observation_date_start,observation_date_end,status
              FROM observation.snapshot
             WHERE snapshot_id=ANY(%s::uuid[])
             ORDER BY observation_date_start,snapshot_id LIMIT %s
        """
        next_date_sql = """
            SELECT observation_date_start
              FROM observation.snapshot
             WHERE status='FROZEN'
               AND observation_date_start > %s
               AND observation_date_start <= %s
             ORDER BY observation_date_start,snapshot_id LIMIT 1
        """
        calendar_sql = """
            WITH event_keys AS (
                SELECT * FROM unnest(%s::uuid[], %s::date[])
                     AS e(observation_id, observation_date)
            )
            SELECT e.observation_id,c.trade_date
              FROM event_keys e
              CROSS JOIN LATERAL (
                  SELECT trade_date FROM std.trading_calendar
                   WHERE exchange='SSE' AND is_open IS TRUE
                     AND trade_date >= e.observation_date
                     AND trade_date <= e.observation_date + 240
                   ORDER BY trade_date LIMIT 121
              ) c
             ORDER BY e.observation_id,c.trade_date
        """
        price_sql = """
            SELECT ts_code,trade_date,adj_close_base,total_return_price,
                   quality_status,source_asof_date,price_coordinate_contract_version
              FROM feature.price_daily_v2
             WHERE trade_date=%s AND ts_code=ANY(%s::text[])
             ORDER BY ts_code LIMIT %s
        """
        benchmark_sql = """
            SELECT ts_code,trade_date,close
              FROM std.index_daily_tushare
             WHERE ts_code=%s AND trade_date=%s AND source='tushare'
             ORDER BY ts_code,trade_date LIMIT 2
        """
        benchmark_scope = WorkScope(
            date_from=observation_date, date_to=observation_date,
            codes=(BENCHMARK_CODE,), partition="market-anomaly-outcome:explain-benchmark-anchor",
        )
        snapshot_scope = WorkScope(
            partition="market-anomaly-outcome:explain-snapshots",
            dimensions={"snapshot_ids": snapshots},
        )
        next_date_scope = WorkScope(
            date_from=observation_date, date_to=observation_date,
            partition="market-anomaly-outcome:explain-next-date",
        )
        return {
            "frozen_anomalies": self.kernel.read._repository_explain(
                context, "observation.stock_anomaly_daily", candidate_sql,
                (observation_date, list(snapshots), policy_hash), scope,
            ),
            "daily_frozen_anomalies": self.kernel.read._repository_explain(
                context, "observation.stock_anomaly_daily", daily_candidate_sql,
                (observation_date, policy_hash), scope,
            ),
            "trading_calendar_paths": self.kernel.read._repository_explain(
                context, "std.trading_calendar", calendar_sql,
                (["11111111-1111-4111-8111-111111111111"], [observation_date]), scope,
            ),
            "price_anchors": self.kernel.read._repository_explain(
                context, "feature.price_daily_v2", price_sql,
                (observation_date, list(codes), len(codes) + 1), scope,
            ),
            "benchmark_anchor": self.kernel.read._repository_explain(
                context, "std.index_daily_tushare", benchmark_sql,
                (BENCHMARK_CODE, observation_date), benchmark_scope,
            ),
            "explicit_snapshot_ids": self.kernel.read._repository_explain(
                context, "observation.snapshot", snapshot_sql,
                (list(snapshots), len(snapshots) + 1), snapshot_scope,
            ),
            "next_frozen_date": self.kernel.read._repository_explain(
                context, "observation.snapshot", next_date_sql,
                (observation_date, observation_date), next_date_scope,
            ),
        }

    def explain_due_queries(
        self,
        through_date: date,
        sample_codes: Sequence[str],
        sample_observation_ids: Sequence[str],
        sample_observation_dates: Sequence[date],
        policy_hash: str,
        context: RunContext,
        *,
        benchmark_code: str = BENCHMARK_CODE,
        sample_snapshot_ids: Sequence[str] = (),
    ) -> dict[str, dict[str, Any]]:
        """EXPLAIN every due-queue/path SELECT after migration 113 is present.

        This method plans only.  ReadGateway rejects ANALYZE and does not run
        the underlying data SELECTs.
        """
        event_ids = tuple(sample_observation_ids[:MAX_EVENTS_PER_BATCH])
        codes = tuple(sample_codes[:MAX_EVENTS_PER_BATCH])
        dates = tuple(sample_observation_dates[:MAX_EVENTS_PER_BATCH])
        if not event_ids or len(event_ids) != len(codes) or len(codes) != len(dates):
            raise ValueError("EXPLAIN requires aligned non-empty event/code/date samples")
        scope = WorkScope(
            date_from=min(dates), date_to=max(dates) + timedelta(days=240),
            codes=tuple(sorted(set(codes))), partition="market-anomaly-outcome:explain-due",
            dimensions={"domain": "market_anomaly_outcome"},
        )
        due_scope = WorkScope(
            date_to=through_date, partition="market-anomaly-outcome:explain-due-queue",
            dimensions={"domain": "market_anomaly_outcome"},
        )
        due_sql = """
            WITH earliest_due AS (
                SELECT next_evaluation_date
                  FROM evaluation.market_anomaly_outcome
                 WHERE maturity_status='PENDING_MATURITY'
                   AND next_evaluation_date IS NOT NULL
                   AND next_evaluation_date <= %s
                   AND evaluation_methodology_hash=%s
                 ORDER BY next_evaluation_date,outcome_id LIMIT 1
            )
            SELECT o.outcome_id,d.snapshot_id,d.observation_date,d.ts_code
              FROM earliest_due x
              JOIN evaluation.market_anomaly_outcome o
                ON o.next_evaluation_date=x.next_evaluation_date
              JOIN observation.stock_anomaly_daily d
                ON d.observation_id=o.observation_id
              JOIN observation.snapshot s
                ON s.snapshot_id=d.snapshot_id AND s.status='FROZEN'
             WHERE o.maturity_status='PENDING_MATURITY'
               AND o.evaluation_methodology_hash=%s
             ORDER BY o.outcome_id LIMIT 101
        """
        due_probe_sql = """
            SELECT 1
              FROM evaluation.market_anomaly_outcome
             WHERE maturity_status='PENDING_MATURITY'
               AND next_evaluation_date IS NOT NULL
               AND next_evaluation_date <= %s
               AND evaluation_methodology_hash=%s
             ORDER BY next_evaluation_date,outcome_id LIMIT 1
        """
        calendar_sql = """
            WITH event_keys AS (
                SELECT * FROM unnest(%s::uuid[],%s::date[])
                     AS e(observation_id,observation_date)
            )
            SELECT e.observation_id,c.trade_date
              FROM event_keys e
              CROSS JOIN LATERAL (
                  SELECT trade_date FROM std.trading_calendar
                   WHERE exchange='SSE' AND is_open IS TRUE
                     AND trade_date >= e.observation_date
                     AND trade_date <= e.observation_date + 240
                   ORDER BY trade_date LIMIT 121
              ) c
             ORDER BY e.observation_id,c.trade_date
        """
        price_path_sql = """
            WITH event_keys AS (
                SELECT * FROM unnest(%s::uuid[],%s::text[],%s::date[])
                     AS e(observation_id,ts_code,observation_date)
            ), calendar_rows AS (
                SELECT e.observation_id,e.ts_code,c.trade_date
                  FROM event_keys e
                  CROSS JOIN LATERAL (
                      SELECT trade_date FROM std.trading_calendar
                       WHERE exchange='SSE' AND is_open IS TRUE
                         AND trade_date >= e.observation_date
                         AND trade_date <= e.observation_date + 240
                       ORDER BY trade_date LIMIT 121
                  ) c
            ), feature_rows AS (
                SELECT k.observation_id,p.ts_code,p.trade_date,p.adj_close_base,
                       p.total_return_price,p.quality_status,p.source_asof_date,
                       p.price_coordinate_contract_version,p.calculated_at
                  FROM feature.price_daily_v2 p
                  JOIN calendar_rows k
                    ON k.ts_code=p.ts_code AND k.trade_date=p.trade_date
                 WHERE p.trade_date <= %s
            ), benchmark_rows AS (
                SELECT k.observation_id,i.trade_date,i.close
                  FROM std.index_daily_tushare i
                  JOIN calendar_rows k ON k.trade_date=i.trade_date
                 WHERE i.ts_code=%s AND i.source='tushare' AND i.trade_date <= %s
            )
            SELECT k.observation_id,k.ts_code,k.trade_date,
                   f.adj_close_base,f.total_return_price,f.quality_status,
                   f.source_asof_date,f.price_coordinate_contract_version,
                   f.calculated_at,b.close AS benchmark_close
              FROM calendar_rows k
              LEFT JOIN feature_rows f
                ON f.observation_id=k.observation_id AND f.trade_date=k.trade_date
              LEFT JOIN benchmark_rows b
                ON b.observation_id=k.observation_id AND b.trade_date=k.trade_date
             ORDER BY k.observation_id,k.trade_date
        """
        event_params = (list(event_ids), list(dates))
        path_params = (
            list(event_ids), list(codes), list(dates), through_date,
            benchmark_code, through_date,
        )
        plans = {
            "due_queue": self.kernel.read._repository_explain(
                context, "evaluation.market_anomaly_outcome", due_sql,
                (through_date, policy_hash, policy_hash), due_scope,
            ),
            "due_probe": self.kernel.read._repository_explain(
                context, "evaluation.market_anomaly_outcome", due_probe_sql,
                (through_date, policy_hash), due_scope,
            ),
            "trading_calendar_paths": self.kernel.read._repository_explain(
                context, "std.trading_calendar", calendar_sql,
                event_params, scope,
            ),
            "price_paths_and_benchmark": self.kernel.read._repository_explain(
                context, "feature.price_daily_v2", price_path_sql,
                path_params, scope,
            ),
        }
        snapshot_ids = tuple(sample_snapshot_ids[:MAX_INITIAL_SNAPSHOTS])
        if snapshot_ids:
            explicit_due_sql = """
                SELECT o.outcome_id,d.snapshot_id,d.observation_date,d.ts_code
                  FROM observation.stock_anomaly_daily d
                  JOIN evaluation.market_anomaly_outcome o
                    ON o.observation_id=d.observation_id
                  JOIN observation.snapshot s
                    ON s.snapshot_id=d.snapshot_id AND s.status='FROZEN'
                 WHERE d.snapshot_id=ANY(%s::uuid[])
                   AND o.maturity_status='PENDING_MATURITY'
                   AND o.next_evaluation_date IS NOT NULL
                   AND o.next_evaluation_date <= %s
                   AND o.evaluation_methodology_hash=%s
                 ORDER BY o.next_evaluation_date,o.outcome_id LIMIT 101
            """
            snapshot_scope = WorkScope(
                date_to=through_date,
                partition="market-anomaly-outcome:explain-explicit-due",
                dimensions={"snapshot_ids": snapshot_ids},
            )
            plans["explicit_due_queue"] = self.kernel.read._repository_explain(
                context, "evaluation.market_anomaly_outcome", explicit_due_sql,
                (list(snapshot_ids), through_date, policy_hash), snapshot_scope,
            )
            explicit_due_probe_sql = """
                SELECT 1
                  FROM observation.stock_anomaly_daily d
                  JOIN evaluation.market_anomaly_outcome o
                    ON o.observation_id=d.observation_id
                 WHERE d.snapshot_id=ANY(%s::uuid[])
                   AND o.maturity_status='PENDING_MATURITY'
                   AND o.next_evaluation_date IS NOT NULL
                   AND o.next_evaluation_date <= %s
                   AND o.evaluation_methodology_hash=%s
                 ORDER BY o.next_evaluation_date,o.outcome_id LIMIT 1
            """
            plans["explicit_due_probe"] = self.kernel.read._repository_explain(
                context, "evaluation.market_anomaly_outcome", explicit_due_probe_sql,
                (list(snapshot_ids), through_date, policy_hash), snapshot_scope,
            )
        return plans


__all__ = [
    "BENCHMARK_CODE", "MAX_EVENTS_PER_BATCH", "MAX_INITIAL_SNAPSHOTS",
    "MarketAnomalyOutcomeRepository",
]
