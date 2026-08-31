"""Repository contracts for the Market Anomaly Observation domain.

The repository deliberately keeps the expensive, set-based input query here.
The job layer receives bounded rows and only performs pure Python calculations.
No caller receives a database connection, cursor, or a generic SQL executor.
"""
from __future__ import annotations

from datetime import date, datetime
import json
from typing import Any, Iterable, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository
from data_collect.observation.contracts import (
    CONTRACT_VERSION,
    METHODOLOGY_ID,
    ObservationIdentity,
    ObservationPolicy,
)
from data_collect.observation.hashing import hash_object


def _stable_uuid(value: str) -> str:
    """Return a retry-stable UUID for a domain identity."""
    return str(uuid5(NAMESPACE_URL, value))


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else [], ensure_ascii=False, sort_keys=True, default=str)


def _date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _date_scope(start: date, end: date, *, codes: Sequence[str] | None = None, snapshot_id: str | None = None) -> WorkScope:
    dimensions = {"domain": "market_anomaly_observation"}
    if snapshot_id:
        dimensions["snapshot_id"] = str(snapshot_id)
    return WorkScope(
        date_from=start,
        date_to=end,
        codes=tuple(codes) if codes else None,
        dimensions=dimensions,
    )


class StockAnomalyObservationRepository(BaseRepository):
    """Input reads and stock-level Observation writes."""

    STOCK_COLUMNS = (
        "observation_id", "snapshot_id", "observation_date", "ts_code",
        "methodology_hash", "data_baseline_id", "amount_ratio_5_60",
        "amount_ratio_20_60", "turnover_ratio_5_60", "turnover_ratio_20_60",
        "amount_zscore_60", "amount_single_ratio_60", "abnormal_days_20",
        "return_1d", "return_5d", "return_20d", "price_position_52w",
        "distance_ma20", "distance_ma60", "max_drawdown_20d",
        "up_volume_ratio_20", "down_volume_ratio_20", "up_down_volume_ratio",
        "price_response_efficiency_20", "anomaly_flag", "anomaly_streak",
        "anomaly_frequency_20", "days_since_first_anomaly", "relative_return_20d",
        "relative_volume_zscore", "trading_anomaly_score", "price_structure_score",
        "persistence_score", "sector_confirmation_score", "risk_score",
        "observation_priority", "primary_state", "state_reason_codes",
        "sector_data_status", "sector_confirmation_status", "calculation_status",
        "quality_status", "missing_reason_codes", "input_row_count",
        "input_manifest_hash", "observation_hash", "calculation_run_id",
    )

    TAG_COLUMNS = ("tag_id", "observation_id", "tag_code", "tag_payload")
    CONFIRMATION_COLUMNS = (
        "confirmation_id", "observation_id", "industry_system", "industry_level",
        "industry_code", "industry_name", "sector_data_status",
        "sector_confirmation_status", "sector_confirmation_score",
        "sector_reason_codes", "input_manifest_hash",
    )

    def observation_dates(self, start: date, end: date, context: RunContext) -> tuple[date, ...]:
        if start > end:
            raise ValueError("observation start must not be after end")
        rows = self.kernel.read._repository_query(
            context=context,
            source="std.trading_calendar",
            sql="""
                SELECT trade_date
                  FROM std.trading_calendar
                 WHERE exchange=%s AND is_open IS TRUE
                   AND trade_date BETWEEN %s AND %s
                 ORDER BY trade_date
            """,
            params=("SSE", start, end),
            scope=_date_scope(start, end),
            max_rows=5_000,
        )
        return tuple(_date(row["trade_date"]) for row in rows)

    def validated_baseline(self, baseline_id: str, context: RunContext) -> dict[str, Any]:
        """Read the single validated data release used by an Observation snapshot."""
        baseline_id = str(baseline_id).strip()
        if not baseline_id:
            raise ValueError("observation baseline id must be non-empty")
        rows = self.kernel.read._repository_query(
            context=context,
            source="ops.tushare_data_release",
            sql="""
                SELECT release_tag, baseline_cutoff_date, status,
                       data_cutoff_timestamp, quality_report_hash
                  FROM ops.tushare_data_release
                 WHERE release_tag=%s AND lower(status)=lower('validated')
                 LIMIT 1
            """,
            params=(baseline_id,),
            scope=WorkScope(partition=f"observation:baseline:{baseline_id}"),
            max_rows=1,
        )
        if not rows:
            raise ValueError(f"validated data baseline not found: {baseline_id}")
        return rows[0]

    def eligible_codes(self, observation_date: date, context: RunContext, *, limit: int = 10_000) -> tuple[str, ...]:
        if limit <= 0 or limit > 10_000:
            raise ValueError("eligible code limit must be in 1..10000")
        rows = self.kernel.read._repository_query(
            context=context,
            source="pit.universe_daily",
            sql="""
                SELECT DISTINCT ts_code
                  FROM pit.universe_daily
                 WHERE trade_date=%s AND eligible_by_market IS TRUE
                 ORDER BY ts_code
                 LIMIT %s
            """,
            params=(observation_date, limit),
            scope=_date_scope(observation_date, observation_date),
            max_rows=limit,
        )
        return tuple(str(row["ts_code"]) for row in rows)

    def input_rows(
        self,
        start: date,
        end: date,
        codes: Sequence[str],
        context: RunContext,
        *,
        max_rows: int = 100_000,
        snapshot_id: str | None = None,
        industry_system: str = "SW2021",
        industry_level: int = 1,
    ) -> list[dict[str, Any]]:
        if start > end:
            raise ValueError("input start must not be after end")
        normalized_codes = tuple(dict.fromkeys(str(code) for code in codes if str(code).strip()))
        if not normalized_codes:
            return []
        if max_rows <= 0 or max_rows > 100_000:
            raise ValueError("input max_rows must be in 1..100000")
        if not str(industry_system).strip() or int(industry_level) <= 0:
            raise ValueError("industry system and level must be valid")
        # The explicit date and code predicates are part of the contract.  The
        # WorkScope is metadata enforcement; it is not a substitute for SQL
        # predicates.
        sql = """
            SELECT u.trade_date AS trade_date,
                   u.ts_code AS ts_code,
                   u.is_listed_asof, u.is_st_asof, u.is_suspended_asof,
                   u.eligible_by_market, u.quality_status AS universe_quality_status,
                   t.is_tradeable, t.can_buy, t.can_sell, t.is_suspended,
                   t.limit_status, t.quality_status AS tradeability_quality_status,
                   d.close AS raw_close, d.volume, d.amount,
                   v.turnover_rate, v.turnover_rate_f, v.free_share,
                   v.quality_status AS valuation_quality_status,
                   p.adj_close_base, p.total_return_price,
                   p.quality_status AS price_quality_status,
                   im.industry_system, im.industry_level, im.industry_code,
                   im.industry_name, im.source_asof_date AS industry_source_asof_date,
                   im.data_available_timestamp AS industry_data_available_timestamp,
                   im.quality_status AS industry_quality_status
              FROM pit.universe_daily u
              LEFT JOIN pit.security_tradeability_daily t
                ON t.trade_date=u.trade_date AND t.ts_code=u.ts_code
              LEFT JOIN std.daily_kline d
                ON d.trade_date=u.trade_date AND d.stock_code=u.ts_code
              LEFT JOIN std.valuation_daily v
                ON v.trade_date=u.trade_date AND v.stock_code=u.ts_code
               AND v.source='tushare'
              LEFT JOIN feature.price_daily_v2 p
                ON p.trade_date=u.trade_date AND p.ts_code=u.ts_code
              LEFT JOIN pit.industry_membership_daily im
                ON im.trade_date=u.trade_date AND im.ts_code=u.ts_code
               AND im.industry_system=%s AND im.industry_level=%s
             WHERE u.trade_date BETWEEN %s AND %s
               AND u.ts_code=ANY(%s::text[])
             ORDER BY u.ts_code, u.trade_date
        """
        return self.kernel.read._repository_query(
            context=context,
            source="pit.universe_daily",
            sql=sql,
            params=(str(industry_system), int(industry_level), start, end, list(normalized_codes)),
            scope=_date_scope(start, end, codes=normalized_codes, snapshot_id=snapshot_id),
            max_rows=max_rows,
        )

    def snapshot_hash_rows(
        self,
        snapshot_id: str,
        start: date,
        end: date,
        context: RunContext,
        *,
        batch_rows: int = 2_000,
    ) -> Iterable[dict[str, Any]]:
        """Yield stable snapshot hash rows without materializing the full series."""
        if start > end:
            raise ValueError("snapshot hash start must not be after end")
        if batch_rows <= 0 or batch_rows > 10_000:
            raise ValueError("snapshot hash batch_rows must be in 1..10000")
        stream = self.kernel.read._repository_stream(
            context=context,
            source="observation.stock_anomaly_daily",
            sql="""
                SELECT observation_date, ts_code, input_manifest_hash, observation_hash
                  FROM observation.stock_anomaly_daily
                 WHERE snapshot_id=%s AND observation_date BETWEEN %s AND %s
                 ORDER BY observation_date, ts_code
            """,
            params=(snapshot_id, start, end),
            scope=_date_scope(start, end, snapshot_id=snapshot_id),
            batch_rows=batch_rows,
        )
        for batch in stream:
            yield from batch

    def market_rows(
        self,
        start: date,
        end: date,
        benchmark_code: str,
        context: RunContext,
    ) -> list[dict[str, Any]]:
        if start > end:
            raise ValueError("market start must not be after end")
        rows = self.kernel.read._repository_query(
            context=context,
            source="std.index_daily_tushare",
            sql="""
                SELECT ts_code, trade_date, close
                  FROM std.index_daily_tushare
                 WHERE ts_code=%s AND trade_date BETWEEN %s AND %s
                 ORDER BY trade_date
            """,
            params=(benchmark_code, start, end),
            scope=_date_scope(start, end),
            max_rows=10_000,
        )
        return rows

    def previous_states(
        self,
        snapshot_id: str,
        before_date: date,
        codes: Sequence[str],
        context: RunContext,
    ) -> dict[str, dict[str, Any]]:
        normalized_codes = tuple(dict.fromkeys(str(code) for code in codes if str(code).strip()))
        if not normalized_codes:
            return {}
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.stock_anomaly_daily",
            sql="""
                SELECT DISTINCT ON (ts_code)
                       ts_code, observation_date, primary_state, anomaly_flag
                  FROM observation.stock_anomaly_daily
                 WHERE snapshot_id=%s AND observation_date < %s
                   AND ts_code=ANY(%s::text[])
                 ORDER BY ts_code, observation_date DESC
            """,
            params=(snapshot_id, before_date, list(normalized_codes)),
            scope=_date_scope(before_date, before_date, codes=normalized_codes, snapshot_id=snapshot_id),
            max_rows=len(normalized_codes),
        )
        return {str(row["ts_code"]): row for row in rows}

    def get_snapshot(self, snapshot_id: str, context: RunContext) -> dict[str, Any]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.snapshot",
            sql="SELECT * FROM observation.snapshot WHERE snapshot_id=%s",
            params=(snapshot_id,),
            scope=WorkScope(partition=f"observation:snapshot:{snapshot_id}"),
            max_rows=1,
        )
        if not rows:
            raise ValueError(f"unknown observation snapshot: {snapshot_id}")
        return rows[0]

    def ensure_snapshot(
        self,
        start: date,
        end: date,
        baseline_id: str,
        policy: ObservationPolicy,
        contract_hash: str,
        context: RunContext,
    ) -> dict[str, Any]:
        identity = f"observation-snapshot|{start.isoformat()}|{end.isoformat()}|{policy.methodology_hash}|{baseline_id}"
        snapshot_id = _stable_uuid(identity)
        row = {
            "snapshot_id": snapshot_id,
            "observation_date_start": start,
            "observation_date_end": end,
            "methodology_hash": policy.methodology_hash,
            "data_baseline_id": baseline_id,
            "observation_policy_id": policy.policy_id,
            "contract_hash": contract_hash,
            "status": "DRAFT",
            "code_identity": _json({
                "contract_version": CONTRACT_VERSION,
                "methodology_id": METHODOLOGY_ID,
                "pipeline_version": context.pipeline_version,
            }),
        }
        scope = _date_scope(start, end, snapshot_id=snapshot_id)
        self.kernel.write.bulk_insert_ignore(
            context=context,
            scope=scope,
            target="observation.snapshot",
            rows=[row],
        )
        return self.get_snapshot(snapshot_id, context)

    def transition_snapshot(
        self,
        snapshot_id: str,
        context: RunContext,
        start: date,
        end: date,
        from_statuses: Sequence[str],
        to_status: str,
        fields: Mapping[str, Any] | None = None,
    ) -> int:
        return self.kernel.write.transition_observation_snapshot(
            context=context,
            scope=_date_scope(start, end, snapshot_id=snapshot_id),
            snapshot_id=snapshot_id,
            from_statuses=from_statuses,
            to_status=to_status,
            fields=fields,
        )

    def write_stock_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
        context: RunContext,
        scope: WorkScope,
    ) -> int:
        if not rows:
            return 0
        projected: list[dict[str, Any]] = []
        for source in rows:
            row = {column: source.get(column) for column in self.STOCK_COLUMNS}
            row["state_reason_codes"] = _json(source.get("state_reason_codes", ()))
            row["missing_reason_codes"] = _json(source.get("missing_reason_codes", ()))
            if not row["snapshot_id"] or not row["observation_id"]:
                raise ValueError("stock observation row requires snapshot_id and observation_id")
            projected.append(row)
        updates = tuple(column for column in self.STOCK_COLUMNS if column not in {"observation_id"})
        return self.kernel.write.upsert(
            context=context,
            scope=scope,
            target="observation.stock_anomaly_daily",
            rows=projected,
            key=("observation_id",),
            update_columns=updates,
            skip_noop=True,
        )

    def write_tags(
        self,
        rows: Sequence[Mapping[str, Any]],
        context: RunContext,
        scope: WorkScope,
    ) -> int:
        if not rows:
            return 0
        projected = []
        for source in rows:
            projected.append({
                "tag_id": source.get("tag_id") or _stable_uuid(f"tag|{source['observation_id']}|{source['tag_code']}"),
                "observation_id": source["observation_id"],
                "tag_code": source["tag_code"],
                "tag_payload": _json(source.get("tag_payload", {})),
            })
        return self.kernel.write.upsert(
            context=context, scope=scope, target="observation.stock_anomaly_tag",
            rows=projected, key=("observation_id", "tag_code"),
            update_columns=("tag_payload",), skip_noop=True,
        )

    def write_confirmations(
        self,
        rows: Sequence[Mapping[str, Any]],
        context: RunContext,
        scope: WorkScope,
    ) -> int:
        if not rows:
            return 0
        projected = []
        for source in rows:
            projected.append({
                "confirmation_id": source.get("confirmation_id") or _stable_uuid(f"confirmation|{source['observation_id']}"),
                "observation_id": source["observation_id"],
                "industry_system": source.get("industry_system"),
                "industry_level": source.get("industry_level"),
                "industry_code": source.get("industry_code"),
                "industry_name": source.get("industry_name"),
                "sector_data_status": source["sector_data_status"],
                "sector_confirmation_status": source["sector_confirmation_status"],
                "sector_confirmation_score": source.get("sector_confirmation_score"),
                "sector_reason_codes": _json(source.get("sector_reason_codes", ())),
                "input_manifest_hash": source["input_manifest_hash"],
            })
        return self.kernel.write.upsert(
            context=context, scope=scope, target="observation.stock_sector_confirmation",
            rows=projected, key=("observation_id",),
            update_columns=tuple(column for column in self.CONFIRMATION_COLUMNS if column not in {"confirmation_id", "observation_id"}),
            skip_noop=True,
        )

    def validate_date(self, snapshot_id: str, observation_date: date, context: RunContext) -> dict[str, Any]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.stock_anomaly_daily",
            sql="""
                SELECT COUNT(*) AS row_count,
                       COUNT(DISTINCT ts_code) AS stock_count,
                       COUNT(*) FILTER (WHERE calculation_status='INVALID') AS invalid_count,
                       COUNT(*) FILTER (WHERE quality_status='INVALID') AS quality_invalid_count,
                       COUNT(*) FILTER (WHERE sector_confirmation_status='UNKNOWN') AS sector_unknown_count
                  FROM observation.stock_anomaly_daily
                 WHERE snapshot_id=%s AND observation_date=%s
            """,
            params=(snapshot_id, observation_date),
            scope=_date_scope(observation_date, observation_date, snapshot_id=snapshot_id),
            max_rows=1,
        )
        return rows[0] if rows else {"row_count": 0, "stock_count": 0}

    def summarize_snapshot(self, snapshot_id: str, start: date, end: date, context: RunContext) -> dict[str, Any]:
        """One final bounded-by-snapshot summary; never called per tile/date."""
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.stock_anomaly_daily",
            sql="""
                SELECT COUNT(*) AS row_count,
                       COUNT(DISTINCT ts_code) AS stock_count,
                       COUNT(DISTINCT observation_date) AS date_count,
                       COUNT(*) FILTER (WHERE sector_confirmation_status <> 'UNKNOWN') AS sector_count
                  FROM observation.stock_anomaly_daily
                 WHERE snapshot_id=%s AND observation_date BETWEEN %s AND %s
            """,
            params=(snapshot_id, start, end),
            scope=_date_scope(start, end, snapshot_id=snapshot_id),
            max_rows=1,
        )
        return rows[0] if rows else {"row_count": 0, "stock_count": 0, "date_count": 0, "sector_count": 0}


class SectorAnomalyObservationRepository(BaseRepository):
    """Persist sector aggregates without making them a stock prerequisite."""

    COLUMNS = (
        "sector_observation_id", "snapshot_id", "observation_date", "industry_system",
        "industry_level", "industry_code", "industry_name", "eligible_stock_count",
        "anomaly_stock_count", "new_anomaly_count", "persistent_anomaly_count",
        "previous_anomaly_count", "exited_anomaly_count", "sector_anomaly_breadth",
        "sector_new_anomaly_ratio", "sector_persistence", "sector_data_status",
        "quality_status", "input_manifest_hash",
    )

    def write_rows(self, rows: Sequence[Mapping[str, Any]], context: RunContext, scope: WorkScope) -> int:
        if not rows:
            return 0
        projected = [{column: source.get(column) for column in self.COLUMNS} for source in rows]
        return self.kernel.write.upsert(
            context=context, scope=scope, target="observation.sector_anomaly_daily",
            rows=projected,
            key=("snapshot_id", "observation_date", "industry_system", "industry_level", "industry_code"),
            update_columns=tuple(column for column in self.COLUMNS if column != "sector_observation_id"),
            skip_noop=True,
        )


class StockSectorConfirmationRepository(BaseRepository):
    """Compatibility façade for callers that own confirmation orchestration."""

    def __init__(self, kernel) -> None:
        super().__init__(kernel)
        self._delegate = StockAnomalyObservationRepository(kernel)

    def write_rows(self, rows: Sequence[Mapping[str, Any]], context: RunContext, scope: WorkScope) -> int:
        return self._delegate.write_confirmations(rows, context, scope)


class ObservationEventRepository(BaseRepository):
    """Typed AS_OF and retrospective event evidence writes."""

    ASOF_COLUMNS = (
        "event_link_id", "observation_id", "event_id", "event_type", "event_visible_at",
        "effective_date", "source_record_id", "source_hash", "evidence_role", "details",
    )
    RETRO_COLUMNS = (
        "retrospective_link_id", "observation_id", "event_id", "event_type", "event_visible_at",
        "effective_date", "source_record_id", "source_hash", "evidence_role", "details",
    )

    @staticmethod
    def _validate_event(row: Mapping[str, Any], *, retrospective: bool) -> None:
        visible = row.get("event_visible_at")
        observed = row.get("observation_date")
        if visible is None or not row.get("source_record_id") or not row.get("source_hash"):
            raise ValueError("event evidence requires visible time, source_record_id and source_hash")
        if observed is not None:
            observed_date = _date(observed)
            visible_date = _date(visible)
            if retrospective and visible_date <= observed_date:
                raise ValueError("retrospective event must be visible after observation date")
            if not retrospective and visible_date > observed_date:
                raise ValueError("AS_OF event must be visible on or before observation date")

    def write_asof_rows(self, rows: Sequence[Mapping[str, Any]], context: RunContext, scope: WorkScope) -> int:
        if not rows:
            return 0
        projected = []
        for source in rows:
            self._validate_event(source, retrospective=False)
            projected.append({
                "event_link_id": source.get("event_link_id") or _stable_uuid(f"asof-event|{source['observation_id']}|{source['source_record_id']}|{source['source_hash']}"),
                "observation_id": source["observation_id"], "event_id": source.get("event_id"),
                "event_type": source["event_type"], "event_visible_at": source["event_visible_at"],
                "effective_date": source.get("effective_date"), "source_record_id": source["source_record_id"],
                "source_hash": source["source_hash"], "evidence_role": "AS_OF_EVIDENCE",
                "details": _json(source.get("details", {})),
            })
        return self.kernel.write.upsert(
            context=context, scope=scope, target="observation.event_link", rows=projected,
            key=("observation_id", "source_record_id", "source_hash"),
            update_columns=("event_id", "event_type", "event_visible_at", "effective_date", "evidence_role", "details"),
            skip_noop=True,
        )

    def write_retrospective_rows(self, rows: Sequence[Mapping[str, Any]], context: RunContext, scope: WorkScope) -> int:
        if not rows:
            return 0
        projected = []
        for source in rows:
            self._validate_event(source, retrospective=True)
            projected.append({
                "retrospective_link_id": source.get("retrospective_link_id") or _stable_uuid(f"retro-event|{source['observation_id']}|{source['source_record_id']}|{source['source_hash']}"),
                "observation_id": source["observation_id"], "event_id": source.get("event_id"),
                "event_type": source["event_type"], "event_visible_at": source["event_visible_at"],
                "effective_date": source.get("effective_date"), "source_record_id": source["source_record_id"],
                "source_hash": source["source_hash"], "evidence_role": "RETROSPECTIVE_CAUSE",
                "details": _json(source.get("details", {})),
            })
        return self.kernel.write.upsert(
            context=context, scope=scope, target="evaluation.retrospective_event_link", rows=projected,
            key=("observation_id", "source_record_id", "source_hash"),
            update_columns=("event_id", "event_type", "event_visible_at", "effective_date", "evidence_role", "details"),
            skip_noop=True,
        )


class MarketAnomalyOutcomeRepository(BaseRepository):
    """Persist strategy-neutral future outcomes."""

    def write_rows(self, rows: Sequence[Mapping[str, Any]], context: RunContext, scope: WorkScope) -> int:
        if not rows:
            return 0
        forbidden = {"entry_price", "exit_price", "fee_bps", "slippage_bps", "stop_loss", "take_profit", "hit_target_before_stop"}
        if any(forbidden.intersection(row) for row in rows):
            raise ValueError("market outcome cannot contain Scenario fields")
        columns = tuple(rows[0].keys())
        if "outcome_id" not in columns or "observation_id" not in columns or "evaluation_methodology_hash" not in columns:
            raise ValueError("outcome requires stable identity columns")
        return self.kernel.write.upsert(
            context=context, scope=scope, target="evaluation.market_anomaly_outcome", rows=rows,
            key=("observation_id", "evaluation_methodology_hash"),
            update_columns=tuple(column for column in columns if column not in {"outcome_id", "observation_id", "evaluation_methodology_hash"}),
            skip_noop=True,
        )


class ScenarioEvaluationRepository(BaseRepository):
    """Persist strategy/scenario results in their separate contract."""

    def write_rows(self, rows: Sequence[Mapping[str, Any]], context: RunContext, scope: WorkScope) -> int:
        if not rows:
            return 0
        columns = tuple(rows[0].keys())
        required = {"scenario_result_id", "observation_id", "scenario_methodology_hash", "scenario_hash", "result_status"}
        if not required.issubset(columns):
            raise ValueError(f"scenario result missing columns: {sorted(required - set(columns))}")
        return self.kernel.write.upsert(
            context=context, scope=scope, target="evaluation.scenario_result", rows=rows,
            key=("observation_id", "scenario_methodology_hash"),
            update_columns=tuple(column for column in columns if column not in {"scenario_result_id", "observation_id", "scenario_methodology_hash"}),
            skip_noop=True,
        )


__all__ = [
    "StockAnomalyObservationRepository", "SectorAnomalyObservationRepository",
    "StockSectorConfirmationRepository", "ObservationEventRepository",
    "MarketAnomalyOutcomeRepository", "ScenarioEvaluationRepository",
]
