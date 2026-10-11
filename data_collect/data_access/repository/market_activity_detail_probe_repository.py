"""Isolated read-only probe for validating Market Activity detail inputs.

This repository is intentionally not wired into the dashboard/application.
It exists only to verify one bounded live sample before any production change.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


_MAX_RECENT_DATES = 10
_LOOKBACK_CALENDAR_DAYS = 60

_RECENT_TRADING_DATES_SQL = """
    SELECT DISTINCT trade_date
      FROM std.trading_calendar
     WHERE exchange='SSE'
       AND is_open IS TRUE
       AND trade_date BETWEEN %s AND %s
     ORDER BY trade_date DESC
     LIMIT %s
"""

_RECENT_RATIO_SERIES_SQL = """
    WITH ranked AS (
        SELECT f.trade_date, f.volume_ratio_previous, f.amount_ratio_previous,
               f.history_observation_count, f.quality_status,
               f.data_baseline_id, f.input_manifest_hash,
               ROW_NUMBER() OVER (
                   PARTITION BY f.trade_date
                   ORDER BY CASE WHEN lower(r.status)='validated' THEN 0 ELSE 1 END,
                            r.baseline_cutoff_date DESC,
                            s.created_at DESC,
                            f.data_baseline_id
               ) AS baseline_rank
          FROM observation.stock_activity_daily f
          JOIN observation.market_activity_snapshot s
            ON s.activity_snapshot_id=f.activity_snapshot_id
          JOIN ops.tushare_data_release r
            ON r.release_tag=f.data_baseline_id
           AND r.baseline_domain='MARKET_ACTIVITY'
         WHERE f.ts_code=%s
           AND f.trade_date=ANY(%s::date[])
           AND f.methodology_hash=%s
           AND s.status='FROZEN'
           AND lower(r.status) IN ('validated','superseded')
    )
    SELECT trade_date, volume_ratio_previous, amount_ratio_previous,
           history_observation_count, quality_status, data_baseline_id,
           input_manifest_hash
      FROM ranked
     WHERE baseline_rank=1
     ORDER BY trade_date
     LIMIT %s
"""


class MarketActivityDetailProbeRepository(BaseRepository):
    """Read at most one daily bar and ten persisted frozen activity observations."""

    @staticmethod
    def _code_scope(trade_date: date, ts_code: str) -> WorkScope:
        return WorkScope(
            date_from=trade_date,
            date_to=trade_date,
            codes=(ts_code,),
        )

    def explain_recent_trading_dates(
        self, trade_date: date, context: RunContext,
    ) -> dict[str, Any]:
        """Plan the fixed, index-bounded calendar lookup for the detail series."""
        start = trade_date - timedelta(days=_LOOKBACK_CALENDAR_DAYS)
        return self.kernel.read._repository_explain(
            context, "std.trading_calendar",
            _RECENT_TRADING_DATES_SQL,
            (start, trade_date, _MAX_RECENT_DATES),
            WorkScope(date_from=start, date_to=trade_date),
        )

    def recent_trading_dates(self, trade_date: date, context: RunContext) -> list[date]:
        start = trade_date - timedelta(days=_LOOKBACK_CALENDAR_DAYS)
        rows = self.kernel.read._repository_query(
            context, "std.trading_calendar",
            _RECENT_TRADING_DATES_SQL,
            (start, trade_date, _MAX_RECENT_DATES),
            WorkScope(date_from=start, date_to=trade_date),
            max_rows=_MAX_RECENT_DATES,
        )
        return [row["trade_date"] for row in rows]

    def explain_recent_ratio_series(
        self, ts_code: str, trade_dates: Sequence[date], methodology_hash: str,
        context: RunContext,
    ) -> dict[str, Any]:
        dates = self._validate_dates(trade_dates)
        start, end = min(dates), max(dates)
        return self.kernel.read._repository_explain(
            context, "observation.stock_activity_daily", _RECENT_RATIO_SERIES_SQL,
            (ts_code, list(dates), methodology_hash, _MAX_RECENT_DATES),
            WorkScope(date_from=start, date_to=end, codes=(ts_code,)),
        )

    def recent_ratio_series(
        self, ts_code: str, trade_dates: Sequence[date], methodology_hash: str,
        context: RunContext,
    ) -> list[dict[str, Any]]:
        dates = self._validate_dates(trade_dates)
        start, end = min(dates), max(dates)
        return self.kernel.read._repository_query(
            context, "observation.stock_activity_daily", _RECENT_RATIO_SERIES_SQL,
            (ts_code, list(dates), methodology_hash, _MAX_RECENT_DATES),
            WorkScope(date_from=start, date_to=end, codes=(ts_code,)),
            max_rows=_MAX_RECENT_DATES,
        )

    @staticmethod
    def _validate_dates(trade_dates: Sequence[date]) -> tuple[date, ...]:
        dates = tuple(sorted(set(trade_dates)))
        if not dates or len(dates) > _MAX_RECENT_DATES:
            raise ValueError("ratio trend requires 1..10 distinct frozen trade dates")
        return dates

    @staticmethod
    def plan_summary(plan: dict[str, Any]) -> dict[str, Any]:
        root = plan.get("Plan", plan)
        nodes = list(_walk_plan_nodes(root))
        return {
            "root_node": root.get("Node Type"),
            "estimated_rows": root.get("Plan Rows"),
            "total_cost": root.get("Total Cost"),
            "scans": [
                {
                    "node": node.get("Node Type"),
                    "relation": node.get("Relation Name"),
                    "rows": node.get("Plan Rows"),
                }
                for node in nodes
                if node.get("Node Type") in {"Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan"}
            ],
        }


def _walk_plan_nodes(value: Any):
    if isinstance(value, dict):
        if "Node Type" in value:
            yield value
        for nested in value.values():
            yield from _walk_plan_nodes(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _walk_plan_nodes(nested)
