"""Read-only, tightly bounded queries for the Market Activity dashboard."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository
from data_collect.market_activity.contracts import MarketActivityRequest
from data_collect.observation.market_activity_rules import MarketActivityObservationPolicy


_MAX_DATES = 260
_MAX_STOCK_ROWS = 100
_MAX_TAG_ROWS = 500
_VIEWS = frozenset({"amount", "volume", "anomaly"})

_DATES_SQL = """
    SELECT DISTINCT d.trade_date
      FROM observation.market_activity_daily d
      JOIN observation.market_activity_snapshot s
        ON s.activity_snapshot_id=d.activity_snapshot_id
     WHERE d.trade_date BETWEEN %s AND %s
       AND d.methodology_hash=%s
       AND s.status='FROZEN'
     ORDER BY d.trade_date DESC
     LIMIT %s
"""
_SUMMARY_SQL = """
    SELECT d.activity_snapshot_id, d.trade_date,
           d.data_baseline_id, d.quality_status,
           d.total_volume, d.total_amount, d.top_n,
           d.top_n_volume, d.top_n_amount,
           d.top_n_volume_share, d.top_n_amount_share,
           d.input_row_count, d.target_row_count,
           d.invalid_target_row_count, d.invalid_history_row_count,
           d.insufficient_history_stock_count,
           s.methodology_hash, s.status AS snapshot_status,
           r.baseline_cutoff_date,
           o.snapshot_id AS observation_snapshot_id,
           o.status AS observation_status
      FROM observation.market_activity_daily d
      JOIN observation.market_activity_snapshot s
        ON s.activity_snapshot_id=d.activity_snapshot_id
      JOIN ops.tushare_data_release r
        ON r.release_tag=d.data_baseline_id
       AND r.baseline_domain='MARKET_ACTIVITY'
      LEFT JOIN LATERAL (
          SELECT os.snapshot_id, os.status
            FROM observation.snapshot os
           WHERE os.source_activity_snapshot_id=d.activity_snapshot_id
             AND os.observation_date_start=d.trade_date
             AND os.observation_date_end=d.trade_date
             AND os.methodology_hash=%s
             AND os.data_baseline_id=d.data_baseline_id
             AND os.status='FROZEN'
           ORDER BY os.created_at DESC, os.snapshot_id
           LIMIT 1
      ) o ON TRUE
     WHERE d.trade_date=%s
       AND d.methodology_hash=%s
       AND s.status='FROZEN'
       AND lower(r.status) IN ('validated','superseded')
     ORDER BY CASE WHEN lower(r.status)='validated' THEN 0 ELSE 1 END,
              r.baseline_cutoff_date DESC,
              s.created_at DESC
     LIMIT 1
"""
_RANK_SQL = """
    SELECT ts_code, volume, amount, amount_share, volume_share,
           amount_rank, volume_rank, amount_ratio_previous,
           volume_ratio_previous, history_observation_count,
           calculation_status, quality_status
      FROM observation.stock_activity_daily
     WHERE activity_snapshot_id=%s AND trade_date=%s
     ORDER BY {order_column}, ts_code
     LIMIT %s
"""
_ANOMALY_SQL = """
    SELECT a.observation_id, a.ts_code, f.volume, f.amount,
           f.amount_share, f.volume_share, f.amount_rank,
           f.volume_rank, a.activity_amount_ratio_previous,
           a.activity_volume_ratio_previous,
           a.activity_history_observation_count,
           a.calculation_status, a.quality_status,
           a.primary_state, a.observation_priority
      FROM observation.stock_anomaly_daily a
      JOIN observation.stock_activity_daily f
        ON f.activity_snapshot_id=a.source_activity_snapshot_id
       AND f.trade_date=a.observation_date
       AND f.ts_code=a.ts_code
     WHERE a.source_activity_snapshot_id=%s
       AND a.snapshot_id=%s AND a.observation_date=%s
       AND a.anomaly_flag IS TRUE
     ORDER BY GREATEST(
                  COALESCE(a.activity_amount_ratio_previous, 0),
                  COALESCE(a.activity_volume_ratio_previous, 0)
              ) DESC,
              a.ts_code
     LIMIT %s
"""
_TAGS_SQL = """
    SELECT t.observation_id, t.tag_code, t.tag_payload
      FROM observation.stock_anomaly_tag t
      JOIN observation.stock_anomaly_daily a
        ON a.observation_id=t.observation_id
     WHERE t.observation_id=ANY(%s::uuid[])
       AND a.snapshot_id=%s AND a.observation_date=%s
     ORDER BY t.observation_id, t.tag_code
     LIMIT %s
"""


def _scope(start: date, end: date, *, snapshot_id: str | None = None) -> WorkScope:
    dimensions = {"domain": "market_activity_dashboard"}
    if snapshot_id:
        dimensions["snapshot_id"] = snapshot_id
    return WorkScope(
        date_from=start,
        date_to=end,
        partition="market-activity-dashboard:read-only",
        dimensions=dimensions,
    )


class MarketActivityDashboardRepository(BaseRepository):
    """Serve frozen daily activity facts; never reads mutable upstream data."""

    def explain_read_paths(
        self, trade_date: date, context: RunContext, *,
        activity_snapshot_id: str, observation_snapshot_id: str,
    ) -> dict[str, Any]:
        """Run bounded, non-ANALYZE plans for every dashboard read shape."""
        start = trade_date - timedelta(days=365)
        methodology = MarketActivityRequest(trade_date=trade_date).methodology_hash
        observation_methodology = MarketActivityObservationPolicy().methodology_hash
        scope = _scope(trade_date, trade_date, snapshot_id=activity_snapshot_id)
        plans = {
            "date_catalog": self.kernel.read._repository_explain(
                context, "observation.market_activity_daily", _DATES_SQL,
                (start, trade_date, methodology, _MAX_DATES),
                _scope(start, trade_date),
            ),
            "frozen_summary": self.kernel.read._repository_explain(
                context, "observation.market_activity_daily", _SUMMARY_SQL,
                (observation_methodology, trade_date, methodology), scope,
            ),
            "amount_ranking": self.kernel.read._repository_explain(
                context, "observation.stock_activity_daily",
                _RANK_SQL.format(order_column="amount_rank"),
                (activity_snapshot_id, trade_date, _MAX_STOCK_ROWS + 1), scope,
            ),
            "anomaly_ranking": self.kernel.read._repository_explain(
                context, "observation.stock_anomaly_daily", _ANOMALY_SQL,
                (activity_snapshot_id, observation_snapshot_id, trade_date,
                 _MAX_STOCK_ROWS + 1), scope,
            ),
            "anomaly_tags": self.kernel.read._repository_explain(
                context, "observation.stock_anomaly_tag", _TAGS_SQL,
                (["00000000-0000-0000-0000-000000000001"],
                 observation_snapshot_id, trade_date, _MAX_TAG_ROWS + 1), scope,
            ),
        }
        return plans

    def available_dates(
        self, as_of: date, context: RunContext, *, days: int = 366,
    ) -> list[date]:
        if not 1 <= days <= 366:
            raise ValueError("dashboard date window must be in 1..366 days")
        start = as_of - timedelta(days=days - 1)
        methodology = MarketActivityRequest(trade_date=as_of).methodology_hash
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.market_activity_daily",
            sql=_DATES_SQL,
            params=(start, as_of, methodology, _MAX_DATES),
            scope=_scope(start, as_of),
            max_rows=_MAX_DATES,
        )
        return [row["trade_date"] for row in rows]

    def frozen_day(
        self, trade_date: date, context: RunContext, *, view: str = "amount",
    ) -> dict[str, Any]:
        if view not in _VIEWS:
            raise ValueError(f"unsupported market activity view: {view!r}")
        if trade_date > date.today():
            raise ValueError("dashboard date cannot be in the future")
        methodology = MarketActivityRequest(trade_date=trade_date).methodology_hash
        observation_methodology = MarketActivityObservationPolicy().methodology_hash
        scope = _scope(trade_date, trade_date)
        summary_rows = self.kernel.read._repository_query(
            context=context,
            source="observation.market_activity_daily",
            sql=_SUMMARY_SQL,
            params=(observation_methodology, trade_date, methodology),
            scope=scope,
            max_rows=1,
        )
        if not summary_rows:
            raise LookupError(f"{trade_date.isoformat()} has no FROZEN Market Activity snapshot")
        summary = summary_rows[0]
        snapshot_id = str(summary["activity_snapshot_id"])
        observation_id = summary.get("observation_snapshot_id")
        if view == "anomaly" and observation_id:
            rows = self._stock_rows(
                trade_date, snapshot_id, context, view,
                observation_snapshot_id=str(observation_id),
            )
            tags = self._tags(rows, context, trade_date, str(observation_id))
            for row in rows:
                row["tags"] = tags.get(str(row["observation_id"]), [])
        elif view == "anomaly":
            rows = []
        else:
            rows = self._stock_rows(trade_date, snapshot_id, context, view)
            for row in rows:
                row["tags"] = []
        truncated = len(rows) > _MAX_STOCK_ROWS
        rows = rows[:_MAX_STOCK_ROWS]
        return {
            "summary": summary,
            "view": view,
            "observation_available": bool(summary.get("observation_snapshot_id")),
            "rows": rows,
            "truncated": truncated,
        }

    def _stock_rows(
        self, trade_date: date, activity_snapshot_id: str,
        context: RunContext, view: str, *,
        observation_snapshot_id: str | None = None,
    ) -> list[dict[str, Any]]:
        scope = _scope(trade_date, trade_date, snapshot_id=activity_snapshot_id)
        if view == "anomaly":
            observation_rows = self.kernel.read._repository_query(
                context=context,
                source="observation.stock_anomaly_daily",
                sql=_ANOMALY_SQL,
                params=(activity_snapshot_id, observation_snapshot_id, trade_date, _MAX_STOCK_ROWS + 1),
                scope=scope,
                max_rows=_MAX_STOCK_ROWS + 1,
            )
            return observation_rows

        order_column = "amount_rank" if view == "amount" else "volume_rank"
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.stock_activity_daily",
            sql=_RANK_SQL.format(order_column=order_column),
            params=(activity_snapshot_id, trade_date, _MAX_STOCK_ROWS + 1),
            scope=scope,
            max_rows=_MAX_STOCK_ROWS + 1,
        )
        return rows

    def _tags(
        self, rows: list[dict[str, Any]], context: RunContext,
        trade_date: date, observation_snapshot_id: str,
    ) -> dict[str, list[dict[str, Any]]]:
        observation_ids = [str(row["observation_id"]) for row in rows]
        if not observation_ids:
            return {}
        result = self.kernel.read._repository_query(
            context=context,
            source="observation.stock_anomaly_tag",
            sql=_TAGS_SQL,
            params=(observation_ids, observation_snapshot_id, trade_date, _MAX_TAG_ROWS + 1),
            scope=_scope(trade_date, trade_date, snapshot_id=observation_snapshot_id),
            max_rows=_MAX_TAG_ROWS + 1,
        )
        if len(result) > _MAX_TAG_ROWS:
            raise RuntimeError("anomaly tag result exceeded dashboard bound")
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in result:
            grouped.setdefault(str(row["observation_id"]), []).append({
                "code": row["tag_code"],
                "evidence": row["tag_payload"],
            })
        return grouped
