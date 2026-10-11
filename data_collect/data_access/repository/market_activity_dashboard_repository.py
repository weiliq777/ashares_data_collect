"""Read-only, tightly bounded queries for the Market Activity dashboard."""
from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, time, timedelta
import re
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository
from data_collect.market_activity.contracts import MarketActivityRequest
from data_collect.market_activity.detail_metrics import (
    ActivityRatioPoint,
    build_activity_ratio_series,
    build_price_behavior,
)
from data_collect.observation.market_activity_rules import MarketActivityObservationPolicy


_MAX_DATES = 260
_MAX_STOCK_ROWS = 100
_MAX_TAG_ROWS = 500
_MAX_DETAIL_TAGS = 100
_MAX_HISTORY_ROWS = 60
_MAX_ACTIVITY_RATIO_ROWS = 10
_ACTIVITY_RATIO_LOOKBACK = 20
_MAX_SUBSEQUENT_ROWS = 20
_VIEWS = frozenset({"amount", "volume", "anomaly"})
_TS_CODE = re.compile(r"\d{6}\.(?:SH|SZ|BJ)\Z")
_SHANGHAI = ZoneInfo("Asia/Shanghai")

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
_DETAIL_SQL = """
    SELECT m.activity_snapshot_id, m.status AS activity_status,
           m.methodology_hash, m.data_baseline_id, m.input_manifest_hash,
           d.trade_date, d.quality_status AS activity_quality_status,
           d.input_row_count, d.target_row_count, d.invalid_target_row_count,
           d.invalid_history_row_count, d.insufficient_history_stock_count,
           f.ts_code, f.close, f.volume, f.amount, f.amount_share, f.volume_share,
           f.amount_rank, f.volume_rank, f.amount_avg_previous,
           f.amount_ratio_previous, f.volume_avg_previous,
           f.volume_ratio_previous, f.history_observation_count,
           f.calculation_status, f.quality_status AS stock_quality_status,
           r.status AS release_status, r.baseline_cutoff_date,
           os.snapshot_id AS observation_snapshot_id,
           os.status AS observation_status,
           a.observation_id, a.primary_state, a.observation_priority,
           a.activity_amount_ratio_previous,
           a.activity_volume_ratio_previous,
           a.activity_history_observation_count,
           a.quality_status AS anomaly_quality_status
      FROM observation.market_activity_snapshot m
      JOIN observation.market_activity_daily d
        ON d.activity_snapshot_id=m.activity_snapshot_id
      JOIN observation.stock_activity_daily f
        ON f.activity_snapshot_id=m.activity_snapshot_id
       AND f.trade_date=d.trade_date
      JOIN ops.tushare_data_release r
        ON r.release_tag=m.data_baseline_id
       AND r.baseline_domain='MARKET_ACTIVITY'
     LEFT JOIN LATERAL (
          SELECT s.snapshot_id, s.status
            FROM observation.snapshot s
           WHERE s.source_activity_snapshot_id=m.activity_snapshot_id
             AND s.observation_date_start=d.trade_date
             AND s.observation_date_end=d.trade_date
             AND s.methodology_hash=%s
             AND s.data_baseline_id=m.data_baseline_id
             AND s.status='FROZEN'
           ORDER BY s.created_at DESC, s.snapshot_id
           LIMIT 1
      ) os ON TRUE
      LEFT JOIN observation.stock_anomaly_daily a
        ON a.snapshot_id=os.snapshot_id
       AND a.source_activity_snapshot_id=m.activity_snapshot_id
       AND a.observation_date=d.trade_date
       AND a.ts_code=f.ts_code
     WHERE m.activity_snapshot_id=%s
       AND d.trade_date=%s
       AND f.ts_code=%s
       AND m.status='FROZEN'
       AND lower(r.status) IN ('validated','superseded')
     LIMIT 1
"""
_DETAIL_TAGS_SQL = """
    SELECT tag_code, tag_payload
      FROM observation.stock_anomaly_tag
     WHERE observation_id=%s
     ORDER BY tag_code
     LIMIT %s
"""
_IDENTITY_SQL = """
    SELECT c.stock_code AS ts_code,
           COALESCE(l.name, b.name) AS name,
           COALESCE(l.symbol, b.symbol) AS symbol,
           COALESCE(l.market, b.market) AS market,
           COALESCE(l.list_date, b.list_date) AS list_date,
           COALESCE(l.delist_date, b.delist_date) AS delist_date,
           CASE WHEN l.name IS NOT NULL THEN l.quality_status
                WHEN b.name IS NOT NULL THEN 'CURRENT_MASTER_FALLBACK'
                ELSE 'UNKNOWN' END AS name_quality_status
      FROM (SELECT %s::varchar AS stock_code) c
      LEFT JOIN std.stock_basic_info b ON b.stock_code=c.stock_code
      LEFT JOIN LATERAL (
          SELECT name, symbol, market, list_date, delist_date, quality_status
            FROM std.security_lifecycle
           WHERE ts_code=c.stock_code
             AND effective_from<=%s
             AND (effective_to IS NULL OR effective_to>=%s)
           ORDER BY effective_from DESC, lifecycle_id DESC
           LIMIT 1
      ) l ON TRUE
     LIMIT 1
"""
_HISTORY_SQL = """
    SELECT trade_date, open, high, low, close, volume, amount
      FROM std.daily_kline
     WHERE stock_code=%s AND trade_date>=%s AND trade_date<%s
     ORDER BY trade_date DESC
     LIMIT %s
"""
_CURRENT_BAR_SQL = """
    SELECT stock_code AS ts_code, trade_date, open, high, low, close, volume, amount
      FROM std.daily_kline
     WHERE stock_code=%s AND trade_date=%s
     LIMIT 1
"""
_RETURN_FEATURE_SQL = """
    SELECT ts_code, trade_date, return_1d, adjustment_mode,
           source_asof_date, quality_status
      FROM feature.price_daily_v2
     WHERE ts_code=%s AND trade_date=%s
     LIMIT 1
"""
_SUBSEQUENT_SQL = """
    SELECT trade_date, open, high, low, close, volume, amount
      FROM std.daily_kline
     WHERE stock_code=%s AND trade_date>%s AND trade_date<=%s
     ORDER BY trade_date
     LIMIT %s
"""
_STATUS_SQL = """
    SELECT is_listed, is_st, is_suspended, is_tradeable, limit_status,
           up_limit, down_limit, close_price, status_reason, quality_status,
           fetched_at
      FROM std.security_status_daily
     WHERE ts_code=%s AND trade_date=%s
     LIMIT 1
"""
_VALUATION_SQL = """
    SELECT stock_code, trade_date, source, turnover_rate, turnover_rate_f,
           free_share, total_mv, circ_mv, pe, pe_ttm, pb, dv_ttm,
           quality_status, data_available_timestamp, fetched_at
      FROM std.valuation_daily
     WHERE stock_code=%s AND trade_date=%s AND source='tushare'
       AND data_available_timestamp<=%s
     LIMIT 1
"""
_FINANCIAL_SQL = """
    SELECT report_date, effective_announce_date, data_available_timestamp,
           roe_ttm, revenue_ttm, net_profit_parent_ttm, operating_cashflow_ttm,
           debt_to_assets, gross_margin_ttm, revenue_growth_yoy,
           profit_growth_yoy, ttm_complete, pit_evidence_level,
           quality_status, source_fingerprint
      FROM feature.company_financial_v2
     WHERE ts_code=%s
       AND report_date BETWEEN %s AND %s
       AND data_available_timestamp<=%s
     ORDER BY data_available_timestamp DESC, report_date DESC,
              source_fingerprint DESC
     LIMIT 1
"""
_INDUSTRY_SQL = """
    SELECT industry_system, industry_level, industry_code, industry_name,
           source_asof_date, data_available_timestamp, quality_status
      FROM pit.industry_membership_daily
     WHERE ts_code=%s AND trade_date=%s
       AND industry_system='TUSHARE_STOCK_BASIC' AND industry_level=1
     LIMIT 1
"""


def _scope(
    start: date, end: date, *, snapshot_id: str | None = None,
    ts_code: str | None = None,
) -> WorkScope:
    dimensions = {"domain": "market_activity_dashboard"}
    if snapshot_id:
        dimensions["snapshot_id"] = snapshot_id
    return WorkScope(
        date_from=start,
        date_to=end,
        codes=(ts_code,) if ts_code else None,
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

    def stock_detail(
        self,
        activity_snapshot_id: str | UUID,
        trade_date: date,
        ts_code: str,
        context: RunContext,
    ) -> dict[str, Any]:
        """Return one bounded security detail tied to one frozen observation."""
        try:
            snapshot_id = str(UUID(str(activity_snapshot_id)))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("activity_snapshot_id must be a UUID") from exc
        code = str(ts_code or "").strip().upper()
        if not _TS_CODE.fullmatch(code):
            raise ValueError("ts_code must use the format 000001.SZ")
        if not isinstance(trade_date, date) or isinstance(trade_date, datetime):
            raise TypeError("trade_date must be datetime.date")
        if trade_date > date.today():
            raise ValueError("trade_date cannot be in the future")

        exact_scope = _scope(trade_date, trade_date, snapshot_id=snapshot_id, ts_code=code)
        activity_rows = self.kernel.read._repository_query(
            context=context,
            source="observation.market_activity_snapshot",
            sql=_DETAIL_SQL,
            params=(
                MarketActivityObservationPolicy().methodology_hash,
                snapshot_id, trade_date, code,
            ),
            scope=exact_scope,
            max_rows=1,
        )
        if not activity_rows:
            raise LookupError("frozen activity snapshot or stock observation not found")
        activity = activity_rows[0]
        observation_id = activity.get("observation_id")
        observation_snapshot_id = activity.get("observation_snapshot_id")
        tags: list[dict[str, Any]] = []
        if observation_id is not None and observation_snapshot_id is not None:
            tag_rows = self.kernel.read._repository_query(
                context=context,
                source="observation.stock_anomaly_tag",
                sql=_DETAIL_TAGS_SQL,
                params=(str(observation_id), _MAX_DETAIL_TAGS + 1),
                scope=_scope(trade_date, trade_date, snapshot_id=str(observation_snapshot_id)),
                max_rows=_MAX_DETAIL_TAGS + 1,
            )
            if len(tag_rows) > _MAX_DETAIL_TAGS:
                raise RuntimeError("stock detail tag result exceeded its hard row cap")
            tags = [{"code": row["tag_code"], "evidence": row["tag_payload"]} for row in tag_rows]

        identity_rows = self.kernel.read._repository_query(
            context=context,
            source="std.security_lifecycle",
            sql=_IDENTITY_SQL,
            params=(code, trade_date, trade_date),
            scope=_scope(trade_date, trade_date, snapshot_id=snapshot_id, ts_code=code),
            max_rows=1,
        )
        identity = identity_rows[0] if identity_rows else {
            "ts_code": code, "name": None, "symbol": None,
            "name_quality_status": "UNKNOWN",
        }

        history_start = trade_date - timedelta(days=366)
        history_rows = self.kernel.read._repository_query(
            context=context,
            source="std.daily_kline",
            sql=_HISTORY_SQL,
            params=(code, history_start, trade_date, _MAX_HISTORY_ROWS),
            scope=_scope(history_start, trade_date, snapshot_id=snapshot_id, ts_code=code),
            max_rows=_MAX_HISTORY_ROWS,
        )
        history_rows.reverse()

        current_bar_rows = self.kernel.read._repository_query(
            context=context,
            source="std.daily_kline",
            sql=_CURRENT_BAR_SQL,
            params=(code, trade_date),
            scope=exact_scope,
            max_rows=1,
        )
        current_bar = current_bar_rows[0] if current_bar_rows else None
        return_feature_rows = self.kernel.read._repository_query(
            context=context,
            source="feature.price_daily_v2",
            sql=_RETURN_FEATURE_SQL,
            params=(code, trade_date),
            scope=exact_scope,
            max_rows=1,
        )
        return_feature = return_feature_rows[0] if return_feature_rows else None

        recent_bar_dates = {
            row["trade_date"] for row in history_rows
            if isinstance(row.get("trade_date"), date)
        }
        recent_bar_dates.add(trade_date)
        ratio_dates = sorted(recent_bar_dates)[-_MAX_ACTIVITY_RATIO_ROWS:]
        ratio_points = build_activity_ratio_series(
            [*history_rows, *current_bar_rows],
            ratio_dates,
            lookback=_ACTIVITY_RATIO_LOOKBACK,
        )
        current_ratio_target = next(
            point for point in ratio_points if point.trade_date == trade_date
        )
        previous_close = history_rows[-1].get("close") if history_rows else None
        price_behavior = build_price_behavior(
            current_bar, previous_close, return_feature,
        )
        frozen_target_comparison = _compare_frozen_target_ratios(
            current_ratio_target, activity,
        )
        valid_ratio_count = sum(
            point.calculation_status == "VALID" for point in ratio_points
        )
        ratio_status = (
            "VALID" if len(ratio_points) == _MAX_ACTIVITY_RATIO_ROWS
            and valid_ratio_count == _MAX_ACTIVITY_RATIO_ROWS
            else "PARTIAL" if valid_ratio_count
            else "NO_VALID_DATA"
        )

        subsequent_end = min(trade_date + timedelta(days=60), date.today())
        subsequent_rows: list[dict[str, Any]] = []
        if subsequent_end > trade_date:
            subsequent_rows = self.kernel.read._repository_query(
                context=context,
                source="std.daily_kline",
                sql=_SUBSEQUENT_SQL,
                params=(code, trade_date, subsequent_end, _MAX_SUBSEQUENT_ROWS),
                scope=_scope(trade_date, subsequent_end, snapshot_id=snapshot_id, ts_code=code),
                max_rows=_MAX_SUBSEQUENT_ROWS,
            )

        status_rows = self.kernel.read._repository_query(
            context=context,
            source="std.security_status_daily",
            sql=_STATUS_SQL,
            params=(code, trade_date),
            scope=_scope(trade_date, trade_date, snapshot_id=snapshot_id, ts_code=code),
            max_rows=1,
        )
        cutoff = datetime.combine(trade_date, time(15, 0), tzinfo=_SHANGHAI)
        valuation_rows = self.kernel.read._repository_query(
            context=context,
            source="std.valuation_daily",
            sql=_VALUATION_SQL,
            params=(code, trade_date, cutoff),
            scope=_scope(trade_date, trade_date, snapshot_id=snapshot_id, ts_code=code),
            max_rows=1,
        )
        financial_rows = self.kernel.read._repository_query(
            context=context,
            source="feature.company_financial_v2",
            sql=_FINANCIAL_SQL,
            params=(code, trade_date - timedelta(days=365 * 5), trade_date, cutoff),
            scope=_scope(
                trade_date - timedelta(days=365 * 5), trade_date,
                snapshot_id=snapshot_id, ts_code=code,
            ),
            max_rows=1,
        )
        industry_rows = self.kernel.read._repository_query(
            context=context,
            source="pit.industry_membership_daily",
            sql=_INDUSTRY_SQL,
            params=(code, trade_date),
            scope=_scope(trade_date, trade_date, snapshot_id=snapshot_id, ts_code=code),
            max_rows=1,
        )

        return {
            "identity": identity,
            "observation_context": {
                "trade_date": trade_date.isoformat(),
                "activity_snapshot_id": snapshot_id,
                "data_baseline_id": activity["data_baseline_id"],
                "methodology_hash": activity["methodology_hash"],
                "input_manifest_hash": activity.get("input_manifest_hash"),
                "quality_status": activity["activity_quality_status"],
                "stock_quality_status": activity["stock_quality_status"],
                "release_status": activity["release_status"],
                "baseline_cutoff_date": activity["baseline_cutoff_date"],
                "activity": {
                    key: activity.get(key) for key in (
                        "close", "volume", "amount", "amount_share", "volume_share",
                        "amount_rank", "volume_rank", "amount_avg_previous",
                        "amount_ratio_previous", "volume_avg_previous",
                        "volume_ratio_previous", "history_observation_count",
                        "calculation_status",
                    )
                },
                "observation_snapshot_id": observation_snapshot_id,
                "observation_status": activity.get("observation_status"),
                "observation_id": observation_id,
                "primary_state": activity.get("primary_state"),
                "observation_priority": activity.get("observation_priority"),
                "tags": tags,
            },
            "price_behavior": price_behavior,
            "activity_ratio_history": {
                "window_start": ratio_dates[0].isoformat() if ratio_dates else None,
                "window_end": trade_date.isoformat(),
                "expected_max_rows": _MAX_ACTIVITY_RATIO_ROWS,
                "returned_rows": len(ratio_points),
                "valid_rows": valid_ratio_count,
                "lookback_valid_bars": _ACTIVITY_RATIO_LOOKBACK,
                "window_semantics": "mean of preceding valid stock bars; target bar excluded",
                "date_scope_semantics": "latest stock bar dates up to the target, plus target date; missing earlier stock bars are omitted",
                "source_status": ratio_status,
                "source_semantics": "CURRENT_STANDARD_CONTEXT; not a frozen historical series",
                "retrieved_as_of_date": date.today().isoformat(),
                "frozen_target_comparison": frozen_target_comparison,
                "rows": [asdict(point) for point in ratio_points],
            },
            "price_activity_history": {
                "window_start": history_start.isoformat(),
                "window_end": (trade_date - timedelta(days=1)).isoformat(),
                "returned_rows": len(history_rows),
                "expected_max_rows": _MAX_HISTORY_ROWS,
                "source_status": (
                    "COMPLETE" if len(history_rows) >= _MAX_HISTORY_ROWS
                    else "PARTIAL_HISTORY" if history_rows else "NO_HISTORY"
                ),
                "source_semantics": "CURRENT_STANDARD_CONTEXT",
                "rows": history_rows,
            },
            "security_context": {
                "listing_status": {
                    "value": {
                        "list_date": identity.get("list_date"),
                        "delist_date": identity.get("delist_date"),
                        "market": identity.get("market"),
                    },
                    "quality_status": identity.get("name_quality_status", "UNKNOWN"),
                },
                "tradeability": status_rows[0] if status_rows else {"quality_status": "UNKNOWN"},
                "valuation": valuation_rows[0] if valuation_rows else {"quality_status": "NOT_AVAILABLE_AS_OF"},
                "financial_pit": financial_rows[0] if financial_rows else {"quality_status": "NOT_AVAILABLE_AS_OF"},
                "industry_observation": industry_rows[0] if industry_rows else {"quality_status": "UNKNOWN"},
            },
            "subsequent_path": {
                "classification": "RETROSPECTIVE_ONLY",
                "window_end": subsequent_end.isoformat(),
                "returned_rows": len(subsequent_rows),
                "expected_max_rows": _MAX_SUBSEQUENT_ROWS,
                "source_status": (
                    "AVAILABLE" if subsequent_rows else "NO_SUBSEQUENT_DATA"
                ),
                "source_semantics": "CURRENT_STANDARD_CONTEXT",
                "rows": subsequent_rows,
            },
            "provenance": {
                "observation": {
                    "source": "FROZEN Market Activity/Observation snapshots",
                    "status": "FROZEN",
                    "data_baseline_id": activity["data_baseline_id"],
                    "release_status": activity["release_status"],
                },
                "standard_context": {
                    "source": [
                        "std.daily_kline", "std.security_lifecycle",
                        "std.stock_basic_info", "std.security_status_daily",
                        "std.valuation_daily", "feature.company_financial_v2",
                        "pit.industry_membership_daily",
                    ],
                    "status": "CURRENT_STANDARD_CONTEXT; post-baseline source corrections may exist",
                },
                "activity_ratio_history": {
                    "source": "std.daily_kline",
                    "semantics": "CURRENT_STANDARD_CONTEXT",
                    "data_baseline_id": None,
                    "retrieved_as_of_date": date.today().isoformat(),
                },
                "financial_cutoff": cutoff.isoformat(),
            },
        }

    def explain_stock_detail_paths(
        self, activity_snapshot_id: str | UUID, trade_date: date,
        ts_code: str, context: RunContext,
    ) -> dict[str, Any]:
        """EXPLAIN every bounded detail query without executing source reads."""
        snapshot_id = str(UUID(str(activity_snapshot_id)))
        code = str(ts_code).strip().upper()
        if not _TS_CODE.fullmatch(code):
            raise ValueError("ts_code must use the format 000001.SZ")
        history_start = trade_date - timedelta(days=366)
        subsequent_end = min(trade_date + timedelta(days=60), date.today())
        cutoff = datetime.combine(trade_date, time(15, 0), tzinfo=_SHANGHAI)
        exact_scope = _scope(trade_date, trade_date, snapshot_id=snapshot_id, ts_code=code)
        plans = {
            "frozen_observation": self.kernel.read._repository_explain(
                context, "observation.market_activity_snapshot", _DETAIL_SQL,
                (
                    MarketActivityObservationPolicy().methodology_hash,
                    snapshot_id, trade_date, code,
                ), exact_scope,
            ),
            "identity": self.kernel.read._repository_explain(
                context, "std.security_lifecycle", _IDENTITY_SQL,
                (code, trade_date, trade_date),
                _scope(trade_date, trade_date, snapshot_id=snapshot_id, ts_code=code),
            ),
            "prior_price_history": self.kernel.read._repository_explain(
                context, "std.daily_kline", _HISTORY_SQL,
                (code, history_start, trade_date, _MAX_HISTORY_ROWS),
                _scope(history_start, trade_date, snapshot_id=snapshot_id, ts_code=code),
            ),
            "current_day_bar": self.kernel.read._repository_explain(
                context, "std.daily_kline", _CURRENT_BAR_SQL,
                (code, trade_date), exact_scope,
            ),
            "return_feature": self.kernel.read._repository_explain(
                context, "feature.price_daily_v2", _RETURN_FEATURE_SQL,
                (code, trade_date), exact_scope,
            ),
            "subsequent_price_path": self.kernel.read._repository_explain(
                context, "std.daily_kline", _SUBSEQUENT_SQL,
                (code, trade_date, subsequent_end, _MAX_SUBSEQUENT_ROWS),
                _scope(trade_date, subsequent_end, snapshot_id=snapshot_id, ts_code=code),
            ) if subsequent_end > trade_date else {"Plan": {"Node Type": "No query: no subsequent dates"}},
            "tradeability": self.kernel.read._repository_explain(
                context, "std.security_status_daily", _STATUS_SQL,
                (code, trade_date), exact_scope,
            ),
            "valuation": self.kernel.read._repository_explain(
                context, "std.valuation_daily", _VALUATION_SQL,
                (code, trade_date, cutoff), exact_scope,
            ),
            "financial_pit": self.kernel.read._repository_explain(
                context, "feature.company_financial_v2", _FINANCIAL_SQL,
                (code, trade_date - timedelta(days=365 * 5), trade_date, cutoff),
                _scope(
                    trade_date - timedelta(days=365 * 5), trade_date,
                    snapshot_id=snapshot_id, ts_code=code,
                ),
            ),
            "industry_observation": self.kernel.read._repository_explain(
                context, "pit.industry_membership_daily", _INDUSTRY_SQL,
                (code, trade_date), exact_scope,
            ),
            "anomaly_tags": self.kernel.read._repository_explain(
                context, "observation.stock_anomaly_tag", _DETAIL_TAGS_SQL,
                ("00000000-0000-0000-0000-000000000001", _MAX_DETAIL_TAGS + 1),
                exact_scope,
            ),
        }
        return plans

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


def _compare_frozen_target_ratios(
    current: ActivityRatioPoint, frozen_activity: dict[str, Any],
) -> dict[str, Any]:
    current_values = {
        "amount_ratio_previous": current.amount_ratio_previous,
        "volume_ratio_previous": current.volume_ratio_previous,
    }
    frozen_values = {
        "amount_ratio_previous": frozen_activity.get("amount_ratio_previous"),
        "volume_ratio_previous": frozen_activity.get("volume_ratio_previous"),
    }
    if current.calculation_status != "VALID":
        status = f"NOT_COMPARABLE_{current.calculation_status}"
    elif all(_same_optional_number(current_values[key], frozen_values[key]) for key in current_values):
        status = "MATCHING_VALUES"
    else:
        status = "DIFFERS_BY_SOURCE_OR_REVISION"
    return {
        "status": status,
        "frozen_values": frozen_values,
        "current_standard_values": current_values,
    }


def _same_optional_number(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return abs(float(left) - float(right)) <= max(
            1e-12, 1e-9 * max(abs(float(left)), abs(float(right)))
        )
    except (TypeError, ValueError):
        return False
