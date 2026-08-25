"""Typed read-only repositories for foundation quality inspection.

The quality job is an orchestration layer.  The inspection SQL lives here so
that every read still carries a RunContext, WorkScope, DatasetPolicy and the
bounded repository-read guard.
"""
from __future__ import annotations

from typing import Any, Mapping

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class QualityInspectionRepository(BaseRepository):
    DATE_COLUMNS = {
        "std.stock_basic_info": "list_date",
        "tushare_stock_basic_raw": None,
        "std.trading_calendar": "trade_date",
        "tushare_trade_cal_raw": "cal_date",
        "std.stock_financial_indicator": "report_date",
        "tushare_fina_indicator_raw": "end_date",
        "tushare_income_raw": "end_date",
        "tushare_balancesheet_raw": "end_date",
        "tushare_cashflow_raw": "end_date",
        "tushare_financial_raw_version": "end_date",
        "fina_indicator_raw_version": "end_date",
        "std.fina_indicator_standard": "report_date",
        "std.financial_income_standard": "report_date",
        "std.financial_balance_sheet_standard": "report_date",
        "std.financial_cash_flow_standard": "report_date",
        "tushare_stock_st_raw": "trade_date",
        "tushare_namechange_raw": "start_date",
        "tushare_suspend_d_raw": "trade_date",
        "tushare_stk_limit_raw": "trade_date",
        "tushare_dividend_raw": "ex_date",
        "tushare_index_daily_raw": "trade_date",
        "std.security_lifecycle": "effective_from",
        "std.security_status_daily": "trade_date",
        "std.dividend_event": "ex_date",
        "std.index_daily_tushare": "trade_date",
        "std.daily_kline": "trade_date",
        "std.valuation_daily": "trade_date",
        "pit.universe_daily": "trade_date",
        "pit.security_tradeability_daily": "trade_date",
        "pit.financial_asof": "decision_date",
        "feature.price_daily_v1": "trade_date",
        "feature.price_adjustment_factor_asof": "trade_date",
        "feature.price_daily_v2": "trade_date",
        "feature.valuation_daily_v1": "trade_date",
        "feature.company_financial_v1": "report_date",
        "feature.company_financial_v2": "report_date",
        "feature.shareholder_return_v1": "asof_date",
        "feature.feature_registry": None,
    }

    _INVARIANT_QUERIES: Mapping[str, tuple[str, str]] = {
        "financial_future_rows": (
            "pit.financial_asof",
            """SELECT COUNT(*) AS value FROM pit.financial_asof
                WHERE (effective_announce_date IS NOT NULL AND effective_announce_date > decision_date)
                   OR (version_available_date IS NOT NULL AND version_available_date > decision_date)""",
        ),
        "financial_standard_invalid_version_availability": (
            "std.fina_indicator_standard",
            """SELECT SUM(failures) AS value FROM (
                SELECT COUNT(*) FILTER (WHERE version_available_date IS NULL OR version_available_date < effective_announce_date) AS failures FROM std.fina_indicator_standard
                UNION ALL SELECT COUNT(*) FILTER (WHERE version_available_date IS NULL OR version_available_date < announce_date) FROM std.financial_income_standard
                UNION ALL SELECT COUNT(*) FILTER (WHERE version_available_date IS NULL OR version_available_date < announce_date) FROM std.financial_balance_sheet_standard
                UNION ALL SELECT COUNT(*) FILTER (WHERE version_available_date IS NULL OR version_available_date < announce_date) FROM std.financial_cash_flow_standard
            ) x""",
        ),
        "financial_feature_invalid_version_availability": (
            "feature.company_financial_v1",
            """SELECT COUNT(*) AS value FROM feature.company_financial_v1
                WHERE version_available_date IS NULL OR version_available_date < effective_announce_date""",
        ),
        "financial_vintage_metadata_missing_or_backdated": (
            "std.fina_indicator_standard",
            """SELECT SUM(failures) AS value FROM (
                SELECT COUNT(*) FILTER (WHERE vintage_quality IS NULL OR source_observed_at IS NULL OR (vintage_quality='WAREHOUSE_OBSERVED_ONLY' AND version_available_date < source_observed_at::date)) AS failures FROM std.fina_indicator_standard
                UNION ALL SELECT COUNT(*) FILTER (WHERE vintage_quality IS NULL OR source_observed_at IS NULL OR (vintage_quality='WAREHOUSE_OBSERVED_ONLY' AND version_available_date < source_observed_at::date)) FROM std.financial_income_standard
                UNION ALL SELECT COUNT(*) FILTER (WHERE vintage_quality IS NULL OR source_observed_at IS NULL OR (vintage_quality='WAREHOUSE_OBSERVED_ONLY' AND version_available_date < source_observed_at::date)) FROM std.financial_balance_sheet_standard
                UNION ALL SELECT COUNT(*) FILTER (WHERE vintage_quality IS NULL OR source_observed_at IS NULL OR (vintage_quality='WAREHOUSE_OBSERVED_ONLY' AND version_available_date < source_observed_at::date)) FROM std.financial_cash_flow_standard
            ) x""",
        ),
        "financial_pit_vintage_future_rows": (
            "pit.financial_asof",
            """SELECT COUNT(*) AS value FROM pit.financial_asof
                WHERE vintage_quality IS NULL OR source_observed_at IS NULL
                   OR (vintage_quality='WAREHOUSE_OBSERVED_ONLY' AND source_observed_at::date > decision_date)""",
        ),
        "tradeability_null_buy_sell": (
            "pit.security_tradeability_daily",
            """SELECT COUNT(*) AS value FROM pit.security_tradeability_daily
                WHERE is_tradeable IS TRUE AND (can_buy IS NULL OR can_sell IS NULL)""",
        ),
        "tradeability_limit_direction_mismatch": (
            "pit.security_tradeability_daily",
            """SELECT COUNT(*) AS value FROM pit.security_tradeability_daily
                WHERE (limit_status='UP_LIMIT' AND can_buy) OR (limit_status='DOWN_LIMIT' AND can_sell)""",
        ),
        "nonpositive_price_tradeable_rows": (
            "std.security_status_daily",
            """SELECT COUNT(*) AS value FROM std.security_status_daily
                WHERE is_tradeable IS TRUE AND (close_price IS NULL OR close_price <= 0)""",
        ),
        "missing_adj_factor_feature_rows": (
            "feature.price_daily_v2",
            """SELECT COUNT(*) AS value FROM feature.price_daily_v2 WHERE quality_status = 'MISSING_SOURCE'""",
        ),
        "raw_phase2_failed_checkpoints": (
            "ops.tushare_history_checkpoint",
            """SELECT COUNT(*) AS value FROM ops.tushare_history_checkpoint
                WHERE dataset IN ('stock_st','suspend_d','stk_limit','namechange','dividend','index_daily') AND status = 'failed'""",
        ),
        "pending_financial_derived_checkpoints": (
            "ops.tushare_history_checkpoint",
            """SELECT COUNT(*) AS value FROM ops.tushare_history_checkpoint
                WHERE dataset='financial_derived_v2' AND status IN ('pending','failed')""",
        ),
        "fina_indicator_standard_missing_or_yoy": (
            "fina_indicator_raw_version",
            """SELECT COUNT(*) AS value FROM (
                SELECT DISTINCT r.payload_hash FROM fina_indicator_raw_version r
                WHERE r.payload ? 'or_yoy' AND NOT EXISTS (
                    SELECT 1 FROM std.fina_indicator_standard s WHERE s.payload_hash = r.payload_hash
                )
            ) missing""",
        ),
        "fina_indicator_standard_missing_lineage": (
            "std.fina_indicator_standard",
            """SELECT COUNT(*) AS value FROM std.fina_indicator_standard
                WHERE source_record_key IS NULL OR payload_hash IS NULL OR record_version IS NULL""",
        ),
        "financial_pit_missing_lineage": (
            "pit.financial_asof",
            """SELECT COUNT(*) AS value FROM pit.financial_asof
                WHERE effective_announce_date IS NOT NULL
                  AND (source_record_key IS NULL OR payload_hash IS NULL OR record_version IS NULL)""",
        ),
        "financial_feature_missing_revenue_mapping": (
            "std.fina_indicator_standard",
            """SELECT COUNT(*) AS value FROM std.fina_indicator_standard s
                WHERE s.or_yoy IS NOT NULL AND (%s IS NULL OR s.version_available_date <= %s)
                  AND NOT EXISTS (
                    SELECT 1 FROM feature.company_financial_v1 f
                    WHERE f.ts_code=s.ts_code AND f.report_date=s.report_date
                      AND f.version_available_date=s.version_available_date AND f.revenue_yoy = s.or_yoy
                  )""",
        ),
        "immature_price_features_exposed": (
            "feature.price_daily_v2",
            """SELECT COUNT(*) AS value FROM feature.price_daily_v2
                WHERE (ma20 IS NOT NULL AND COALESCE(ma20_observation_count,0)<20)
                   OR (ma60 IS NOT NULL AND COALESCE(ma60_observation_count,0)<60)
                   OR (ma120 IS NOT NULL AND COALESCE(ma120_observation_count,0)<120)""",
        ),
        "immature_valuation_percentiles_exposed": (
            "feature.valuation_daily_v1",
            """SELECT COUNT(*) AS value FROM feature.valuation_daily_v1
                WHERE pe_pct_5y IS NOT NULL AND (
                    NOT COALESCE(has_full_pe_5y_history,FALSE)
                    OR COALESCE(pe_observation_count_5y,0)<1000
                    OR pe_window_start_date_5y IS NULL
                    OR trade_date-pe_window_start_date_5y<1794)""",
        ),
    }

    @staticmethod
    def _qualified_table(table: str) -> str:
        parts = table.split(".")
        if len(parts) not in (1, 2) or any(not part.replace("_", "").isalnum() for part in parts):
            raise ValueError(f"invalid table name: {table}")
        return ".".join(f'"{part}"' for part in parts)

    def inspect_table(self, table: str, keys: tuple[str, ...], context: RunContext) -> dict[str, Any]:
        if not keys or any(not key.replace("_", "").isalnum() for key in keys):
            raise ValueError("invalid quality key columns")
        canonical_table = {
            "adj_factor_daily": "std.adj_factor_daily",
            "valuation_daily": "std.valuation_daily",
        }.get(table, table)
        qualified = self._qualified_table(canonical_table)
        date_column = self.DATE_COLUMNS.get(canonical_table, self.DATE_COLUMNS.get(table, "trade_date"))
        scope = WorkScope(partition=f"quality:{table}")
        if date_column:
            row = self.kernel.read._repository_query(
                context, canonical_table,
                f'SELECT COUNT(*) AS rows, MIN("{date_column}") AS min_date, MAX("{date_column}") AS max_date FROM {qualified}',
                scope=scope, max_rows=2,
            )[0]
        else:
            row = self.kernel.read._repository_query(
                context, canonical_table,
                f"SELECT COUNT(*) AS rows FROM {qualified}", scope=scope, max_rows=2,
            )[0]
            row["min_date"] = row["max_date"] = None
        key_expr = ", ".join(f'"{key}"' for key in keys)
        duplicate = self.kernel.read._repository_query(
            context, canonical_table,
            f"SELECT COUNT(*) AS duplicate_groups FROM (SELECT {key_expr}, COUNT(*) FROM {qualified} GROUP BY {key_expr} HAVING COUNT(*) > 1) duplicates",
            scope=scope, max_rows=2,
        )[0]["duplicate_groups"]
        null_payload = 0
        if table.split(".")[-1].startswith("tushare_"):
            null_payload = self.kernel.read._repository_query(
                context, canonical_table,
                f"SELECT COUNT(*) AS null_payload FROM {qualified} WHERE payload IS NULL",
                scope=scope, max_rows=2,
            )[0]["null_payload"]
        result = {
            "rows": row["rows"],
            "min_date": str(row["min_date"]) if row["min_date"] else None,
            "max_date": str(row["max_date"]) if row["max_date"] else None,
            "duplicate_key_groups": duplicate,
            "null_payload": null_payload,
        }
        if table == "tushare_daily_raw":
            result["zero_or_negative_price_rows"] = self.kernel.read._repository_query(
                context, table,
                """SELECT COUNT(*) AS value FROM tushare_daily_raw
                   WHERE COALESCE((payload->>'open')::numeric, 0) = 0
                      OR COALESCE((payload->>'high')::numeric, 0) = 0
                      OR COALESCE((payload->>'low')::numeric, 0) = 0
                      OR COALESCE((payload->>'close')::numeric, 0) < 0""",
                scope=scope, max_rows=2,
            )[0]["value"]
        elif table in {"daily_kline", "std.daily_kline"}:
            result["zero_or_negative_price_rows"] = self.kernel.read._repository_query(
                context, "std.daily_kline",
                "SELECT COUNT(*) AS value FROM std.daily_kline WHERE open=0 OR high=0 OR low=0 OR close<0",
                scope=scope, max_rows=2,
            )[0]["value"]
        return result

    def checkpoint_summary(self, context: RunContext) -> list[dict[str, Any]]:
        rows = self.kernel.read._repository_query(
            context, "ops.tushare_history_checkpoint",
            """SELECT dataset, status, COUNT(*) AS count FROM ops.tushare_history_checkpoint
               GROUP BY dataset, status ORDER BY dataset, status""",
            scope=WorkScope(partition="quality:checkpoints"), max_rows=100_000,
        )
        return [{"dataset": r["dataset"], "status": r["status"], "count": r["count"]} for r in rows]

    def phase2_checkpoint_completeness(self, context: RunContext) -> dict[str, Any]:
        scope = WorkScope(partition="quality:phase2-checkpoints")
        result: dict[str, Any] = {}
        rows = self.kernel.read._repository_query(
            context, "ops.tushare_history_checkpoint",
            """WITH date_bounds AS (
                SELECT dataset, MIN(to_date(checkpoint_key, 'YYYYMMDD')) AS start_date,
                       MAX(to_date(checkpoint_key, 'YYYYMMDD')) AS end_date
                  FROM ops.tushare_history_checkpoint
                 WHERE dataset IN ('stock_st', 'suspend_d', 'stk_limit') AND checkpoint_key ~ '^[0-9]{8}$'
                 GROUP BY dataset
            ), expected_dates AS (
                SELECT b.dataset, COUNT(c.cal_date) AS expected FROM date_bounds b
                LEFT JOIN tushare_trade_cal_raw c ON c.exchange='SSE'
                 AND (c.payload->>'is_open')::int=1 AND c.cal_date BETWEEN b.start_date AND b.end_date
                GROUP BY b.dataset
            ), actual_dates AS (
                SELECT dataset, COUNT(*) FILTER (WHERE status IN ('done','empty')) AS actual,
                       COUNT(*) FILTER (WHERE status='failed') AS failed
                  FROM ops.tushare_history_checkpoint WHERE dataset IN ('stock_st','suspend_d','stk_limit') GROUP BY dataset
            )
            SELECT e.dataset, e.expected, COALESCE(a.actual,0) AS completed, COALESCE(a.failed,0) AS failed
              FROM expected_dates e LEFT JOIN actual_dates a USING (dataset) ORDER BY e.dataset""",
            scope=scope, max_rows=100,
        )
        for row in rows:
            result[row["dataset"]] = self._completeness(row["expected"], row["completed"], row["failed"])
        rows = self.kernel.read._repository_query(
            context, "ops.tushare_history_checkpoint",
            """SELECT d.dataset, MAX(s.stock_count) AS expected,
                       COUNT(c.checkpoint_key) FILTER (WHERE c.status IN ('done','empty')) AS completed,
                       COUNT(*) FILTER (WHERE c.status='failed') AS failed
                  FROM (VALUES ('namechange'), ('dividend')) AS d(dataset)
                  CROSS JOIN (SELECT COUNT(*) AS stock_count FROM std.stock_basic_info WHERE list_status IN ('L','D','P','G')) s
                  LEFT JOIN ops.tushare_history_checkpoint c ON c.dataset=d.dataset
                 GROUP BY d.dataset, s.stock_count ORDER BY d.dataset""",
            scope=scope, max_rows=10,
        )
        for row in rows:
            result[row["dataset"]] = self._completeness(row["expected"], row["completed"], row["failed"])
        row = self.kernel.read._repository_query(
            context, "ops.tushare_history_checkpoint",
            """SELECT COUNT(*) AS expected,
                       COUNT(*) FILTER (WHERE status IN ('done','empty')) AS completed,
                       COUNT(*) FILTER (WHERE status='failed') AS failed
                  FROM (VALUES ('000300.SH'), ('000905.SH'), ('000852.SH'), ('000016.SH')) AS d(ts_code)
                  LEFT JOIN ops.tushare_history_checkpoint c ON c.dataset='index_daily' AND c.checkpoint_key=d.ts_code""",
            scope=scope, max_rows=2,
        )[0]
        result["index_daily"] = self._completeness(row["expected"], row["completed"], row["failed"])
        return result

    @staticmethod
    def _completeness(expected, completed, failed) -> dict[str, Any]:
        return {"expected": expected, "completed": completed, "missing": max(expected - completed, 0), "failed": failed}

    def phase2_invariants(
        self,
        context: RunContext,
        cutoff=None,
        skip_names: set[str] | frozenset[str] | None = None,
    ) -> dict[str, Any]:
        skipped = skip_names or frozenset()
        result = {}
        for name, (source, sql) in self._INVARIANT_QUERIES.items():
            if name in skipped:
                result[name] = 0
                continue
            try:
                params = (cutoff, cutoff) if name == "financial_feature_missing_revenue_mapping" else ()
                result[name] = self.kernel.read._repository_query(
                    context, source, sql, params=params,
                    scope=WorkScope(partition=f"quality:invariant:{name}"), max_rows=2,
                )[0]["value"]
            except Exception as exc:
                result[name] = {"error": str(exc)}
        return result
