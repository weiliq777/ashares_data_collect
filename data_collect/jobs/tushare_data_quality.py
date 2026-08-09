"""阶段一 Raw/Standard 数据质量检查；只执行聚合 SQL，不把大表加载进内存。"""
from __future__ import annotations

import argparse
import json
from data_collect.utils.db import get_connection


CHECKS = {
    "stock_basic_info": ("stock_code",),
    "tushare_stock_basic_raw": ("ts_code",),
    "trading_calendar": ("exchange", "trade_date", "source"),
    "tushare_daily_raw": ("ts_code", "trade_date"),
    "tushare_daily_basic_raw": ("ts_code", "trade_date"),
    "tushare_adj_factor_raw": ("ts_code", "trade_date"),
    "tushare_trade_cal_raw": ("exchange", "cal_date"),
    "tushare_market_raw_version": ("dataset", "business_key", "payload_hash"),
    "daily_kline": ("stock_code", "trade_date"),
    "valuation_daily": ("stock_code", "trade_date", "source"),
    "adj_factor_daily": ("stock_code", "trade_date", "source"),
    "stock_financial_indicator": ("stock_code", "report_date", "announce_date", "source"),
    "tushare_fina_indicator_raw": ("ts_code", "ann_date", "end_date"),
    "tushare_income_raw": ("ts_code", "ann_date", "end_date"),
    "tushare_balancesheet_raw": ("ts_code", "ann_date", "end_date"),
    "tushare_cashflow_raw": ("ts_code", "ann_date", "end_date"),
    "tushare_financial_raw_version": ("dataset", "business_key", "payload_hash"),
    "fina_indicator_raw_version": ("business_key", "payload_hash"),
    "fina_indicator_standard": ("source", "source_record_key", "record_version", "payload_hash"),
    "financial_income_standard": ("source", "source_record_key", "record_version"),
    "financial_balance_sheet_standard": ("source", "source_record_key", "record_version"),
    "financial_cash_flow_standard": ("source", "source_record_key", "record_version"),
    "tushare_stock_st_raw": ("ts_code", "trade_date", "type", "payload_hash"),
    "tushare_namechange_raw": ("ts_code", "payload_hash"),
    "tushare_suspend_d_raw": ("ts_code", "trade_date", "suspend_type", "payload_hash"),
    "tushare_stk_limit_raw": ("ts_code", "trade_date", "payload_hash"),
    "tushare_dividend_raw": ("ts_code", "payload_hash"),
    "tushare_index_daily_raw": ("ts_code", "trade_date", "payload_hash"),
    "std.security_lifecycle": ("ts_code", "effective_from", "effective_to", "source"),
    "std.security_status_daily": ("trade_date", "ts_code"),
    "std.dividend_event": ("source", "source_record_key", "record_version"),
    "std.index_daily_tushare": ("ts_code", "trade_date", "source"),
    "pit.universe_daily": ("trade_date", "ts_code"),
    "pit.security_tradeability_daily": ("trade_date", "ts_code"),
    "pit.financial_asof": ("decision_date", "ts_code"),
    "feature.price_daily_v1": ("ts_code", "trade_date"),
    "feature.price_adjustment_factor_asof": ("ts_code", "trade_date"),
    "feature.price_daily_v2": ("ts_code", "trade_date"),
    "feature.valuation_daily_v1": ("ts_code", "trade_date"),
    "feature.company_financial_v1": ("ts_code", "report_date", "source_record_key", "record_version"),
    "feature.shareholder_return_v1": ("ts_code", "asof_date"),
    "ops.feature_registry": ("feature_name", "feature_version"),
}
DATE_COLUMNS = {
    "stock_basic_info": "list_date",
    "tushare_stock_basic_raw": None,
    "trading_calendar": "trade_date",
    "tushare_trade_cal_raw": "cal_date",
    "stock_financial_indicator": "report_date",
    "tushare_fina_indicator_raw": "end_date",
    "tushare_income_raw": "end_date",
    "tushare_balancesheet_raw": "end_date",
    "tushare_cashflow_raw": "end_date",
    "tushare_financial_raw_version": "end_date",
    "fina_indicator_raw_version": "end_date",
    "fina_indicator_standard": "report_date",
    "financial_income_standard": "report_date",
    "financial_balance_sheet_standard": "report_date",
    "financial_cash_flow_standard": "report_date",
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
    "pit.universe_daily": "trade_date",
    "pit.security_tradeability_daily": "trade_date",
    "pit.financial_asof": "decision_date",
    "feature.price_daily_v1": "trade_date",
    "feature.price_adjustment_factor_asof": "trade_date",
    "feature.price_daily_v2": "trade_date",
    "feature.valuation_daily_v1": "trade_date",
    "feature.company_financial_v1": "report_date",
    "feature.shareholder_return_v1": "asof_date",
    "ops.feature_registry": None,
}


def _qualified_table(table: str) -> str:
    parts = table.split(".")
    if len(parts) == 1:
        return f'"{parts[0]}"'
    if len(parts) == 2:
        return f'"{parts[0]}"."{parts[1]}"'
    raise ValueError(f"非法表名: {table}")


def inspect_table(table: str, keys: tuple[str, ...]) -> dict:
    with get_connection() as conn, conn.cursor() as cur:
        date_column = DATE_COLUMNS.get(table, "trade_date")
        qualified = _qualified_table(table)
        table_name = table.split(".")[-1]
        if date_column:
            cur.execute(f'SELECT COUNT(*), MIN("{date_column}"), MAX("{date_column}") FROM {qualified}')
            count, date_min, date_max = cur.fetchone()
        else:
            cur.execute(f'SELECT COUNT(*) FROM {qualified}')
            count = cur.fetchone()[0]
            date_min = date_max = None
        key_expr = ", ".join(f'"{key}"' for key in keys)
        cur.execute(f'SELECT COUNT(*) FROM (SELECT {key_expr}, COUNT(*) FROM {qualified} GROUP BY {key_expr} HAVING COUNT(*) > 1) duplicates')
        duplicate_groups = cur.fetchone()[0]
        cur.execute(f'SELECT COUNT(*) FROM {qualified} WHERE payload IS NULL') if table_name.startswith("tushare_") else cur.execute("SELECT 0")
        null_payload = cur.fetchone()[0]
        price_quality = {}
        if table in {"tushare_daily_raw", "daily_kline"}:
            if table_name.startswith("tushare_"):
                cur.execute("""SELECT COUNT(*) FROM "tushare_daily_raw"
                    WHERE COALESCE((payload->>'open')::numeric, 0) = 0
                       OR COALESCE((payload->>'high')::numeric, 0) = 0
                       OR COALESCE((payload->>'low')::numeric, 0) = 0
                       OR COALESCE((payload->>'close')::numeric, 0) < 0""")
            else:
                cur.execute('SELECT COUNT(*) FROM "daily_kline" WHERE open=0 OR high=0 OR low=0 OR close<0')
            price_quality["zero_or_negative_price_rows"] = cur.fetchone()[0]
    result = {"rows": count, "min_date": str(date_min) if date_min else None, "max_date": str(date_max) if date_max else None, "duplicate_key_groups": duplicate_groups, "null_payload": null_payload}
    result.update(price_quality)
    return result


def run() -> dict:
    result = {}
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT dataset, status, COUNT(*) FROM tushare_history_checkpoint GROUP BY dataset, status ORDER BY dataset, status")
        result["checkpoints"] = [{"dataset": row[0], "status": row[1], "count": row[2]} for row in cur.fetchall()]
    for table, keys in CHECKS.items():
        try:
            result[table] = inspect_table(table, keys)
        except Exception as exc:
            result[table] = {"error": str(exc)}
    result["phase2_invariants"] = inspect_phase2_invariants()
    result["phase2_checkpoint_completeness"] = inspect_phase2_checkpoint_completeness()
    return result


def inspect_phase2_checkpoint_completeness() -> dict:
    """检查二阶段六类 Raw 任务是否已覆盖预期 checkpoint；空响应是合法状态。"""
    result = {}
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH date_bounds AS (
                SELECT dataset, MIN(to_date(checkpoint_key, 'YYYYMMDD')) AS start_date,
                       MAX(to_date(checkpoint_key, 'YYYYMMDD')) AS end_date
                FROM tushare_history_checkpoint
                WHERE dataset IN ('stock_st', 'suspend_d', 'stk_limit')
                  AND checkpoint_key ~ '^[0-9]{8}$'
                GROUP BY dataset
            ), expected_dates AS (
                SELECT b.dataset, COUNT(c.cal_date) AS expected
                FROM date_bounds b
                LEFT JOIN tushare_trade_cal_raw c
                  ON c.exchange = 'SSE'
                 AND (c.payload->>'is_open')::int = 1
                 AND c.cal_date BETWEEN b.start_date AND b.end_date
                GROUP BY b.dataset
            ), actual_dates AS (
                SELECT dataset, COUNT(*) FILTER (WHERE status IN ('done','empty')) AS actual,
                       COUNT(*) FILTER (WHERE status = 'failed') AS failed
                FROM tushare_history_checkpoint
                WHERE dataset IN ('stock_st', 'suspend_d', 'stk_limit')
                GROUP BY dataset
            )
            SELECT e.dataset, e.expected, COALESCE(a.actual, 0), COALESCE(a.failed, 0)
            FROM expected_dates e
            LEFT JOIN actual_dates a USING (dataset)
            ORDER BY e.dataset
            """
        )
        for dataset, expected, actual, failed in cur.fetchall():
            result[dataset] = {
                "expected": expected,
                "completed": actual,
                "missing": max(expected - actual, 0),
                "failed": failed,
            }

        cur.execute(
            """
            SELECT d.dataset,
                   MAX(s.stock_count) AS expected,
                   COUNT(c.checkpoint_key) FILTER (WHERE c.status IN ('done','empty')) AS completed,
                   COUNT(*) FILTER (WHERE c.status = 'failed') AS failed
            FROM (VALUES ('namechange'), ('dividend')) AS d(dataset)
            CROSS JOIN (SELECT COUNT(*) AS stock_count
                        FROM stock_basic_info
                        WHERE list_status IN ('L','D','P','G')) s
            LEFT JOIN tushare_history_checkpoint c ON c.dataset = d.dataset
            GROUP BY d.dataset, s.stock_count
            ORDER BY d.dataset
            """
        )
        for dataset, expected, completed, failed in cur.fetchall():
            result[dataset] = {
                "expected": expected,
                "completed": completed,
                "missing": max(expected - completed, 0),
                "failed": failed,
            }

        cur.execute(
            """
            SELECT COUNT(*) AS expected,
                   COUNT(*) FILTER (WHERE status IN ('done','empty')) AS completed,
                   COUNT(*) FILTER (WHERE status = 'failed') AS failed
            FROM (VALUES ('000300.SH'), ('000905.SH'), ('000852.SH'), ('000016.SH')) AS d(ts_code)
            LEFT JOIN tushare_history_checkpoint c
              ON c.dataset = 'index_daily' AND c.checkpoint_key = d.ts_code
            """
        )
        expected, completed, failed = cur.fetchone()
        result["index_daily"] = {
            "expected": expected,
            "completed": completed,
            "missing": max(expected - completed, 0),
            "failed": failed,
        }
    return result


def inspect_phase2_invariants() -> dict:
    """检查阶段二的时间可见性和交易状态不变量；只读聚合查询。"""
    queries = {
        "financial_future_rows": """
            SELECT COUNT(*) FROM pit.financial_asof
            WHERE (effective_announce_date IS NOT NULL
                   AND effective_announce_date > decision_date)
               OR (version_available_date IS NOT NULL
                   AND version_available_date > decision_date)
        """,
        "financial_standard_invalid_version_availability": """
            SELECT SUM(failures) FROM (
              SELECT COUNT(*) FILTER (WHERE version_available_date IS NULL
                    OR version_available_date < effective_announce_date) AS failures
                FROM fina_indicator_standard
              UNION ALL
              SELECT COUNT(*) FILTER (WHERE version_available_date IS NULL
                    OR version_available_date < announce_date)
                FROM financial_income_standard
              UNION ALL
              SELECT COUNT(*) FILTER (WHERE version_available_date IS NULL
                    OR version_available_date < announce_date)
                FROM financial_balance_sheet_standard
              UNION ALL
              SELECT COUNT(*) FILTER (WHERE version_available_date IS NULL
                    OR version_available_date < announce_date)
                FROM financial_cash_flow_standard
            ) x
        """,
        "financial_feature_invalid_version_availability": """
            SELECT COUNT(*) FROM feature.company_financial_v1
            WHERE version_available_date IS NULL
               OR version_available_date < effective_announce_date
        """,
        "financial_vintage_metadata_missing_or_backdated": """
            SELECT SUM(failures) FROM (
              SELECT COUNT(*) FILTER (WHERE vintage_quality IS NULL
                    OR source_observed_at IS NULL
                    OR (vintage_quality='WAREHOUSE_OBSERVED_ONLY'
                        AND version_available_date < source_observed_at::date)) AS failures
                FROM fina_indicator_standard
              UNION ALL
              SELECT COUNT(*) FILTER (WHERE vintage_quality IS NULL
                    OR source_observed_at IS NULL
                    OR (vintage_quality='WAREHOUSE_OBSERVED_ONLY'
                        AND version_available_date < source_observed_at::date))
                FROM financial_income_standard
              UNION ALL
              SELECT COUNT(*) FILTER (WHERE vintage_quality IS NULL
                    OR source_observed_at IS NULL
                    OR (vintage_quality='WAREHOUSE_OBSERVED_ONLY'
                        AND version_available_date < source_observed_at::date))
                FROM financial_balance_sheet_standard
              UNION ALL
              SELECT COUNT(*) FILTER (WHERE vintage_quality IS NULL
                    OR source_observed_at IS NULL
                    OR (vintage_quality='WAREHOUSE_OBSERVED_ONLY'
                        AND version_available_date < source_observed_at::date))
                FROM financial_cash_flow_standard
            ) x
        """,
        "financial_pit_vintage_future_rows": """
            SELECT COUNT(*) FROM pit.financial_asof
             WHERE vintage_quality IS NULL OR source_observed_at IS NULL
                OR (vintage_quality='WAREHOUSE_OBSERVED_ONLY'
                    AND source_observed_at::date > decision_date)
        """,
        "tradeability_null_buy_sell": """
            SELECT COUNT(*) FROM pit.security_tradeability_daily
            WHERE is_tradeable IS TRUE AND (can_buy IS NULL OR can_sell IS NULL)
        """,
        "nonpositive_price_tradeable_rows": """
            SELECT COUNT(*) FROM std.security_status_daily
            WHERE is_tradeable IS TRUE AND (close_price IS NULL OR close_price <= 0)
        """,
        "missing_adj_factor_feature_rows": """
            SELECT COUNT(*) FROM feature.price_daily_v2
            WHERE quality_status = 'MISSING_SOURCE'
        """,
        "raw_phase2_failed_checkpoints": """
            SELECT COUNT(*) FROM tushare_history_checkpoint
            WHERE dataset IN ('stock_st','suspend_d','stk_limit','namechange','dividend','index_daily')
              AND status = 'failed'
        """,
        "fina_indicator_standard_missing_or_yoy": """
            /*
             * The immutable version table may contain the same payload under
             * legacy business-key spellings.  Standard deliberately keeps one
             * row per payload_hash, so lineage completeness must be checked at
             * the payload level rather than treating duplicate source keys as
             * missing facts.
             */
            SELECT COUNT(*) FROM (
              SELECT DISTINCT r.payload_hash
                FROM fina_indicator_raw_version r
               WHERE r.payload ? 'or_yoy'
                 AND NOT EXISTS (
                     SELECT 1 FROM fina_indicator_standard s
                      WHERE s.payload_hash = r.payload_hash
                 )
            ) missing
        """,
        "fina_indicator_standard_missing_lineage": """
            SELECT COUNT(*) FROM fina_indicator_standard
            WHERE source_record_key IS NULL OR payload_hash IS NULL OR record_version IS NULL
        """,
        "financial_pit_missing_lineage": """
            SELECT COUNT(*) FROM pit.financial_asof
            WHERE effective_announce_date IS NOT NULL
              AND (source_record_key IS NULL OR payload_hash IS NULL OR record_version IS NULL)
        """,
        "financial_feature_missing_revenue_mapping": """
            SELECT COUNT(*) FROM fina_indicator_standard s
            WHERE s.or_yoy IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM feature.company_financial_v1 f
                  WHERE f.ts_code=s.ts_code AND f.report_date=s.report_date
                    AND f.version_available_date=s.version_available_date
                    AND f.revenue_yoy = s.or_yoy
              )
        """,
        "immature_price_features_exposed": """
            SELECT COUNT(*) FROM feature.price_daily_v2
             WHERE (ma20 IS NOT NULL AND COALESCE(ma20_observation_count,0)<20)
                OR (ma60 IS NOT NULL AND COALESCE(ma60_observation_count,0)<60)
                OR (ma120 IS NOT NULL AND COALESCE(ma120_observation_count,0)<120)
        """,
        "immature_valuation_percentiles_exposed": """
            SELECT COUNT(*) FROM feature.valuation_daily_v1
             WHERE pe_pct_5y IS NOT NULL AND (
                   NOT COALESCE(has_full_pe_5y_history,FALSE)
                   OR COALESCE(pe_observation_count_5y,0)<1000
                   OR pe_window_start_date_5y IS NULL
                   OR trade_date-pe_window_start_date_5y<1794)
        """,
    }
    result = {}
    with get_connection() as conn, conn.cursor() as cur:
        for name, sql in queries.items():
            try:
                cur.execute(sql)
                result[name] = cur.fetchone()[0]
            except Exception as exc:
                conn.rollback()
                result[name] = {"error": str(exc)}
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="检查阶段一 Tushare Raw/Standard 数据")
    parser.parse_args()
    print(json.dumps(run(), ensure_ascii=False, indent=2))
