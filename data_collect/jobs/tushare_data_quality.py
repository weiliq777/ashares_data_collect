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
    "financial_income_standard": ("source", "source_record_key", "record_version"),
    "financial_balance_sheet_standard": ("source", "source_record_key", "record_version"),
    "financial_cash_flow_standard": ("source", "source_record_key", "record_version"),
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
    "financial_income_standard": "report_date",
    "financial_balance_sheet_standard": "report_date",
    "financial_cash_flow_standard": "report_date",
}


def inspect_table(table: str, keys: tuple[str, ...]) -> dict:
    with get_connection() as conn, conn.cursor() as cur:
        date_column = DATE_COLUMNS.get(table, "trade_date")
        if date_column:
            cur.execute(f'SELECT COUNT(*), MIN("{date_column}"), MAX("{date_column}") FROM "{table}"')
            count, date_min, date_max = cur.fetchone()
        else:
            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            count = cur.fetchone()[0]
            date_min = date_max = None
        key_expr = ", ".join(f'"{key}"' for key in keys)
        cur.execute(f'SELECT COUNT(*) FROM (SELECT {key_expr}, COUNT(*) FROM "{table}" GROUP BY {key_expr} HAVING COUNT(*) > 1) duplicates')
        duplicate_groups = cur.fetchone()[0]
        cur.execute(f'SELECT COUNT(*) FROM "{table}" WHERE payload IS NULL') if table.startswith("tushare_") else cur.execute("SELECT 0")
        null_payload = cur.fetchone()[0]
        price_quality = {}
        if table in {"tushare_daily_raw", "daily_kline"}:
            prefix = "payload->>" if table.startswith("tushare_") else '"'
            if table.startswith("tushare_"):
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
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="检查阶段一 Tushare Raw/Standard 数据")
    parser.parse_args()
    print(json.dumps(run(), ensure_ascii=False, indent=2))
