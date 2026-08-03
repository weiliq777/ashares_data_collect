"""从 Tushare Raw 表重建当前阶段一的 Standard 表。

Raw 是事实层；本任务不访问 Tushare，只读取 PostgreSQL，因此可重复执行。
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime

import pandas as pd

from data_collect.normalize.tushare_daily import normalize_daily, normalize_daily_basic
from data_collect.utils.db import get_connection, save_to_postgres


def _payload_frames(table: str, chunk_size: int = 5000):
    """使用服务端游标分块读取 JSON，避免把千万级 Raw 表整体载入内存。"""
    with get_connection() as conn:
        with conn.cursor(name=f"raw_to_standard_{table}") as cur:
            cur.itersize = chunk_size
            cur.execute(f'SELECT payload FROM "{table}" ORDER BY 1')
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                yield pd.json_normalize([value if isinstance(value, dict) else json.loads(value) for (value,) in rows])


def _write_chunks(table: str, transform, chunk_size: int = 5000) -> tuple[int, int]:
    attempted = affected = 0
    for raw in _payload_frames(table, chunk_size):
        frame = transform(raw)
        if frame is None or frame.empty:
            continue
        current, inserted = save_to_postgres(frame, table_name=transform.table_name)
        attempted += current
        affected += inserted
    return attempted, affected


def rebuild_daily() -> tuple[int, int]:
    def transform(frame):
        return normalize_daily(frame)
    transform.table_name = "daily_kline"
    return _write_chunks("tushare_daily_raw", transform)


def rebuild_valuation() -> tuple[int, int]:
    def transform(frame):
        frame = normalize_daily_basic(frame)
        if frame.empty:
            return frame
        keep = ["stock_code", "trade_date", "turnover_rate", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "total_mv", "circ_mv"]
        frame = frame[[column for column in keep if column in frame.columns]].copy()
        frame["source"] = "tushare"
        frame["fetched_at"] = datetime.now()
        return frame
    transform.table_name = "valuation_daily"
    return _write_chunks("tushare_daily_basic_raw", transform)


def rebuild_adj_factor() -> tuple[int, int]:
    def transform(frame):
        frame = frame.rename(columns={"ts_code": "stock_code"})
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], format="%Y%m%d", errors="coerce").dt.date
        frame["adj_factor"] = pd.to_numeric(frame["adj_factor"], errors="coerce")
        frame = frame[["stock_code", "trade_date", "adj_factor"]].dropna(subset=["stock_code", "trade_date"])
        frame["source"] = "tushare"
        frame["fetched_at"] = datetime.now()
        return frame
    transform.table_name = "adj_factor_daily"
    return _write_chunks("tushare_adj_factor_raw", transform)


def rebuild_calendar() -> tuple[int, int]:
    def transform(frame):
        frame["trade_date"] = pd.to_datetime(frame["cal_date"], format="%Y%m%d", errors="coerce").dt.date
        pretrade = frame["pretrade_date"] if "pretrade_date" in frame else pd.Series(pd.NaT, index=frame.index)
        frame["pretrade_date"] = pd.to_datetime(pretrade, format="%Y%m%d", errors="coerce").dt.date
        frame["is_open"] = pd.to_numeric(frame["is_open"], errors="coerce").fillna(0).astype(bool)
        frame["source"] = "tushare"
        frame["fetched_at"] = datetime.now()
        return frame[["exchange", "trade_date", "is_open", "pretrade_date", "source", "fetched_at"]]
    transform.table_name = "trading_calendar"
    return _write_chunks("tushare_trade_cal_raw", transform)


def rebuild_financial() -> tuple[int, int]:
    def transform(frame):
        frame = frame.rename(columns={"ts_code": "stock_code", "end_date": "report_date", "ann_date": "announce_date"})
        for column in ("report_date", "announce_date"):
            frame[column] = pd.to_datetime(frame[column], format="%Y%m%d", errors="coerce").dt.date
        keep = ["stock_code", "report_date", "announce_date", "roe", "roa", "debt_to_assets", "grossprofit_margin", "netprofit_margin", "ocf_to_or", "revenue_yoy", "netprofit_yoy", "eps", "bps"]
        frame = frame[[column for column in keep if column in frame.columns]]
        frame["source"] = "tushare"
        frame["fetched_at"] = datetime.now()
        return frame
    transform.table_name = "stock_financial_indicator"
    return _write_chunks("tushare_fina_indicator_raw", transform)


def rebuild_all() -> dict[str, tuple[int, int]]:
    return {"calendar": rebuild_calendar(), "daily": rebuild_daily(), "valuation": rebuild_valuation(), "adj_factor": rebuild_adj_factor(), "financial": rebuild_financial()}


def main() -> None:
    parser = argparse.ArgumentParser(description="从 Tushare Raw 表重建 Standard 表")
    parser.add_argument("--table", choices=["calendar", "daily", "valuation", "adj_factor", "financial", "all"], default="all")
    args = parser.parse_args()
    result = rebuild_all() if args.table == "all" else {args.table: globals()[f"rebuild_{args.table}"]()}
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
