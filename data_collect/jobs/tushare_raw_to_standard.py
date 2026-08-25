"""Project immutable Tushare Raw facts into bounded Standard upserts.

Database reads, writes, conflict handling, and transaction boundaries belong to
``RawStandardRepository``. This module contains only API-specific field
normalization and job orchestration.
"""
from __future__ import annotations

import argparse
import json
import time as time_module
from datetime import datetime, time
from zoneinfo import ZoneInfo

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.raw_standard_repository import RawStandardRepository
from data_collect.normalize.tushare_daily import normalize_daily, normalize_daily_basic


def _service(token: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(token, "tushare-raw-to-standard", "unknown", "unknown", "v1")
    return kernel, RawStandardRepository(kernel), context


def _payload_frames(
    table: str, chunk_size: int = 5000, *, version_dataset: str | None = None,
    key_kind: str = "security_date", stock_codes: list[str] | None = None,
    start_date=None, end_date=None,
):
    kernel, repository, context = _service(f"raw-standard-read-{table}")
    try:
        for rows in repository.iter_payload_rows(
            table, context, chunk_size, version_dataset=version_dataset,
            key_kind=key_kind, stock_codes=stock_codes,
            start_date=start_date, end_date=end_date,
        ):
            yield pd.json_normalize(rows)
    finally:
        kernel.close()


def _upsert_frame(frame: pd.DataFrame, target: str, conflict_columns: tuple[str, ...], page_size: int = 5000) -> int:
    if frame.empty:
        return 0
    clean = frame.astype(object).where(pd.notna(frame), None)
    rows = clean.to_dict("records")
    kernel, repository, context = _service(f"raw-standard-write-{target}")
    try:
        total = 0
        for offset in range(0, len(rows), page_size):
            total += repository.upsert_standard_rows(
                target, rows[offset:offset + page_size], conflict_columns, context
            )
        return total
    finally:
        kernel.close()


def _write_chunks(
    table: str, transform, chunk_size: int = 5000, *,
    version_dataset: str | None = None, key_kind: str = "security_date",
    conflict_columns: tuple[str, ...] | None = None, stock_codes: list[str] | None = None,
    start_date=None, end_date=None,
) -> tuple[int, int]:
    if not conflict_columns:
        raise ValueError(f"{transform.table_name} missing Standard conflict key")
    attempted = affected = 0
    for frame in _payload_frames(
        table, chunk_size, version_dataset=version_dataset, key_kind=key_kind,
        stock_codes=stock_codes, start_date=start_date, end_date=end_date,
    ):
        normalized = transform(frame)
        if normalized is None or normalized.empty:
            continue
        attempted += len(normalized)
        affected += _upsert_frame(normalized, transform.table_name, conflict_columns)
    return attempted, affected


def rebuild_daily() -> tuple[int, int]:
    def transform(frame):
        return normalize_daily(frame)
    transform.table_name = "std.daily_kline"
    return _write_chunks("tushare_daily_raw", transform, version_dataset="daily", conflict_columns=("stock_code", "trade_date"))


def _valuation_transform(frame: pd.DataFrame) -> pd.DataFrame:
    frame = normalize_daily_basic(frame)
    if frame.empty:
        return frame
    for column in ("turnover_rate", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm", "total_mv", "circ_mv"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["pe_ttm_vendor"] = frame.get("pe_ttm")
    frame["ps_ttm_vendor"] = frame.get("ps_ttm")
    frame["pb_vendor"] = frame.get("pb")
    frame["dv_ttm_vendor"] = frame.get("dv_ttm")
    frame["dv_ttm_source"] = "TUSHARE_DAILY_BASIC"
    frame["source_observed_at"] = pd.to_datetime(frame.get("_source_observed_at"), errors="coerce")
    shanghai = ZoneInfo("Asia/Shanghai")
    frame["data_available_timestamp"] = frame["trade_date"].map(
        lambda value: datetime.combine(value, time(17, 0), tzinfo=shanghai) if pd.notna(value) else None
    )
    keep = [
        "stock_code", "trade_date", "turnover_rate", "pe", "pe_ttm", "pb", "ps", "ps_ttm",
        "dv_ratio", "total_mv", "circ_mv", "pe_ttm_vendor", "ps_ttm_vendor", "pb_vendor",
        "dv_ttm", "dv_ttm_vendor", "dv_ttm_source", "source_observed_at", "data_available_timestamp",
    ]
    frame = frame[[column for column in keep if column in frame.columns]].copy()
    frame["source"] = "tushare"
    frame["fetched_at"] = frame["source_observed_at"].fillna(datetime.now())
    return frame


def rebuild_valuation(stock_codes=None, start_date=None, end_date=None, *, set_batch_trade_dates=1, throttle_seconds=0.1) -> tuple[int, int]:
    if stock_codes is None and start_date is not None and end_date is not None:
        return _rebuild_valuation_set_based(start_date, end_date, set_batch_trade_dates, throttle_seconds)
    return _write_chunks(
        "tushare_daily_basic_raw", _valuation_transform, version_dataset="daily_basic",
        conflict_columns=("stock_code", "trade_date", "source"),
        stock_codes=stock_codes, start_date=start_date, end_date=end_date,
    )


_valuation_transform.table_name = "std.valuation_daily"


def _rebuild_valuation_set_based(start_date, end_date, batch_trade_dates=1, throttle_seconds=0.1) -> tuple[int, int]:
    if batch_trade_dates <= 0:
        raise ValueError("batch_trade_dates must be positive")
    if throttle_seconds < 0:
        raise ValueError("throttle_seconds must not be negative")
    kernel, repository, context = _service("raw-standard-valuation-set-based")
    try:
        return repository.rebuild_valuation_set_based(
            start_date, end_date, context,
            batch_trade_dates=batch_trade_dates,
            throttle_seconds=throttle_seconds,
        )
    finally:
        kernel.close()


def rebuild_adj_factor() -> tuple[int, int]:
    def transform(frame):
        frame = frame.rename(columns={"ts_code": "stock_code"})
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], format="%Y%m%d", errors="coerce").dt.date
        frame["adj_factor"] = pd.to_numeric(frame["adj_factor"], errors="coerce")
        frame = frame[["stock_code", "trade_date", "adj_factor"]].dropna(subset=["stock_code", "trade_date"])
        frame["source"], frame["fetched_at"] = "tushare", datetime.now()
        return frame
    transform.table_name = "std.adj_factor_daily"
    return _write_chunks("tushare_adj_factor_raw", transform, version_dataset="adj_factor", conflict_columns=("stock_code", "trade_date", "source"))


def rebuild_calendar(start_date=None, end_date=None) -> tuple[int, int]:
    def transform(frame):
        frame["trade_date"] = pd.to_datetime(frame["cal_date"], format="%Y%m%d", errors="coerce").dt.date
        pretrade = frame["pretrade_date"] if "pretrade_date" in frame else pd.Series(pd.NaT, index=frame.index)
        frame["pretrade_date"] = pd.to_datetime(pretrade, format="%Y%m%d", errors="coerce").dt.date
        frame["is_open"] = pd.to_numeric(frame["is_open"], errors="coerce").fillna(0).astype(bool)
        frame["source"], frame["fetched_at"] = "tushare", datetime.now()
        return frame[["exchange", "trade_date", "is_open", "pretrade_date", "source", "fetched_at"]]
    transform.table_name = "std.trading_calendar"
    return _write_chunks(
        "tushare_trade_cal_raw", transform, version_dataset="trade_cal",
        key_kind="calendar", conflict_columns=("exchange", "trade_date", "source"),
        start_date=start_date, end_date=end_date,
    )


def rebuild_financial() -> tuple[int, int]:
    def transform(frame):
        frame = frame.rename(columns={"ts_code": "stock_code", "end_date": "report_date", "ann_date": "announce_date"})
        for column in ("report_date", "announce_date"):
            frame[column] = pd.to_datetime(frame[column], format="%Y%m%d", errors="coerce").dt.date
        keep = ["stock_code", "report_date", "announce_date", "roe", "roa", "debt_to_assets", "grossprofit_margin", "netprofit_margin", "ocf_to_or", "revenue_yoy", "netprofit_yoy", "eps", "bps"]
        frame = frame[[column for column in keep if column in frame.columns]]
        frame["source"], frame["fetched_at"] = "tushare", datetime.now()
        return frame
    transform.table_name = "std.stock_financial_indicator"
    return _write_chunks("tushare_fina_indicator_raw", transform, conflict_columns=("stock_code", "report_date", "announce_date", "source"))


def rebuild_all() -> dict[str, tuple[int, int]]:
    return {"calendar": rebuild_calendar(), "daily": rebuild_daily(), "valuation": rebuild_valuation(), "adj_factor": rebuild_adj_factor(), "financial": rebuild_financial()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Project Tushare Raw into Standard")
    parser.add_argument("--table", choices=["calendar", "daily", "valuation", "adj_factor", "financial", "all"], default="all")
    args = parser.parse_args()
    result = rebuild_all() if args.table == "all" else {args.table: globals()[f"rebuild_{args.table}"]()}
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
