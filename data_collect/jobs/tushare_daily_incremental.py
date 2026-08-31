"""Shared market-version helpers backed by the Data Access Kernel.

The former production writer in this module is intentionally fail-closed. New
market ingestion must use the typed jobs and repositories below.
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime, time as datetime_time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.history_repository import HistoryRepository
from data_collect.data_access.repository.market_repository import MarketRepository
from data_collect.jobs.tushare_job_lock import warehouse_write_job
from data_collect.normalize.tushare_daily import normalize_daily, normalize_daily_basic
from data_collect.providers.tushare_client import call_api


def _service(token: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(token, "tushare-daily-incremental", "unknown", "unknown", "v1")
    return kernel, HistoryRepository(kernel), MarketRepository(kernel), context


def _date(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    parsed = pd.to_datetime(value, format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _version_and_snapshot(dataset, frame, batch_id, page_size=500):
    del page_size  # chunking is owned by Raw/Write Gateway
    if frame is None or frame.empty:
        return 0, 0
    kernel, _history, market, context = _service(f"market-version-{batch_id}")
    try:
        rows = frame.astype(object).where(pd.notna(frame), None).to_dict("records")
        return market.append_market_versions(dataset, rows, batch_id, context)
    finally:
        kernel.close()


def _checkpoint(dataset, key, status="done", rows=0, error=None):
    kernel, history, _market, context = _service(f"market-checkpoint-{dataset}-{key}")
    try:
        history.save(dataset, key, status, rows, context, error)
    finally:
        kernel.close()


def _done(dataset, key):
    kernel, history, _market, context = _service(f"market-done-{dataset}-{key}")
    try:
        return history.status(dataset, key, context) == "done"
    finally:
        kernel.close()


def _trade_dates(pro, end, lookback):
    start = (pd.to_datetime(end) - timedelta(days=max(lookback * 3, 10))).strftime("%Y%m%d")
    frame = call_api(pro.trade_cal, exchange="SSE", start_date=start, end_date=end, is_open="1")
    if frame is None or frame.empty:
        return frame
    return frame.sort_values("cal_date").tail(lookback)


def _codes():
    kernel, history, _market, context = _service("market-codes")
    try:
        return history.active_stock_codes(context)
    finally:
        kernel.close()


def _rebuild_market_date(frame, dataset, trade_date):
    kernel, _history, market, context = _service(f"market-rebuild-{dataset}-{trade_date}")
    try:
        if dataset == "daily":
            out = normalize_daily(frame)
            if out.empty:
                raise RuntimeError(f"daily {trade_date} normalized to empty; preserve existing Standard")
            return market.replace_daily_partition("daily_kline", trade_date, out.to_dict("records"), context)
        if dataset == "daily_basic":
            out = normalize_daily_basic(frame)
            if out.empty:
                raise RuntimeError(f"daily_basic {trade_date} normalized to empty; preserve existing Standard")
            keep = [
                "stock_code", "trade_date", "turnover_rate", "turnover_rate_f", "free_share",
                "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm", "total_mv", "circ_mv",
            ]
            out = out[[column for column in keep if column in out.columns]].copy()
            if "free_share" in out:
                out["free_share"] = pd.to_numeric(out["free_share"], errors="coerce") * 10000.0
            out["free_share_unit"] = "shares"
            out["source_asof_date"] = out["trade_date"]
            out["quality_status"] = out.apply(
                lambda row: "VALID" if (
                    pd.notna(row.get("turnover_rate_f")) and pd.notna(row.get("free_share"))
                    and float(row.get("turnover_rate_f")) >= 0 and float(row.get("free_share")) >= 0
                ) else "WARNING",
                axis=1,
            )
            out["pe_ttm_vendor"] = out.get("pe_ttm")
            out["ps_ttm_vendor"] = out.get("ps_ttm")
            out["pb_vendor"] = out.get("pb")
            out["dv_ttm_vendor"] = out.get("dv_ttm")
            out["dv_ttm_source"] = "TUSHARE_DAILY_BASIC"
            observed_at = pd.Timestamp.now(tz="UTC")
            out["source_observed_at"] = observed_at
            out["data_available_timestamp"] = pd.Timestamp(datetime.combine(
                trade_date, datetime_time(17, 0), tzinfo=ZoneInfo("Asia/Shanghai")
            ))
            out["source"] = "tushare"
            out["fetched_at"] = observed_at.tz_localize(None)
            return market.replace_daily_partition("valuation_daily", trade_date, out.to_dict("records"), context)
        out = frame.rename(columns={"ts_code": "stock_code"}).copy()
        out["trade_date"] = out["trade_date"].map(_date)
        out["adj_factor"] = pd.to_numeric(out["adj_factor"], errors="coerce")
        out = out[["stock_code", "trade_date", "adj_factor"]].dropna(subset=["stock_code", "trade_date"])
        out["source"], out["fetched_at"] = "tushare", pd.Timestamp.now()
        return market.replace_daily_partition("adj_factor_daily", trade_date, out.to_dict("records"), context)
    finally:
        kernel.close()


def _assert_standard_count(dataset, trade_date, standard_table):
    kernel, _history, market, context = _service(f"market-count-{dataset}-{trade_date}")
    try:
        expected = len(market.latest_market_versions(dataset, trade_date, context))
        actual = market.standard_count(standard_table, trade_date, context)
        if expected != actual:
            raise RuntimeError(f"{dataset} latest Raw versions={expected}, Standard={actual}")
        return actual
    finally:
        kernel.close()


@warehouse_write_job
def run(*args, limit_stocks=None, dry_run=False, **kwargs):
    if limit_stocks and not dry_run:
        raise ValueError("隔离 smoke 表不能替代生产数据")
    if not dry_run:
        raise RuntimeError("已废弃生产写入")
    return "dry-run only: legacy market incremental writer is retired"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tushare market incremental compatibility check")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(run(dry_run=args.dry_run))
