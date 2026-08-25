"""Collect ETF spot and official net-asset-value data."""
from __future__ import annotations

import logging
import sys
from typing import List

import pandas as pd
from tqdm import tqdm

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_repository import MarketRepository
from data_collect.utils.akshare_utils import (
    build_nav_official_df,
    build_nav_spot_df,
    get_etf_nav_hist,
    get_etf_spot,
)
from data_collect.utils.date_utils import is_market_day
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.etf_utils import to_bare_code
from data_collect.utils.notify import send_dingtalk
from data_collect.utils.xtquant_utils import get_etf_codes

logger = logging.getLogger(__name__)
TABLE_NAME = "etf_nav"


def _upsert(df: pd.DataFrame, value_cols: List[str]) -> tuple[int, int]:
    if df.empty:
        return 0, 0
    cols = ["code", "trade_date"] + value_cols
    frame = df[cols].where(pd.notna(df[cols]), None)
    rows = frame.to_dict("records")
    first_date = pd.to_datetime(frame["trade_date"]).min().date()
    last_date = pd.to_datetime(frame["trade_date"]).max().date()
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"etf-nav-{first_date}-{last_date}",
        job_name="etf_nav",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="v1",
    )
    try:
        affected = MarketRepository(kernel).upsert_etf_nav(
            rows, context, first_date, last_date, tuple(value_cols)
        )
    finally:
        kernel.close()
    return len(rows), affected


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} ETF NAV spot skipped"
    frame = build_nav_spot_df(get_etf_spot(), pd.to_datetime(trade_date).date())
    tried, affected = _upsert(frame, ["close", "iopv", "discount_rate"])
    return f"{trade_date} ETF NAV spot complete: {tried}, upsert {affected}"


def _official_backfill(start: str, end: str, limit_stocks=None) -> tuple[int, int]:
    codes = get_etf_codes()
    if limit_stocks:
        codes = codes[:limit_stocks]
    start_key, end_key = start.replace("-", ""), end.replace("-", "")
    total_tried = total_affected = 0
    for code in tqdm(codes, desc="ETF official NAV", unit="code", file=sys.stdout):
        bare = to_bare_code(code)
        try:
            frame = build_nav_official_df(
                get_etf_nav_hist(bare, start_key, end_key), bare
            )
            tried, affected = _upsert(frame, ["unit_nav", "accum_nav", "daily_growth", "nav_date"])
            total_tried += tried
            total_affected += affected
        except Exception as exc:
            logger.debug("ETF official NAV failed for %s: %s", bare, exc)
    return total_tried, total_affected


def run_backfill(start_date: str, end_date: str, limit_stocks=None) -> str:
    tried, affected = _official_backfill(start_date, end_date, limit_stocks)
    message = f"ETF official NAV backfill complete ({start_date}~{end_date}): {tried}, upsert {affected}"
    send_dingtalk(message)
    return message


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    tried, affected = _official_backfill(start_date, end_date, kwargs.get("limit_stocks"))
    return f"ETF official NAV repair complete ({start_date}~{end_date}): {tried}, upsert {affected}"
