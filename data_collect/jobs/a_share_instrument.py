"""Collect stable A-share instrument metadata and change records."""
from __future__ import annotations

import logging
import sys

import pandas as pd
from tqdm import tqdm

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.security_repository import SecurityRepository
from data_collect.utils.date_utils import is_market_day
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.notify import send_dingtalk
from data_collect.utils.xtquant_utils import get_a_share_codes, require_xtdata

logger = logging.getLogger(__name__)
_DAILY_CHANGING_FIELDS = {"PreClose", "UpStopPrice", "DownStopPrice", "SettlementPrice", "TradingDay", "IsTrading", "InstrumentStatus", "LastVolume"}


def _fetch_instrument(stock_code: str) -> dict | None:
    detail = require_xtdata().get_instrument_detail(stock_code, iscomplete=True)
    return {key: value for key, value in detail.items() if key not in _DAILY_CHANGING_FIELDS} if detail else None


def _collect_all(limit_stocks: int | None = None) -> dict[str, dict]:
    codes = get_a_share_codes()
    if limit_stocks and limit_stocks > 0:
        codes = codes[:limit_stocks]
    details, failures = {}, 0
    for code in tqdm(codes, desc="instrument metadata", unit="stock", file=sys.stdout):
        try:
            value = _fetch_instrument(code)
            if value:
                details[code] = value
        except Exception:
            failures += 1
    if failures:
        logger.warning("instrument collection failures=%s", failures)
    return details


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} instrument sync skipped"
    details = _collect_all(kwargs.get("limit_stocks"))
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"instrument-{trade_date}", job_name="a_share_instrument", code_version="unknown",
        config_hash="unknown", pipeline_version="v1",
    )
    try:
        new, updated, changes = SecurityRepository(kernel).sync_instruments(
            details, pd.to_datetime(trade_date).date(), context
        )
    finally:
        kernel.close()
    return f"{trade_date} instruments new={new} updated={updated} changes={changes}"


def run_backfill(start_date: str, end_date: str, limit_stocks: int | None = None) -> str:
    result = run(end_date, limit_stocks=limit_stocks)
    send_dingtalk(result)
    return result
