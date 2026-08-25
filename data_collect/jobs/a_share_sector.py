"""Synchronize current sector membership snapshots."""
from __future__ import annotations

import logging
import sys
from typing import Dict, List, Set

import pandas as pd
from tqdm import tqdm

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.security_repository import SecurityRepository
from data_collect.utils.date_utils import is_market_day
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.notify import send_dingtalk
from data_collect.utils.xtquant_utils import require_xtdata

logger = logging.getLogger(__name__)
_SECTOR_PREFIXES = ("GICS1", "GICS2", "GICS3", "GICS4", "THY1", "THY2", "THY3", "TDGN", "TGN", "TFG")


def _fetch_all_sector_stocks(sectors: List[str]) -> Dict[str, Set[str]]:
    xtdata = require_xtdata()
    result: Dict[str, Set[str]] = {}
    failures = 0
    for sector in tqdm(sectors, desc="sector members", unit="sector", file=sys.stdout):
        try:
            codes = xtdata.get_stock_list_in_sector(sector)
            if codes:
                result[sector] = set(codes)
        except Exception:
            failures += 1
    if failures:
        logger.warning("sector collection failures=%s", failures)
    return result


def _filter_sector_names(sectors: List[str]) -> list[str]:
    """Keep only supported industry/sector namespaces before collection."""
    return [name for name in sectors if name.startswith(_SECTOR_PREFIXES)]


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} sector sync skipped"
    xtdata = require_xtdata()
    sectors = _filter_sector_names(xtdata.get_sector_list())
    data = _fetch_all_sector_stocks(sectors)
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"sector-{trade_date}", job_name="a_share_sector", code_version="unknown",
        config_hash="unknown", pipeline_version="v1",
    )
    try:
        added, removed, changes = SecurityRepository(kernel).sync_sector_members(
            data, pd.to_datetime(trade_date).date(), context
        )
    finally:
        kernel.close()
    return f"{trade_date} sectors={len(sectors)} added={added} removed={removed} changes={changes}"


def run_backfill(start_date: str, end_date: str, limit_stocks: int | None = None) -> str:
    result = run(end_date)
    send_dingtalk(result)
    return result
