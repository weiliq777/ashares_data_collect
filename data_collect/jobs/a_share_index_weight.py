"""Synchronize current index constituent weights."""
from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.security_repository import SecurityRepository
from data_collect.utils.date_utils import is_market_day
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.notify import send_dingtalk
from data_collect.utils.xtquant_utils import require_xtdata

logger = logging.getLogger(__name__)
INDEX_CODES = ["000300.SH", "000905.SH", "000852.SH", "000016.SH", "399006.SZ"]


def _fetch_all_weights() -> Dict[str, Dict[str, float]]:
    xtdata = require_xtdata()
    try:
        xtdata.download_index_weight()
    except Exception as exc:
        logger.warning("index weight download failed: %s", exc)
    result = {}
    for index_code in INDEX_CODES:
        try:
            weights = xtdata.get_index_weight(index_code)
            if isinstance(weights, dict) and weights:
                result[index_code] = weights
        except Exception:
            logger.warning("index weight read failed: %s", index_code)
    return result


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} index weight sync skipped"
    data = _fetch_all_weights()
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"index-weight-{trade_date}", job_name="a_share_index_weight", code_version="unknown",
        config_hash="unknown", pipeline_version="v1",
    )
    try:
        added, removed, changes = SecurityRepository(kernel).sync_index_weights(
            data, pd.to_datetime(trade_date).date(), context
        )
    finally:
        kernel.close()
    total = sum(len(value) for value in data.values())
    return f"{trade_date} indexes={len(data)} members={total} added={added} removed={removed} changes={changes}"


def run_backfill(start_date: str, end_date: str, limit_stocks: int | None = None) -> str:
    result = run(end_date)
    send_dingtalk(result)
    return result
