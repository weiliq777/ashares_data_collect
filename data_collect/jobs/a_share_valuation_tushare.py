from __future__ import annotations

from datetime import datetime
from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.normalize.tushare_daily import normalize_daily_basic
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.collector_repository import CollectorRepository
from data_collect.utils.date_utils import is_market_day
from data_collect.utils.df_utils import normalize_trade_date


def _save(df):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(run_id="a-share-valuation", job_name="a_share_valuation_tushare", code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1")
    try:
        return CollectorRepository(kernel).append("valuation_daily", df.to_dict("records"), context, partition="valuation-daily")
    finally:
        kernel.close()


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，估值跳过"
    df = normalize_daily_basic(call_api(get_pro().daily_basic, trade_date=trade_date))
    if df.empty:
        return f"{trade_date} Tushare daily_basic无数据"
    keep = ["stock_code", "trade_date", "turnover_rate", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "total_mv", "circ_mv"]
    out = df[[c for c in keep if c in df.columns]].copy()
    out["source"] = "tushare"
    out["fetched_at"] = datetime.now()
    tried, inserted = _save(out)
    return f"Tushare估值完成：{inserted}/{tried}，日期={trade_date}"
