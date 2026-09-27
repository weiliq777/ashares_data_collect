from __future__ import annotations

from datetime import datetime
import logging
import uuid
from zoneinfo import ZoneInfo
import pandas as pd

from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.industry_membership_repository import (
    IndustryMembershipRepository,
)
from data_collect.data_access.repository.instrument_repository import InstrumentRepository


logger = logging.getLogger(__name__)
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def run(run_date: str | None = None, dry_run: bool = False, **kwargs) -> str:
    pro = get_pro()
    df = call_api(
        pro.stock_basic,
        exchange="",
        list_status="",
        fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,list_status",
    )
    if df is None or df.empty:
        raise RuntimeError("Tushare stock_basic 返回空响应，拒绝将缺失采集报告为成功")
    raw_rows = df.astype(object).where(pd.notna(df), None).to_dict("records")
    if dry_run:
        return f"dry-run stock_basic rows={len(raw_rows)}"
    fetched_at = datetime.now(_SHANGHAI)
    capture_day = run_date or fetched_at.strftime("%Y%m%d")
    batch_id = f"stock-basic-{capture_day}-{uuid.uuid4().hex}"
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"stock-basic-{capture_day}",
        job_name="a_share_instrument_tushare",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    try:
        InstrumentRepository(kernel).persist_stock_basic(raw_rows, batch_id, context)
        industry_result = IndustryMembershipRepository(kernel).persist_daily_capture(
            raw_rows,
            run_date=run_date,
            fetched_at=fetched_at,
            context=context,
        )
        if industry_result.status.startswith("SKIPPED_"):
            logger.info(
                "industry daily capture skipped: status=%s reason=%s",
                industry_result.status,
                industry_result.reason,
            )
    finally:
        kernel.close()
    return (
        f"Tushare 股票基础信息完成：{len(raw_rows)} 条；"
        f"行业每日观测={industry_result.status}，日期={industry_result.trade_date}，"
        f"写入={industry_result.inserted_rows}，行业未知={industry_result.unknown_industry_rows}"
        + (f"，原因={industry_result.reason}" if industry_result.reason else "")
    )
