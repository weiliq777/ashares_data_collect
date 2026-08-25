from __future__ import annotations

from datetime import datetime
import hashlib
import json
import uuid
import pandas as pd

from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.instrument_repository import InstrumentRepository


def run(run_date: str, dry_run: bool = False, **kwargs) -> str:
    pro = get_pro()
    df = call_api(pro.stock_basic, exchange="", list_status="", fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,list_status")
    if df is None or df.empty:
        return "Tushare stock_basic 无数据"
    raw_rows = df.astype(object).where(pd.notna(df), None).to_dict("records")
    if dry_run:
        return f"dry-run stock_basic rows={len(raw_rows)}"
    batch_id = f"stock-basic-{run_date}-{uuid.uuid4().hex}"
    out = df.rename(columns={"ts_code": "stock_code"}).copy()
    for col in ["list_date", "delist_date"]:
        out[col] = pd.to_datetime(out[col], format="%Y%m%d", errors="coerce").dt.date
    out["source"] = "tushare"
    out["fetched_at"] = datetime.now()
    out = out.astype(object).where(pd.notna(out), None)
    columns = ["stock_code", "symbol", "name", "area", "industry", "market", "list_date", "delist_date", "list_status", "source", "fetched_at"]
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(run_id=f"stock-basic-{run_date}", job_name="a_share_instrument_tushare", code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1")
    try:
        InstrumentRepository(kernel).persist_stock_basic(raw_rows, batch_id, context)
    finally:
        kernel.close()
    return f"Tushare 股票基础信息完成：{len(out)}/{len(out)}"
