"""Fetch Tushare financial reports and persist them through the access kernel."""
from __future__ import annotations

import json
from datetime import datetime

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.data_access.repository.security_repository import SecurityRepository
from data_collect.jobs.tushare_job_lock import warehouse_write_job
from data_collect.providers.tushare_client import call_api, get_pro


def _date(value):
    parsed = pd.to_datetime(value, format="%Y%m%d", errors="coerce")
    return parsed.date() if value and not pd.isna(parsed) else None


def _raw_rows(rows: list[dict]) -> list[dict]:
    result = []
    for row in rows:
        clean = {key: (None if pd.isna(value) else value) for key, value in row.items()}
        result.append({
            "ts_code": clean.get("ts_code"),
            "ann_date": _date(clean.get("ann_date")),
            "end_date": _date(clean.get("end_date")),
            "payload": json.dumps(clean, ensure_ascii=False, allow_nan=False, default=str),
        })
    return result


@warehouse_write_job
def run(run_date: str, **kwargs) -> str:
    pro = get_pro()
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"a-share-financial-{run_date}",
        job_name="a_share_financial_tushare",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="v1",
    )
    try:
        codes = kwargs.get("stock_codes")
        if not codes:
            codes = SecurityRepository(kernel).stock_codes(context)
        if isinstance(codes, str):
            codes = [code.strip() for code in codes.split(",") if code.strip()]
        codes = list(codes or ["000001.SZ"])
        codes = codes[: int(kwargs.get("limit_stocks") or len(codes))]
        repository = FinancialRepository(kernel)
        total = 0
        for code in codes:
            indicator = call_api(pro.fina_indicator, ts_code=code)
            if indicator is None or indicator.empty:
                continue
            rows = indicator.to_dict("records")
            repository.append_legacy_raw_reports(
                "tushare_fina_indicator_raw", _raw_rows(rows), [code], context
            )
            for api, target in (
                (pro.income, "tushare_income_raw"),
                (pro.balancesheet, "tushare_balancesheet_raw"),
                (pro.cashflow, "tushare_cashflow_raw"),
            ):
                frame = call_api(api, ts_code=code)
                if frame is not None and not frame.empty:
                    repository.append_legacy_raw_reports(
                        target, _raw_rows(frame.to_dict("records")), [code], context
                    )
            out = indicator.rename(
                columns={"ts_code": "stock_code", "end_date": "report_date", "ann_date": "announce_date"}
            ).copy()
            out["report_date"] = out["report_date"].map(_date)
            out["announce_date"] = out["announce_date"].map(_date)
            out["source"] = "tushare"
            out["fetched_at"] = datetime.now()
            keep = [
                "stock_code", "report_date", "announce_date", "roe", "roa", "debt_to_assets",
                "grossprofit_margin", "netprofit_margin", "ocf_to_or", "eps", "bps", "source", "fetched_at",
            ]
            rows_out = out[[column for column in keep if column in out]].astype(object)
            rows_out = rows_out.where(pd.notna(rows_out), None)
            total += repository.upsert_legacy_indicator(rows_out.to_dict("records"), [code], context)
        return f"Tushare financial complete: {total} rows, stocks={len(codes)}"
    finally:
        kernel.close()
