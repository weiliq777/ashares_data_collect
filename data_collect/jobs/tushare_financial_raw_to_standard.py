"""Project immutable financial Raw versions into Standard facts.

Only field normalization and orchestration live here.  Version selection,
high-water paging, duplicate suppression and Standard writes are owned by
``FinancialStandardRepository``.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from uuid import uuid4

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_standard_repository import FinancialStandardRepository
from data_collect.data_access.run_context import build_run_context
from data_collect.jobs.tushare_job_lock import warehouse_write_job


CONFIG = {
    "income": ("tushare_income_raw", "std.financial_income_standard", [
        "stock_code", "report_date", "ann_date", "f_ann_date", "announce_date", "report_type", "comp_type", "end_type", "update_flag",
        "revenue", "total_revenue", "oper_cost", "total_cogs", "operate_profit", "total_profit", "income_tax",
        "net_profit", "net_profit_consolidated", "n_income_attr_p", "net_profit_parent", "basic_eps", "diluted_eps",
    ]),
    "balancesheet": ("tushare_balancesheet_raw", "std.financial_balance_sheet_standard", [
        "stock_code", "report_date", "ann_date", "f_ann_date", "announce_date", "report_type", "comp_type", "end_type", "update_flag",
        "total_assets", "total_liab", "total_hldr_eqy_exc_min_int", "parent_equity_end", "total_hldr_eqy_inc_min_int", "total_equity_end",
        "cash", "accounts_receiv", "inventories", "goodwill", "total_cur_assets", "total_cur_liab", "st_borr", "lt_borr",
    ]),
    "cashflow": ("tushare_cashflow_raw", "std.financial_cash_flow_standard", [
        "stock_code", "report_date", "ann_date", "f_ann_date", "announce_date", "report_type", "comp_type", "end_type", "update_flag",
        "n_cashflow_act", "operating_cashflow_ytd", "n_cashflow_inv_act", "n_cash_flows_fnc_act", "c_fr_sale_sg", "c_paid_goods_s",
        "c_paid_for_taxes", "c_pay_acq_const_fiolta", "c_recp_cap_contrib", "c_recp_borrow", "c_pay_debt", "n_incr_cash_cash_equ",
        "c_cash_equ_beg_period", "c_cash_equ_end_period", "free_cashflow", "net_profit", "cashflow_net_profit_reconciliation",
    ]),
}


def _date_series(frame, column):
    if column not in frame:
        return pd.Series(pd.NaT, index=frame.index)
    return pd.to_datetime(frame[column], format="%Y%m%d", errors="coerce").dt.date


def transform(frame: pd.DataFrame, dataset: str) -> pd.DataFrame:
    _, _, keep = CONFIG[dataset]
    extra = ["version_available_date", "data_available_timestamp", "vintage_quality", "source_observed_at", "source", "source_record_key", "record_version", "payload_hash", "quality_status", "fetched_at"]
    if frame.empty:
        return pd.DataFrame(columns=keep + extra)
    out = frame.rename(columns={"ts_code": "stock_code", "end_date": "report_date"}).copy()
    out["report_date"] = _date_series(out, "report_date")
    out["ann_date"] = _date_series(out, "ann_date")
    out["f_ann_date"] = _date_series(out, "f_ann_date")
    out["announce_date"] = out["f_ann_date"].fillna(out["ann_date"])
    fetched_source = out["_fetched_at"] if "_fetched_at" in out else pd.Series(datetime.now(), index=out.index)
    fetched = pd.to_datetime(fetched_source, errors="coerce")
    announced = pd.to_datetime(out["announce_date"], errors="coerce")
    ambiguous = out.get("_ambiguous_same_day_revision", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    available = (announced + pd.Timedelta(days=1)).fillna(fetched + pd.Timedelta(days=1))
    out["version_available_date"] = available.dt.date
    out["data_available_timestamp"] = pd.to_datetime(out["version_available_date"], errors="coerce").dt.tz_localize("Asia/Shanghai")
    out["vintage_quality"] = announced.notna().map({True: "SOURCE_ANNOUNCEMENT_ASSUMED", False: "WAREHOUSE_OBSERVED_ONLY"})
    out["source_observed_at"] = fetched
    if dataset == "income":
        consolidated = pd.to_numeric(out.get("n_income"), errors="coerce")
        parent = pd.to_numeric(out.get("n_income_attr_p"), errors="coerce")
        out["net_profit_consolidated"], out["net_profit_parent"], out["net_profit"] = consolidated, parent, consolidated
    elif dataset == "balancesheet":
        out["parent_equity_end"] = pd.to_numeric(out.get("total_hldr_eqy_exc_min_int"), errors="coerce")
        out["total_equity_end"] = pd.to_numeric(out.get("total_hldr_eqy_inc_min_int"), errors="coerce")
    elif dataset == "cashflow":
        out["operating_cashflow_ytd"] = pd.to_numeric(out.get("n_cashflow_act"), errors="coerce")
        out["cashflow_net_profit_reconciliation"] = pd.to_numeric(out.get("net_profit"), errors="coerce")
    for column in keep:
        if column not in out:
            out[column] = None
    for column in keep:
        if column not in {"stock_code", "report_date", "ann_date", "f_ann_date", "announce_date", "report_type", "comp_type", "end_type", "update_flag"}:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    out["source"] = "tushare"
    out["source_record_key"] = out.get("_logical_business_key", pd.Series(index=out.index, dtype=object))
    fallback_key = out.apply(lambda row: f"{row.get('stock_code')}:{row.get('report_date')}:{row.get('announce_date')}:{row.get('report_type')}:{row.get('comp_type')}:{row.get('update_flag')}", axis=1)
    out["source_record_key"] = out["source_record_key"].fillna(fallback_key)
    out["record_version"] = pd.to_numeric(out.get("_record_id", 0), errors="coerce").fillna(0).astype("int64")
    out["payload_hash"] = out.get("_payload_hash", pd.Series(index=out.index, dtype=object))
    out["quality_status"] = out["report_date"].notna().map({True: "VALID", False: "INVALID"})
    out["fetched_at"] = out.get("_fetched_at", datetime.now())
    out.loc[ambiguous & (out["quality_status"] == "VALID"), "quality_status"] = "WARNING"
    return out[keep + extra].dropna(subset=["stock_code", "report_date"])


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = build_run_context(
        "tushare_financial_raw_to_standard",
        pipeline_version="data-access-v1",
        run_id=f"financial-standard-{label}-{uuid4().hex}",
    )
    return kernel, FinancialStandardRepository(kernel), context


def _version_frames(dataset: str, chunk_size: int, stock_codes=None, report_start=None, report_end=None):
    kernel, repository, context = _service(f"read-{dataset}")
    try:
        for rows in repository.iter_version_rows(dataset, context, chunk_size, stock_codes, report_start, report_end):
            yield pd.DataFrame(rows)
    finally:
        kernel.close()


def _upsert_standard(frame: pd.DataFrame, target: str, page_size: int = 2000) -> int:
    if frame.empty:
        return 0
    if page_size <= 0 or page_size > 5000:
        raise ValueError("page_size must be in 1..5000")
    clean = frame.astype(object).where(pd.notna(frame), None)
    rows = clean.to_dict("records")
    kernel, repository, context = _service(f"write-{target}")
    try:
        affected = 0
        for offset in range(0, len(rows), page_size):
            affected += repository.upsert_standard_rows(target, rows[offset:offset + page_size], context, page_size=page_size)
        return affected
    finally:
        kernel.close()


def _run_unlocked(dataset="all", chunk_size=5000, stock_codes=None, report_start=None, report_end=None):
    datasets = CONFIG if dataset == "all" else {dataset: CONFIG[dataset]}
    results = {}
    for name, (source, target, _) in datasets.items():
        attempted = inserted = 0
        for raw in _version_frames(name, chunk_size, stock_codes, report_start, report_end):
            out = transform(raw, name)
            if out.empty:
                continue
            attempted += len(out)
            inserted += _upsert_standard(out, target)
        results[name] = {"source": source, "target": target, "attempted": attempted, "inserted": inserted}
    return results


@warehouse_write_job
def run(dataset="all", chunk_size=5000, stock_codes=None, report_start=None, report_end=None):
    """Run the standalone Standard projection under the warehouse lock."""
    return _run_unlocked(dataset, chunk_size, stock_codes, report_start, report_end)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Project financial Raw into Standard")
    parser.add_argument("--dataset", choices=["income", "balancesheet", "cashflow", "all"], default="all")
    parser.add_argument("--chunk-size", type=int, default=5000)
    parser.add_argument("--codes", nargs="*")
    parser.add_argument("--report-start")
    parser.add_argument("--report-end")
    args = parser.parse_args()
    print(json.dumps(run(args.dataset, args.chunk_size, args.codes, args.report_start, args.report_end), ensure_ascii=False, indent=2))
