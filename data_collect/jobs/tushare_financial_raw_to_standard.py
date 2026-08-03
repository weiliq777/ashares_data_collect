"""将三张财务 Raw 表转换为独立的 Standard 财务事实表。

只读 Raw；不访问 Tushare，不修改或删除任何 Raw 数据。
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime

import pandas as pd

from data_collect.jobs.tushare_raw_to_standard import _payload_frames
from data_collect.utils.db import save_to_postgres


CONFIG = {
    "income": ("tushare_income_raw", "financial_income_standard", [
        "stock_code", "report_date", "ann_date", "f_ann_date", "announce_date", "report_type", "comp_type", "end_type", "update_flag",
        "revenue", "total_revenue", "oper_cost", "total_cogs", "operate_profit", "total_profit", "income_tax", "net_profit", "n_income_attr_p", "basic_eps", "diluted_eps",
    ]),
    "balancesheet": ("tushare_balancesheet_raw", "financial_balance_sheet_standard", [
        "stock_code", "report_date", "ann_date", "f_ann_date", "announce_date", "report_type", "comp_type", "end_type", "update_flag",
        "total_assets", "total_liab", "total_hldr_eqy_exc_min_int", "total_hldr_eqy_inc_min_int", "cash", "accounts_receiv", "inventories", "goodwill", "total_cur_assets", "total_cur_liab", "st_borr", "lt_borr",
    ]),
    "cashflow": ("tushare_cashflow_raw", "financial_cash_flow_standard", [
        "stock_code", "report_date", "ann_date", "f_ann_date", "announce_date", "report_type", "comp_type", "end_type", "update_flag",
        "n_cashflow_act", "n_cashflow_inv_act", "n_cash_flows_fnc_act", "c_fr_sale_sg", "c_paid_goods_s", "c_paid_for_taxes", "c_pay_acq_const_fiolta", "c_recp_cap_contrib", "c_recp_borrow", "c_pay_debt", "n_incr_cash_cash_equ", "c_cash_equ_beg_period", "c_cash_equ_end_period", "free_cashflow", "net_profit",
    ]),
}


def _date_series(frame, column):
    if column not in frame:
        return pd.Series(pd.NaT, index=frame.index)
    return pd.to_datetime(frame[column], format="%Y%m%d", errors="coerce").dt.date


def transform(frame: pd.DataFrame, dataset: str) -> pd.DataFrame:
    _, _, keep = CONFIG[dataset]
    if frame.empty:
        return pd.DataFrame(columns=keep + ["source", "source_record_key", "record_version", "quality_status", "fetched_at"])
    out = frame.rename(columns={"ts_code": "stock_code", "end_date": "report_date"}).copy()
    out["report_date"] = _date_series(out, "report_date")
    out["ann_date"] = _date_series(out, "ann_date")
    out["f_ann_date"] = _date_series(out, "f_ann_date")
    out["announce_date"] = out["f_ann_date"].fillna(out["ann_date"])
    for column in keep:
        if column not in out:
            out[column] = None
    for column in keep:
        if column not in {"stock_code", "report_date", "ann_date", "f_ann_date", "announce_date", "report_type", "comp_type", "end_type", "update_flag"}:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    out["source"] = "tushare"
    out["source_record_key"] = out.apply(lambda row: f"{row.get('stock_code')}:{row.get('report_date')}:{row.get('announce_date')}:{row.get('report_type')}:{row.get('comp_type')}:{row.get('update_flag')}", axis=1)
    out["record_version"] = 0
    out["quality_status"] = out["report_date"].notna().map({True: "VALID", False: "INVALID"})
    out["fetched_at"] = datetime.now()
    return out[keep + ["source", "source_record_key", "record_version", "quality_status", "fetched_at"]].dropna(subset=["stock_code", "report_date"])


def run(dataset="all", chunk_size=5000):
    datasets = CONFIG if dataset == "all" else {dataset: CONFIG[dataset]}
    results = {}
    for name, (source, target, _) in datasets.items():
        attempted = inserted = 0
        for raw in _payload_frames(source, chunk_size):
            out = transform(raw, name)
            if out.empty:
                continue
            current, actual = save_to_postgres(out, table_name=target)
            attempted += current
            inserted += actual
        results[name] = {"source": source, "target": target, "attempted": attempted, "inserted": inserted}
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="财务 Raw 到三张 Standard 事实表")
    parser.add_argument("--dataset", choices=["income", "balancesheet", "cashflow", "all"], default="all")
    parser.add_argument("--chunk-size", type=int, default=5000)
    args = parser.parse_args()
    print(json.dumps(run(args.dataset, args.chunk_size), ensure_ascii=False, indent=2))
