"""将三张财务 Raw 表转换为独立的 Standard 财务事实表。

只读 Raw；不访问 Tushare，不修改或删除任何 Raw 数据。
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime

import pandas as pd

from data_collect.utils.db import get_connection, save_to_postgres


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
        return pd.DataFrame(columns=keep + ["source", "source_record_key", "record_version", "payload_hash", "quality_status", "fetched_at"])
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
    out["source_record_key"] = out.get("_business_key", pd.Series(index=out.index, dtype=object))
    fallback_key = out.apply(lambda row: f"{row.get('stock_code')}:{row.get('report_date')}:{row.get('announce_date')}:{row.get('report_type')}:{row.get('comp_type')}:{row.get('update_flag')}", axis=1)
    out["source_record_key"] = out["source_record_key"].fillna(fallback_key)
    out["record_version"] = pd.to_numeric(out.get("_record_id", 0), errors="coerce").fillna(0).astype("int64")
    out["payload_hash"] = out.get("_payload_hash", pd.Series(index=out.index, dtype=object))
    out["quality_status"] = out["report_date"].notna().map({True: "VALID", False: "INVALID"})
    out["fetched_at"] = out.get("_fetched_at", datetime.now())
    return out[keep + ["source", "source_record_key", "record_version", "payload_hash", "quality_status", "fetched_at"]].dropna(subset=["stock_code", "report_date"])


def _version_frames(dataset: str, chunk_size: int):
    with get_connection() as conn, conn.cursor(name=f"financial_version_{dataset}") as cur:
        cur.itersize = chunk_size
        cur.execute("""SELECT record_id, business_key, payload, payload_hash, fetched_at
                       FROM tushare_financial_raw_version
                       WHERE dataset=%s ORDER BY ts_code, end_date, ann_date, record_id""", (dataset,))
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break
            records = []
            for record_id, business_key, payload, payload_hash, fetched_at in rows:
                data = payload if isinstance(payload, dict) else json.loads(payload)
                data.update({"_record_id": record_id, "_business_key": business_key,
                             "_payload_hash": payload_hash, "_fetched_at": fetched_at})
                records.append(data)
            yield pd.DataFrame(records)


def run(dataset="all", chunk_size=5000):
    datasets = CONFIG if dataset == "all" else {dataset: CONFIG[dataset]}
    results = {}
    for name, (source, target, _) in datasets.items():
        attempted = inserted = 0
        # Standard 是可重建派生层；先清理目标表，避免旧的 record_version=0
        # 行阻挡带 payload_hash 的新版本写入。绝不触碰 Raw/版本表。
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(f'TRUNCATE TABLE "{target}" RESTART IDENTITY')
            conn.commit()
        for raw in _version_frames(name, chunk_size):
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
