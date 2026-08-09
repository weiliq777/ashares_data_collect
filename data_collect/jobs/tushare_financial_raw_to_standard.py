"""将三张财务 Raw 表转换为独立的 Standard 财务事实表。

只读 Raw；不访问 Tushare，不修改或删除任何 Raw 数据。
"""
from __future__ import annotations

import argparse
import json
import uuid
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
    extra = [
        "version_available_date", "vintage_quality", "source_observed_at",
        "source", "source_record_key", "record_version", "payload_hash",
        "quality_status", "fetched_at",
    ]
    if frame.empty:
        return pd.DataFrame(columns=keep + extra)
    out = frame.rename(columns={"ts_code": "stock_code", "end_date": "report_date"}).copy()
    out["report_date"] = _date_series(out, "report_date")
    out["ann_date"] = _date_series(out, "ann_date")
    out["f_ann_date"] = _date_series(out, "f_ann_date")
    out["announce_date"] = out["f_ann_date"].fillna(out["ann_date"])
    fetched_source = (
        out["_fetched_at"] if "_fetched_at" in out
        else pd.Series(datetime.now(), index=out.index)
    )
    fetched = pd.to_datetime(fetched_source, errors="coerce")
    announced = pd.to_datetime(out["announce_date"], errors="coerce")
    ambiguous = out.get(
        "_ambiguous_same_day_revision", pd.Series(False, index=out.index)
    ).fillna(False).astype(bool)
    # Distinct payloads sharing one logical report and the same source
    # announcement date have no reliable source-side ordering.  They are not
    # projected backwards: both become visible only when this warehouse
    # observed them.  Unambiguous source records retain their published date,
    # with an explicit vintage label so historical studies can disclose the
    # source-announcement assumption.
    available = announced + pd.Timedelta(days=1)
    available.loc[ambiguous] = pd.concat(
        [announced.loc[ambiguous], fetched.loc[ambiguous]], axis=1
    ).max(axis=1) + pd.Timedelta(days=1)
    available = available.fillna(fetched + pd.Timedelta(days=1))
    out["version_available_date"] = available.dt.date
    out["vintage_quality"] = ambiguous.map({
        True: "WAREHOUSE_OBSERVED_ONLY",
        False: "SOURCE_ANNOUNCEMENT_ASSUMED",
    })
    out["source_observed_at"] = fetched
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


def _version_frames(dataset: str, chunk_size: int):
    with get_connection() as conn, conn.cursor(name=f"financial_version_{dataset}") as cur:
        cur.itersize = chunk_size
        cur.execute("""
            WITH base AS (
                SELECT r.*,
                       concat_ws(':', ts_code, end_date::text,
                                 COALESCE(report_type,''), COALESCE(comp_type,''),
                                 COALESCE(end_type,'')) AS logical_business_key,
                       COALESCE(f_ann_date, ann_date) AS effective_date
                  FROM tushare_financial_raw_version r
                 WHERE dataset=%s
            ), deduplicated AS (
                SELECT DISTINCT ON (logical_business_key, payload_hash) *
                  FROM base
                 ORDER BY logical_business_key, payload_hash, fetched_at, record_id
            ), annotated AS (
                SELECT d.*,
                       COUNT(*) OVER (
                           PARTITION BY logical_business_key, effective_date
                       ) > 1 AS ambiguous_same_day_revision
                  FROM deduplicated d
            )
            SELECT record_id, logical_business_key, payload, payload_hash,
                   fetched_at, ambiguous_same_day_revision
              FROM annotated
             ORDER BY ts_code, end_date, effective_date, fetched_at, record_id
        """, (dataset,))
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break
            records = []
            for record_id, logical_business_key, payload, payload_hash, fetched_at, ambiguous in rows:
                data = payload if isinstance(payload, dict) else json.loads(payload)
                data.update({"_record_id": record_id,
                             "_logical_business_key": logical_business_key,
                             "_payload_hash": payload_hash, "_fetched_at": fetched_at,
                             "_ambiguous_same_day_revision": ambiguous})
                records.append(data)
            yield pd.DataFrame(records)


def run(dataset="all", chunk_size=5000):
    datasets = CONFIG if dataset == "all" else {dataset: CONFIG[dataset]}
    results = {}
    for name, (source, target, _) in datasets.items():
        attempted = inserted = 0
        staging = f"{target}_build_{uuid.uuid4().hex[:12]}"
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                f'CREATE UNLOGGED TABLE "{staging}" '
                f'(LIKE "{target}" INCLUDING ALL)'
            )
            conn.commit()
        try:
            for raw in _version_frames(name, chunk_size):
                out = transform(raw, name)
                if out.empty:
                    continue
                current, actual = save_to_postgres(out, table_name=staging)
                attempted += current
                inserted += actual
            # Only the final swap touches Standard.  A crash during conversion
            # leaves the previous complete generation visible and Raw untouched.
            with get_connection() as conn, conn.cursor() as cur:
                cur.execute(f'LOCK TABLE "{target}" IN ACCESS EXCLUSIVE MODE')
                cur.execute(f'TRUNCATE TABLE "{target}"')
                cur.execute(f'INSERT INTO "{target}" SELECT * FROM "{staging}"')
                cur.execute(f'DROP TABLE "{staging}"')
                conn.commit()
        except Exception:
            with get_connection() as conn, conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{staging}"')
                conn.commit()
            raise
        results[name] = {"source": source, "target": target, "attempted": attempted, "inserted": inserted}
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="财务 Raw 到三张 Standard 事实表")
    parser.add_argument("--dataset", choices=["income", "balancesheet", "cashflow", "all"], default="all")
    parser.add_argument("--chunk-size", type=int, default=5000)
    args = parser.parse_args()
    print(json.dumps(run(args.dataset, args.chunk_size), ensure_ascii=False, indent=2))
