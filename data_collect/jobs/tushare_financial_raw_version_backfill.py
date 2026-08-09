"""从现有财务 Raw 表建立版本层；只读现有 Raw，写入新版本表。"""
from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from datetime import datetime

import pandas as pd

from data_collect.jobs.tushare_financial_raw_to_standard import CONFIG
from data_collect.utils.db import get_connection, require_psycopg2


def _date(value):
    parsed = pd.to_datetime(value, format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _payload_values(data: dict, dataset: str, batch_id: str, fetched_at=None):
    encoded = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    # A logical report identity must not include announcement/version metadata.
    # ann_date/update_flag describe revisions of the same report, not different
    # reports.  Existing immutable version rows keep their stored key; Standard
    # rebuilds derive the same logical identity from explicit columns.
    business_key = ":".join(
        str(data.get(key) or "")
        for key in ("ts_code", "end_date", "report_type", "comp_type", "end_type")
    )
    return (
        dataset, business_key, data.get("ts_code"), _date(data.get("ann_date")),
        _date(data.get("f_ann_date")), _date(data.get("end_date")),
        data.get("report_type"), data.get("comp_type"), data.get("end_type"),
        data.get("update_flag"), encoded, "tushare", batch_id, digest,
        fetched_at or datetime.now(),
    )


def save_financial_versions(dataset: str, records: list[dict], batch_id: str | None = None) -> int:
    """追加三张财务报表的不可变版本；不修改 Raw。"""
    if dataset not in {"income", "balancesheet", "cashflow"} or not records:
        return 0
    batch_id = batch_id or f"financial-{dataset}-{uuid.uuid4().hex}"
    values = [_payload_values(record, dataset, batch_id) for record in records]
    sql = """
        INSERT INTO tushare_financial_raw_version
        (dataset,business_key,ts_code,ann_date,f_ann_date,end_date,report_type,
         comp_type,end_type,update_flag,payload,source,batch_id,payload_hash,fetched_at)
        VALUES %s
        ON CONFLICT (dataset,business_key,payload_hash) DO NOTHING
    """
    _, execute_values = require_psycopg2()
    with get_connection() as conn, conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=2000)
        inserted = max(cur.rowcount, 0)
        conn.commit()
    return inserted


def run(dataset="all", batch_size=1000):
    selected = CONFIG if dataset == "all" else {dataset: CONFIG[dataset]}
    batch_id = f"historical-backfill-{uuid.uuid4().hex}"
    result = {}
    for name, (source, _, _) in selected.items():
        inserted = 0
        # 读取和写入使用不同连接；写入时提交不能销毁读取游标。
        with get_connection() as read_conn, read_conn.cursor(name=f"financial_backfill_{name}") as read_cur:
            read_cur.itersize = batch_size
            read_cur.execute(f'SELECT payload FROM "{source}" ORDER BY 1')
            with get_connection() as write_conn, write_conn.cursor() as write_cur:
                while True:
                    rows = read_cur.fetchmany(batch_size)
                    if not rows:
                        break
                    for (payload,) in rows:
                        data = payload if isinstance(payload, dict) else json.loads(payload)
                        values = _payload_values(data, name, batch_id)
                        write_cur.execute("""INSERT INTO tushare_financial_raw_version
                            (dataset,business_key,ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,update_flag,payload,source,batch_id,payload_hash,fetched_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,COALESCE(%s,CURRENT_TIMESTAMP))
                            ON CONFLICT (dataset,business_key,payload_hash) DO NOTHING""", values)
                        inserted += write_cur.rowcount
                    write_conn.commit()
        result[name] = inserted
    return {"batch_id": batch_id, "inserted": result}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="从现有财务 Raw 建立不可变版本记录")
    parser.add_argument("--dataset", choices=["income", "balancesheet", "cashflow", "all"], default="all")
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    print(run(args.dataset, args.batch_size))
