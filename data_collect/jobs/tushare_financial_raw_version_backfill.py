"""从现有财务 Raw 表建立版本层；只读现有 Raw，写入新版本表。"""
from __future__ import annotations

import argparse
import hashlib
import json
import uuid

import pandas as pd

from data_collect.jobs.tushare_financial_raw_to_standard import CONFIG
from data_collect.utils.db import get_connection


def _date(value):
    parsed = pd.to_datetime(value, format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


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
                        encoded = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)
                        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
                        business_key = ":".join(str(data.get(key) or "") for key in ("ts_code", "ann_date", "end_date", "report_type", "comp_type", "update_flag"))
                        write_cur.execute("""INSERT INTO tushare_financial_raw_version
                            (dataset,business_key,ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,update_flag,payload,source,batch_id,payload_hash)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'tushare',%s,%s)
                            ON CONFLICT (dataset,business_key,payload_hash) DO NOTHING""",
                                    (name, business_key, data.get("ts_code"), _date(data.get("ann_date")), _date(data.get("f_ann_date")), _date(data.get("end_date")), data.get("report_type"), data.get("comp_type"), data.get("end_type"), data.get("update_flag"), encoded, batch_id, digest))
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
