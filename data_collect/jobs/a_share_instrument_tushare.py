from __future__ import annotations

from datetime import datetime
import pandas as pd

from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.utils.db import get_connection


def run(run_date: str, **kwargs) -> str:
    pro = get_pro()
    df = call_api(pro.stock_basic, exchange="", list_status="", fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,list_status")
    if df is None or df.empty:
        return "Tushare stock_basic 无数据"
    raw_rows = df.astype(object).where(pd.notna(df), None).to_dict("records")
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in raw_rows:
                import json
                cur.execute(
                    "INSERT INTO tushare_stock_basic_raw (ts_code,payload) VALUES (%s,%s) "
                    "ON CONFLICT (ts_code) DO UPDATE SET payload=EXCLUDED.payload,fetched_at=CURRENT_TIMESTAMP",
                    (row["ts_code"], json.dumps(row, ensure_ascii=False, allow_nan=False)),
                )
        conn.commit()
    out = df.rename(columns={"ts_code": "stock_code"}).copy()
    for col in ["list_date", "delist_date"]:
        out[col] = pd.to_datetime(out[col], format="%Y%m%d", errors="coerce").dt.date
    out["source"] = "tushare"
    out["fetched_at"] = datetime.now()
    out = out.astype(object).where(pd.notna(out), None)
    columns = ["stock_code", "symbol", "name", "area", "industry", "market", "list_date", "delist_date", "list_status", "source", "fetched_at"]
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in out[columns].itertuples(index=False, name=None):
                cur.execute(
                    '''INSERT INTO stock_basic_info (stock_code,symbol,"name",area,industry,market,list_date,delist_date,list_status,source,fetched_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (stock_code) DO UPDATE SET symbol=EXCLUDED.symbol,"name"=EXCLUDED."name",area=EXCLUDED.area,industry=EXCLUDED.industry,market=EXCLUDED.market,list_date=EXCLUDED.list_date,delist_date=EXCLUDED.delist_date,list_status=EXCLUDED.list_status,source=EXCLUDED.source,fetched_at=EXCLUDED.fetched_at''', row)
        conn.commit()
    return f"Tushare 股票基础信息完成：{len(out)}/{len(out)}"
