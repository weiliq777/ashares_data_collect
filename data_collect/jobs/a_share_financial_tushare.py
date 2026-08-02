from __future__ import annotations

import json
from datetime import datetime
import pandas as pd

from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.utils.db import get_connection, save_to_postgres


INDICATOR_FIELDS = "ts_code,ann_date,end_date,roe,roa,debt_to_assets,grossprofit_margin,netprofit_margin,ocf_to_or,eps,bps"


def _date(v):
    return pd.to_datetime(v, format="%Y%m%d", errors="coerce").date() if v else None


def _save_raw(table, rows):
    if not rows:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                clean = {k: (None if pd.isna(v) else v) for k, v in row.items()}
                cur.execute(f'''INSERT INTO "{table}" (ts_code, ann_date, end_date, payload) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING''', (row["ts_code"], _date(row.get("ann_date")), _date(row.get("end_date")), json.dumps(clean, ensure_ascii=False, allow_nan=False)))
        conn.commit()
    return len(rows)


def run(run_date: str, **kwargs) -> str:
    pro = get_pro()
    codes = kwargs.get("stock_codes")
    if not codes:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT stock_code FROM stock_basic_info ORDER BY stock_code")
                codes = [row[0] for row in cur.fetchall()]
        if not codes:
            codes = ["000001.SZ"]
    if isinstance(codes, str):
        codes = [x.strip() for x in codes.split(",") if x.strip()]
    total = 0
    for code in codes[: int(kwargs.get("limit_stocks") or len(codes))]:
        indicator = call_api(pro.fina_indicator, ts_code=code, fields=INDICATOR_FIELDS)
        if indicator is None or indicator.empty:
            continue
        rows = indicator.to_dict("records")
        _save_raw("tushare_fina_indicator_raw", rows)
        for api, table in [(pro.income, "tushare_income_raw"), (pro.balancesheet, "tushare_balancesheet_raw"), (pro.cashflow, "tushare_cashflow_raw")]:
            raw_df = call_api(api, ts_code=code)
            if raw_df is not None and not raw_df.empty:
                _save_raw(table, raw_df.to_dict("records"))
        out = indicator.rename(columns={"ts_code": "stock_code", "end_date": "report_date", "ann_date": "announce_date"}).copy()
        out["report_date"] = out["report_date"].map(_date)
        out["announce_date"] = out["announce_date"].map(_date)
        out["source"] = "tushare"
        out["fetched_at"] = datetime.now()
        keep = ["stock_code", "report_date", "announce_date", "roe", "roa", "debt_to_assets", "grossprofit_margin", "netprofit_margin", "ocf_to_or", "eps", "bps", "source", "fetched_at"]
        out = out[[c for c in keep if c in out.columns]]
        t, i = save_to_postgres(out, pre_aligned_df=out, table_name="stock_financial_indicator")
        total += i
    return f"Tushare财务指标完成：写入{total}条，股票数={len(codes)}"
