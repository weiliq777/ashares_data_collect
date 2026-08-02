from __future__ import annotations

import pandas as pd


def normalize_financial_indicator(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.rename(columns={"ts_code": "stock_code", "end_date": "report_date", "ann_date": "announce_date"}).copy()
    for col in ["report_date", "announce_date"]:
        if col in out:
            out[col] = pd.to_datetime(out[col], format="%Y%m%d", errors="coerce").dt.date
    numeric = [c for c in out.columns if c not in {"stock_code", "report_date", "announce_date"}]
    for col in numeric:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out
