from __future__ import annotations

import pandas as pd


def normalize_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["stock_code", "trade_date", "open", "high", "low", "close", "volume", "amount"])
    out = df.rename(columns={"ts_code": "stock_code", "vol": "volume"}).copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"], format="%Y%m%d", errors="coerce").dt.date
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["volume"] = out["volume"] * 100.0
    out["amount"] = out["amount"] * 1000.0
    return out[["stock_code", "trade_date", "open", "high", "low", "close", "volume", "amount"]].dropna(subset=["stock_code", "trade_date"])


def normalize_daily_basic(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.rename(columns={"ts_code": "stock_code"}).copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"], format="%Y%m%d", errors="coerce").dt.date
    return out
