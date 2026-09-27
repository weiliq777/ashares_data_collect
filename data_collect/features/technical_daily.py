"""Reusable technical-feature calculations for the foundation layer."""
from __future__ import annotations

import numpy as np
import pandas as pd

from data_collect.utils.indicators import kd, wr

FEATURE_VERSION = "technical_daily_v1"


def _rsi_wilder(close: pd.Series, period: int = 9) -> pd.Series:
    """TA-Lib-compatible RSI using Wilder's recursive average."""
    values = close.astype(float).to_numpy()
    output = np.full(len(values), np.nan, dtype=float)
    if len(values) <= period:
        return pd.Series(output, index=close.index)
    changes = np.diff(values)
    gains = np.maximum(changes, 0.0)
    losses = np.maximum(-changes, 0.0)
    average_gain = float(gains[:period].mean())
    average_loss = float(losses[:period].mean())
    for index in range(period, len(values)):
        if index > period:
            average_gain = ((average_gain * (period - 1)) + gains[index - 1]) / period
            average_loss = ((average_loss * (period - 1)) + losses[index - 1]) / period
        if average_loss == 0:
            output[index] = 100.0 if average_gain > 0 else 0.0
        else:
            output[index] = 100.0 - (100.0 / (1.0 + average_gain / average_loss))
    return pd.Series(output, index=close.index)


def apply_forward_ratio(price_df: pd.DataFrame, dividend_df: pd.DataFrame) -> pd.DataFrame:
    """Return price coordinates using the existing wave3 equal-ratio rule."""
    if price_df.empty:
        return price_df.copy()
    df = price_df.copy()
    if dividend_df.empty:
        df["close_raw"] = df["close"]
        return df
    results: list[pd.DataFrame] = []
    dividend_groups = dict(list(dividend_df.groupby("stock_code")))
    for stock_code, group in df.groupby("stock_code"):
        group = group.sort_values("trade_date").copy()
        group["close_raw"] = group["close"]
        stock_dividends = dividend_groups.get(stock_code)
        if stock_dividends is None or stock_dividends.empty:
            results.append(group)
            continue
        stock_dividends = stock_dividends.sort_values("date")
        dividend_dates = stock_dividends["date"].values
        dividend_ratios = stock_dividends["dr"].values
        cumulative = np.empty(len(group), dtype=float)
        index = 0
        product = 1.0
        for row_index, trade_date in enumerate(group["trade_date"].values):
            while index < len(dividend_dates) and dividend_dates[index] <= trade_date:
                product *= dividend_ratios[index]
                index += 1
            cumulative[row_index] = product
        last_ratio = cumulative[-1]
        ratio = cumulative / last_ratio if last_ratio else np.ones(len(group))
        for column in ("open", "high", "low", "close"):
            group[column] = (group[column].astype(float) * ratio).round(2)
        results.append(group)
    return pd.concat(results, ignore_index=True)


def apply_adjustment_factor(price_df: pd.DataFrame, factor_df: pd.DataFrame) -> pd.DataFrame:
    """Apply the formal PIT mechanical-price coordinate to OHLC values.

    ``adj_adjustment_factor`` is a cumulative factor as of each trade date.
    To create a forward-adjusted window, each day's factor is normalized by
    the last factor in that stock's input window.  Multiplying every OHLC
    field by that same ratio preserves candle geometry and keeps technical
    indicators on the same coordinate as ``feature.price_daily_v2``.
    """
    if price_df.empty:
        return price_df.copy()
    if factor_df.empty:
        raise ValueError("formal price adjustment factors are required")
    required = {"stock_code", "trade_date", "adjustment_factor"}
    missing_columns = required.difference(factor_df.columns)
    if missing_columns:
        raise ValueError(f"adjustment factor data missing columns: {sorted(missing_columns)}")

    factors = factor_df.loc[:, list(required)].copy()
    factors["adjustment_factor"] = pd.to_numeric(factors["adjustment_factor"], errors="coerce")
    if factors.duplicated(["stock_code", "trade_date"]).any():
        raise ValueError("formal price adjustment factors contain duplicate stock/date rows")

    results: list[pd.DataFrame] = []
    for stock_code, group in price_df.groupby("stock_code"):
        group = group.sort_values("trade_date").copy()
        stock_factors = factors.loc[factors["stock_code"].eq(stock_code), ["trade_date", "adjustment_factor"]]
        merged = group.merge(stock_factors, on="trade_date", how="left", validate="one_to_one")
        values = merged["adjustment_factor"].to_numpy(dtype=float)
        if not np.isfinite(values).all() or (values <= 0).any():
            raise ValueError(f"invalid formal price adjustment factor for {stock_code}")
        terminal = values[-1]
        ratio = values / terminal
        for column in ("open", "high", "low", "close"):
            merged[column] = (merged[column].astype(float) * ratio).round(2)
        merged["close_raw"] = merged["close"] / ratio
        results.append(merged.drop(columns=["adjustment_factor"]))
    return pd.concat(results, ignore_index=True)


def calculate_technical_features(price_df: pd.DataFrame, adjustment_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate WR10/20, RSI9 and the existing Tongdaxin KDJ semantics."""
    adjusted = (
        apply_adjustment_factor(price_df, adjustment_df)
        if not adjustment_df.empty and "adjustment_factor" in adjustment_df.columns
        else apply_forward_ratio(price_df, adjustment_df)
    )
    if adjusted.empty:
        return adjusted
    results: list[pd.DataFrame] = []
    for _, group in adjusted.groupby("stock_code"):
        group = group.sort_values("trade_date").copy()
        high = group["high"].astype(float)
        low = group["low"].astype(float)
        close = group["close"].astype(float)
        group["wr10"] = wr(high, low, close, 10)
        group["wr20"] = wr(high, low, close, 20)
        group["rsi9"] = _rsi_wilder(close, 9)
        k_value, d_value = kd(close, high, low, 9)
        group["k"] = k_value
        group["d"] = d_value
        group["j"] = 3 * k_value - 2 * d_value
        results.append(group)
    output = pd.concat(results, ignore_index=True)
    indicator_columns = ["wr10", "wr20", "rsi9", "k", "d", "j"]
    output["quality_status"] = np.where(output[indicator_columns].notna().all(axis=1), "VALID", "INSUFFICIENT_HISTORY")
    output["feature_contract_version"] = FEATURE_VERSION
    return output


def to_feature_rows(features: pd.DataFrame) -> list[dict]:
    columns = ("stock_code", "trade_date", "wr10", "wr20", "rsi9", "k", "d", "j",
               "quality_status", "feature_contract_version")
    if features.empty:
        return []
    return features.loc[:, columns].replace({np.nan: None}).to_dict("records")
