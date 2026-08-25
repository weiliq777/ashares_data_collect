"""Pure financial-period calculations for Feature Contract v2."""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from statistics import mean, pstdev
from typing import Iterable


PERIOD_BY_END_TYPE = {"1": "Q1", "2": "H1", "3": "Q3", "4": "FY"}
PREVIOUS_PERIOD = {"H1": (3, 31), "Q3": (6, 30), "FY": (9, 30)}
DIVIDEND_PERCENTILE_MIN_OBSERVATIONS = 250
VALUATION_WINDOW_YEARS = 5
VALUATION_WINDOW_COMPLETENESS_TOLERANCE_DAYS = 31


def finite(value):
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def period_kind(report_date: date, end_type: str | None) -> str | None:
    explicit = PERIOD_BY_END_TYPE.get(str(end_type or ""))
    inferred = {
        (3, 31): "Q1",
        (6, 30): "H1",
        (9, 30): "Q3",
        (12, 31): "FY",
    }.get((report_date.month, report_date.day))
    if explicit and inferred and explicit != inferred:
        return None
    return explicit or inferred


def prior_same_period(report_date: date) -> date:
    return report_date.replace(year=report_date.year - 1)


def prior_fiscal_year_end(report_date: date) -> date:
    return date(report_date.year - 1, 12, 31)


def prior_ytd_period(report_date: date, kind: str) -> date | None:
    month_day = PREVIOUS_PERIOD.get(kind)
    return date(report_date.year, *month_day) if month_day else None


@dataclass(frozen=True)
class BridgeResult:
    value: float | None
    basis: str
    required: int
    actual: int
    complete: bool


def quarter_bridge(current, previous_ytd, kind: str | None) -> BridgeResult:
    current_value = finite(current)
    if kind == "Q1":
        return BridgeResult(current_value, "DIRECT_Q1", 1, int(current_value is not None), current_value is not None)
    if kind in {"H1", "Q3", "FY"}:
        previous_value = finite(previous_ytd)
        actual = int(current_value is not None) + int(previous_value is not None)
        value = current_value - previous_value if actual == 2 else None
        return BridgeResult(value, "YTD_DIFFERENCE", 2, actual, actual == 2)
    return BridgeResult(None, "UNAVAILABLE", 0, 0, False)


def ttm_bridge(current, prior_fy, prior_same, kind: str | None) -> BridgeResult:
    current_value = finite(current)
    if kind == "FY":
        return BridgeResult(current_value, "DIRECT_FY", 1, int(current_value is not None), current_value is not None)
    if kind in {"Q1", "H1", "Q3"}:
        fy_value = finite(prior_fy)
        same_value = finite(prior_same)
        actual = sum(value is not None for value in (current_value, fy_value, same_value))
        value = current_value + fy_value - same_value if actual == 3 else None
        return BridgeResult(value, "YTD_BRIDGE", 3, actual, actual == 3)
    return BridgeResult(None, "UNAVAILABLE", 0, 0, False)


def safe_growth(current, previous) -> float | None:
    current_value, previous_value = finite(current), finite(previous)
    # A non-positive base makes percentage growth economically misleading.
    # Loss-to-profit crossings are represented by recovery facts instead.
    if current_value is None or previous_value is None or previous_value <= 0:
        return None
    return current_value / previous_value - 1.0


def safe_cagr(current, previous, years: int) -> float | None:
    current_value, previous_value = finite(current), finite(previous)
    if years <= 0 or current_value is None or previous_value is None:
        return None
    if current_value <= 0 or previous_value <= 0:
        return None
    return (current_value / previous_value) ** (1.0 / years) - 1.0


def safe_margin(numerator, denominator) -> float | None:
    numerator_value, denominator_value = finite(numerator), finite(denominator)
    if numerator_value is None or denominator_value in (None, 0):
        return None
    return numerator_value / denominator_value


def ocf_to_profit(operating_cashflow_ttm, net_profit_ttm) -> tuple[float | None, str]:
    ocf, profit = finite(operating_cashflow_ttm), finite(net_profit_ttm)
    if profit is None:
        return None, "MISSING_NET_PROFIT"
    if profit <= 0:
        return None, "NEGATIVE_OR_ZERO_PROFIT"
    if ocf is None:
        return None, "MISSING_OPERATING_CASHFLOW"
    return ocf / profit, "VALID"


def roe_ttm(net_profit_parent_ttm, equity_begin, equity_end) -> tuple[float | None, str]:
    profit = finite(net_profit_parent_ttm)
    begin, end = finite(equity_begin), finite(equity_end)
    if profit is None:
        return None, "MISSING_NET_PROFIT"
    if begin is None or end is None:
        return None, "MISSING_EQUITY"
    average = (begin + end) / 2.0
    if average <= 0:
        return None, "NEGATIVE_OR_ZERO_EQUITY"
    return profit / average, "VALID"


def last_complete_years(
    values: Iterable[tuple[date, float | None]],
    count: int,
    *,
    anchor_year: int | None = None,
):
    annual = sorted(
        ((period, finite(value)) for period, value in values if period.month == 12 and period.day == 31),
        key=lambda item: item[0],
    )
    if not annual:
        return []
    latest_year = anchor_year if anchor_year is not None else annual[-1][0].year
    first_year = latest_year - count + 1
    return [item for item in annual if first_year <= item[0].year <= latest_year]


def is_consecutive_annual_window(values: list[tuple[date, float | None]], count: int) -> bool:
    if len(values) != count:
        return False
    years = [period.year for period, _ in values]
    return years == list(range(years[-1] - count + 1, years[-1] + 1))


def annual_window_stats(
    parent_profit: Iterable[tuple[date, float | None]],
    operating_cashflow: Iterable[tuple[date, float | None]],
    annual_roe: Iterable[tuple[date, float | None]],
) -> dict:
    parent_values = list(parent_profit)
    annual_profit_periods = [
        period for period, _ in parent_values
        if period.month == 12 and period.day == 31
    ]
    anchor_year = (
        max(annual_profit_periods).year if annual_profit_periods else None
    )
    profits = last_complete_years(parent_values, 5, anchor_year=anchor_year)
    cashflows = last_complete_years(
        operating_cashflow, 5, anchor_year=anchor_year
    )
    roes = last_complete_years(annual_roe, 5, anchor_year=anchor_year)
    complete_profit = (
        is_consecutive_annual_window(profits, 5)
        and all(value is not None for _, value in profits)
    )
    complete_cashflow = (
        is_consecutive_annual_window(cashflows, 5)
        and all(value is not None for _, value in cashflows)
    )
    valid_roes = [value for _, value in roes if value is not None]
    losses = sum(value <= 0 for _, value in profits if value is not None)
    return {
        "profit_observation_years_5y": sum(value is not None for _, value in profits),
        "profit_window_complete": complete_profit,
        "profit_loss_event_5y": losses > 0 if complete_profit else None,
        "loss_year_count_5y": losses if complete_profit else None,
        "profit_positive_years_5y": sum(value > 0 for _, value in profits if value is not None),
        "cashflow_observation_years_5y": sum(value is not None for _, value in cashflows),
        "cashflow_window_complete": complete_cashflow,
        "ocf_positive_years_5y": sum(value > 0 for _, value in cashflows if value is not None),
        "roe_observation_years": len(valid_roes),
        "roe_avg_5y": mean(valid_roes) if valid_roes else None,
        "roe_std_5y": pstdev(valid_roes) if len(valid_roes) > 1 else (0.0 if valid_roes else None),
    }
