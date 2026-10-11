"""Pure, bounded metrics used to enrich one-stock detail views."""
from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
import math


MAX_DETAIL_TRADE_DATES = 10
MAX_DETAIL_BARS = 250


@dataclass(frozen=True, slots=True)
class ActivityRatioPoint:
    """One stock's amount/volume ratios for a requested market date."""

    trade_date: date
    volume: float | None
    amount: float | None
    volume_ratio_previous: float | None
    amount_ratio_previous: float | None
    history_observation_count: int
    calculation_status: str


def build_activity_ratio_series(
    bars: Iterable[Mapping[str, object]],
    trade_dates: Sequence[date],
    *,
    lookback: int = 20,
) -> tuple[ActivityRatioPoint, ...]:
    """Calculate bounded per-stock ratios using preceding valid bars only.

    Missing market dates are returned as ``NO_STOCK_BAR`` rather than being
    filled with zero. A bar contributes to the lookback only when both volume
    and amount are finite and non-negative, matching the Market Activity
    calculation contract. The target bar is evaluated before it is appended
    to its own history.
    """
    if not 1 <= lookback <= 120:
        raise ValueError("lookback must be in 1..120")
    requested = tuple(trade_dates)
    if not requested or len(requested) > MAX_DETAIL_TRADE_DATES:
        raise ValueError(
            f"trade_dates must contain 1..{MAX_DETAIL_TRADE_DATES} dates"
        )
    if any(isinstance(day, datetime) or not isinstance(day, date) for day in requested):
        raise TypeError("trade_dates must contain datetime.date values")
    if len(set(requested)) != len(requested):
        raise ValueError("trade_dates must be distinct")
    targets = tuple(sorted(requested))
    target_set = set(targets)

    parsed: dict[date, tuple[float | None, float | None]] = {}
    for row in bars:
        if len(parsed) >= MAX_DETAIL_BARS:
            raise ValueError(f"bars exceed the detail cap of {MAX_DETAIL_BARS}")
        day = _as_date(row.get("trade_date"))
        if day > targets[-1]:
            raise ValueError("bars contain a date after the latest requested date")
        if day in parsed:
            raise ValueError(f"duplicate stock daily bar for {day.isoformat()}")
        parsed[day] = (_valid_measure(row.get("volume")), _valid_measure(row.get("amount")))

    history: deque[tuple[float, float]] = deque(maxlen=lookback)
    points: dict[date, ActivityRatioPoint] = {}
    for day in sorted(parsed):
        volume, amount = parsed[day]
        valid = volume is not None and amount is not None
        if day in target_set:
            count = len(history)
            if not valid:
                point = ActivityRatioPoint(
                    day, volume, amount, None, None, count, "INVALID_BAR",
                )
            elif count < lookback:
                point = ActivityRatioPoint(
                    day, volume, amount, None, None, count, "INSUFFICIENT_HISTORY",
                )
            else:
                previous_volume = sum(item[0] for item in history) / lookback
                previous_amount = sum(item[1] for item in history) / lookback
                point = ActivityRatioPoint(
                    day,
                    volume,
                    amount,
                    _ratio(volume, previous_volume),
                    _ratio(amount, previous_amount),
                    count,
                    "VALID",
                )
            points[day] = point
        if valid:
            history.append((volume, amount))

    for day in targets:
        if day in points:
            continue
        prior_valid_count = sum(
            1 for bar_day, measures in parsed.items()
            if bar_day < day and measures[0] is not None and measures[1] is not None
        )
        points[day] = ActivityRatioPoint(
            day, None, None, None, None, min(prior_valid_count, lookback),
            "NO_STOCK_BAR",
        )
    return tuple(points[day] for day in targets)


def build_price_behavior(
    bar: Mapping[str, object] | None,
    previous_close: object,
    return_feature: Mapping[str, object] | None,
) -> dict[str, object]:
    """Describe one current Standard candle without changing frozen facts."""
    if bar is None:
        return {
            "status": "NO_STANDARD_DAILY_BAR",
            "bar": None,
            "return_1d": None,
            "amplitude_pct_raw_price": None,
            "close_position": None,
            "source_semantics": "CURRENT_STANDARD_CONTEXT",
        }

    prices = {name: _positive_number(bar.get(name)) for name in ("open", "high", "low", "close")}
    open_price, high, low, close = (prices[name] for name in ("open", "high", "low", "close"))
    valid_candle = (
        all(value is not None for value in prices.values())
        and high >= low
        and low <= close <= high
        and low <= open_price <= high
    )
    if not valid_candle:
        return {
            "status": "INVALID_OHLC",
            "bar": {name: bar.get(name) for name in ("trade_date", "open", "high", "low", "close")},
            "return_1d": None,
            "return_1d_quality_status": "NOT_USED_INVALID_OHLC",
            "return_1d_adjustment_mode": None,
            "amplitude_pct_raw_price": None,
            "amplitude_status": "INVALID_OHLC",
            "close_position": None,
            "close_position_status": "INVALID_OHLC",
            "source_semantics": "CURRENT_STANDARD_CONTEXT",
        }

    prior = _positive_number(previous_close)
    width = high - low
    amplitude = width / prior * 100.0 if prior is not None else None
    close_position = (close - low) / width if width > 0 else None
    feature_return = _finite_number(return_feature.get("return_1d")) if return_feature else None
    return {
        "status": "AVAILABLE",
        "bar": {name: bar.get(name) for name in ("trade_date", "open", "high", "low", "close")},
        "return_1d": feature_return,
        "return_1d_quality_status": (
            return_feature.get("quality_status") if return_feature else "NOT_AVAILABLE"
        ),
        "return_1d_adjustment_mode": (
            return_feature.get("adjustment_mode") if return_feature else None
        ),
        "return_1d_source_asof_date": (
            return_feature.get("source_asof_date") if return_feature else None
        ),
        "amplitude_pct_raw_price": amplitude,
        "amplitude_status": "RAW_PRICE_REFERENCE" if amplitude is not None else "INSUFFICIENT_REFERENCE",
        "close_position": close_position,
        "close_position_status": "AVAILABLE" if close_position is not None else "ZERO_RANGE",
        "source_semantics": "CURRENT_STANDARD_CONTEXT; OHLC may reflect post-baseline corrections",
    }


def _as_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    raise TypeError("bar trade_date must be a date, datetime, or ISO date string")


def _valid_measure(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _positive_number(value: object) -> float | None:
    number = _finite_number(value)
    return number if number is not None and number > 0 else None


def _finite_number(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _ratio(value: float, baseline: float) -> float | None:
    return value / baseline if baseline > 0 else None
