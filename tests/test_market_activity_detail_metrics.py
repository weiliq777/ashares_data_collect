from datetime import date, timedelta

import pytest

from data_collect.market_activity.detail_metrics import (
    build_activity_ratio_series,
    build_price_behavior,
)


def test_activity_ratio_uses_exactly_the_preceding_twenty_bars():
    start = date(2026, 1, 1)
    bars = [
        {
            "trade_date": start + timedelta(days=offset),
            "volume": 10,
            "amount": 100,
        }
        for offset in range(20)
    ]
    target = start + timedelta(days=20)
    bars.append({"trade_date": target, "volume": 30, "amount": 300})

    points = build_activity_ratio_series(bars, [target], lookback=20)

    assert len(points) == 1
    assert points[0].trade_date == target
    assert points[0].history_observation_count == 20
    assert points[0].volume_ratio_previous == pytest.approx(3.0)
    assert points[0].amount_ratio_previous == pytest.approx(3.0)
    assert points[0].calculation_status == "VALID"


def test_missing_market_date_is_not_fabricated_and_next_date_uses_prior_valid_bars():
    start = date(2026, 2, 1)
    bars = [
        {
            "trade_date": start + timedelta(days=offset),
            "volume": 10,
            "amount": 100,
        }
        for offset in range(20)
    ]
    first_target = start + timedelta(days=20)
    next_target = start + timedelta(days=22)
    bars.extend([
        {"trade_date": first_target, "volume": 30, "amount": 300},
        {"trade_date": next_target, "volume": 40, "amount": 400},
    ])

    points = build_activity_ratio_series(
        bars, [first_target, first_target + timedelta(days=1), next_target], lookback=20,
    )

    assert points[0].volume_ratio_previous == pytest.approx(3.0)
    assert points[1].calculation_status == "NO_STOCK_BAR"
    assert points[1].volume is None
    assert points[1].volume_ratio_previous is None
    assert points[2].history_observation_count == 20
    assert points[2].volume_ratio_previous == pytest.approx(40 / 11)
    assert points[2].amount_ratio_previous == pytest.approx(40 / 11)


def test_invalid_history_bar_does_not_count_toward_the_twenty_observations():
    start = date(2026, 3, 1)
    bars = [
        {
            "trade_date": start + timedelta(days=offset),
            "volume": -1 if offset == 5 else 10,
            "amount": 100,
        }
        for offset in range(20)
    ]
    target = start + timedelta(days=20)
    bars.append({"trade_date": target, "volume": 30, "amount": 300})

    point = build_activity_ratio_series(bars, [target], lookback=20)[0]

    assert point.history_observation_count == 19
    assert point.calculation_status == "INSUFFICIENT_HISTORY"
    assert point.volume_ratio_previous is None
    assert point.amount_ratio_previous is None


def test_price_behavior_keeps_raw_candle_geometry_separate_from_feature_return():
    result = build_price_behavior(
        {
            "trade_date": date(2026, 4, 1),
            "open": 10.4,
            "high": 10.8,
            "low": 10.2,
            "close": 10.5,
        },
        10.4,
        {
            "return_1d": 0.01,
            "adjustment_mode": "point_in_time_total_return",
            "source_asof_date": date(2026, 4, 1),
            "quality_status": "VALID",
        },
    )

    assert result["status"] == "AVAILABLE"
    assert result["bar"]["close"] == 10.5
    assert result["return_1d"] == 0.01
    assert result["return_1d_adjustment_mode"] == "point_in_time_total_return"
    assert result["amplitude_pct_raw_price"] == pytest.approx(0.6 / 10.4 * 100)
    assert result["close_position"] == pytest.approx(0.5)
    assert "CURRENT_STANDARD_CONTEXT" in result["source_semantics"]
