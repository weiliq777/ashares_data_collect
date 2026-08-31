from datetime import date, timedelta

import pytest

from data_collect.observation import (
    ObservationPolicy,
    aggregate_sector_rows,
    calculate_scores,
    compute_outcome,
    compute_stock_series,
    confirm_stock_sector,
    hash_rows,
    select_primary_state,
)


def _rows(count=400):
    start = date(2021, 1, 4)
    rows = []
    for index in range(count):
        price = 100.0
        if index >= count - 20:
            price = 100.0 - (index - (count - 20)) * 0.25
        rows.append({
            "trade_date": start + timedelta(days=index),
            "ts_code": "000001.SZ",
            "amount": 200.0 if index >= count - 5 else 100.0,
            "turnover_rate_f": 2.0 if index >= count - 5 else 1.0,
            "volume": 1000.0,
            "adj_close_base": price,
            "total_return_price": price,
            "eligible_by_market": True,
            "is_tradeable": True,
        })
    return rows


def test_stock_metrics_use_non_overlapping_amount_baseline_and_explicit_prices():
    result = compute_stock_series(_rows())[-1]

    assert result["amount_ratio_5_60"] == 2.0
    assert result["turnover_ratio_5_60"] == 2.0
    assert result["amount_ratio_20_60"] is not None
    assert result["return_20d"] is not None
    assert result["return_20d"] < 0
    assert result["total_return_price"] is not None
    assert "MISSING_TOTAL_RETURN_PRICE" not in result["missing_reason_codes"]


def test_insufficient_history_is_missing_not_zero():
    result = compute_stock_series(_rows(30))[-1]

    assert result["amount_ratio_5_60"] is None
    assert result["amount_zscore_60"] is None
    assert result["price_position_52w"] is None
    assert "INSUFFICIENT_AMOUNT_HISTORY" in result["missing_reason_codes"]
    assert "INSUFFICIENT_ADJ_CLOSE_BASE_HISTORY" in result["missing_reason_codes"]


def test_state_priority_and_accumulation_has_no_sector_dependency():
    policy = ObservationPolicy()
    metrics = {
        "anomaly_flag": True,
        "price_position_52w": 0.20,
        "amount_ratio_5_60": 1.8,
        "turnover_ratio_5_60": 1.5,
        "anomaly_frequency_20": 0.70,
        "return_20d": 0.03,
        "max_drawdown_20d": -0.04,
        "up_down_volume_ratio": 1.3,
        "anomaly_streak": 5,
        "distance_ma20": 0.01,
    }
    state, reasons = select_primary_state(metrics, policy)
    assert state == "ACCUMULATION_LIKE"
    assert "LOW_POSITION" in reasons

    panic = dict(metrics, return_20d=-0.20, max_drawdown_20d=-0.20, down_volume_ratio_20=0.8, up_volume_ratio_20=0.2)
    assert select_primary_state(panic, policy)[0] == "PANIC_SELLING"


def test_scores_do_not_impute_unknown_sector_as_positive_or_negative():
    metrics = {
        "amount_ratio_20_60": 2.0,
        "turnover_ratio_20_60": 1.8,
        "amount_zscore_60": 2.5,
        "price_position_52w": 0.2,
        "return_20d": 0.03,
        "price_response_efficiency_20": 0.1,
        "anomaly_frequency_20": 0.7,
        "anomaly_streak": 5,
        "max_drawdown_20d": -0.04,
        "volatility_20d": 0.03,
    }
    scores, missing, priority = calculate_scores(metrics)
    assert scores["sector_confirmation_score"] is None
    assert "SECTOR_CONFIRMATION_MISSING" in missing
    assert priority is not None


def test_sector_unknown_is_distinct_from_not_confirmed():
    policy = ObservationPolicy(min_universe_size=2)
    metrics = {"anomaly_flag": True}
    unknown = confirm_stock_sector(metrics, None, policy)
    assert unknown["sector_confirmation_status"] == "UNKNOWN"

    sector = {
        "sector_data_status": "CONFIRMED",
        "eligible_stock_count": 2,
        "sector_anomaly_breadth": 0.0,
        "sector_new_anomaly_ratio": 0.0,
        "sector_persistence": None,
    }
    not_confirmed = confirm_stock_sector(metrics, sector, policy)
    assert not_confirmed["sector_confirmation_status"] == "NOT_CONFIRMED"


def test_sector_denominator_is_eligible_universe_only():
    rows = [
        {"observation_date": date(2026, 8, 31), "industry_system": "SW2021", "industry_level": 1, "industry_code": "A", "industry_name": "行业A", "eligible_by_market": True, "anomaly_flag": True, "anomaly_streak": 1, "sector_data_status": "CONFIRMED"},
        {"observation_date": date(2026, 8, 31), "industry_system": "SW2021", "industry_level": 1, "industry_code": "A", "industry_name": "行业A", "eligible_by_market": True, "anomaly_flag": False, "anomaly_streak": 0, "sector_data_status": "CONFIRMED"},
        {"observation_date": date(2026, 8, 31), "industry_system": "SW2021", "industry_level": 1, "industry_code": "A", "industry_name": "行业A", "eligible_by_market": False, "anomaly_flag": True, "anomaly_streak": 1, "sector_data_status": "CONFIRMED"},
    ]
    result = aggregate_sector_rows(rows, ObservationPolicy(min_universe_size=2))[0]
    assert result["eligible_stock_count"] == 2
    assert result["anomaly_stock_count"] == 1
    assert result["sector_anomaly_breadth"] == 0.5


def test_outcome_keeps_future_strategy_semantics_out():
    rows = [{"trade_date": date(2026, 8, 31), "ts_code": "000001.SZ", "adj_close_base": 100.0, "total_return_price": 100.0}]
    rows.extend({"trade_date": date(2026, 9, 1) + timedelta(days=i), "ts_code": "000001.SZ", "adj_close_base": 100.0 + i + 1, "total_return_price": 100.0 + i + 1} for i in range(120))
    result = compute_outcome(rows)
    assert result["future_price_return_5d"] == pytest.approx(0.05)
    assert result["future_total_return_5d"] == pytest.approx(0.05)
    assert "entry_price" not in result
    assert "hit_target_before_stop" not in result


def test_outcome_does_not_compress_missing_future_path():
    rows = [
        {"trade_date": date(2026, 8, 31), "ts_code": "000001.SZ", "adj_close_base": 100.0, "total_return_price": 100.0},
        {"trade_date": date(2026, 9, 1), "ts_code": "000001.SZ", "adj_close_base": 101.0, "total_return_price": 101.0},
        {"trade_date": date(2026, 9, 2), "ts_code": "000001.SZ", "adj_close_base": None, "total_return_price": None},
    ]
    rows.extend(
        {"trade_date": date(2026, 9, 3) + timedelta(days=i), "ts_code": "000001.SZ", "adj_close_base": 103.0 + i, "total_return_price": 103.0 + i}
        for i in range(5)
    )
    result = compute_outcome(rows)
    assert result["future_price_return_5d"] is not None
    assert result["max_drawdown_5d"] is None
    assert "MISSING_PRICE_PATH_5D" in result["missing_reason_codes"]


def test_hash_is_streaming_and_order_deterministic():
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    digest, count = hash_rows((row for row in rows), columns=("a", "b"))
    same, same_count = hash_rows(rows, columns=("a", "b"))
    different, _ = hash_rows(reversed(rows), columns=("a", "b"))
    assert (digest, count) == (same, same_count)
    assert digest != different
