from datetime import date, datetime, timedelta, timezone

import pytest

from data_collect.observation import (
    ObservationPolicy,
    aggregate_sector_rows,
    calculate_scores,
    compute_outcome,
    compute_stock_point,
    compute_stock_series,
    confirm_stock_sector,
    hash_rows,
    select_primary_state,
)
from data_collect.jobs.market_anomaly_observation import _calculation_status, _stable_run_id
from data_collect.data_access.repository.observation_repository import ObservationEventRepository


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


def test_target_point_optimizer_matches_series_semantics():
    rows = _rows(300)
    expected = compute_stock_series(rows)[-1]
    actual = compute_stock_point(rows)
    assert actual is not None
    for key in (
        "amount_ratio_5_60", "amount_ratio_20_60", "turnover_ratio_5_60",
        "turnover_ratio_20_60", "amount_zscore_60", "amount_single_ratio_60",
        "return_1d", "return_5d", "return_20d", "price_position_52w",
        "distance_ma20", "distance_ma60", "max_drawdown_20d",
        "up_volume_ratio_20", "down_volume_ratio_20", "up_down_volume_ratio",
        "price_response_efficiency_20", "anomaly_frequency_20", "volatility_20d",
    ):
        assert actual[key] == pytest.approx(expected[key])
    for key in (
        "observation_date", "ts_code", "anomaly_flag", "anomaly_streak",
        "days_since_first_anomaly", "abnormal_days_20", "missing_reason_codes",
    ):
        assert actual[key] == expected[key]


def test_long_anomaly_streak_is_not_capped_at_frequency_window():
    rows = _rows(420)
    for row in rows[-30:]:
        row["amount"] = 1_000.0
    result = compute_stock_series(rows)[-1]

    assert result["anomaly_streak"] >= 30
    assert result["days_since_first_anomaly"] >= 29
    assert 0.0 <= result["anomaly_frequency_20"] <= 1.0


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


def test_optional_sector_missing_does_not_make_core_calculation_partial():
    assert _calculation_status(("SECTOR_CONFIRMATION_MISSING",)) == "VALID"
    assert _calculation_status(("INSUFFICIENT_AMOUNT_HISTORY",)) == "PARTIAL"


def test_observation_default_run_identity_is_stable():
    policy = ObservationPolicy()
    first = _stable_run_id(date(2026, 8, 1), date(2026, 8, 31), "baseline", policy)
    second = _stable_run_id(date(2026, 8, 1), date(2026, 8, 31), "baseline", policy)
    assert first == second
    assert first.startswith("observation-")


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
    start = date(2026, 8, 31)
    sessions = []
    cursor = start
    while len(sessions) <= 120:
        if cursor.weekday() < 5:
            sessions.append(cursor)
        cursor += timedelta(days=1)
    event = {
        "observation_id": "11111111-1111-4111-8111-111111111111",
        "snapshot_id": "22222222-2222-4222-8222-222222222222",
        "observation_date": start,
        "ts_code": "000001.SZ",
        "methodology_hash": "c" * 64,
        "data_baseline_id": "market-activity-test",
        "input_manifest_hash": "a" * 64,
        "observation_hash": "d" * 64,
        "anchor_adj_close_base": 100.0,
        "anchor_total_return_price": 100.0,
        "anchor_benchmark_close": 200.0,
        "anchor_quality_status": "VALID",
        "anchor_source_asof_date": start,
        "anchor_price_coordinate_contract_version": "market-price-coordinate-v1",
        "anchor_source_hash": "b" * 64,
    }
    stock_rows = [
        {
            "trade_date": sessions[index], "ts_code": "000001.SZ",
            "adj_close_base": 100.0 + index,
            "total_return_price": 100.0 + index,
            "quality_status": "VALID", "source_asof_date": sessions[index],
            "price_coordinate_contract_version": "market-price-coordinate-v1",
        }
        for index in range(1, 121)
    ]
    benchmark_rows = [
        {"trade_date": sessions[index], "ts_code": "000985.CSI", "close": 200.0}
        for index in range(1, 121)
    ]
    result = compute_outcome(
        event, calendar_dates=sessions, stock_rows=stock_rows,
        benchmark_rows=benchmark_rows, through_date=sessions[-1],
    )
    assert result["future_price_return_5d"] == pytest.approx(0.05)
    assert result["future_total_return_5d"] == pytest.approx(0.05)
    assert "entry_price" not in result
    assert "hit_target_before_stop" not in result


def test_outcome_does_not_compress_missing_future_path():
    start = date(2026, 8, 31)
    sessions = []
    cursor = start
    while len(sessions) <= 120:
        if cursor.weekday() < 5:
            sessions.append(cursor)
        cursor += timedelta(days=1)
    event = {
        "observation_id": "11111111-1111-4111-8111-111111111111",
        "snapshot_id": "22222222-2222-4222-8222-222222222222",
        "observation_date": start,
        "ts_code": "000001.SZ",
        "methodology_hash": "c" * 64,
        "data_baseline_id": "market-activity-test",
        "input_manifest_hash": "a" * 64,
        "observation_hash": "d" * 64,
        "anchor_adj_close_base": 100.0,
        "anchor_total_return_price": 100.0,
        "anchor_benchmark_close": 200.0,
        "anchor_quality_status": "VALID",
        "anchor_source_asof_date": start,
        "anchor_price_coordinate_contract_version": "market-price-coordinate-v1",
        "anchor_source_hash": "b" * 64,
    }
    stock_rows = [
        {
            "trade_date": sessions[index], "ts_code": "000001.SZ",
            "adj_close_base": (None if index == 2 else 100.0 + index),
            "total_return_price": (None if index == 2 else 100.0 + index),
            "quality_status": "VALID", "source_asof_date": sessions[index],
            "price_coordinate_contract_version": "market-price-coordinate-v1",
        }
        for index in range(1, 7)
    ]
    benchmark_rows = [
        {"trade_date": sessions[index], "ts_code": "000985.CSI", "close": 200.0}
        for index in range(1, 7)
    ]
    result = compute_outcome(
        event, calendar_dates=sessions, stock_rows=stock_rows,
        benchmark_rows=benchmark_rows, through_date=sessions[5],
    )
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


def test_event_visibility_validation_uses_asia_shanghai_business_date():
    row = {
        "observation_date": date(2026, 8, 30),
        "event_visible_at": datetime(2026, 8, 30, 16, 30, tzinfo=timezone.utc),
        "source_record_id": "event-1",
        "source_hash": "hash-1",
    }
    with pytest.raises(ValueError, match="AS_OF event"):
        ObservationEventRepository._validate_event(row, retrospective=False)
