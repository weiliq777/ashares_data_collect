from datetime import date, timedelta

import pytest

from data_collect.alpha_research.contracts import EVALUATION_VERSION, FactorSpec
from data_collect.alpha_research.evaluation import (
    classify_market_stages,
    evaluate_cross_section,
    summarize_evaluations,
)
from data_collect.alpha_research.factors import FactorValue


def _rows(count=10):
    rows = []
    for index in range(count):
        rows.append({
            "ts_code": f"{index:06d}.SZ",
            "label_status": "COMPLETE",
            "prediction_total_return": index / 100,
            "total_mv": 1_000_000 * (index + 1),
            "factor_values": {
                "factor": FactorValue("factor", float(index), "VALID"),
            },
        })
    return rows


def test_rank_ic_five_groups_and_direction_adjustment_are_cross_sectional():
    factor = FactorSpec("factor", "test", ("x",), -1, "test")
    rows = _rows()
    result = evaluate_cross_section(
        date(2025, 1, 2), 20, rows, factor_specs=(factor,),
    )[0]
    assert result["rank_ic"] == 1.0
    assert result["oriented_rank_ic"] == -1.0
    assert [item["n_stocks"] for item in result["group_returns"]] == [2, 2, 2, 2, 2]
    assert result["group_spread_q5_q1"] > 0
    assert result["oriented_group_spread_q5_q1"] < 0
    assert result["industry_control_status"].startswith("NOT AVAILABLE")


def test_incomplete_labels_and_missing_factor_values_are_counted():
    factor = FactorSpec("factor", "test", ("x",), 1, "test")
    rows = _rows(5)
    rows[-1]["label_status"] = "INCOMPLETE"
    rows[-2]["factor_values"]["factor"] = FactorValue("factor", None, "MISSING", "NO_VALUE")
    result = evaluate_cross_section(date(2025, 1, 2), 20, rows, factor_specs=(factor,))[0]
    assert result["n_total"] == 5
    assert result["n_label_complete"] == 4
    assert result["n_stocks"] == 3
    assert result["n_missing_pairs"] == 2
    assert result["factor_exclusion_reasons"] == {"NO_VALUE": 1}


def test_moving_block_interval_is_reproducible_and_preserves_missing_dates():
    factor = FactorSpec("factor", "test", ("x",), 1, "test")
    start = date(2024, 1, 1)
    dates = [start + timedelta(days=index) for index in range(120)]
    daily = []
    for index, day in enumerate(dates):
        if index == 50:
            continue
        daily.append({
            "factor": "factor",
            "horizon": 20,
            "signal_date": day.isoformat(),
            "year": day.year,
            "n_stocks": 100,
            "rank_ic": (index % 7) / 10,
            "oriented_rank_ic": (index % 7) / 10,
            "group_spread_q5_q1": 0.02,
            "oriented_group_spread_q5_q1": 0.02,
            "size_adjusted_rank_ic": 0.1,
        })
    one = summarize_evaluations(
        daily, horizons=(20,), expected_signal_dates=dates,
        factor_specs=(factor,), bootstrap_repetitions=100,
        min_bootstrap_dates=60, seed=13,
    )["factors"][0]
    two = summarize_evaluations(
        daily, horizons=(20,), expected_signal_dates=dates,
        factor_specs=(factor,), bootstrap_repetitions=100,
        min_bootstrap_dates=60, seed=13,
    )["factors"][0]
    assert one["n_dates"] == 120
    assert one["n_valid_dates"] == 119
    assert one["n_missing_dates"] == 1
    assert one["rank_ic_confidence_interval_95"] == two["rank_ic_confidence_interval_95"]
    assert len(one["annual_stability"]) == 1


def test_market_stage_uses_frozen_trailing_benchmark_window_only():
    calendar = tuple(date(2025, 1, 1) + timedelta(days=index) for index in range(63))
    closes = {day: 100 + index for index, day in enumerate(calendar)}
    closes[calendar[61]] = 90
    stages = classify_market_stages(
        (calendar[1], calendar[60], calendar[61], calendar[62]),
        calendar=calendar,
        benchmark_closes=closes,
        lookback=60,
    )
    assert stages[calendar[1]]["stage"] == "UNKNOWN"
    assert stages[calendar[60]]["stage"] == "UPTREND"
    assert stages[calendar[61]]["stage"] == "DOWNTREND"
    assert stages[calendar[62]]["stage"] == "UPTREND"


def test_evaluation_report_version_matches_run_identity_contract():
    result = summarize_evaluations([], horizons=(20,), factor_specs=())

    assert result["evaluation_version"] == EVALUATION_VERSION


def test_summary_reports_equally_weighted_daily_returns_for_each_factor_group():
    factor = FactorSpec("factor", "test", ("x",), 1, "test")
    signal_date = date(2025, 1, 2)
    daily_row = evaluate_cross_section(signal_date, 20, _rows(10), factor_specs=(factor,))[0]
    summary = summarize_evaluations(
        [daily_row], horizons=(20,), expected_signal_dates=(signal_date,),
        factor_specs=(factor,), bootstrap_repetitions=100,
        min_bootstrap_dates=2,
    )["factors"][0]

    groups = summary["group_return_summary"]
    assert len(groups) == 5
    assert [group["group"] for group in groups] == [1, 2, 3, 4, 5]
    assert [group["mean_stocks_per_date"] for group in groups] == [2.0] * 5
    assert groups[0]["mean_daily_cross_sectional_return"] == 0.005
    assert groups[-1]["mean_daily_cross_sectional_return"] == pytest.approx(0.085)
