from datetime import date, timedelta

import pytest

from data_collect.backtest.strategy.contract import EvaluationConfig
from data_collect.evaluation.standard import (
    build_capital_sensitivity,
    build_standard_evaluation,
    evaluate_calendar_periods,
    evaluate_market_regimes,
    evaluate_rolling_windows,
)


def _performance(values):
    rows = []
    previous = None
    for index, equity in enumerate(values):
        trade_date = date(2024, 1, 1) + timedelta(days=index)
        daily_return = None if previous is None else equity / previous - 1
        rows.append({
            "trade_date": trade_date,
            "total_equity": equity,
            "daily_return": daily_return,
            "cash_balance": equity / 2,
        })
        previous = equity
    return rows


def test_evaluation_config_freezes_standard_analysis_identity():
    config = EvaluationConfig()
    assert config.calendar_periods == ("month", "quarter", "year")
    assert config.rolling_window_trading_days == 252
    assert config.rolling_step_trading_days == 21
    assert config.market_regime_definition == "BENCHMARK_MA120_SLOPE_V1"
    assert config.market_regime_ma_window == 120


def test_calendar_evaluation_has_strategy_benchmark_and_excess_returns():
    performance = _performance([100, 110, 99])
    benchmark = [
        {"trade_date": date(2024, 1, 1), "close": 100},
        {"trade_date": date(2024, 1, 2), "close": 105},
        {"trade_date": date(2024, 1, 3), "close": 110},
    ]
    rows = evaluate_calendar_periods(performance, benchmark, "month")
    assert len(rows) == 1
    assert rows[0]["strategy_return"] == pytest.approx(-0.01)
    assert rows[0]["benchmark_return"] == pytest.approx(0.10)
    assert rows[0]["excess_return"] == pytest.approx(-0.11)
    assert rows[0]["max_drawdown"] == pytest.approx(0.10)


def test_rolling_evaluation_uses_bounded_trading_day_windows():
    performance = _performance([100, 101, 102, 103, 104])
    benchmark = [
        {"trade_date": row["trade_date"], "close": 100 + index}
        for index, row in enumerate(performance)
    ]
    rows = evaluate_rolling_windows(performance, benchmark, window_trading_days=3, step_trading_days=1)
    assert len(rows) == 3
    assert rows[0]["window_start"] == "2024-01-01"
    assert rows[0]["window_end"] == "2024-01-03"
    assert rows[0]["strategy_return"] == pytest.approx(0.02)


def test_market_regime_marks_insufficient_history_unknown_and_does_not_fill_sideways():
    benchmark = [
        {"trade_date": date(2024, 1, 1) + timedelta(days=index), "close": close}
        for index, close in enumerate([1, 1, 1, 2, 3, 4])
    ]
    result = evaluate_market_regimes(_performance([100] * 6), benchmark, ma_window=3)
    assert result["daily"][0]["regime"] == "UNKNOWN"
    assert result["daily"][2]["regime"] == "UNKNOWN"
    assert result["daily"][3]["regime"] == "BULL"
    assert any(row["regime"] == "BULL" for row in result["summary"])


def _capital_report(capital, run_id):
    return {
        "run": {
            "run_id": run_id,
            "dataset_id": "dataset-1",
            "strategy_id": "strategy-1",
            "strategy_version": "1.0.0",
            "input_hash": "input-1",
            "execution_model_version": "engine-1",
            "initial_cash": capital,
            "status": "SUCCEEDED",
            "parameters": {
                "evaluation_hash": "eval-1",
                "scenario": {
                    "scenario_id": f"CAP_{int(capital)}",
                    "initial_capital": capital,
                    "target_positions": 3,
                },
            },
        },
        "scope": {"signal_date_from": "2024-01-01", "signal_date_to": "2024-01-31"},
        "persisted_metrics": {
            "final_equity": capital * 1.1,
            "total_return": 0.1,
            "cagr": 0.1,
            "max_drawdown": -0.05,
            "sharpe": 1.0,
        },
        "row_counts": {"orders": 10, "fills": 8},
        "order_rejections": {"total_orders": 10, "filled_orders": 8, "rejected_orders": 2},
        "trade_summary": {"trade_count": 4},
        "equity_curve": [{"cash_ratio": 0.4}, {"cash_ratio": 0.5}],
    }


def test_capital_sensitivity_requires_same_experiment_identity_and_reports_missing_capital():
    result = build_capital_sensitivity([
        _capital_report(10_000, "run-10k"),
        _capital_report(30_000, "run-30k"),
    ])
    assert result["comparable"] is True
    assert result["rows"][0]["final_equity"] == 11_000.0
    assert result["rows"][1]["run_id"] == "run-30k"
    assert result["rows"][2]["status"] == "MISSING"

    incompatible = _capital_report(50_000, "run-bad")
    incompatible["run"]["input_hash"] = "different"
    with pytest.raises(ValueError, match="不可比较"):
        build_capital_sensitivity([_capital_report(10_000, "run-10k"), incompatible])


def test_capital_sensitivity_normalizes_legacy_evaluation_metadata():
    legacy = _capital_report(10_000, "run-legacy")
    legacy["run"]["parameters"] = {
        "evaluation_hash": "legacy-hash",
        "scenario": {
            "scenario_id": "legacy-10k",
            "initial_capital": 10_000,
            "target_positions": 3,
        },
    }
    current = _capital_report(30_000, "run-current")
    current["run"]["parameters"]["evaluation"] = {
        "calendar_periods": ["month", "quarter", "year"],
        "rolling_window_trading_days": 252,
        "rolling_step_trading_days": 21,
        "market_regime_definition": "BENCHMARK_MA120_SLOPE_V1",
        "market_regime_ma_window": 120,
    }
    current["scope"] = {"signal_date_from": "2024-02-01", "signal_date_to": "2024-02-29"}
    result = build_capital_sensitivity([legacy, current])
    assert result["comparable"] is True


def test_capital_sensitivity_rejects_different_execution_engine():
    baseline = _capital_report(10_000, "run-10k")
    changed_engine = _capital_report(30_000, "run-30k")
    changed_engine["run"]["execution_model_version"] = "engine-2"
    with pytest.raises(ValueError, match="不可比较"):
        build_capital_sensitivity([baseline, changed_engine])


def test_standard_evaluation_contains_all_fixed_dimensions():
    result = build_standard_evaluation(_performance([100, 101]), [])
    assert set(result["calendar"]) == {"month", "quarter", "year"}
    assert result["rolling_12m"] == []
    assert result["market_regime"]["definition"] == "BENCHMARK_MA120_SLOPE_V1"
