from datetime import date

import pytest

from data_collect.jobs.backtest_diagnostics import (
    aggregate_period_returns,
    build_report_scope,
    build_entry_diagnostics,
    build_equity_curve,
    build_trade_records,
    classify_rejection,
    summarize_exit_attribution,
    summarize_forward_returns,
    summarize_rejections,
    summarize_trades,
)


def test_report_scope_separates_evaluation_window_from_signal_coverage():
    run = {
        "parameters": {
            "evaluation": {
                "evaluation_start": "2021-09-01",
                "evaluation_end": "2026-08-21",
            }
        }
    }
    scope = build_report_scope(
        run,
        [{"signal_date": date(2021, 11, 5)}, {"signal_date": date(2023, 9, 8)}],
        [{"trade_date": date(2021, 9, 1)}, {"trade_date": date(2026, 8, 21)}],
    )
    assert scope["evaluation_date_from"] == "2021-09-01"
    assert scope["evaluation_date_to"] == "2026-08-21"
    assert scope["signal_date_from"] == "2021-11-05"
    assert scope["signal_date_to"] == "2023-09-08"


def _trade_fixture():
    signals = [{
        "signal_id": "signal-buy",
        "research_snapshot_id": "snapshot-1",
        "ts_code": "000001.SZ",
        "signal_date": date(2024, 1, 5),
        "direction": "BUY",
        "reason": {"ret60": 0.25, "rank": 2, "avg_amount_20": 60_000_000},
    }, {
        "signal_id": "signal-sell",
        "research_snapshot_id": "snapshot-2",
        "ts_code": "000001.SZ",
        "signal_date": date(2024, 1, 22),
        "direction": "SELL",
        "reason": {"rule": "close_below_ma60"},
    }]
    orders = [{
        "order_id": "order-buy", "signal_id": "signal-buy", "ts_code": "000001.SZ",
        "direction": "BUY", "status": "FILLED",
    }, {
        "order_id": "order-sell", "signal_id": "signal-sell", "ts_code": "000001.SZ",
        "direction": "SELL", "status": "FILLED", "stop_loss_price_raw": None,
    }]
    fills = [{
        "fill_id": "fill-buy", "order_id": "order-buy", "ts_code": "000001.SZ",
        "fill_date": date(2024, 1, 8), "direction": "BUY", "fill_price": 10,
        "fill_shares": 100, "commission": 5, "stamp_duty": 0, "transfer_fee": 0,
    }, {
        "fill_id": "fill-sell", "order_id": "order-sell", "ts_code": "000001.SZ",
        "fill_date": date(2024, 1, 22), "direction": "SELL", "fill_price": 12,
        "fill_shares": 100, "commission": 5, "stamp_duty": 1.2, "transfer_fee": 0,
    }]
    lots = [{
        "lot_id": "lot-1", "ts_code": "000001.SZ", "open_fill_id": "fill-buy",
        "open_shares": 100, "remaining_shares": 0, "cost_price": 10,
        "close_fill_id": "fill-sell", "accrued_cash_dividend_per_share": 0,
    }]
    return signals, orders, fills, lots


def test_rejection_classifier_keeps_fill_separate_from_rejected_orders():
    assert classify_rejection({"status": "FILLED"}) == "FILLED"
    assert classify_rejection({"status": "REJECTED", "reject_reason": "向下取整后不足一手"}) == "LOT_OR_CASH_LIMIT"
    assert classify_rejection({"status": "REJECTED", "reject_reason": "达到最大持仓数"}) == "MAX_POSITIONS"
    assert classify_rejection({"status": "REJECTED", "reject_reason": "T+1 开盘按实际价格重算仓位失败"}) == "EXECUTION_RECALCULATION"


def test_trade_reconstruction_includes_net_costs_and_exit_rule():
    signals, orders, fills, lots = _trade_fixture()
    trades = build_trade_records(
        lots, fills, orders, signals,
        [date(2024, 1, 8), date(2024, 1, 9), date(2024, 1, 10), date(2024, 1, 22)],
    )
    assert len(trades) == 1
    trade = trades[0]
    assert trade["buy_signal_id"] == "signal-buy"
    assert trade["sell_signal_id"] == "signal-sell"
    assert trade["gross_pnl"] == 200.0
    assert trade["net_pnl"] == 188.8
    assert trade["exit_reason"] == "MA60_EXIT"
    assert trade["entry_ret60"] == 0.25


def test_rejection_trade_and_exit_summaries_are_stable():
    orders = [
        {"status": "FILLED"},
        {"status": "REJECTED", "reject_reason": "现金上限"},
        {"status": "REJECTED", "reject_reason": "现金上限"},
    ]
    summary = summarize_rejections(orders)
    assert summary["total_orders"] == 3
    assert summary["filled_orders"] == 1
    assert summary["rejected_orders"] == 2
    assert summary["by_category"]["LOT_OR_CASH_LIMIT"] == 2

    signals, orders, fills, lots = _trade_fixture()
    trades = build_trade_records(lots, fills, orders, signals)
    assert summarize_trades(trades)["trade_count"] == 1
    assert summarize_exit_attribution(trades)["MA60_EXIT"]["trade_count"] == 1


def test_forward_returns_use_future_execution_closes_not_position_state():
    signals, orders, fills, lots = _trade_fixture()
    trades = build_trade_records(lots, fills, orders, signals)
    future = [
        {"ts_code": "000001.SZ", "buy_date": date(2024, 1, 8), "trade_date": date(2024, 1, 15), "raw_close": 11, "horizon": 5},
        {"ts_code": "000001.SZ", "buy_date": date(2024, 1, 8), "trade_date": date(2024, 2, 5), "raw_close": 9, "horizon": 20},
        {"ts_code": "000001.SZ", "buy_date": date(2024, 1, 8), "trade_date": date(2024, 4, 1), "raw_close": 8, "horizon": 60},
    ]
    report = summarize_forward_returns(trades, future)
    assert report["horizon_5d"]["returns"]["count"] == 1
    assert report["horizon_5d"]["returns"]["mean"] == 0.1
    assert report["horizon_20d"]["returns"]["mean"] == -0.1
    assert report["horizon_60d"]["returns"]["mean"] == -0.2


def test_entry_diagnostics_flags_historical_st_only_when_pit_says_true():
    signals, orders, fills, lots = _trade_fixture()
    trades = build_trade_records(lots, fills, orders, signals)
    # build_trade_records exposes the opening signal id after the report's
    # enrichment step; this mirrors the persisted signal-key contract.
    diagnostics = build_entry_diagnostics(
        trades,
        {row["signal_id"]: row for row in signals},
        [{"signal_id": "signal-buy", "is_st_asof": False, "quality_status": "VALID", "snapshot_id": "snapshot-1"}],
    )
    assert diagnostics["historical_st_violations"] == []
    assert diagnostics["entries"][0]["is_st_asof"] is False


def test_equity_curve_contains_benchmark_cash_and_position_utilization():
    performance = [
        {"trade_date": date(2024, 1, 2), "total_equity": 10000, "cash_balance": 7000,
         "position_value": 3000, "drawdown": 0, "daily_return": None, "valuation_status": "VALID",
         "decision_reason": {"decision_type": "ACTIONS", "buy_signal_count": 1}},
        {"trade_date": date(2024, 1, 3), "total_equity": 9500, "cash_balance": 6500,
         "position_value": 3000, "drawdown": -0.05, "daily_return": -0.05, "valuation_status": "VALID",
         "decision_reason": {"decision_type": "NO_ACTION", "no_action_reason_code": "NOT_SCHEDULED_SIGNAL_DAY"}},
    ]
    positions = [
        {"trade_date": date(2024, 1, 2), "ts_code": "000001.SZ", "hold_shares": 100},
        {"trade_date": date(2024, 1, 3), "ts_code": "000001.SZ", "hold_shares": 100},
    ]
    benchmark = [
        {"trade_date": date(2024, 1, 2), "close": 100},
        {"trade_date": date(2024, 1, 3), "close": 102},
    ]
    curve = build_equity_curve(performance, positions, benchmark, 10000)
    assert curve[0]["nav"] == 1.0
    assert curve[1]["benchmark_nav"] == 1.02
    assert curve[1]["cash_ratio"] == pytest.approx(6500 / 9500, abs=1e-8)
    assert curve[1]["position_count"] == 1
    assert curve[0]["decision_reason"]["buy_signal_count"] == 1
    assert curve[1]["decision_reason"]["no_action_reason_code"] == "NOT_SCHEDULED_SIGNAL_DAY"


def test_decision_log_summarizes_daily_reasons():
    performance = [
        {"trade_date": date(2024, 1, 2), "decision_reason": {
            "decision_type": "NO_ACTION", "no_action_reason_code": "NO_ELIGIBLE_CANDIDATE",
            "dominant_failure_code": "ROE_MISSING",
        }},
        {"trade_date": date(2024, 1, 3), "decision_reason": {
            "decision_type": "ACTIONS", "dominant_failure_code": "ROE_MISSING",
        }},
    ]
    from data_collect.jobs.backtest_diagnostics import build_decision_log

    result = build_decision_log(performance)
    assert result["summary"]["days"] == 2
    assert result["summary"]["no_action_days"] == 1
    assert result["summary"]["reason_counts"]["NO_ELIGIBLE_CANDIDATE"] == 1
    assert result["summary"]["dominant_failure_counts"]["ROE_MISSING"] == 2


def test_period_returns_compound_daily_returns():
    performance = [
        {"trade_date": date(2024, 1, 2), "daily_return": 0.10, "position_value": 100},
        {"trade_date": date(2024, 1, 3), "daily_return": -0.10, "position_value": 100},
        {"trade_date": date(2024, 2, 1), "daily_return": 0.05, "position_value": 0},
    ]
    monthly = aggregate_period_returns(performance, "month")
    annual = aggregate_period_returns(performance, "year")
    assert monthly[0] == {
        "period": "2024-01",
        "return": -0.01,
        "observation_days": 2,
        "invested_days": 2,
        "cash_only_days": 0,
        "activity_status": "INVESTED",
    }
    assert annual[0]["period"] == "2024"
    assert annual[0]["return"] == 0.0395
    assert annual[0]["activity_status"] == "INVESTED"


def test_period_returns_marks_cash_only_period():
    performance = [
        {"trade_date": date(2024, 2, 1), "daily_return": 0, "position_value": 0},
        {"trade_date": date(2024, 2, 2), "daily_return": 0, "position_value": 0},
    ]
    monthly = aggregate_period_returns(performance, "month")
    assert monthly[0]["activity_status"] == "CASH_ONLY"
    assert monthly[0]["invested_days"] == 0
    assert monthly[0]["cash_only_days"] == 2
