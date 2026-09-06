from datetime import date

from data_collect.jobs.backtest_diagnostics import (
    build_entry_diagnostics,
    build_trade_records,
    classify_rejection,
    summarize_exit_attribution,
    summarize_forward_returns,
    summarize_rejections,
    summarize_trades,
)


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
    ]
    report = summarize_forward_returns(trades, future)
    assert report["horizon_5d"]["returns"]["count"] == 1
    assert report["horizon_5d"]["returns"]["mean"] == 0.1
    assert report["horizon_20d"]["returns"]["mean"] == -0.1


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
