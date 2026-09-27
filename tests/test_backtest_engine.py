from datetime import date

import pytest

from data_collect.backtest import (
    BacktestConfig,
    BacktestDataset,
    BacktestEngine,
    DividendEvent,
    FundamentalValueGrowthTrendStrategy,
    MarketRule,
    MarketBar,
    Observation,
    PositionLotState,
    SignalIntent,
    ValueTrendStrategy,
    assert_no_future_rows,
    FeeRule,
    RiskPolicy,
    open_position_stop_risk,
    truncate_dataset_asof,
    validate_observation_cutoffs,
    size_position,
)


def _bar(day, open_price=10.0, close=10.0, low=9.8, can_buy=True, can_sell=True):
    return MarketBar(
        trade_date=day,
        ts_code="000001.SZ",
        raw_open=open_price,
        raw_high=max(open_price, close) + 0.2,
        raw_low=low,
        raw_close=close,
        volume=100000,
        amount=1000000,
        can_buy=can_buy,
        can_sell=can_sell,
    )


def _observation(day, close=10.0, ma20=9.0, ma60=8.0):
    return Observation(day, "000001.SZ", {
        "raw_close": close,
        "adj_close_base": close,
        "ma20": ma20,
        "ma60": ma60,
        "volume_ratio_20d": 1.2,
        "pe_pct_5y": 0.2,
        "is_pe_meaningful": True,
        "is_listed_asof": True,
        "is_st_asof": False,
        "eligible_by_market": True,
        "quality_status": "VALID",
    })


def _dataset(days, bars, observations, dividends=None):
    return BacktestDataset(
        dataset_id="dataset-test",
        calendar=tuple(days),
        warmup_start=days[0],
        evaluation_start=days[1],
        evaluation_end=days[-1],
        bars={(bar.trade_date, bar.ts_code): bar for bar in bars},
        observations={day: tuple(observations.get(day, ())) for day in days},
        dividends=dividends or {},
    )


def test_position_sizer_includes_sell_cost_and_lot_rounding():
    result = size_position(10000, 0.02, 10.0, 9.0)
    assert result.eligible is True
    assert result.shares % 100 == 0
    assert result.max_loss_at_stop <= 200.0
    assert result.buy_fees > 0
    assert result.estimated_sell_fees > 0


def test_existing_position_risk_includes_exit_slippage_gap_and_sell_fees():
    risk = open_position_stop_risk(
        100, 10.0, 9.0,
        FeeRule(slippage_bps=10), MarketRule(),
        RiskPolicy(exit_slippage_bps=10, gap_stress_pct=0.05),
    )
    assert risk > 100 * (10.0 - 9.0)


def test_strategy_uses_adjusted_price_for_trend_and_raw_price_for_stop():
    day = date(2026, 1, 5)
    observation = Observation(day, "000001.SZ", {
        "raw_close": 12.0,
        "adj_close_base": 10.0,
        "ma20": 9.0,
        "ma60": 8.0,
        "volume_ratio_20d": 1.2,
        "pe_pct_5y": 0.2,
        "is_pe_meaningful": True,
        "is_listed_asof": True,
        "is_st_asof": False,
        "eligible_by_market": True,
    })
    signals = ValueTrendStrategy().generate_signals(day, (observation,), {})
    assert len(signals) == 1
    assert signals[0].raw_reference_price == 12.0
    assert signals[0].indicator_price == 10.0
    assert signals[0].stop_loss_price_raw == 12.0 * 0.92


def test_strategy_does_not_treat_missing_volume_ratio_as_confirmed_momentum():
    day = date(2026, 1, 5)
    observation = Observation(day, "000001.SZ", {
        "raw_close": 12.0,
        "adj_close_base": 10.0,
        "ma20": 9.0,
        "ma60": 8.0,
        "pe_pct_5y": 0.2,
        "is_pe_meaningful": True,
        "is_listed_asof": True,
        "is_st_asof": False,
        "eligible_by_market": True,
    })
    assert ValueTrendStrategy().generate_signals(day, (observation,), {}) == []


def test_formal_strategy_requires_mature_samples_and_fundamental_track():
    day = date(2026, 8, 3)
    base = {
        "raw_close": 10.0, "adj_close_base": 10.0, "ma20": 9.5, "ma60": 9.0,
        "ma20_observation_count": 20, "ma60_observation_count": 60,
        "volume_ratio_20d": 1.6, "is_listed_asof": True,
        "eligible_by_market": True, "is_st_asof": False,
        "debt_to_assets": 30.0, "list_date": "2018-01-01",
        "has_full_pe_5y_history": True, "valuation_observation_count": 1100,
        "is_pe_meaningful": True, "pe_pct_5y": 0.20,
        "annual_report_count_5y": 5, "roe_5y_all_above_15": True,
        "roe_5y_min": 16.0, "ocf_report_count_3y": 3,
        "ocf_3y_all_positive": True, "quarterly_report_count_4q": 0,
        "q_sales_yoy_4q_all_above_30": False,
    }
    mature = Observation(day, "000001.SZ", base)
    immature = Observation(day, "000002.SZ", base | {"ma60_observation_count": 59})
    signals = FundamentalValueGrowthTrendStrategy().generate_signals(
        day, (mature, immature), {},
    )
    assert [item.ts_code for item in signals] == ["000001.SZ"]
    assert signals[0].reason["track"] == "traditional"


def test_signal_at_close_executes_only_next_open():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)]
    dataset = _dataset(
        days,
        [_bar(day, open_price=10.0 + i * 0.05, close=10.0 + i * 0.05) for i, day in enumerate(days)],
        {days[1]: [_observation(days[1])], days[2]: [_observation(days[2], close=10.2)]},
    )
    result = BacktestEngine(BacktestConfig(initial_cash=10000, reserve_cash_pct=0)).run(
        dataset, ValueTrendStrategy()
    )
    assert result.signals
    assert result.orders[0]["order_date"] == days[2]
    assert all(fill["fill_date"] != days[1] for fill in result.fills)
    assert result.fills[0]["fill_date"] == days[2]
    assert result.signals[0]["status"] == "FILLED"
    assert result.fills[0]["slippage_cost"] == 0.0
    assert result.metrics["final_equity_mark_to_market"] == result.final_equity
    assert "final_equity_forced_liquidation" in result.metrics
    assert result.audit_metadata["end_of_data_liquidation"]["event_type"] == "END_OF_DATA_LIQUIDATION"
    assert result.audit_metadata["end_of_data_liquidation"]["is_hypothetical"] is True


def test_same_day_buy_orders_use_score_descending_before_code_tiebreaker():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)]

    class ScoredBuys:
        strategy_id = "scored-buys"
        strategy_version = "1"

        def generate_signals(self, trade_date, observations, positions):
            if trade_date == days[0]:
                return [
                    SignalIntent("000001.SZ", "BUY", 10.0, 10.0, 9.0, score=1.0),
                    SignalIntent("000002.SZ", "BUY", 10.0, 10.0, 9.0, score=2.0),
                ]
            return []

    bars = {}
    observations = {}
    for day in days:
        observations[day] = tuple(
            Observation(day, code, {"raw_close": 10.0})
            for code in ("000001.SZ", "000002.SZ")
        )
        for code in ("000001.SZ", "000002.SZ"):
            bars[(day, code)] = MarketBar(
                day, code, 10.0, 10.2, 9.8, 10.0,
                volume=1000, amount=10000,
            )
    result = BacktestEngine(
        BacktestConfig(initial_cash=10000, max_positions=1, reserve_cash_pct=0)
    ).run(
        BacktestDataset(
            dataset_id="score-order-test", calendar=tuple(days),
            warmup_start=days[0], evaluation_start=days[0], evaluation_end=days[-1],
            observations=observations, bars=bars,
        ),
        ScoredBuys(),
    )
    buy_fills = [fill for fill in result.fills if fill["direction"] == "BUY"]
    assert len(buy_fills) == 1
    assert buy_fills[0]["ts_code"] == "000002.SZ"


def test_t_plus_one_open_reprices_position_size_instead_of_rejecting_gap():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)]

    class GapBuy:
        strategy_id = "gap-resize"
        strategy_version = "1"

        def generate_signals(self, trade_date, observations, positions):
            if trade_date == days[0]:
                return [SignalIntent("000001.SZ", "BUY", 10.0, 10.0, 9.0)]
            return []

    bars = [
        _bar(days[0], open_price=10.0, close=10.0),
        _bar(days[1], open_price=12.0, close=12.0),
        _bar(days[2], open_price=12.0, close=12.0),
    ]
    result = BacktestEngine(
        BacktestConfig(
            initial_cash=100000, max_single_position_pct=0.50, reserve_cash_pct=0,
        )
    ).run(
        BacktestDataset(
            dataset_id="gap-resize-test", calendar=tuple(days),
            warmup_start=days[0], evaluation_start=days[0], evaluation_end=days[-1],
            observations={day: (_observation(day, close=10.0),) for day in days},
            bars={(bar.trade_date, bar.ts_code): bar for bar in bars},
        ),
        GapBuy(),
    )
    buy_order = next(order for order in result.orders if order["direction"] == "BUY")
    buy_fill = next(fill for fill in result.fills if fill["direction"] == "BUY")
    assert buy_order["planned_shares"] > buy_fill["fill_shares"]
    assert buy_order["resized_at_execution"] is True


def test_engine_event_ids_are_deterministic_for_same_dataset_namespace():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)]
    dataset = _dataset(
        days,
        [_bar(day) for day in days],
        {days[1]: [_observation(days[1])], days[2]: [_observation(days[2])]},
    )
    config = BacktestConfig(initial_cash=10000, reserve_cash_pct=0)
    first = BacktestEngine(config).run(dataset, ValueTrendStrategy(), run_id="run-stable")
    second = BacktestEngine(config).run(dataset, ValueTrendStrategy(), run_id="run-stable")
    assert first.signals == second.signals
    assert first.orders == second.orders
    assert first.fills == second.fills
    assert first.performance_daily == second.performance_daily


def test_stop_loss_uses_previous_close_stop_and_blocks_limit_down():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]

    class BuyOnce:
        strategy_id = "test-buy"
        strategy_version = "1"

        def generate_signals(self, trade_date, observations, positions):
            if trade_date == days[1]:
                return [SignalIntent("000001.SZ", "BUY", 10.0, 10.0, 9.0)]
            return []

    bars = [
        _bar(days[0]),
        _bar(days[1]),
        _bar(days[2], open_price=10.0, close=10.0),
        _bar(days[3], open_price=8.8, close=9.0, low=8.5, can_sell=False),
    ]
    obs = {day: [_observation(day)] for day in days[1:]}
    result = BacktestEngine(BacktestConfig(initial_cash=10000, reserve_cash_pct=0)).run(
        _dataset(days, bars, obs), BuyOnce()
    )
    assert result.fills[0]["fill_date"] == days[2]
    assert any("无法卖出" in warning for warning in result.warnings)
    assert len(result.fills) == 1


def test_cash_dividend_is_paid_only_for_shares_held_before_ex_date():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]

    class BuyOnce:
        strategy_id = "test-dividend"
        strategy_version = "1"

        def generate_signals(self, trade_date, observations, positions):
            if trade_date == days[1]:
                return [SignalIntent("000001.SZ", "BUY", 10.0, 10.0, 9.0)]
            return []

    from data_collect.backtest import DividendEvent

    event = DividendEvent("000001.SZ", days[3], cash_div=0.10, pay_date=days[3], source_record_key="div-1")
    result = BacktestEngine(BacktestConfig(initial_cash=10000, reserve_cash_pct=0)).run(
        _dataset(days, [_bar(day) for day in days], {day: [_observation(day)] for day in days[1:]}, {days[3]: (event,)}),
        BuyOnce(),
    )
    assert any(entry["event_type"] == "DIVIDEND" and entry["amount"] > 0 for entry in result.cash_ledger)


def test_stock_action_is_not_assigned_to_lot_bought_after_ex_date():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
    event = DividendEvent(
        "000001.SZ", days[1], stock_div=1.0, pay_date=days[3],
        div_list_date=days[3], source_record_key="stock-action-1",
    )
    dataset = BacktestDataset(
        dataset_id="stock-action-test", calendar=tuple(days),
        warmup_start=days[0], evaluation_start=days[0], evaluation_end=days[-1],
        observations={}, bars={}, dividends={days[1]: (event,)},
    )
    engine = BacktestEngine()
    engine._positions = {
        "000001.SZ": [PositionLotState(
            lot_id="pre-ex", ts_code="000001.SZ", open_date=days[0],
            available_sell_date=days[0], open_shares=100, remaining_shares=100,
            cost_price=10.0, stop_loss_price_raw=9.0,
        )]
    }
    engine._last_close["000001.SZ"] = 10.0
    engine._capture_dividend_entitlements(days[1], {"000001.SZ": 100}, dataset)
    assert engine._positions["000001.SZ"][0].cost_price == pytest.approx(5.0)
    assert engine._positions["000001.SZ"][0].stop_loss_price_raw == pytest.approx(4.5)
    # 原批次在除权日后已退出，之后新买入的批次不能继承原批次的送转权益。
    engine._positions = {
        "000001.SZ": [PositionLotState(
            lot_id="post-ex", ts_code="000001.SZ", open_date=days[2],
            available_sell_date=days[2], open_shares=100, remaining_shares=100,
            cost_price=5.0, stop_loss_price_raw=4.5,
        )]
    }
    engine._apply_due_dividend_entitlements(days[3], dataset)
    assert engine._positions["000001.SZ"][0].lot_id == "post-ex"
    assert engine._positions["000001.SZ"][0].remaining_shares == 100
    action_lots = [
        lot for lot in engine._positions["000001.SZ"]
        if lot.opening_event_type == "CORPORATE_ACTION"
    ]
    assert len(action_lots) == 1
    assert action_lots[0].remaining_shares == 100
    assert action_lots[0].cost_price == pytest.approx(5.0)
    assert action_lots[0].stop_loss_price_raw == pytest.approx(4.5)


def test_cash_and_stock_actions_can_settle_on_different_dates():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
    event = DividendEvent(
        "000001.SZ", days[1], cash_div=0.10, stock_div=1.0,
        pay_date=days[2], div_list_date=days[3], source_record_key="mixed-action-1",
    )
    dataset = BacktestDataset(
        dataset_id="mixed-action-test", calendar=tuple(days),
        warmup_start=days[0], evaluation_start=days[0], evaluation_end=days[-1],
        observations={}, bars={}, dividends={days[1]: (event,)},
    )
    engine = BacktestEngine()
    engine._positions = {
        "000001.SZ": [PositionLotState(
            lot_id="pre-ex", ts_code="000001.SZ", open_date=days[0],
            available_sell_date=days[0], open_shares=100, remaining_shares=100,
            cost_price=10.0, stop_loss_price_raw=9.0,
        )]
    }
    engine._last_close["000001.SZ"] = 10.0
    engine._capture_dividend_entitlements(days[1], {"000001.SZ": 100}, dataset)
    engine._apply_due_dividend_entitlements(days[2], dataset)
    assert any(entry["event_type"] == "DIVIDEND" for entry in engine._cash_ledger)
    assert len(engine._entitlements) == 1
    engine._apply_due_dividend_entitlements(days[3], dataset)
    action_lots = [
        lot for lot in engine._positions["000001.SZ"]
        if lot.opening_event_type == "CORPORATE_ACTION"
    ]
    assert len(action_lots) == 1
    assert action_lots[0].remaining_shares == 100
    assert engine._entitlements == []


def test_unsettled_dividend_and_stock_rights_are_carried_in_daily_nav():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)]
    event = DividendEvent(
        "000001.SZ", days[1], cash_div=0.10, stock_div=1.0,
        pay_date=days[2], div_list_date=days[2], source_record_key="receivable-1",
    )
    bar = MarketBar(days[1], "000001.SZ", 4.95, 5.0, 4.9, 4.95, volume=1000, amount=4950)
    dataset = BacktestDataset(
        dataset_id="receivable-nav", calendar=tuple(days), warmup_start=days[0],
        evaluation_start=days[1], evaluation_end=days[2],
        observations={}, bars={(days[1], "000001.SZ"): bar},
        dividends={days[1]: (event,)},
    )
    engine = BacktestEngine()
    engine._positions = {"000001.SZ": [PositionLotState(
        lot_id="pre-ex", ts_code="000001.SZ", open_date=days[0],
        available_sell_date=days[0], open_shares=100, remaining_shares=100,
        cost_price=10.0, stop_loss_price_raw=9.0,
    )]}
    engine._last_close["000001.SZ"] = 10.0
    engine._capture_dividend_entitlements(days[1], {"000001.SZ": 100}, dataset)
    engine._record_daily(days[1], dataset)
    row = engine._performance_daily[-1]
    assert row["cash_receivable"] == pytest.approx(10.0)
    assert row["stock_receivable_value"] == pytest.approx(495.0)
    assert row["total_equity"] == pytest.approx(
        row["cash_balance"] + row["position_value"] + row["receivable_total"]
    )


def test_dividend_uses_pre_tax_amount_for_coordinate_and_after_tax_for_income():
    days = [date(2026, 1, 2), date(2026, 1, 5)]
    event = DividendEvent(
        "000001.SZ", days[1], cash_div=0.08, cash_div_tax=0.10,
        pay_date=days[1], source_record_key="cash-coordinate-1",
    )
    dataset = BacktestDataset(
        dataset_id="cash-coordinate", calendar=tuple(days), warmup_start=days[0],
        evaluation_start=days[0], evaluation_end=days[-1], observations={}, bars={},
        dividends={days[1]: (event,)},
    )
    engine = BacktestEngine()
    engine._positions = {"000001.SZ": [PositionLotState(
        lot_id="pre-ex", ts_code="000001.SZ", open_date=days[0],
        available_sell_date=days[0], open_shares=100, remaining_shares=100,
        cost_price=10.0, stop_loss_price_raw=9.0,
    )]}
    engine._last_close["000001.SZ"] = 10.0
    engine._capture_dividend_entitlements(days[1], {"000001.SZ": 100}, dataset)
    lot = engine._positions["000001.SZ"][0]
    assert lot.stop_loss_price_raw == pytest.approx(9.0 * 0.99)
    assert lot.accrued_cash_dividend_per_share == pytest.approx(0.08)


def test_realized_trade_pnl_and_r_include_accrued_cash_dividend():
    day = date(2026, 1, 5)
    dataset = BacktestDataset(
        dataset_id="dividend-pnl", calendar=(day,), warmup_start=day,
        evaluation_start=day, evaluation_end=day, observations={}, bars={},
    )
    engine = BacktestEngine(BacktestConfig(initial_cash=100, reserve_cash_pct=0))
    engine._dataset = dataset
    engine._positions = {"000001.SZ": [PositionLotState(
        lot_id="lot", ts_code="000001.SZ", open_date=day,
        available_sell_date=day, open_shares=100, remaining_shares=100,
        cost_price=10.0, stop_loss_price_raw=9.0,
        accrued_cash_dividend_per_share=0.10,
    )]}
    order = {"order_id": "order", "signal_id": None, "ts_code": "000001.SZ", "direction": "SELL"}
    engine._sell_position(
        "000001.SZ", 100, 10.0, day, order, "TEST",
        FeeRule(commission_rate=0, min_commission=0, stamp_duty_sell_rate=0, transfer_fee_rate=0),
        MarketRule(cash_settlement_days=0),
    )
    trade = engine._realized_trades[-1]
    assert trade["cash_dividend_income"] == pytest.approx(10.0)
    assert trade["pnl"] == pytest.approx(10.0)
    assert trade["r_multiple"] == pytest.approx(0.1)


def test_benchmark_and_out_of_sample_metrics_are_reported():
    days = [date(2026, 1, 2), date(2026, 1, 5)]

    class NoSignals:
        strategy_id = "none"
        strategy_version = "1"

        def generate_signals(self, trade_date, observations, positions):
            return []

    dataset = BacktestDataset(
        dataset_id="benchmark-metrics", calendar=tuple(days), warmup_start=days[0],
        evaluation_start=days[0], evaluation_end=days[-1], observations={}, bars={},
        benchmark_code="000300.SH", benchmark_closes={days[0]: 100.0, days[1]: 110.0},
        out_of_sample_start=days[1],
    )
    result = BacktestEngine(BacktestConfig()).run(dataset, NoSignals())
    assert result.metrics["benchmark_total_return"] == pytest.approx(0.10)
    assert result.metrics["excess_total_return"] == pytest.approx(-0.10)
    assert result.metrics["out_of_sample_excess_return"] == pytest.approx(-0.10)
    decisions = [row["decision_reason"] for row in result.performance_daily]
    assert all(row["decision_type"] == "NO_ACTION" for row in decisions)
    assert all(row["signal_count"] == 0 for row in decisions)
    assert all(row["actions"] == [] for row in decisions)


def test_cutoff_validation_hides_future_rows_and_rejects_future_financial_visibility():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)]
    dataset = _dataset(
        days,
        [_bar(day) for day in days],
        {day: [_observation(day)] for day in days},
    )
    truncated = truncate_dataset_asof(dataset, days[1])
    assert truncated.evaluation_end == days[1]
    assert_no_future_rows(truncated, days[1])
    future = Observation(days[1], "000001.SZ", {"effective_announce_date": days[2]})
    assert validate_observation_cutoffs((future,), days[1])


def test_prior_close_strategy_sell_executes_at_open_before_intraday_stop():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]

    class BuyThenSell:
        strategy_id = "test-stop-priority"
        strategy_version = "1"

        def generate_signals(self, trade_date, observations, positions):
            if trade_date == days[1]:
                return [SignalIntent("000001.SZ", "BUY", 10.0, 10.0, 9.0)]
            if trade_date == days[2] and positions:
                return [SignalIntent("000001.SZ", "SELL", 10.0, 10.0)]
            return []

    bars = [
        _bar(days[0]),
        _bar(days[1]),
        _bar(days[2]),
        _bar(days[3], open_price=8.8, close=8.9, low=8.5),
    ]
    observations = {day: [_observation(day)] for day in days[1:]}
    result = BacktestEngine(BacktestConfig(initial_cash=10000, reserve_cash_pct=0)).run(
        _dataset(days, bars, observations), BuyThenSell()
    )

    assert [fill["direction"] for fill in result.fills] == ["BUY", "SELL"]
    stop_orders = [order for order in result.orders if order.get("kind") == "STOP"]
    strategy_sell_orders = [
        order for order in result.orders
        if order["direction"] == "SELL" and order.get("kind") != "STOP"
    ]
    assert stop_orders == []
    assert strategy_sell_orders[0]["status"] == "FILLED"
    assert result.fills[-1]["fill_price"] == pytest.approx(8.8)


def test_open_limit_decision_ignores_close_derived_limit_status():
    open_tradeable = MarketBar(
        trade_date=date(2026, 1, 5), ts_code="000001.SZ",
        raw_open=9.8, raw_high=10.0, raw_low=9.7, raw_close=10.0,
        up_limit=10.0, down_limit=8.0, limit_status="UP_LIMIT",
    )
    locked_at_open = MarketBar(
        trade_date=date(2026, 1, 5), ts_code="000001.SZ",
        raw_open=10.0, raw_high=10.0, raw_low=9.8, raw_close=10.0,
        up_limit=10.0, down_limit=8.0, limit_status="UP_LIMIT",
    )
    assert BacktestEngine._limit_blocked(open_tradeable, "BUY") is False
    assert BacktestEngine._limit_blocked(locked_at_open, "BUY") is True


def test_frozen_dataset_requires_explicit_market_rules():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)]
    dataset = BacktestDataset(
        dataset_id="strict-rules",
        calendar=tuple(days),
        warmup_start=days[0],
        evaluation_start=days[1],
        evaluation_end=days[-1],
        observations={days[1]: (_observation(days[1]),)},
        bars={(day, "000001.SZ"): _bar(day) for day in days},
        require_explicit_rules=True,
    )
    with pytest.raises(RuntimeError, match="市场规则"):
        BacktestEngine(BacktestConfig(initial_cash=10000, reserve_cash_pct=0)).run(
            dataset, ValueTrendStrategy()
        )


def test_sale_at_dataset_end_does_not_make_unsettled_cash_available():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]

    class BuyThenSell:
        strategy_id = "test-settlement-boundary"
        strategy_version = "1"

        def generate_signals(self, trade_date, observations, positions):
            if trade_date == days[1] and not positions:
                return [SignalIntent("000001.SZ", "BUY", 10.0, 10.0, 9.0)]
            if trade_date == days[2] and positions:
                return [SignalIntent("000001.SZ", "SELL", 10.0, 10.0)]
            return []

    config = BacktestConfig(
        initial_cash=10000,
        reserve_cash_pct=0,
        market_rule=MarketRule(cash_settlement_days=1),
    )
    result = BacktestEngine(config).run(
        _dataset(
            days,
            [_bar(day) for day in days],
            {day: [_observation(day)] for day in days[1:]},
        ),
        BuyThenSell(),
    )
    assert [fill["direction"] for fill in result.fills] == ["BUY"]
    rejected = [order for order in result.orders if order["direction"] == "SELL"]
    assert rejected and rejected[0]["status"] == "REJECTED"
    assert "Dataset" in rejected[0]["reject_reason"]


def test_liquidity_rejection_keeps_actual_shares_zero():
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)]

    class BuyOnly:
        strategy_id = "test-actual-shares-audit"
        strategy_version = "1"

        def generate_signals(self, trade_date, observations, positions):
            if trade_date == days[1]:
                return [SignalIntent("000001.SZ", "BUY", 10.0, 10.0, 9.0)]
            return []

    # The opening fill must use the previous completed daily bar. One share of
    # prior-day volume cannot execute the normal whole-lot order.
    low_volume_bar = MarketBar(
        trade_date=days[1],
        ts_code="000001.SZ",
        raw_open=10.0,
        raw_high=10.2,
        raw_low=9.8,
        raw_close=10.0,
        volume=1,
        amount=10,
        can_buy=True,
        can_sell=True,
    )
    result = BacktestEngine(BacktestConfig(initial_cash=10000, reserve_cash_pct=0)).run(
        _dataset(
            days,
            [_bar(days[0]), low_volume_bar, _bar(days[2])],
            {day: [_observation(day)] for day in days[1:]},
        ),
        BuyOnly(),
    )

    orders = [order for order in result.orders if order["direction"] == "BUY"]
    assert len(orders) == 1
    assert orders[0]["status"] == "REJECTED"
    assert orders[0]["liquidity_status"] == "REJECTED_VOLUME_CAPACITY"
    assert orders[0]["liquidity_reference_date"] == days[1]
    assert orders[0]["actual_shares"] == 0
    assert result.fills == ()


def test_open_equity_and_reserve_cash_do_not_use_same_day_close_or_decay_per_order():
    days = [date(2026, 1, 2), date(2026, 1, 5)]
    holding_bar = MarketBar(
        days[1], "000001.SZ", 10.0, 101.0, 9.5, 100.0,
        volume=100000, amount=1000000,
    )
    dataset = _dataset(
        days,
        [_bar(days[0]), holding_bar],
        {day: () for day in days},
    )
    engine = BacktestEngine(BacktestConfig(initial_cash=10000, reserve_cash_pct=0.10))
    engine._dataset = dataset
    engine._previous_by_date = {days[0]: None, days[1]: days[0]}
    engine._positions = {
        "000001.SZ": [
            PositionLotState(
                "lot-open", "000001.SZ", days[0], days[0],
                100, 100, 10.0, 9.0,
            )
        ]
    }
    engine._last_close["000001.SZ"] = 10.0

    # Cash 10,000 + 100 shares at the known open 10; close 100 is unavailable.
    open_equity = engine._current_equity_at_open(days[1], dataset)
    assert open_equity == pytest.approx(11000.0)
    assert engine._investable_cash(open_equity) == pytest.approx(8900.0)
    engine.available_cash = 4000.0
    # Reserve remains 1,100 of portfolio equity; it is not 10% of each remainder.
    assert engine._investable_cash(open_equity) == pytest.approx(2900.0)


def test_stop_loss_closes_the_triggering_lot_instead_of_fifo_lot():
    days = [date(2026, 1, 2), date(2026, 1, 5)]
    current = MarketBar(
        days[1], "000001.SZ", 10.0, 10.2, 9.0, 9.4,
        volume=100000, amount=950000,
    )
    dataset = _dataset(days, [_bar(days[0]), current], {day: () for day in days})
    engine = BacktestEngine(BacktestConfig(initial_cash=10000, reserve_cash_pct=0))
    engine._dataset = dataset
    engine._previous_by_date = {days[0]: None, days[1]: days[0]}
    older = PositionLotState(
        "lot-older", "000001.SZ", days[0], days[0], 100, 100, 10.0, 8.0
    )
    triggered = PositionLotState(
        "lot-triggered", "000001.SZ", days[0], days[0], 100, 100, 10.0, 9.5
    )
    engine._positions = {"000001.SZ": [older, triggered]}

    engine._process_stop_losses(days[1], dataset)

    assert older.remaining_shares == 100
    assert triggered.remaining_shares == 0
    filled = [row for row in engine._orders if row["status"] == "FILLED"]
    assert len(filled) == 1
    assert filled[0]["target_lot_ids"] == ["lot-triggered"]
    assert filled[0]["liquidity_reference_date"] == days[0]
