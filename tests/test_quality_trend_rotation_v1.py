from datetime import date, timedelta

import pytest

from data_collect.backtest import (
    BacktestConfig,
    BacktestDataset,
    BacktestEngine,
    MarketBar,
    Observation,
    PositionLotState,
    QualityTrendRotationV1,
    ScenarioConfig,
    EvaluationConfig,
    SignalIntent,
    StrategyVersionSpec,
    backtest_fingerprint,
    size_target_position,
)
from data_collect.jobs.backtest_dataset import _dividend_event_from_materialized_row


def _values(day, *, close=12.0, ma20=11.0, ma60=10.0, amount_ok=True, **overrides):
    values = {
        "raw_close": close,
        "adj_close_base": close,
        "ma20": ma20,
        "ma60": ma60,
        "is_listed_asof": True,
        "is_st_asof": False,
        "is_tradeable": True,
        "can_buy": True,
        "can_sell": True,
        "eligible_by_market": True,
        "quality_status": "VALID",
        "effective_announce_date": day,
        "financial_data_available_timestamp": day,
        "operating_cashflow_ttm": 100.0,
        "ocf_to_net_profit_consolidated_ttm": 1.2,
        "roe_ttm": 0.10,
        "debt_to_assets": 30.0,
        "ttm_complete": True,
        "roe_quality_status": "VALID",
        "cashflow_quality_status": "VALID",
        "financial_feature_contract_version": "feature-contract-v2.0",
        "financial_source_fingerprint": "f" * 64,
        "ma20_observation_count": 20,
        "ma60_observation_count": 60,
        "available_observation_count": 300,
        "amount_ok": amount_ok,
    }
    values.update(overrides)
    values.pop("amount_ok", None)
    return values


def _bar(day, code, amount=60_000_000.0):
    return MarketBar(
        trade_date=day,
        ts_code=code,
        raw_open=10.0,
        raw_high=12.0,
        raw_low=9.0,
        raw_close=10.0,
        volume=100_000.0,
        amount=amount,
    )


def _candidate_fixture():
    days = tuple(date(2026, 1, 1) + timedelta(days=i) for i in range(62))
    codes = ("000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ")
    current = days[-1]
    final_prices = {
        codes[0]: (14.0, 12.0, 10.0),
        codes[1]: (13.0, 11.0, 10.0),
        codes[2]: (12.0, 10.5, 9.5),
        codes[3]: (11.0, 10.0, 9.5),
    }
    observations = {}
    bars = {}
    for day in days:
        observations[day] = []
        for code in codes:
            if day == current:
                close, ma20, ma60 = final_prices[code]
            else:
                close, ma20, ma60 = 10.0, 9.0, 8.0
            observations[day].append(
                Observation(day, code, _values(day, close=close, ma20=ma20, ma60=ma60))
            )
            bars[(day, code)] = _bar(day, code)
    return days, current, tuple(codes), BacktestDataset(
        dataset_id="quality-trend-fixture",
        calendar=days,
        warmup_start=days[0],
        evaluation_start=current,
        evaluation_end=current,
        observations={day: tuple(rows) for day, rows in observations.items()},
        bars=bars,
    )


def test_signal_day_is_last_trading_day_of_natural_week_not_hardcoded_friday():
    calendar = (date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 9))
    assert QualityTrendRotationV1.is_signal_day(calendar[0], calendar)
    assert not QualityTrendRotationV1.is_signal_day(calendar[1], calendar)
    assert not QualityTrendRotationV1.is_signal_day(calendar[-1], calendar)


def test_candidates_are_ranked_ret60_then_amount_then_code_and_only_top_three_buy():
    days, current, codes, dataset = _candidate_fixture()
    strategy = QualityTrendRotationV1(min_listing_trading_days=60)
    for day in days:
        strategy.on_day(day, dataset.observations_for(day), dataset)
    signals = strategy.generate_signals(current, dataset.observations_for(current), {})
    buys = [signal for signal in signals if signal.direction == "BUY"]
    assert [signal.ts_code for signal in buys] == list(codes[:3])
    assert all(signal.stop_loss_price_raw is None for signal in buys)
    assert [signal.reason["rank"] for signal in buys] == [1, 2, 3]


def test_only_ma60_is_hard_trend_exit():
    day = date(2026, 8, 21)
    strategy = QualityTrendRotationV1()
    below_ma20 = Observation(day, "000001.SZ", _values(day, close=10.0, ma20=11.0, ma60=9.0))
    assert strategy.generate_signals(
        day, (below_ma20,), {"000001.SZ": {"hold_shares": 100}}
    ) == []
    below_ma60 = Observation(day, "000001.SZ", _values(day, close=8.0, ma20=9.0, ma60=9.0))
    signals = strategy.generate_signals(
        day, (below_ma60,), {"000001.SZ": {"hold_shares": 100}}
    )
    assert len(signals) == 1
    assert signals[0].reason["rule"] == "close_below_ma60"
    assert signals[0].reason["retryable_sell"] is True


def test_missing_pit_financial_input_is_not_treated_as_quality_pass():
    day = date(2026, 8, 21)
    observation = Observation(day, "000001.SZ", _values(day, **{
        "financial_data_available_timestamp": None,
    }))
    strategy = QualityTrendRotationV1()
    strategy._return_60["000001.SZ"] = 0.20
    strategy._avg_amount_20["000001.SZ"] = 60_000_000.0
    strategy._current_amount["000001.SZ"] = 60_000_000.0
    assert strategy.generate_signals(day, (observation,), {}) == []
    decision = strategy.last_decision_reason()
    assert decision["no_action_reason_code"] == "NO_ELIGIBLE_CANDIDATE"
    assert decision["failure_counts"]["FINANCIAL_AVAILABLE_DATE_UNAVAILABLE"] == 1
    assert decision["failure_examples"]["FINANCIAL_AVAILABLE_DATE_UNAVAILABLE"] == ["000001.SZ"]


def test_missing_st_status_is_fail_closed_for_entry_and_existing_holding():
    day = date(2026, 8, 21)
    observation = Observation(day, "000001.SZ", _values(day, is_st_asof=None))
    strategy = QualityTrendRotationV1()
    strategy._return_60["000001.SZ"] = 0.20
    strategy._avg_amount_20["000001.SZ"] = 60_000_000.0
    strategy._current_amount["000001.SZ"] = 60_000_000.0
    assert strategy.generate_signals(day, (observation,), {}) == []
    signals = strategy.generate_signals(
        day, (observation,), {"000001.SZ": {"hold_shares": 100}}
    )
    assert signals[0].reason["rule"] == "missing_st_status"


def test_legacy_frozen_financial_statuses_use_only_provable_equivalent_fields():
    day = date(2026, 8, 21)
    strategy = QualityTrendRotationV1()
    compatible = Observation(day, "000001.SZ", _values(
        day, roe_quality_status=None, cashflow_quality_status=None,
    ))
    assert strategy._financial_quality_ok(compatible, day) is True
    unprovable_cashflow = Observation(day, "000001.SZ", _values(
        day, roe_quality_status=None, cashflow_quality_status=None,
        operating_cashflow_ttm=None,
    ))
    assert strategy._financial_quality_ok(unprovable_cashflow, day) is False
    explicit_invalid = Observation(day, "000001.SZ", _values(
        day, roe_quality_status="NEGATIVE_OR_ZERO_EQUITY",
    ))
    assert strategy._financial_quality_ok(explicit_invalid, day) is False


def test_listing_day_count_is_inclusive_at_exactly_250_days():
    day = date(2026, 8, 21)
    observation = Observation(
        day, "000001.SZ", _values(day, available_observation_count=250)
    )
    strategy = QualityTrendRotationV1()
    strategy._return_60["000001.SZ"] = 0.20
    strategy._avg_amount_20["000001.SZ"] = 60_000_000.0
    strategy._current_amount["000001.SZ"] = 60_000_000.0
    assert strategy._entry_candidate(day, observation) is not None
    too_young = Observation(
        day, "000001.SZ", _values(day, available_observation_count=249)
    )
    assert strategy._entry_candidate(day, too_young) is None


def test_rank_exit_uses_hold_hysteresis_and_pending_sell_blocks_duplicate_signal():
    day = date(2026, 8, 21)
    strategy = QualityTrendRotationV1(top_n=1, rank_exit_threshold=1)
    held = Observation(day, "000001.SZ", _values(day, close=12.0, ma20=11.0, ma60=10.0))
    leader = Observation(day, "000002.SZ", _values(day, close=14.0, ma20=12.0, ma60=10.0))
    for code in (held.ts_code, leader.ts_code):
        strategy._return_60[code] = 0.10 if code == held.ts_code else 0.20
        strategy._avg_amount_20[code] = 60_000_000.0
        strategy._current_amount[code] = 60_000_000.0
    signals = strategy.generate_signals(day, (held, leader), {held.ts_code: {"hold_shares": 100}})
    assert [(signal.ts_code, signal.direction) for signal in signals] == [(held.ts_code, "SELL"), (leader.ts_code, "BUY")]
    assert strategy.generate_signals(
        day, (held, leader), {held.ts_code: {"hold_shares": 100, "pending_exit": True}}
    ) == [signals[1]]


def test_target_position_sizer_uses_target_value_and_a_share_lots():
    result = size_target_position(
        available_cash=10_000.0,
        equity_base=10_000.0,
        entry_price=30.0,
        target_position_ratio=0.30,
        max_position_ratio=0.35,
    )
    assert result.eligible is True
    assert result.actual_shares == 100
    assert result.actual_shares % 100 == 0
    assert result.max_loss_at_stop == 0.0


def test_target_value_engine_retries_failed_sell_on_each_following_trading_day():
    days = (date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7))

    class BuyThenRetrySell:
        strategy_id = "quality-trend-engine-fixture"
        strategy_version = "1"

        def generate_signals(self, trade_date, observations, positions):
            if trade_date == days[0]:
                return [SignalIntent("000001.SZ", "BUY", 10.0, 10.0)]
            if trade_date == days[1] and positions:
                return [SignalIntent(
                    "000001.SZ", "SELL", 10.0, 10.0,
                    reason={"retryable_sell": True, "rule": "fixture"},
                )]
            return []

    bars = {
        (day, "000001.SZ"): MarketBar(
            day, "000001.SZ", 10.0, 10.2, 9.8, 10.0,
            volume=100_000.0, amount=1_000_000.0,
            can_sell=(day != days[2]),
        )
        for day in days
    }
    dataset = BacktestDataset(
        dataset_id="target-engine-fixture", calendar=days,
        warmup_start=days[0], evaluation_start=days[0], evaluation_end=days[-1],
        observations={day: (Observation(day, "000001.SZ", {"raw_close": 10.0}),) for day in days},
        bars=bars,
    )
    config = BacktestConfig(
        initial_cash=10_000.0, max_positions=3, max_single_position_pct=0.35,
        reserve_cash_pct=0.0, allocation_mode="target_value",
        target_position_ratio=0.30, max_entry_position_ratio=0.35,
        entry_slippage_bps=10.0, exit_slippage_bps=10.0,
    )
    result = BacktestEngine(config).run(dataset, BuyThenRetrySell())
    sell_fills = [fill for fill in result.fills if fill["direction"] == "SELL"]
    assert len(sell_fills) == 1
    assert sell_fills[0]["fill_date"] == days[3]
    sell_orders = [order for order in result.orders if order["direction"] == "SELL"]
    assert sell_orders[0]["retryable_sell"] is True
    assert sell_orders[0]["retry_count"] == 1


def test_pending_sell_frees_a_slot_for_replacement_order_creation_only():
    engine = BacktestEngine(BacktestConfig(
        max_positions=1, max_single_position_pct=0.35, reserve_cash_pct=0.0,
        allocation_mode="target_value", target_position_ratio=0.30,
        max_entry_position_ratio=0.35,
    ))
    engine._positions = {"000001.SZ": [PositionLotState(
        lot_id="lot", ts_code="000001.SZ", open_date=date(2026, 1, 1),
        available_sell_date=date(2026, 1, 1), open_shares=100,
        remaining_shares=100, cost_price=10.0, stop_loss_price_raw=None,
    )]}
    engine._pending_orders = [{
        "ts_code": "000001.SZ", "direction": "SELL", "status": "PENDING",
        "retryable_sell": True,
    }]
    assert engine._position_count_after_pending_sells() == 0


def test_evaluation_cagr_uses_calendar_days_not_252_divided_by_observation_count():
    start, end = date(2021, 1, 1), date(2023, 1, 1)
    dataset = BacktestDataset(
        dataset_id="cagr-contract", calendar=(start, end),
        warmup_start=start, evaluation_start=start, evaluation_end=end,
        observations={}, bars={},
    )
    engine = BacktestEngine()
    engine._performance_daily = [
        {"trade_date": start, "total_equity": 10_000.0, "daily_return": 0.0, "turnover": 0.0},
        {"trade_date": end, "total_equity": 12_100.0, "daily_return": 0.21, "turnover": 0.0},
    ]
    metrics = engine._calculate_metrics(dataset)
    expected = (1.21 ** (365.2425 / (end - start).days)) - 1.0
    assert metrics["cagr"] == pytest.approx(expected)


def test_strategy_scenario_evaluation_and_backtest_hashes_are_separate_and_stable():
    strategy = QualityTrendRotationV1()
    spec = StrategyVersionSpec(strategy.strategy_id, strategy.strategy_version, strategy.parameters)
    scenario = ScenarioConfig()
    evaluation = EvaluationConfig()
    assert spec.strategy_hash != scenario.scenario_hash
    assert scenario.scenario_hash != evaluation.evaluation_hash
    assert backtest_fingerprint(
        spec.strategy_hash, scenario.scenario_hash, "dataset-hash", "engine-v1"
    ) == backtest_fingerprint(
        spec.strategy_hash, scenario.scenario_hash, "dataset-hash", "engine-v1"
    )


def test_materialized_dividend_row_is_explicitly_mapped_not_positionally_unpacked():
    event = _dividend_event_from_materialized_row((
        "000001.SZ", date(2025, 12, 31), date(2026, 4, 1), "实施",
        0.3, 0.1, 0.2, 0.5, 0.4, date(2026, 5, 1),
        date(2026, 5, 2), date(2026, 5, 8), date(2026, 5, 9),
        date(2026, 4, 20), "event-key", 7, "payload-hash",
    ))
    assert event.ts_code == "000001.SZ"
    assert event.ex_date == date(2026, 5, 2)
    assert event.cash_div == 0.5
    assert event.cash_div_tax == 0.4
    assert event.stock_rate == pytest.approx(0.3)
    assert event.source_record_version == 7
