from datetime import date
from pathlib import Path

import pytest

from data_collect.backtest import (
    BacktestConfig,
    BacktestDataset,
    BacktestEngine,
    DividendEvent,
    FeeRule,
    MarketBar,
    MarketRule,
    MarketTemperatureScaler,
    MonteCarloTester,
    Observation,
    SignalIntent,
)
from data_collect.ops.market_coverage import evaluate_market_coverage
from data_collect.jobs.backtest_rules import (
    assert_rule_document_approved,
    normalize_rule_records,
    rule_content_fingerprint,
)
from data_collect.jobs.backtest_dataset import _execution_limit_safety

def test_market_coverage_gate_blocks_drop_over_threshold():
    result = evaluate_market_coverage(
        "daily", date(2026, 8, 6), actual_rows=980,
        baseline_counts=[1000, 1000, 1000, 1000, 1000],
    )
    assert result.status == "BLOCKED"
    assert result.publishable is False
    assert result.drop_pct == pytest.approx(0.02)


def test_market_coverage_sample_and_missing_baseline_are_not_publishable():
    sample = evaluate_market_coverage(
        "daily", date(2026, 8, 6), 10, [1000], sample_mode=True,
    )
    no_baseline = evaluate_market_coverage("daily", date(2026, 8, 6), 1000, [])
    assert sample.status == "SKIPPED_SAMPLE"
    assert no_baseline.status == "NO_BASELINE"
    assert not sample.publishable and not no_baseline.publishable


def test_market_temperature_scaler_matches_reference_bands():
    scaler = MarketTemperatureScaler()
    assert scaler.multiplier(0.90) == 0.25
    assert scaler.multiplier(0.75) == 0.50
    assert scaler.multiplier(0.50) == 1.00
    assert scaler.multiplier(0.20) == 1.25
    assert scaler.multiplier(0.10) == 1.50
    with pytest.raises(ValueError, match="市场温度缺失"):
        scaler.adjust_risk_percent(0.02, None)
    assert scaler.adjust_risk_percent(0.02, None, missing_policy="unchanged") == 0.02


def test_rule_importer_rejects_overlapping_effective_ranges():
    base = {
        "rule_version": "r1", "market_rule_version": "m1", "fee_rule_version": "f1",
        "effective_from": "2026-01-01", "market": "主板", "min_buy_lot": 100,
        "allow_sell_odd_lot": True, "t_plus_one": True, "price_tick": 0.01,
        "commission_rate": 0.0003, "min_commission": 5,
        "stamp_duty_sell_rate": 0.0005, "transfer_fee_rate": 0.00001,
        "parameters": {"provenance": "unit-test"},
    }
    with pytest.raises(ValueError, match="规则有效区间重叠"):
        normalize_rule_records([
            base,
            dict(base, rule_version="r2", effective_from="2026-06-01"),
        ])


def test_rule_importer_rejects_nonfinite_or_fractional_execution_values():
    base = {
        "rule_version": "r1", "market_rule_version": "m1", "fee_rule_version": "f1",
        "effective_from": "2026-01-01", "market": "主板", "min_buy_lot": 100,
        "allow_sell_odd_lot": True, "t_plus_one": True, "price_tick": 0.01,
        "commission_rate": 0.0003, "min_commission": 5,
        "stamp_duty_sell_rate": 0.0005, "transfer_fee_rate": 0.00001,
        "parameters": {"provenance": "unit-test"},
    }
    with pytest.raises(ValueError, match="price_tick"):
        normalize_rule_records([dict(base, price_tick=float("nan"))])
    with pytest.raises(ValueError, match="min_buy_lot"):
        normalize_rule_records([dict(base, min_buy_lot=100.5)])
    with pytest.raises(ValueError, match="max_volume_participation_pct"):
        normalize_rule_records([dict(base, parameters={
            "provenance": "unit-test", "max_volume_participation_pct": 0,
        })])


def test_rule_content_fingerprint_is_stable_after_numeric_normalization():
    row = normalize_rule_records([{
        "rule_version": "r1", "market_rule_version": "m1", "fee_rule_version": "f1",
        "effective_from": "2026-01-01", "market": "主板", "min_buy_lot": 100,
        "allow_sell_odd_lot": True, "t_plus_one": True, "price_tick": "0.01",
        "commission_rate": "0.0003", "min_commission": "5",
        "stamp_duty_sell_rate": "0.0005", "transfer_fee_rate": "0.00001",
        "parameters": {"provenance": "unit-test"},
    }])[0]
    assert row["price_tick"] == 0.01
    assert rule_content_fingerprint(row) == rule_content_fingerprint(dict(row))


def test_review_required_rule_document_cannot_be_confirmed():
    with pytest.raises(RuntimeError, match="缺少 metadata"):
        assert_rule_document_approved({})
    with pytest.raises(RuntimeError, match="未 APPROVED"):
        assert_rule_document_approved({"status": "REVIEW_REQUIRED"})
    with pytest.raises(RuntimeError, match="approved_by"):
        assert_rule_document_approved({"status": "APPROVED", "approved_at": "2026-08-06"})
    assert_rule_document_approved({
        "status": "APPROVED", "approved_by": "operator", "approved_at": "2026-08-06",
    })


def test_missing_or_sentinel_limits_fail_closed_without_destroying_prices():
    assert _execution_limit_safety(100000, None, False, True, True) == (
        None, None, False, False, True
    )
    assert _execution_limit_safety(float("nan"), 9.0, False, True, True) == (
        None, 9.0, False, False, True
    )
    assert _execution_limit_safety(11.0, 9.0, False, True, True) == (
        11.0, 9.0, True, True, False
    )


def test_default_tushare_pipeline_is_raw_first():
    config = Path("config.yaml").read_text(encoding="utf-8")
    assert "tushare_market_incremental_by_date" in config
    assert "tushare_phase2_incremental" in config
    assert "tushare_financial_vip_incremental" in config
    assert "data_collect.jobs.a_share_daily_tushare" not in config
    assert "data_collect.jobs.a_share_valuation_tushare" not in config
    assert "data_collect.jobs.a_share_financial_tushare" not in config


def test_incremental_pipeline_has_one_bounded_derived_rebuild_owner():
    config = Path("config.yaml").read_text(encoding="utf-8")
    market_block = config.split("name: tushare_market_raw_first", 1)[1].split(
        "name: tushare_phase2_raw_first", 1
    )[0]
    phase2_block = config.split("name: tushare_phase2_raw_first", 1)[1].split(
        "tushare_financial:", 1
    )[0]
    assert "rebuild_derived: false" in market_block
    assert "rebuild_derived: true" in phase2_block

    source = Path(
        "data_collect/jobs/tushare_phase2_incremental.py"
    ).read_text(encoding="utf-8")
    rebuild_block = source.split("if rebuild_standard and not dry_run", 1)[1]
    assert "affected_days" in rebuild_block
    assert "trade_days" in rebuild_block
    assert "event_days" in rebuild_block
    assert "dividend_ex_dates" in rebuild_block


def test_stock_basic_raw_is_append_only_and_versioned():
    source = Path(
        "data_collect/data_access/repository/instrument_repository.py"
    ).read_text(encoding="utf-8")
    assert "tushare_stock_basic_raw_version" in source
    assert "raw_versions" in source
    assert "update_columns=()" in source
    assert "bulk_insert_ignore" in source
    assert '"tushare_stock_basic_raw", raw_current' in source


def test_market_incremental_appends_blocked_gate_after_materialization_failure(monkeypatch):
    import data_collect.jobs.tushare_market_incremental_by_date as job

    blocked_calls = []

    def fail_impl(*args, **kwargs):
        raise RuntimeError("synthetic materialization failure")

    monkeypatch.setattr(job, "_sync_one_date_impl", fail_impl)
    monkeypatch.setattr(
        job,
        "persist_market_coverage_blocked",
        lambda *args, **kwargs: blocked_calls.append((args, kwargs)),
    )
    with pytest.raises(RuntimeError, match="synthetic materialization failure"):
        job._sync_one_date(None, "daily", "20260803", "batch-smoke")
    assert blocked_calls
    assert blocked_calls[0][0][0:3] == ("daily", job.market_shared._date("20260803"), 0)
    assert "synthetic materialization failure" in blocked_calls[0][0][4]


def test_dividend_dates_cannot_precede_ex_date():
    with pytest.raises(ValueError, match="pay_date"):
        DividendEvent("000001.SZ", date(2026, 8, 6), pay_date=date(2026, 8, 5))
    with pytest.raises(ValueError, match="div_list_date"):
        DividendEvent("000001.SZ", date(2026, 8, 6), div_list_date=date(2026, 8, 5))


def test_dividend_stock_rate_uses_total_and_rejects_inconsistent_components():
    event = DividendEvent(
        "000001.SZ", date(2026, 8, 6), stock_div=2.0,
        stock_bonus_rate=0.8, stock_conversion_rate=1.2,
    )
    assert event.stock_rate == pytest.approx(2.0)
    with pytest.raises(ValueError, match="送转股合计与送股/转增分项不一致"):
        DividendEvent(
            "000001.SZ", date(2026, 8, 6), stock_div=2.0,
            stock_bonus_rate=1.0, stock_conversion_rate=1.0 + 1e-3,
        )


def test_monte_carlo_records_method_input_hash_and_parameters():
    result = MonteCarloTester.run_ruin_test([1.0, -0.5], num_simulations=10, random_seed=7)
    assert result["method"] == "bootstrap_with_replacement"
    assert len(result["input_trade_sequence_hash"]) == 64
    assert result["parameters"]["random_seed"] == 7


def _simple_bar(day):
    return MarketBar(
        day, "000001.SZ", 10.0, 10.2, 9.8, 10.0,
        volume=1000, amount=10000,
    )


def test_pending_duplicate_buy_is_rechecked_at_execution():
    days = [date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)]

    class DuplicateBuy:
        strategy_id = "duplicate-buy"
        strategy_version = "1"

        def generate_signals(self, trade_date, observations, positions):
            if trade_date == days[0]:
                return [
                    SignalIntent("000001.SZ", "BUY", 10.0, 10.0, 9.0),
                    SignalIntent("000001.SZ", "BUY", 10.0, 10.0, 9.0),
                ]
            return []

    observations = {
        day: (Observation(day, "000001.SZ", {"raw_close": 10.0}),)
        for day in days
    }
    dataset = BacktestDataset(
        dataset_id="duplicate-buy-test",
        calendar=tuple(days),
        warmup_start=days[0], evaluation_start=days[0], evaluation_end=days[-1],
        observations=observations,
        bars={(day, "000001.SZ"): _simple_bar(day) for day in days},
    )
    result = BacktestEngine(BacktestConfig(reserve_cash_pct=0)).run(dataset, DuplicateBuy())
    assert len([fill for fill in result.fills if fill["direction"] == "BUY"]) == 1
    assert any("执行时已存在持仓" in (order.get("reject_reason") or "") for order in result.orders)


def test_t_plus_one_boundary_rejects_final_day_fill_without_future_date():
    days = [date(2026, 8, 5), date(2026, 8, 6)]

    class BuyOnFirstDay:
        strategy_id = "boundary-buy"
        strategy_version = "1"

        def generate_signals(self, trade_date, observations, positions):
            if trade_date == days[0]:
                return [SignalIntent("000001.SZ", "BUY", 10.0, 10.0, 9.0)]
            return []

    dataset = BacktestDataset(
        dataset_id="boundary-buy-test",
        calendar=tuple(days),
        warmup_start=days[0], evaluation_start=days[0], evaluation_end=days[-1],
        observations={day: (Observation(day, "000001.SZ", {"raw_close": 10.0}),) for day in days},
        bars={(day, "000001.SZ"): _simple_bar(day) for day in days},
        rules={(day, None): (MarketRule(), FeeRule()) for day in days},
        require_explicit_rules=True,
    )
    result = BacktestEngine(BacktestConfig(reserve_cash_pct=0)).run(dataset, BuyOnFirstDay())
    assert result.fills == ()
    assert any("T+1 可卖日期超出 Dataset" in (order.get("reject_reason") or "") for order in result.orders)


def test_delisted_holding_fails_closed_instead_of_using_stale_close():
    days = [date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)]

    class BuyOnce:
        strategy_id = "delist-fail-closed"
        strategy_version = "1"

        def generate_signals(self, trade_date, observations, positions):
            if trade_date == days[0]:
                return [SignalIntent("000001.SZ", "BUY", 10.0, 10.0, 9.0)]
            return []

    observations = {
        day: (Observation(day, "000001.SZ", {
            "raw_close": 10.0, "delist_date": days[2],
        }),)
        for day in days
    }
    bars = {(day, "000001.SZ"): _simple_bar(day) for day in days}
    dataset = BacktestDataset(
        dataset_id="delist-fail-closed-test", calendar=tuple(days),
        warmup_start=days[0], evaluation_start=days[0], evaluation_end=days[-1],
        observations=observations, bars=bars,
        delist_dates={"000001.SZ": days[2]},
    )
    with pytest.raises(RuntimeError, match="退市持仓"):
        BacktestEngine(BacktestConfig(reserve_cash_pct=0)).run(dataset, BuyOnce())
