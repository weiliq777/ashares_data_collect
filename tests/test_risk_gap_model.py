from data_collect.backtest.risk import (
    FeeRule,
    MarketRule,
    MonteCarloTester,
    RiskPolicy,
    check_liquidity,
    size_position,
)


def test_gap_stress_is_versioned_and_reduces_position_size():
    base = size_position(
        100_000, 0.02, 10.0, 9.0,
        fee_rule=FeeRule(), market_rule=MarketRule(),
        risk_policy=RiskPolicy(risk_policy_version="risk-test-v1"),
    )
    stressed = size_position(
        100_000, 0.02, 10.0, 9.0,
        fee_rule=FeeRule(), market_rule=MarketRule(),
        risk_policy=RiskPolicy(
            risk_policy_version="risk-test-v2",
            entry_slippage_bps=20,
            exit_slippage_bps=20,
            gap_stress_pct=0.10,
        ),
    )
    assert base.eligible and stressed.eligible
    assert stressed.actual_shares < base.actual_shares
    assert stressed.gap_stress_loss > 0
    assert stressed.risk_policy_version == "risk-test-v2"
    assert stressed.max_loss_at_stop <= 100_000 * 0.02 + 1e-6


def test_adverse_tick_rounding_never_improves_simulated_fill():
    rule = MarketRule(price_tick=0.01)
    assert rule.round_adverse(10.001, "BUY") == 10.01
    assert rule.round_adverse(9.999, "SELL") == 9.99


def test_liquidity_gate_rejects_missing_zero_and_over_capacity_orders():
    rule = MarketRule(max_volume_participation_pct=0.10)
    assert check_liquidity(None, 100, rule).liquidity_status == "MISSING_VOLUME"
    assert check_liquidity(0, 100, rule).liquidity_status == "ZERO_VOLUME"
    assert check_liquidity(1_000, 100, rule).liquidity_status == "MISSING_AMOUNT"
    assert check_liquidity(1_000, 101, rule, amount=10_000, close_price=10).liquidity_status == "REJECTED_VOLUME_CAPACITY"
    assert check_liquidity(2_000, 100, rule, amount=20_000, close_price=10).allowed


def test_monte_carlo_permutation_is_reproducible_and_hashed():
    kwargs = dict(
        r_multiples=[-1.0, 0.5, 1.2, -0.2],
        num_simulations=20,
        random_seed=7,
        method="permutation_without_replacement",
    )
    first = MonteCarloTester.run_ruin_test(**kwargs)
    second = MonteCarloTester.run_ruin_test(**kwargs)
    assert first == second
    assert first["method"] == "permutation_without_replacement"
    assert first["input_trade_sequence_hash"]
    assert first["parameters_hash"]
