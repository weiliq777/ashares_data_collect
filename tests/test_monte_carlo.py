from data_collect.backtest.risk import MonteCarloTester


def test_bootstrap_and_permutation_record_distinct_reproducible_methods():
    values = [-1.0, 0.5, 1.0, -0.25]
    bootstrap = MonteCarloTester.run_ruin_test(
        values, num_simulations=10, random_seed=19,
        method="bootstrap_with_replacement",
    )
    permutation = MonteCarloTester.run_ruin_test(
        values, num_simulations=10, random_seed=19,
        method="permutation_without_replacement",
    )
    assert bootstrap["method"] == "bootstrap_with_replacement"
    assert permutation["method"] == "permutation_without_replacement"
    assert bootstrap["parameters_hash"] != permutation["parameters_hash"]
    assert bootstrap["input_trade_sequence_hash"] == permutation["input_trade_sequence_hash"]
