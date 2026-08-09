from data_collect.backtest.risk import MarketRule, check_liquidity


def test_liquidity_capacity_uses_normalized_shares_and_rejects_full_order():
    rule = MarketRule(max_volume_participation_pct=0.05)
    result = check_liquidity(
        volume=10_000,
        requested_shares=501,
        market_rule=rule,
        amount=100_000,
        close_price=10,
        low_price=9,
        high_price=11,
    )
    assert result.volume_capacity_shares == 500
    assert result.liquidity_status == "REJECTED_VOLUME_CAPACITY"
    assert not result.allowed


def test_liquidity_rejects_unit_suspect_amount_volume_pair():
    result = check_liquidity(
        volume=10_000,
        requested_shares=100,
        market_rule=MarketRule(),
        amount=1_000_000,
        close_price=10,
        low_price=9,
        high_price=11,
    )
    assert result.liquidity_status == "UNIT_SUSPECTED"
    assert not result.allowed
