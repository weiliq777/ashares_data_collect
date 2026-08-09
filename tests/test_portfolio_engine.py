from datetime import date

import pytest

from data_collect.jobs.portfolio_simulation import _valuation_input_hash
from data_collect.portfolio import PaperIntent, PaperPortfolioEngine, PortfolioConfig


def _intent(
    intent_id="i-1", direction="BUY", shares=100,
    execution=date(2026, 1, 5), ts_code="000001.SZ",
):
    return PaperIntent(
        intent_id=intent_id,
        ts_code=ts_code,
        direction=direction,
        decision_date=date(2026, 1, 2),
        intended_execution_date=execution,
        requested_shares=shares,
        reference_price=10.0,
        stop_loss_price_raw=9.0 if direction == "BUY" else None,
    )


def test_portfolio_requires_human_approval_and_enforces_t_plus_one():
    engine = PaperPortfolioEngine(PortfolioConfig(initial_cash=10000, reserve_cash_pct=0))
    engine.register_intent(_intent())
    with pytest.raises(ValueError, match="人工批准"):
        engine.execute_approved_fill("i-1", date(2026, 1, 5), 10.0, next_trade_date=date(2026, 1, 6))

    engine.approve_intent("i-1", "owner")
    engine.execute_approved_fill("i-1", date(2026, 1, 5), 10.0, next_trade_date=date(2026, 1, 6))
    sell = _intent("i-2", "SELL", 100, date(2026, 1, 6))
    engine.register_intent(sell)
    engine.approve_intent("i-2", "owner")
    engine.execute_approved_fill("i-2", date(2026, 1, 6), 10.2)
    assert engine.result().fills[-1]["direction"] == "SELL"


def test_portfolio_rejects_average_down_and_partial_fill():
    engine = PaperPortfolioEngine(PortfolioConfig(initial_cash=10000, reserve_cash_pct=0))
    engine.register_intent(_intent())
    engine.approve_intent("i-1", "owner")
    engine.execute_approved_fill("i-1", date(2026, 1, 5), 10.0, next_trade_date=date(2026, 1, 6))

    add = _intent("i-2", "BUY", 100, date(2026, 1, 6))
    engine.register_intent(add)
    engine.approve_intent("i-2", "owner")
    with pytest.raises(ValueError, match="加仓"):
        engine.execute_approved_fill("i-2", date(2026, 1, 6), 9.0, next_trade_date=date(2026, 1, 7))

    partial = _intent("i-3", "BUY", 100, date(2026, 1, 6))
    engine.register_intent(partial)
    engine.approve_intent("i-3", "owner")
    with pytest.raises(ValueError, match="部分成交"):
        engine.execute_approved_fill("i-3", date(2026, 1, 6), 10.0, next_trade_date=date(2026, 1, 7), fill_shares=50)


def test_portfolio_mark_to_market_records_daily_equity_and_missing_price():
    engine = PaperPortfolioEngine(PortfolioConfig(initial_cash=10000, reserve_cash_pct=0))
    engine.register_intent(_intent())
    engine.approve_intent("i-1", "owner")
    engine.execute_approved_fill("i-1", date(2026, 1, 5), 10.0, next_trade_date=date(2026, 1, 6))
    row = engine.mark_to_market(date(2026, 1, 6), {"000001.SZ": 10.5})
    assert row["total_equity"] > 10000
    missing = engine.mark_to_market(date(2026, 1, 7), {})
    assert missing["valuation_status"] == "MISSING_PRICE"


def test_portfolio_t_plus_one_requires_a_later_trade_date():
    engine = PaperPortfolioEngine(PortfolioConfig(initial_cash=10000, reserve_cash_pct=0))
    engine.register_intent(_intent())
    engine.approve_intent("i-1", "owner")
    with pytest.raises(ValueError, match="later than fill_date"):
        engine.execute_approved_fill(
            "i-1", date(2026, 1, 5), 10.0, next_trade_date=date(2026, 1, 5)
        )


def test_portfolio_valuation_input_hash_is_deterministic_and_state_bound():
    lots = [("000001.SZ", 100, 10.0, "lot-1")]
    first = _valuation_input_hash(
        "snapshot-1", date(2026, 1, 6), 8990.0, lots, {"000001.SZ": 10.5}
    )
    same = _valuation_input_hash(
        "snapshot-1", date(2026, 1, 6), 8990.0,
        list(reversed(lots)), {"000001.SZ": 10.5}
    )
    changed = _valuation_input_hash(
        "snapshot-1", date(2026, 1, 6), 8990.0, lots, {"000001.SZ": 10.6}
    )
    assert first == same
    assert first != changed


def test_portfolio_reserve_cash_is_a_fixed_equity_floor():
    engine = PaperPortfolioEngine(PortfolioConfig(initial_cash=10000, reserve_cash_pct=0.10))
    engine.register_intent(_intent("i-1", "BUY", 100))
    engine.approve_intent("i-1", "owner")
    engine.execute_approved_fill(
        "i-1", date(2026, 1, 5), 10.0, next_trade_date=date(2026, 1, 6)
    )

    # After the first purchase, a decaying-cash implementation would allow
    # this second order (9000 * 90% = 8100).  The fixed portfolio floor is
    # 1000, so the remaining budget is exactly 8000 before fees and this
    # order must be rejected once fees are included.
    second = _intent("i-2", "BUY", 800, date(2026, 1, 6), "000002.SZ")
    engine.register_intent(second)
    engine.approve_intent("i-2", "owner")
    with pytest.raises(ValueError, match="预留现金"):
        engine.execute_approved_fill(
            "i-2", date(2026, 1, 6), 10.0, next_trade_date=date(2026, 1, 7)
        )
