-- T-037: preserve lot-level cash-dividend attribution used by trade PnL/R metrics.
-- No Raw, Standard, PIT or Feature fact is modified.

ALTER TABLE backtest.position_lot
    ADD COLUMN IF NOT EXISTS accrued_cash_dividend_per_share
        NUMERIC(20,8) NOT NULL DEFAULT 0;

ALTER TABLE backtest.position_lot
    DROP CONSTRAINT IF EXISTS position_lot_accrued_cash_dividend_nonnegative;
ALTER TABLE backtest.position_lot
    ADD CONSTRAINT position_lot_accrued_cash_dividend_nonnegative
    CHECK (accrued_cash_dividend_per_share >= 0);

