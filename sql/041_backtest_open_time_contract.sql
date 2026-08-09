-- T-037: eliminate open-time look-ahead and persist the liquidity reference.
-- No Raw or Standard/PIT/Feature fact is modified by this migration.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE IF EXISTS backtest.order
    ADD COLUMN IF NOT EXISTS liquidity_reference_date DATE;

ALTER TABLE IF EXISTS backtest.dataset_manifest
    ALTER COLUMN market_rule_version SET DEFAULT 'cn-a-share-v2',
    ALTER COLUMN fee_rule_version SET DEFAULT 'research-fee-assumption-v3';
ALTER TABLE IF EXISTS backtest.run_manifest
    ALTER COLUMN market_rule_version SET DEFAULT 'cn-a-share-v2',
    ALTER COLUMN fee_rule_version SET DEFAULT 'research-fee-assumption-v3';

-- Existing SUCCEEDED Run children are immutable audit records. They retain the
-- v1 hash and NULL reference; only daily-next-open-v2 orders use this contract.

CREATE OR REPLACE FUNCTION backtest.set_order_audit_hash()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    dataset_version VARCHAR(40);
    payload JSONB;
BEGIN
    SELECT d.dataset_version
      INTO dataset_version
      FROM backtest.run_manifest r
      JOIN backtest.dataset_manifest d ON d.dataset_id = r.dataset_id
     WHERE r.run_id = NEW.run_id;

    IF dataset_version IS NULL THEN
        RAISE EXCEPTION 'cannot hash Backtest order without a bound dataset: %', NEW.order_id;
    END IF;

    payload := jsonb_build_object(
        'actual_risk_pct', NEW.actual_risk_pct,
        'actual_shares', NEW.actual_shares,
        'buy_fees', NEW.buy_fees,
        'dataset_version', dataset_version,
        'decision_date', NEW.decision_date::text,
        'direction', NEW.direction,
        'estimated_sell_fees', NEW.estimated_sell_fees,
        'estimated_slippage', NEW.estimated_slippage,
        'expire_date', NEW.expire_date::text,
        'gap_stress_loss', NEW.gap_stress_loss,
        'liquidity_reference_date', NEW.liquidity_reference_date::text,
        'liquidity_reject_reason', NEW.liquidity_reject_reason,
        'liquidity_status', NEW.liquidity_status,
        'max_loss_at_stop', NEW.max_loss_at_stop,
        'order_date', NEW.order_date::text,
        'order_id', NEW.order_id::text,
        'order_shares', NEW.order_shares,
        'planned_shares', NEW.planned_shares,
        'reference_price', NEW.reference_price,
        'requested_shares', NEW.requested_shares,
        'risk_policy_version', NEW.risk_policy_version,
        'signal_id', CASE WHEN NEW.signal_id IS NULL THEN NULL ELSE NEW.signal_id::text END,
        'status', NEW.status,
        'stop_loss_price_raw', NEW.stop_loss_price_raw,
        'theoretical_shares', NEW.theoretical_shares,
        'ts_code', NEW.ts_code
    );

    NEW.order_audit_hash := encode(
        digest(convert_to(payload::text, 'UTF8'), 'sha256'),
        'hex'
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_order_audit_hash ON backtest.order;
CREATE TRIGGER trg_backtest_order_audit_hash
BEFORE INSERT OR UPDATE ON backtest.order
FOR EACH ROW EXECUTE FUNCTION backtest.set_order_audit_hash();

CREATE OR REPLACE FUNCTION backtest.assert_order_liquidity_reference()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE execution_version VARCHAR(80);
BEGIN
    SELECT execution_model_version INTO execution_version
      FROM backtest.run_manifest WHERE run_id=NEW.run_id;
    IF execution_version='daily-next-open-v2' AND NEW.status='FILLED'
       AND (NEW.liquidity_reference_date IS NULL
            OR NEW.liquidity_reference_date >= NEW.order_date) THEN
        RAISE EXCEPTION 'FILLED order requires a prior-day liquidity reference: %', NEW.order_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_order_liquidity_reference ON backtest.order;
CREATE TRIGGER trg_backtest_order_liquidity_reference
BEFORE INSERT OR UPDATE ON backtest.order
FOR EACH ROW EXECUTE FUNCTION backtest.assert_order_liquidity_reference();

COMMENT ON COLUMN backtest.order.liquidity_reference_date IS
    'daily-next-open-v2: last completed daily bar used for liquidity capacity; legacy v1 rows remain NULL';
