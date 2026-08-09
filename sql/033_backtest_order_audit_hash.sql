-- Stage 3 final audit closure: bind Portfolio Air-Gap hashes to the exact
-- persisted Backtest order row. This migration does not read or mutate Raw.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE IF EXISTS backtest.order
    ADD COLUMN IF NOT EXISTS order_audit_hash CHAR(64);

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

-- Recompute legacy rows, if a deployment already had Backtest orders.
UPDATE backtest.order
   SET order_audit_hash = order_audit_hash
 WHERE order_audit_hash IS NULL;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM backtest.order WHERE order_audit_hash IS NULL) THEN
        RAISE EXCEPTION 'Backtest orders without order_audit_hash remain after migration';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'backtest.order'::regclass
           AND conname = 'ck_backtest_order_audit_hash'
    ) THEN
        ALTER TABLE backtest.order
            ADD CONSTRAINT ck_backtest_order_audit_hash
            CHECK (order_audit_hash IS NOT NULL AND length(btrim(order_audit_hash)) = 64);
    END IF;
END $$;

CREATE OR REPLACE FUNCTION portfolio.assert_backtest_airgap_source()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    order_run UUID;
    order_dataset UUID;
    order_signal UUID;
    order_audit_hash CHAR(64);
    run_status VARCHAR(20);
    dataset_status VARCHAR(20);
    order_status VARCHAR(20);
    account_snapshot UUID;
    account_watchlist UUID;
    order_ts_code VARCHAR(20);
    order_direction VARCHAR(10);
    order_date DATE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'portfolio intents are append-only: %', OLD.intent_id;
    END IF;

    IF NEW.source_type <> 'BACKTEST' THEN
        IF NEW.source_backtest_run_id IS NOT NULL
           OR NEW.source_backtest_order_id IS NOT NULL
           OR NEW.source_dataset_id IS NOT NULL
           OR NEW.source_signal_id IS NOT NULL
           OR NEW.source_order_hash IS NOT NULL THEN
            RAISE EXCEPTION 'non-BACKTEST intent cannot carry backtest source metadata: %', NEW.intent_id;
        END IF;
        RETURN NEW;
    END IF;

    IF (TG_OP = 'INSERT' AND NEW.status <> 'PENDING_APPROVAL')
       OR NEW.source_backtest_run_id IS NULL
       OR NEW.source_backtest_order_id IS NULL
       OR NEW.source_dataset_id IS NULL
       OR NEW.source_order_hash IS NULL
       OR length(btrim(NEW.source_order_hash)) <> 64 THEN
        RAISE EXCEPTION 'BACKTEST intent must start pending with complete source metadata: %', NEW.intent_id;
    END IF;

    SELECT o.run_id, r.dataset_id, o.signal_id, o.order_audit_hash,
           r.status, d.status, o.status, o.ts_code, o.direction, o.order_date
      INTO order_run, order_dataset, order_signal, order_audit_hash,
           run_status, dataset_status, order_status, order_ts_code, order_direction, order_date
      FROM backtest.order o
      JOIN backtest.run_manifest r ON r.run_id = o.run_id
      JOIN backtest.dataset_manifest d ON d.dataset_id = r.dataset_id
     WHERE o.order_id = NEW.source_backtest_order_id;
    IF order_run IS NULL THEN
        RAISE EXCEPTION 'BACKTEST source order does not exist: %', NEW.source_backtest_order_id;
    END IF;
    IF order_run <> NEW.source_backtest_run_id
       OR order_dataset <> NEW.source_dataset_id
       OR run_status <> 'SUCCEEDED'
       OR dataset_status <> 'FROZEN'
       OR order_status <> 'FILLED'
       OR order_audit_hash IS NULL
       OR btrim(NEW.source_order_hash) <> btrim(order_audit_hash) THEN
        RAISE EXCEPTION 'BACKTEST source must be a FILLED order with matching database audit hash: %', NEW.source_backtest_order_id;
    END IF;
    IF order_signal IS DISTINCT FROM NEW.source_signal_id THEN
        RAISE EXCEPTION 'BACKTEST source signal mismatch: %', NEW.source_backtest_order_id;
    END IF;
    IF order_ts_code <> NEW.ts_code OR order_direction <> NEW.direction
       OR order_date <> NEW.intended_execution_date THEN
        RAISE EXCEPTION 'BACKTEST source order does not match Portfolio intent: %', NEW.intent_id;
    END IF;

    SELECT a.source_snapshot_id, a.source_watchlist_id
      INTO account_snapshot, account_watchlist
      FROM portfolio.account_manifest a
     WHERE a.portfolio_id = NEW.portfolio_id;
    IF account_snapshot IS NULL THEN
        RAISE EXCEPTION 'BACKTEST intent account does not exist: %', NEW.portfolio_id;
    END IF;
    IF NEW.snapshot_id <> account_snapshot
       OR (account_watchlist IS NOT NULL AND NEW.watchlist_id IS DISTINCT FROM account_watchlist) THEN
        RAISE EXCEPTION 'BACKTEST intent must use the account snapshot/watchlist: %', NEW.intent_id;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF ROW(OLD.source_type, OLD.source_backtest_run_id, OLD.source_backtest_order_id,
               OLD.source_dataset_id, OLD.source_signal_id, OLD.source_order_hash)
           IS DISTINCT FROM
           ROW(NEW.source_type, NEW.source_backtest_run_id, NEW.source_backtest_order_id,
               NEW.source_dataset_id, NEW.source_signal_id, NEW.source_order_hash) THEN
            RAISE EXCEPTION 'approved BACKTEST source metadata is immutable: %', OLD.intent_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_portfolio_backtest_airgap ON portfolio.order_intent;
CREATE TRIGGER trg_portfolio_backtest_airgap
BEFORE INSERT OR UPDATE OR DELETE ON portfolio.order_intent
FOR EACH ROW EXECUTE FUNCTION portfolio.assert_backtest_airgap_source();

COMMENT ON COLUMN backtest.order.order_audit_hash IS
    'Database-generated SHA-256 of the exact persisted order payload and bound Dataset version';
COMMENT ON COLUMN portfolio.order_intent.source_order_hash IS
    'Must equal backtest.order.order_audit_hash for the imported FILLED order';
