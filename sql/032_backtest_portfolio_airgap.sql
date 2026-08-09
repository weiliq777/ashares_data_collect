-- 阶段三生产增强版：风险/流动性审计字段与 Backtest -> Portfolio 人工隔离。
-- 本迁移只扩展 Backtest/Portfolio 模型，不读取、修改或删除任何 Raw 表。

ALTER TABLE IF EXISTS backtest.order
    ADD COLUMN IF NOT EXISTS requested_shares INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS actual_shares INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS volume_capacity_shares INTEGER,
    ADD COLUMN IF NOT EXISTS liquidity_status VARCHAR(40) NOT NULL DEFAULT 'NOT_CHECKED',
    ADD COLUMN IF NOT EXISTS liquidity_reject_reason TEXT,
    ADD COLUMN IF NOT EXISTS risk_policy_version VARCHAR(80),
    ADD COLUMN IF NOT EXISTS theoretical_shares DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS required_capital DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS max_loss_at_stop DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS gap_stress_loss DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS buy_fees DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS estimated_sell_fees DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS estimated_slippage DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS actual_risk_pct DOUBLE PRECISION;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'backtest.order'::regclass
           AND conname = 'ck_backtest_order_risk_liquidity'
    ) THEN
        ALTER TABLE backtest.order
            ADD CONSTRAINT ck_backtest_order_risk_liquidity CHECK (
                requested_shares >= 0
                AND actual_shares >= 0
                AND (volume_capacity_shares IS NULL OR volume_capacity_shares >= 0)
                AND liquidity_status IN (
                    'NOT_CHECKED','VALID','MISSING_VOLUME','MISSING_AMOUNT',
                    'ZERO_VOLUME','ZERO_AMOUNT','UNIT_SUSPECTED',
                    'REJECTED_VOLUME_CAPACITY','INVALID_REQUEST'
                )
            );
    END IF;
END $$;

-- Backtest source metadata is append-only and can be imported only as a pending
-- human intent. A backtest order never becomes a Portfolio fill directly.
ALTER TABLE IF EXISTS portfolio.order_intent
    ADD COLUMN IF NOT EXISTS source_backtest_run_id UUID REFERENCES backtest.run_manifest(run_id),
    ADD COLUMN IF NOT EXISTS source_backtest_order_id UUID REFERENCES backtest.order(order_id),
    ADD COLUMN IF NOT EXISTS source_dataset_id UUID REFERENCES backtest.dataset_manifest(dataset_id),
    ADD COLUMN IF NOT EXISTS source_signal_id UUID REFERENCES backtest.signal(signal_id),
    ADD COLUMN IF NOT EXISTS source_order_hash CHAR(64);

DO $$
BEGIN
    ALTER TABLE portfolio.order_intent DROP CONSTRAINT IF EXISTS order_intent_source_type_check;
    ALTER TABLE portfolio.order_intent DROP CONSTRAINT IF EXISTS ck_portfolio_intent_source_type;
    ALTER TABLE portfolio.order_intent
        ADD CONSTRAINT ck_portfolio_intent_source_type
        CHECK (source_type IN ('MANUAL','RESEARCH','BACKTEST'));
EXCEPTION WHEN duplicate_object THEN
    NULL;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_intent_backtest_order
    ON portfolio.order_intent (portfolio_id, source_backtest_order_id)
    WHERE source_backtest_order_id IS NOT NULL;

CREATE OR REPLACE FUNCTION portfolio.assert_backtest_airgap_source()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    order_run UUID;
    order_dataset UUID;
    order_signal UUID;
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

    SELECT o.run_id, r.dataset_id, o.signal_id, r.status, d.status, o.status,
           o.ts_code, o.direction, o.order_date
      INTO order_run, order_dataset, order_signal, run_status, dataset_status, order_status,
           order_ts_code, order_direction, order_date
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
       OR order_status <> 'FILLED' THEN
        RAISE EXCEPTION 'BACKTEST source must be a FILLED order from SUCCEEDED/FROZEN inputs: %', NEW.source_backtest_order_id;
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

COMMENT ON COLUMN portfolio.order_intent.source_type IS
    'MANUAL/RESEARCH/BACKTEST; BACKTEST starts PENDING_APPROVAL and never auto-fills';
COMMENT ON COLUMN portfolio.order_intent.source_order_hash IS
    'Canonical hash of the exact filled Backtest order fields imported for human review';
