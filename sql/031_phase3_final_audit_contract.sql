-- 阶段三正式回测收口补丁。
-- 只增加回测审计字段和 Run/Dataset 一致性门禁，不修改 Raw/Standard/PIT/Feature/Research 数据。

ALTER TABLE IF EXISTS backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS baseline_quality_report_hash CHAR(64);

DO $$
BEGIN
    IF to_regclass('backtest.dataset_manifest') IS NOT NULL
       AND NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'backtest.dataset_manifest'::regclass
           AND conname = 'backtest_dataset_baseline_quality_hash_check'
    ) THEN
        ALTER TABLE backtest.dataset_manifest
            ADD CONSTRAINT backtest_dataset_baseline_quality_hash_check
            CHECK (status <> 'FROZEN' OR baseline_quality_report_hash IS NOT NULL);
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION backtest.assert_dataset_baseline_binding()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    release_status VARCHAR(20);
    release_quality_hash CHAR(64);
BEGIN
    SELECT status, quality_report_hash
      INTO release_status, release_quality_hash
      FROM ops.tushare_data_release
     WHERE release_tag = NEW.data_baseline_tag;
    IF NEW.baseline_quality_report_hash IS NULL
       OR (release_status = 'validated'
           AND NEW.baseline_quality_report_hash IS DISTINCT FROM release_quality_hash)
       OR (NEW.status IN ('BUILDING', 'VALIDATED', 'FROZEN')
           AND release_status IS DISTINCT FROM 'validated') THEN
        RAISE EXCEPTION
            'Dataset baseline quality hash does not match validated release: %',
            NEW.data_baseline_tag;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_dataset_baseline_binding ON backtest.dataset_manifest;
CREATE TRIGGER trg_backtest_dataset_baseline_binding
BEFORE INSERT OR UPDATE ON backtest.dataset_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_baseline_binding();

ALTER TABLE IF EXISTS backtest.signal
    ADD COLUMN IF NOT EXISTS signal_score DOUBLE PRECISION NOT NULL DEFAULT 0;

ALTER TABLE IF EXISTS backtest.order
    ADD COLUMN IF NOT EXISTS planned_shares INTEGER NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'backtest.order'::regclass
           AND conname = 'backtest_order_planned_shares_check'
    ) THEN
        ALTER TABLE backtest.order
            ADD CONSTRAINT backtest_order_planned_shares_check
            CHECK (planned_shares >= 0);
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION backtest.assert_run_manifest_state()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    dataset_status VARCHAR(20);
    dataset_input_hash CHAR(64);
    dataset_market_rule_version VARCHAR(80);
    dataset_fee_rule_version VARCHAR(80);
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.status IN ('RUNNING','SUCCEEDED') THEN
            RAISE EXCEPTION 'running/succeeded backtest run cannot be deleted: %', OLD.run_id;
        END IF;
        RETURN OLD;
    END IF;
    IF OLD.status = 'SUCCEEDED' THEN
        RAISE EXCEPTION 'succeeded backtest run is immutable: %', OLD.run_id;
    END IF;
    IF OLD.status = 'RUNNING' AND NEW.status IN ('SUCCEEDED', 'FAILED', 'CANCELLED') THEN
        IF NEW.run_id IS DISTINCT FROM OLD.run_id
           OR NEW.dataset_id IS DISTINCT FROM OLD.dataset_id
           OR NEW.strategy_id IS DISTINCT FROM OLD.strategy_id
           OR NEW.strategy_version IS DISTINCT FROM OLD.strategy_version
           OR NEW.run_version IS DISTINCT FROM OLD.run_version
           OR NEW.initial_cash IS DISTINCT FROM OLD.initial_cash
           OR NEW.max_positions IS DISTINCT FROM OLD.max_positions
           OR NEW.max_single_position_pct IS DISTINCT FROM OLD.max_single_position_pct
           OR NEW.max_portfolio_risk_pct IS DISTINCT FROM OLD.max_portfolio_risk_pct
           OR NEW.reserve_cash_pct IS DISTINCT FROM OLD.reserve_cash_pct
           OR NEW.market_rule_version IS DISTINCT FROM OLD.market_rule_version
           OR NEW.fee_rule_version IS DISTINCT FROM OLD.fee_rule_version
           OR NEW.execution_model_version IS DISTINCT FROM OLD.execution_model_version
           OR NEW.random_seed IS DISTINCT FROM OLD.random_seed
           OR NEW.parameters IS DISTINCT FROM OLD.parameters
           OR NEW.parameters_hash IS DISTINCT FROM OLD.parameters_hash
           OR NEW.input_hash IS DISTINCT FROM OLD.input_hash
           OR NEW.code_version IS DISTINCT FROM OLD.code_version
           OR NEW.git_commit IS DISTINCT FROM OLD.git_commit
           OR NEW.git_dirty IS DISTINCT FROM OLD.git_dirty
           OR NEW.diff_hash IS DISTINCT FROM OLD.diff_hash
           OR NEW.code_archive_path IS DISTINCT FROM OLD.code_archive_path
           OR NEW.created_at IS DISTINCT FROM OLD.created_at
           OR NEW.started_at IS DISTINCT FROM OLD.started_at THEN
            RAISE EXCEPTION 'RUNNING 回测结束时不允许改写运行输入: %', OLD.run_id;
        END IF;
    END IF;
    IF NEW.status <> OLD.status AND NOT (
        (OLD.status = 'DRAFT' AND NEW.status IN ('RUNNING','FAILED','CANCELLED')) OR
        (OLD.status = 'RUNNING' AND NEW.status IN ('SUCCEEDED','FAILED','CANCELLED')) OR
        (OLD.status = 'FAILED' AND NEW.status = 'RUNNING')
    ) THEN
        RAISE EXCEPTION 'invalid backtest run status transition: % -> %', OLD.status, NEW.status;
    END IF;
    IF NEW.status IN ('RUNNING','SUCCEEDED') THEN
        SELECT status, input_manifest_hash, market_rule_version, fee_rule_version
          INTO dataset_status, dataset_input_hash,
               dataset_market_rule_version, dataset_fee_rule_version
          FROM backtest.dataset_manifest
         WHERE dataset_id = NEW.dataset_id;
        IF dataset_status IS DISTINCT FROM 'FROZEN' THEN
            RAISE EXCEPTION 'formal backtest run requires FROZEN Dataset: % (%)',
                NEW.dataset_id, dataset_status;
        END IF;
        IF NEW.input_hash IS NULL OR NEW.parameters_hash IS NULL THEN
            RAISE EXCEPTION 'formal backtest run requires input and parameters hashes: %', NEW.run_id;
        END IF;
        IF NEW.input_hash IS DISTINCT FROM dataset_input_hash THEN
            RAISE EXCEPTION 'Run input_hash must equal Dataset input_manifest_hash: %', NEW.run_id;
        END IF;
        IF NEW.market_rule_version IS DISTINCT FROM dataset_market_rule_version
           OR NEW.fee_rule_version IS DISTINCT FROM dataset_fee_rule_version THEN
            RAISE EXCEPTION 'Run rule versions must equal Dataset rule versions: %', NEW.run_id;
        END IF;
        IF NEW.git_dirty AND NEW.code_archive_path IS NULL THEN
            RAISE EXCEPTION 'dirty code requires an explicit code archive before running: %', NEW.run_id;
        END IF;
    END IF;
    IF NEW.status = 'RUNNING' AND OLD.status IN ('DRAFT','FAILED') THEN
        NEW.started_at := CURRENT_TIMESTAMP;
    ELSIF NEW.status IN ('SUCCEEDED','FAILED','CANCELLED') AND OLD.status = 'RUNNING' THEN
        NEW.finished_at := CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_run_manifest_state ON backtest.run_manifest;
CREATE TRIGGER trg_backtest_run_manifest_state
BEFORE UPDATE OR DELETE ON backtest.run_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_run_manifest_state();
