-- Dataset 按交易日批次构建 checkpoint。
-- 只记录 backtest 物化进度，不写入或修改任何 Raw/Standard/PIT/Feature 表。
CREATE SCHEMA IF NOT EXISTS backtest;

CREATE TABLE IF NOT EXISTS backtest.dataset_build_batch_checkpoint (
    dataset_id UUID NOT NULL REFERENCES backtest.dataset_manifest(dataset_id),
    stage VARCHAR(20) NOT NULL DEFAULT 'BUILD',
    batch_start DATE NOT NULL,
    batch_end DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    row_count BIGINT NOT NULL DEFAULT 0,
    batch_hash CHAR(64),
    code_version VARCHAR(180),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    heartbeat_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    PRIMARY KEY (dataset_id, stage, batch_start),
    CHECK (stage = 'BUILD'),
    CHECK (batch_start <= batch_end),
    CHECK (status IN ('RUNNING','COMPLETED','FAILED')),
    CHECK (row_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_dataset_build_batch_checkpoint_status
    ON backtest.dataset_build_batch_checkpoint (dataset_id, stage, status, batch_start);

CREATE OR REPLACE FUNCTION backtest.assert_dataset_build_batch_mutable()
RETURNS TRIGGER AS $$
DECLARE
    dataset_status VARCHAR(20);
BEGIN
    SELECT status INTO dataset_status
      FROM backtest.dataset_manifest
     WHERE dataset_id = COALESCE(NEW.dataset_id, OLD.dataset_id);
    IF dataset_status IN ('VALIDATED', 'FROZEN') THEN
        RAISE EXCEPTION 'validated/frozen Dataset batch checkpoint is immutable: %',
            COALESCE(NEW.dataset_id, OLD.dataset_id);
    END IF;
    IF TG_OP = 'DELETE' AND dataset_status = 'FROZEN' THEN
        RAISE EXCEPTION 'frozen Dataset batch checkpoint cannot be deleted: %', OLD.dataset_id;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dataset_build_batch_mutable
    ON backtest.dataset_build_batch_checkpoint;
CREATE TRIGGER trg_dataset_build_batch_mutable
    BEFORE INSERT OR UPDATE OR DELETE ON backtest.dataset_build_batch_checkpoint
    FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_build_batch_mutable();
