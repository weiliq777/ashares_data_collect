-- Backtest Dataset 阶段级构建检查点。
-- 只记录 Dataset 构建状态，不保存或修改任何 Raw/Standard/PIT/Feature 数据。
CREATE SCHEMA IF NOT EXISTS backtest;

CREATE TABLE IF NOT EXISTS backtest.dataset_build_checkpoint (
    dataset_id UUID NOT NULL REFERENCES backtest.dataset_manifest(dataset_id),
    stage VARCHAR(30) NOT NULL,
    status VARCHAR(20) NOT NULL,
    progress_done BIGINT NOT NULL DEFAULT 0,
    progress_total BIGINT,
    last_trade_date DATE,
    stage_hash CHAR(64),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    heartbeat_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    PRIMARY KEY (dataset_id, stage),
    CHECK (stage IN ('BUILD','VALIDATE','FREEZE')),
    CHECK (status IN ('RUNNING','COMPLETED','FAILED')),
    CHECK (progress_done >= 0),
    CHECK (progress_total IS NULL OR progress_total >= 0)
);

CREATE INDEX IF NOT EXISTS idx_dataset_build_checkpoint_status
    ON backtest.dataset_build_checkpoint (status, heartbeat_at DESC);

CREATE OR REPLACE FUNCTION backtest.assert_dataset_checkpoint_mutable()
RETURNS TRIGGER AS $$
DECLARE
    current_status VARCHAR(20);
BEGIN
    SELECT status INTO current_status
      FROM backtest.dataset_manifest
     WHERE dataset_id = COALESCE(NEW.dataset_id, OLD.dataset_id);
    IF current_status = 'FROZEN' THEN
        RAISE EXCEPTION 'FROZEN Dataset 的构建 checkpoint 不可修改';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dataset_build_checkpoint_mutable
    ON backtest.dataset_build_checkpoint;
CREATE TRIGGER trg_dataset_build_checkpoint_mutable
    BEFORE INSERT OR UPDATE OR DELETE ON backtest.dataset_build_checkpoint
    FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_checkpoint_mutable();
