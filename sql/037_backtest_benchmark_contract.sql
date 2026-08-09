-- T-037: immutable benchmark input and excess-return contract.

ALTER TABLE backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS benchmark_code VARCHAR(20) NOT NULL DEFAULT '000300.SH',
    ADD COLUMN IF NOT EXISTS benchmark_hash CHAR(64);

CREATE TABLE IF NOT EXISTS backtest.dataset_benchmark_bar (
    dataset_id UUID NOT NULL REFERENCES backtest.dataset_manifest(dataset_id),
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    source_record_key VARCHAR(180) NOT NULL,
    source_record_version BIGINT NOT NULL,
    source_payload_hash CHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(dataset_id,trade_date),
    CHECK (close > 0)
);

DROP TRIGGER IF EXISTS trg_backtest_dataset_benchmark_mutable
    ON backtest.dataset_benchmark_bar;
CREATE TRIGGER trg_backtest_dataset_benchmark_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.dataset_benchmark_bar
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_child_mutable();

CREATE OR REPLACE FUNCTION backtest.assert_dataset_benchmark_complete()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE member_count BIGINT; benchmark_count BIGINT; wrong_code BIGINT;
BEGIN
    IF NEW.status='FROZEN' THEN
        SELECT COUNT(*) INTO member_count
          FROM backtest.dataset_member WHERE dataset_id=NEW.dataset_id;
        SELECT COUNT(*), COUNT(*) FILTER (WHERE ts_code<>NEW.benchmark_code)
          INTO benchmark_count,wrong_code
          FROM backtest.dataset_benchmark_bar WHERE dataset_id=NEW.dataset_id;
        IF NEW.benchmark_hash IS NULL OR member_count=0
           OR benchmark_count<>member_count OR wrong_code<>0 THEN
            RAISE EXCEPTION 'FROZEN Dataset benchmark incomplete: %', NEW.dataset_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dataset_benchmark_complete ON backtest.dataset_manifest;
CREATE TRIGGER trg_dataset_benchmark_complete
BEFORE INSERT OR UPDATE ON backtest.dataset_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_benchmark_complete();

