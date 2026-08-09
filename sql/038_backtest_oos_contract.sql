-- T-037: every formal Dataset declares a deterministic out-of-sample period.

ALTER TABLE backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS out_of_sample_start DATE;

ALTER TABLE backtest.dataset_manifest
    DROP CONSTRAINT IF EXISTS backtest_dataset_oos_range_check;
ALTER TABLE backtest.dataset_manifest
    ADD CONSTRAINT backtest_dataset_oos_range_check
    CHECK (out_of_sample_start IS NULL OR
           (evaluation_start <= out_of_sample_start
            AND out_of_sample_start <= evaluation_end));

CREATE OR REPLACE FUNCTION backtest.assert_dataset_oos_contract()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE open_days BIGINT;
BEGIN
    IF NEW.status='FROZEN' THEN
        IF NEW.out_of_sample_start IS NULL THEN
            RAISE EXCEPTION 'FROZEN Dataset requires out_of_sample_start: %', NEW.dataset_id;
        END IF;
        SELECT COUNT(*) INTO open_days
          FROM backtest.dataset_calendar
         WHERE dataset_id=NEW.dataset_id AND is_open IS TRUE
           AND trade_date BETWEEN NEW.out_of_sample_start AND NEW.evaluation_end;
        IF open_days=0 THEN
            RAISE EXCEPTION 'FROZEN Dataset has empty out-of-sample period: %', NEW.dataset_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dataset_oos_contract ON backtest.dataset_manifest;
CREATE TRIGGER trg_dataset_oos_contract
BEFORE INSERT OR UPDATE ON backtest.dataset_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_oos_contract();

