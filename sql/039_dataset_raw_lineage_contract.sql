-- T-037: close the remaining immutable Raw lineage gaps for formal Dataset inputs.
-- This migration changes only release/Dataset audit structures. It never mutates Raw facts.

ALTER TABLE backtest.dataset_execution_bar
    ADD COLUMN IF NOT EXISTS limit_source_record_id BIGINT,
    ADD COLUMN IF NOT EXISTS limit_source_payload_hash CHAR(64),
    ADD COLUMN IF NOT EXISTS suspend_source_record_id BIGINT,
    ADD COLUMN IF NOT EXISTS suspend_source_payload_hash CHAR(64);

ALTER TABLE backtest.dataset_calendar
    ADD COLUMN IF NOT EXISTS source_record_version BIGINT;

CREATE OR REPLACE FUNCTION public.assert_release_immutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' AND OLD.status IN ('validated','superseded') THEN
        RAISE EXCEPTION 'validated/superseded data release is immutable: %', OLD.release_tag;
    END IF;
    IF TG_OP='UPDATE' AND OLD.status IN ('validated','superseded') THEN
        IF OLD.status='validated' AND NEW.status='superseded'
           AND NEW.release_id IS NOT DISTINCT FROM OLD.release_id
           AND NEW.release_tag IS NOT DISTINCT FROM OLD.release_tag
           AND NEW.baseline_cutoff_date IS NOT DISTINCT FROM OLD.baseline_cutoff_date
           AND NEW.quality_report_hash IS NOT DISTINCT FROM OLD.quality_report_hash
           AND NEW.quality_report IS NOT DISTINCT FROM OLD.quality_report
           AND NEW.source_manifest IS NOT DISTINCT FROM OLD.source_manifest
           AND NEW.source_manifest_hash IS NOT DISTINCT FROM OLD.source_manifest_hash
           AND NEW.schema_version IS NOT DISTINCT FROM OLD.schema_version
           AND NEW.notes IS NOT DISTINCT FROM OLD.notes
           AND NEW.created_at IS NOT DISTINCT FROM OLD.created_at
           AND NEW.validated_at IS NOT DISTINCT FROM OLD.validated_at THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'validated/superseded data release is immutable: %', OLD.release_tag;
    END IF;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_release_immutable ON ops.tushare_data_release;
CREATE TRIGGER trg_release_immutable
BEFORE UPDATE OR DELETE ON ops.tushare_data_release
FOR EACH ROW EXECUTE FUNCTION public.assert_release_immutable();

CREATE OR REPLACE FUNCTION backtest.assert_dataset_extended_freeze_identity()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status='VALIDATED' AND NEW.status='FROZEN' AND (
        NEW.baseline_source_manifest_hash IS DISTINCT FROM OLD.baseline_source_manifest_hash
        OR NEW.out_of_sample_start IS DISTINCT FROM OLD.out_of_sample_start
        OR NEW.benchmark_code IS DISTINCT FROM OLD.benchmark_code
        OR NEW.benchmark_hash IS DISTINCT FROM OLD.benchmark_hash
    ) THEN
        RAISE EXCEPTION 'VALIDATED Dataset extended input identity cannot change during freeze: %', OLD.dataset_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dataset_extended_freeze_identity ON backtest.dataset_manifest;
CREATE TRIGGER trg_dataset_extended_freeze_identity
BEFORE UPDATE ON backtest.dataset_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_extended_freeze_identity();

CREATE OR REPLACE FUNCTION backtest.assert_dataset_raw_lineage_complete()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE bad_execution BIGINT; bad_calendar BIGINT; bad_dividend BIGINT;
BEGIN
    IF NEW.status='FROZEN' THEN
        SELECT COUNT(*) INTO bad_execution
          FROM backtest.dataset_execution_bar
         WHERE dataset_id=NEW.dataset_id
           AND (
               ((can_buy IS TRUE OR can_sell IS TRUE)
                AND (source_payload_hash IS NULL
                     OR limit_source_record_id IS NULL
                     OR limit_source_payload_hash IS NULL))
               OR (is_suspended IS TRUE
                   AND suspend_source_record_id IS NULL)
           );
        SELECT COUNT(*) INTO bad_calendar
          FROM backtest.dataset_calendar
         WHERE dataset_id=NEW.dataset_id
           AND source_payload_hash IS NULL;
        SELECT COUNT(*) INTO bad_dividend
          FROM backtest.dataset_dividend_event
         WHERE dataset_id=NEW.dataset_id
           AND (source_record_version IS NULL OR source_payload_hash IS NULL);
        IF bad_execution<>0 OR bad_calendar<>0 OR bad_dividend<>0 THEN
            RAISE EXCEPTION
                'FROZEN Dataset Raw lineage incomplete: % execution=% calendar=% dividend=%',
                NEW.dataset_id,bad_execution,bad_calendar,bad_dividend;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dataset_raw_lineage_complete ON backtest.dataset_manifest;
CREATE TRIGGER trg_dataset_raw_lineage_complete
BEFORE INSERT OR UPDATE ON backtest.dataset_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_raw_lineage_complete();
