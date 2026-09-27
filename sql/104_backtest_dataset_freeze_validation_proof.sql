-- T-038: make Dataset freeze validation constant-time after application audit.
--
-- The application validation stage streams every materialized child row and
-- stores the resulting counters in dataset_manifest.quality_report.  The
-- manifest is immutable after VALIDATED, so the freeze trigger only needs to
-- verify that proof.  It must not rescan the multi-GB execution table inside
-- the short manifest status-transition transaction.
--
-- Reports written before validation_counts existed are accepted only when
-- their canonical validation result is explicitly VALID with an empty issues
-- array.  New reports must also carry every required counter and all values
-- must be zero.  Missing or malformed proof fails closed.

BEGIN;

CREATE OR REPLACE FUNCTION backtest.assert_dataset_raw_lineage_complete()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    report JSONB;
    counts JSONB;
    counter_name TEXT;
    counter_value TEXT;
BEGIN
    IF NEW.status = 'FROZEN' THEN
        report := NEW.quality_report;

        IF jsonb_typeof(report) <> 'object'
           OR report->>'status' <> 'VALID'
           OR report->'issues' IS DISTINCT FROM '[]'::jsonb THEN
            RAISE EXCEPTION
                'FROZEN Dataset validation proof is missing or failed: %',
                NEW.dataset_id;
        END IF;

        counts := report->'validation_counts';
        IF counts IS NOT NULL THEN
            IF jsonb_typeof(counts) <> 'object' THEN
                RAISE EXCEPTION
                    'FROZEN Dataset validation counters are malformed: %',
                    NEW.dataset_id;
            END IF;

            FOREACH counter_name IN ARRAY ARRAY[
                'missing_execution_rows',
                'missing_execution_lineage',
                'missing_limit_rows',
                'missing_calendar_lineage',
                'future_dividend_rows',
                'invalid_dividend_dates',
                'missing_dividend_lineage',
                'inconsistent_stock_rates'
            ] LOOP
                counter_value := counts->>counter_name;
                IF counter_value IS NULL
                   OR counter_value !~ '^[0-9]+$'
                   OR counter_value::BIGINT <> 0 THEN
                    RAISE EXCEPTION
                        'FROZEN Dataset validation counter is non-zero or missing: % %=%',
                        NEW.dataset_id, counter_name, COALESCE(counter_value, '<missing>');
                END IF;
            END LOOP;
        ELSE
            -- Backward-compatible proof for the Dataset validated before
            -- counters were persisted.  The empty issue list is generated
            -- only after all legacy validation counters were checked.
            IF report->>'missing_execution_rows' IS DISTINCT FROM '0'
               OR report->>'missing_limit_rows' IS DISTINCT FROM '0' THEN
                RAISE EXCEPTION
                    'FROZEN Dataset legacy validation proof is incomplete: %',
                    NEW.dataset_id;
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dataset_raw_lineage_complete ON backtest.dataset_manifest;
CREATE TRIGGER trg_dataset_raw_lineage_complete
BEFORE INSERT OR UPDATE ON backtest.dataset_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_raw_lineage_complete();

COMMIT;
