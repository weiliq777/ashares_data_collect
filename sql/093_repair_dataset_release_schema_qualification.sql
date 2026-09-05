-- Repair the database functions used by Dataset (回测数据集) triggers after
-- the operational release table moved from public to ops.
-- This is a metadata-only repair: it changes no Dataset or foundation facts.

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

CREATE OR REPLACE FUNCTION backtest.assert_dataset_source_boundary()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    release_hash CHAR(64);
    bad_members BIGINT;
BEGIN
    IF NEW.status = 'FROZEN' THEN
        SELECT source_manifest_hash
          INTO release_hash
          FROM ops.tushare_data_release
         WHERE release_tag = NEW.data_baseline_tag;
        IF NEW.baseline_source_manifest_hash IS NULL
           OR release_hash IS NULL
           OR btrim(NEW.baseline_source_manifest_hash) <> btrim(release_hash) THEN
            RAISE EXCEPTION 'FROZEN Dataset Raw boundary mismatch: %', NEW.dataset_id;
        END IF;
        SELECT COUNT(*)
          INTO bad_members
          FROM backtest.dataset_member d
          JOIN research.snapshot_manifest s
            ON s.snapshot_id = d.research_snapshot_id
         WHERE d.dataset_id = NEW.dataset_id
           AND s.baseline_source_manifest_hash IS DISTINCT FROM NEW.baseline_source_manifest_hash;
        IF bad_members <> 0 THEN
            RAISE EXCEPTION 'Dataset members use different Raw boundaries: %/%',
                NEW.dataset_id, bad_members;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION backtest.assert_dataset_baseline_binding() IS
    'Dataset trigger: bind to validated ops.tushare_data_release';
COMMENT ON FUNCTION backtest.assert_dataset_source_boundary() IS
    'Dataset trigger: bind frozen datasets to ops.tushare_data_release source manifest';
