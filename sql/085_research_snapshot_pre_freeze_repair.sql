-- Allow an explicit, auditable repair before a research snapshot is frozen.
-- FROZEN remains immutable. The application path must provide a non-empty
-- PRE_FREEZE_REPAIR reason and hold the series advisory lock.

CREATE OR REPLACE FUNCTION research.assert_snapshot_status_transition()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'research snapshot manifest is append-only; DELETE is forbidden: %', OLD.snapshot_id;
    END IF;
    IF OLD.status = 'FROZEN' THEN
        RAISE EXCEPTION 'FROZEN research snapshot is immutable: %', OLD.snapshot_id;
    END IF;
    IF NEW.status <> OLD.status AND NOT (
        (OLD.status = 'DRAFT' AND NEW.status IN ('BUILDING','FAILED')) OR
        (OLD.status = 'FAILED' AND NEW.status IN ('BUILDING','FAILED')) OR
        (OLD.status = 'BUILDING' AND NEW.status IN ('VALIDATED','FAILED')) OR
        (OLD.status = 'VALIDATED' AND NEW.status IN ('FROZEN','FAILED'))
    ) THEN
        RAISE EXCEPTION 'invalid research snapshot status transition: % -> %', OLD.status, NEW.status;
    END IF;
    IF OLD.status = 'VALIDATED' AND NEW.status = 'FAILED'
       AND NULLIF(BTRIM(COALESCE(NEW.failure_reason, '')), '') IS NULL THEN
        RAISE EXCEPTION 'pre-freeze VALIDATED repair requires failure_reason: %', NEW.snapshot_id;
    END IF;
    IF NEW.status = 'FROZEN' THEN
        IF OLD.status <> 'VALIDATED' THEN
            RAISE EXCEPTION 'only VALIDATED snapshot can be FROZEN: %', NEW.snapshot_id;
        END IF;
        IF NEW.quality_report_hash IS NULL OR NEW.input_manifest_hash IS NULL THEN
            RAISE EXCEPTION 'FROZEN snapshot requires quality and input hashes: %', NEW.snapshot_id;
        END IF;
        IF NEW.schema_version = 'research-v1.2' THEN
            IF NEW.baseline_quality_report_hash IS NULL THEN
                RAISE EXCEPTION 'research-v1.2 requires a bound baseline quality hash: %', NEW.snapshot_id;
            END IF;
            IF NEW.git_dirty AND (
                NEW.code_archive_path IS NULL OR NEW.code_archive_sha256 IS NULL
                OR NEW.code_archive_size_bytes IS NULL OR NEW.code_archive_size_bytes <= 0
            ) THEN
                RAISE EXCEPTION 'dirty research-v1.2 code requires a content-addressed archive: %', NEW.snapshot_id;
            END IF;
        ELSIF NEW.git_dirty AND NEW.code_archive_path IS NULL THEN
            RAISE EXCEPTION 'dirty code requires an explicit code archive: %', NEW.snapshot_id;
        END IF;
        NEW.frozen_at := CURRENT_TIMESTAMP;
    END IF;
    IF NEW.status = 'BUILDING' AND OLD.status IN ('DRAFT','FAILED') THEN
        NEW.built_at := CURRENT_TIMESTAMP;
    ELSIF NEW.status = 'VALIDATED' THEN
        NEW.validated_at := CURRENT_TIMESTAMP;
    ELSIF NEW.status = 'FAILED' AND OLD.status = 'VALIDATED' THEN
        NEW.failed_at := CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_research_snapshot_status ON research.snapshot_manifest;
CREATE TRIGGER trg_research_snapshot_status
BEFORE UPDATE OR DELETE ON research.snapshot_manifest
FOR EACH ROW EXECUTE FUNCTION research.assert_snapshot_status_transition();
