-- Separate validated release pointers for independent data domains.
-- This migration changes release metadata only; it never reads or rewrites fact tables.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15s';

ALTER TABLE ops.tushare_data_release
    ADD COLUMN IF NOT EXISTS baseline_domain VARCHAR(32);

-- Existing release rows predate the domain field. Temporarily remove only the
-- immutability trigger inside this transaction so their derived classification
-- can be backfilled; no release payload, status, or business fact is changed.
DROP TRIGGER IF EXISTS trg_release_immutable ON ops.tushare_data_release;

UPDATE ops.tushare_data_release
   SET baseline_domain = CASE
       WHEN schema_version = 'market-activity-v1' THEN 'MARKET_ACTIVITY'
       ELSE 'RESEARCH'
   END
 WHERE baseline_domain IS NULL;

ALTER TABLE ops.tushare_data_release
    ALTER COLUMN baseline_domain SET NOT NULL;

ALTER TABLE ops.tushare_data_release
    DROP CONSTRAINT IF EXISTS tushare_data_release_baseline_domain_check;
ALTER TABLE ops.tushare_data_release
    ADD CONSTRAINT tushare_data_release_baseline_domain_check
    CHECK (baseline_domain IN ('RESEARCH', 'MARKET_ACTIVITY'));

-- Replace the historical global singleton constraint with one current release
-- per domain. Market Activity and Research baselines may coexist.
DROP INDEX IF EXISTS ops.uq_tushare_validated_release;
CREATE UNIQUE INDEX IF NOT EXISTS uq_tushare_validated_release_domain
    ON ops.tushare_data_release (baseline_domain)
    WHERE status = 'validated';

CREATE INDEX IF NOT EXISTS idx_tushare_release_domain_status_cutoff
    ON ops.tushare_data_release (baseline_domain, status, baseline_cutoff_date DESC);

CREATE OR REPLACE FUNCTION public.assert_release_immutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' AND OLD.status IN ('validated','superseded','revoked') THEN
        RAISE EXCEPTION 'released baseline is immutable: %', OLD.release_tag;
    END IF;

    IF TG_OP = 'UPDATE' AND OLD.status IN ('validated','superseded','revoked') THEN
        IF OLD.status = 'validated' AND NEW.status = 'superseded'
           AND NEW.release_id IS NOT DISTINCT FROM OLD.release_id
           AND NEW.release_tag IS NOT DISTINCT FROM OLD.release_tag
           AND NEW.baseline_domain IS NOT DISTINCT FROM OLD.baseline_domain
           AND NEW.baseline_cutoff_date IS NOT DISTINCT FROM OLD.baseline_cutoff_date
           AND NEW.quality_report_hash IS NOT DISTINCT FROM OLD.quality_report_hash
           AND NEW.quality_report IS NOT DISTINCT FROM OLD.quality_report
           AND NEW.source_manifest IS NOT DISTINCT FROM OLD.source_manifest
           AND NEW.source_manifest_hash IS NOT DISTINCT FROM OLD.source_manifest_hash
           AND NEW.schema_version IS NOT DISTINCT FROM OLD.schema_version
           AND NEW.notes IS NOT DISTINCT FROM OLD.notes
           AND NEW.created_at IS NOT DISTINCT FROM OLD.created_at
           AND NEW.validated_at IS NOT DISTINCT FROM OLD.validated_at
           AND NEW.data_cutoff_timestamp IS NOT DISTINCT FROM OLD.data_cutoff_timestamp
           AND NEW.visibility_policy_version IS NOT DISTINCT FROM OLD.visibility_policy_version
           AND NEW.temporal_evidence_level IS NOT DISTINCT FROM OLD.temporal_evidence_level
           AND NEW.revoked_at IS NOT DISTINCT FROM OLD.revoked_at
           AND NEW.revoked_by IS NOT DISTINCT FROM OLD.revoked_by
           AND NEW.revocation_reason IS NOT DISTINCT FROM OLD.revocation_reason
           AND NEW.revocation_evidence_hash IS NOT DISTINCT FROM OLD.revocation_evidence_hash THEN
            RETURN NEW;
        END IF;

        IF OLD.status IN ('validated','superseded') AND NEW.status = 'revoked'
           AND NEW.release_id IS NOT DISTINCT FROM OLD.release_id
           AND NEW.release_tag IS NOT DISTINCT FROM OLD.release_tag
           AND NEW.baseline_domain IS NOT DISTINCT FROM OLD.baseline_domain
           AND NEW.baseline_cutoff_date IS NOT DISTINCT FROM OLD.baseline_cutoff_date
           AND NEW.quality_report_hash IS NOT DISTINCT FROM OLD.quality_report_hash
           AND NEW.quality_report IS NOT DISTINCT FROM OLD.quality_report
           AND NEW.source_manifest IS NOT DISTINCT FROM OLD.source_manifest
           AND NEW.source_manifest_hash IS NOT DISTINCT FROM OLD.source_manifest_hash
           AND NEW.schema_version IS NOT DISTINCT FROM OLD.schema_version
           AND NEW.notes IS NOT DISTINCT FROM OLD.notes
           AND NEW.created_at IS NOT DISTINCT FROM OLD.created_at
           AND NEW.validated_at IS NOT DISTINCT FROM OLD.validated_at
           AND NEW.data_cutoff_timestamp IS NOT DISTINCT FROM OLD.data_cutoff_timestamp
           AND NEW.visibility_policy_version IS NOT DISTINCT FROM OLD.visibility_policy_version
           AND NEW.temporal_evidence_level IS NOT DISTINCT FROM OLD.temporal_evidence_level THEN
            RETURN NEW;
        END IF;

        RAISE EXCEPTION 'validated/superseded/revoked release is immutable: %', OLD.release_tag;
    END IF;

    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_release_immutable
BEFORE UPDATE OR DELETE ON ops.tushare_data_release
FOR EACH ROW EXECUTE FUNCTION public.assert_release_immutable();

COMMENT ON COLUMN ops.tushare_data_release.baseline_domain IS
    '独立数据发布域；RESEARCH 与 MARKET_ACTIVITY 的 validated 基线互不替代。';

COMMIT;
