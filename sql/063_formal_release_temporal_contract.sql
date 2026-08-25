-- T-063: formal release temporal semantics and revocation audit.
-- This migration is metadata-only.  It does not copy or rewrite any fact
-- table and therefore cannot create a large transaction.

ALTER TABLE ops.tushare_data_release
    ADD COLUMN IF NOT EXISTS data_cutoff_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS visibility_policy_version VARCHAR(80),
    ADD COLUMN IF NOT EXISTS temporal_evidence_level VARCHAR(30),
    ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS revoked_by VARCHAR(120),
    ADD COLUMN IF NOT EXISTS revocation_reason TEXT,
    ADD COLUMN IF NOT EXISTS revocation_evidence_hash CHAR(64);

-- The previous trigger intentionally blocks all writes to released rows.  The
-- following backfill only populates newly-added temporal metadata, so remove
-- that old trigger for the duration of this metadata-only compatibility step;
-- the hardened trigger is recreated below before the migration ends.
DROP TRIGGER IF EXISTS trg_release_immutable ON ops.tushare_data_release;

UPDATE ops.tushare_data_release
   SET visibility_policy_version = COALESCE(visibility_policy_version, 'legacy-business-pit-v1'),
       temporal_evidence_level = COALESCE(temporal_evidence_level, 'LEGACY')
 WHERE visibility_policy_version IS NULL OR temporal_evidence_level IS NULL;

ALTER TABLE ops.tushare_data_release
    DROP CONSTRAINT IF EXISTS tushare_data_release_status_check;
ALTER TABLE ops.tushare_data_release
    ADD CONSTRAINT tushare_data_release_status_check
    CHECK (status IN ('draft','validated','superseded','revoked'));

ALTER TABLE ops.tushare_data_release
    DROP CONSTRAINT IF EXISTS tushare_data_release_temporal_evidence_check;
ALTER TABLE ops.tushare_data_release
    ADD CONSTRAINT tushare_data_release_temporal_evidence_check
    CHECK (temporal_evidence_level IS NULL OR temporal_evidence_level IN
           ('OFFICIAL_BUSINESS_PIT','WAREHOUSE_OBSERVED','LEGACY'));

ALTER TABLE ops.tushare_data_release
    DROP CONSTRAINT IF EXISTS tushare_data_release_revocation_check;
ALTER TABLE ops.tushare_data_release
    ADD CONSTRAINT tushare_data_release_revocation_check
    CHECK (status <> 'revoked' OR (
        revoked_at IS NOT NULL
        AND NULLIF(BTRIM(revoked_by), '') IS NOT NULL
        AND NULLIF(BTRIM(revocation_reason), '') IS NOT NULL
        AND revocation_evidence_hash IS NOT NULL
        AND length(BTRIM(revocation_evidence_hash)) = 64
    ));

CREATE TABLE IF NOT EXISTS ops.data_release_event (
    event_id BIGSERIAL PRIMARY KEY,
    release_tag VARCHAR(100) NOT NULL,
    event_type VARCHAR(30) NOT NULL
        CHECK (event_type IN ('VALIDATED','SUPERSEDED','REVOKED')),
    status_before VARCHAR(30),
    status_after VARCHAR(30) NOT NULL,
    reason TEXT,
    evidence_hash CHAR(64),
    actor VARCHAR(120) NOT NULL DEFAULT CURRENT_USER,
    event_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_data_release_event_tag_time
    ON ops.data_release_event(release_tag, event_at, event_id);

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

DROP TRIGGER IF EXISTS trg_release_immutable ON ops.tushare_data_release;
CREATE TRIGGER trg_release_immutable
BEFORE UPDATE OR DELETE ON ops.tushare_data_release
FOR EACH ROW EXECUTE FUNCTION public.assert_release_immutable();

CREATE OR REPLACE FUNCTION public.audit_release_status_change()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO ops.data_release_event
            (release_tag, event_type, status_before, status_after,
             reason, evidence_hash, actor)
        VALUES
            (NEW.release_tag,
             UPPER(NEW.status), OLD.status, NEW.status,
             NEW.revocation_reason, NEW.revocation_evidence_hash,
             COALESCE(NULLIF(current_setting('app.release_actor', true), ''), CURRENT_USER));
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_release_status_audit ON ops.tushare_data_release;
CREATE TRIGGER trg_release_status_audit
AFTER UPDATE ON ops.tushare_data_release
FOR EACH ROW EXECUTE FUNCTION public.audit_release_status_change();

COMMENT ON COLUMN ops.tushare_data_release.baseline_cutoff_date IS
    '业务数据截止日；不得解释为本地抓取时间。';
COMMENT ON COLUMN ops.tushare_data_release.data_cutoff_timestamp IS
    '仓库可见性截止时间，仅对具备仓库观测证据的来源生效。';
COMMENT ON COLUMN ops.tushare_data_release.temporal_evidence_level IS
    'OFFICIAL_BUSINESS_PIT、WAREHOUSE_OBSERVED 或兼容历史数据 LEGACY。';
COMMENT ON COLUMN ops.tushare_data_release.status IS
    'validated=当前合格，superseded=正常替代且允许精确复现，revoked=污染/缺陷基线禁止新研究。';

GRANT SELECT, INSERT ON ops.data_release_event TO contract_publisher;
GRANT USAGE, SELECT ON SEQUENCE ops.data_release_event_event_id_seq TO contract_publisher;
