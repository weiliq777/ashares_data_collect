-- Bounded, immutable candidates cut from the transactional release-change ledger.
-- This migration creates control-plane metadata only; it does not scan or
-- rewrite Raw/Standard/PIT/Feature facts and does not promote delta packages.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15s';

ALTER TABLE ops.tushare_data_release
    ADD COLUMN IF NOT EXISTS release_change_watermark BIGINT;
ALTER TABLE ops.tushare_data_release
    DROP CONSTRAINT IF EXISTS tushare_data_release_change_watermark_check;
ALTER TABLE ops.tushare_data_release
    ADD CONSTRAINT tushare_data_release_change_watermark_check
    CHECK (release_change_watermark IS NULL OR release_change_watermark >= 0);

-- The immutable-release trigger from migration 109 predates this field. Permit
-- the watermark only on the one draft -> validated transition; all later edits
-- (including validated -> superseded) must preserve it exactly.
CREATE OR REPLACE FUNCTION public.assert_release_change_watermark_immutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.release_change_watermark IS DISTINCT FROM NEW.release_change_watermark
       AND NOT (
           OLD.status = 'draft'
           AND NEW.status = 'validated'
           AND NEW.release_change_watermark IS NOT NULL
       ) THEN
        RAISE EXCEPTION 'release change watermark is immutable after validation: %', OLD.release_tag;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_release_change_watermark_immutable
    ON ops.tushare_data_release;
CREATE TRIGGER trg_release_change_watermark_immutable
BEFORE UPDATE OF release_change_watermark ON ops.tushare_data_release
FOR EACH ROW EXECUTE FUNCTION public.assert_release_change_watermark_immutable();

CREATE TABLE IF NOT EXISTS ops.release_delta_package (
    package_id UUID PRIMARY KEY,
    baseline_release_id BIGINT NOT NULL
        REFERENCES ops.tushare_data_release(release_id),
    baseline_domain VARCHAR(32) NOT NULL
        CHECK (baseline_domain IN ('RESEARCH', 'MARKET_ACTIVITY')),
    event_from_id BIGINT NOT NULL CHECK (event_from_id > 0),
    event_through_id BIGINT NOT NULL CHECK (event_through_id >= event_from_id),
    event_count INTEGER NOT NULL CHECK (event_count BETWEEN 1 AND 1000),
    event_manifest_hash CHAR(64) NOT NULL
        CHECK (event_manifest_hash ~ '^[0-9a-f]{64}$'),
    scope_manifest JSONB NOT NULL
        CHECK (jsonb_typeof(scope_manifest) = 'object'),
    captured_run_id TEXT NOT NULL,
    package_status VARCHAR(16) NOT NULL DEFAULT 'CAPTURED'
        CHECK (package_status = 'CAPTURED'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (baseline_release_id, event_manifest_hash),
    UNIQUE (package_id, baseline_domain)
);

CREATE INDEX IF NOT EXISTS idx_release_delta_package_parent
    ON ops.release_delta_package (baseline_release_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ops.release_delta_event (
    package_id UUID NOT NULL
        REFERENCES ops.release_delta_package(package_id),
    baseline_domain VARCHAR(32) NOT NULL
        CHECK (baseline_domain IN ('RESEARCH', 'MARKET_ACTIVITY')),
    event_id BIGINT NOT NULL
        REFERENCES ops.release_change_event(event_id),
    PRIMARY KEY (package_id, event_id),
    FOREIGN KEY (package_id, baseline_domain)
        REFERENCES ops.release_delta_package(package_id, baseline_domain),
    UNIQUE (baseline_domain, event_id)
);

CREATE OR REPLACE FUNCTION ops.reject_release_delta_package_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'captured release delta packages are immutable';
END;
$$;

DROP TRIGGER IF EXISTS trg_release_delta_package_append_only
    ON ops.release_delta_package;
CREATE TRIGGER trg_release_delta_package_append_only
BEFORE UPDATE OR DELETE ON ops.release_delta_package
FOR EACH ROW EXECUTE FUNCTION ops.reject_release_delta_package_mutation();

DROP TRIGGER IF EXISTS trg_release_delta_event_append_only
    ON ops.release_delta_event;
CREATE TRIGGER trg_release_delta_event_append_only
BEFORE UPDATE OR DELETE ON ops.release_delta_event
FOR EACH ROW EXECUTE FUNCTION ops.reject_release_delta_package_mutation();

GRANT SELECT ON ops.release_delta_package, ops.release_delta_event
    TO contract_publisher, research_reader;
GRANT INSERT ON ops.release_delta_package, ops.release_delta_event
    TO contract_publisher;
REVOKE UPDATE, DELETE, TRUNCATE ON ops.release_delta_package, ops.release_delta_event
    FROM PUBLIC, raw_writer, standard_writer, pit_writer, feature_writer,
         contract_publisher, research_reader;

COMMENT ON COLUMN ops.tushare_data_release.release_change_watermark IS
    'Event ledger boundary captured before full quality audit and verified unchanged at publication.';
COMMENT ON TABLE ops.release_delta_package IS
    'Immutable bounded candidate package; CAPTURED is not quality-validated and is not an active release.';
COMMENT ON TABLE ops.release_delta_event IS
    'Exact event membership for a captured candidate; one event can be linked at most once per baseline domain.';

COMMIT;
