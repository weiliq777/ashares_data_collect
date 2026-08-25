-- Reversible physical schema-migration control plane.
-- Metadata only: no existing fact table is moved or copied.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.foundation_table_migration (
    migration_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_table VARCHAR(127) NOT NULL,
    target_table VARCHAR(127) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'REGISTERED',
    source_row_count BIGINT,
    target_row_count BIGINT,
    source_content_hash CHAR(64),
    target_content_hash CHAR(64),
    validated_at TIMESTAMPTZ,
    cutover_at TIMESTAMPTZ,
    rollback_at TIMESTAMPTZ,
    approved_by VARCHAR(120),
    notes TEXT NOT NULL DEFAULT '',
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (source_table <> target_table),
    CHECK (status IN ('REGISTERED', 'DUAL_READ', 'DUAL_WRITE', 'VALIDATED',
                      'CUTOVER', 'ROLLED_BACK', 'ABORTED')),
    CHECK (source_row_count IS NULL OR source_row_count >= 0),
    CHECK (target_row_count IS NULL OR target_row_count >= 0),
    CHECK (source_content_hash IS NULL OR source_content_hash ~ '^[0-9a-f]{64}$'),
    CHECK (target_content_hash IS NULL OR target_content_hash ~ '^[0-9a-f]{64}$'),
    CHECK (status <> 'VALIDATED' OR (
        source_row_count = target_row_count AND
        source_content_hash IS NOT NULL AND
        target_content_hash IS NOT NULL AND
        source_content_hash = target_content_hash AND validated_at IS NOT NULL)),
    CHECK (status <> 'CUTOVER' OR cutover_at IS NOT NULL),
    CHECK (status <> 'ROLLED_BACK' OR rollback_at IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_foundation_table_migration_pair
    ON ops.foundation_table_migration (source_table, target_table)
    WHERE status NOT IN ('ROLLED_BACK', 'ABORTED');

CREATE OR REPLACE FUNCTION ops.guard_foundation_table_migration()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := CURRENT_TIMESTAMP;
    IF TG_OP = 'INSERT' AND NEW.status <> 'REGISTERED'
       AND NOT EXISTS (
           SELECT 1 FROM ops.foundation_table_migration
            WHERE migration_id = NEW.migration_id
       ) THEN
        RAISE EXCEPTION 'new migration must start in REGISTERED state';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF NEW.status IS DISTINCT FROM OLD.status AND NOT (
            (OLD.status = 'REGISTERED' AND NEW.status IN ('DUAL_READ', 'DUAL_WRITE', 'VALIDATED', 'ABORTED')) OR
            (OLD.status = 'DUAL_READ' AND NEW.status IN ('DUAL_WRITE', 'VALIDATED', 'ABORTED')) OR
            (OLD.status = 'DUAL_WRITE' AND NEW.status IN ('VALIDATED', 'ABORTED')) OR
            (OLD.status = 'VALIDATED' AND NEW.status = 'CUTOVER') OR
            (OLD.status = 'CUTOVER' AND NEW.status = 'ROLLED_BACK')
        ) THEN
            RAISE EXCEPTION 'invalid migration transition: % -> %', OLD.status, NEW.status;
        END IF;
        IF OLD.status IN ('CUTOVER', 'ROLLED_BACK', 'ABORTED')
           AND NEW.status <> OLD.status THEN
            RAISE EXCEPTION 'terminal migration state % cannot transition', OLD.status;
        END IF;
        IF OLD.status = 'VALIDATED' AND NEW.status NOT IN ('VALIDATED', 'CUTOVER') THEN
            RAISE EXCEPTION 'VALIDATED migration can only remain VALIDATED or move to CUTOVER';
        END IF;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.status = 'CUTOVER' AND (OLD.status IS DISTINCT FROM 'VALIDATED') THEN
        RAISE EXCEPTION 'CUTOVER requires a previously VALIDATED migration';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.status = 'ROLLED_BACK' AND (OLD.status IS DISTINCT FROM 'CUTOVER') THEN
        RAISE EXCEPTION 'ROLLBACK requires a previously CUTOVER migration';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_foundation_table_migration_guard
    ON ops.foundation_table_migration;
CREATE TRIGGER trg_foundation_table_migration_guard
BEFORE INSERT OR UPDATE ON ops.foundation_table_migration
FOR EACH ROW EXECUTE FUNCTION ops.guard_foundation_table_migration();

GRANT SELECT ON ops.foundation_table_migration TO ashares_app;
