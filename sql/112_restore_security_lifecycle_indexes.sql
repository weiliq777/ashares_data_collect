-- TABLE_REPLACE previously created shadow tables without indexes. Replacing
-- std.security_lifecycle therefore removed its keys and lookup indexes.
-- Run this file outside an explicit transaction: CREATE INDEX CONCURRENTLY
-- cannot run inside a transaction block. Unique index creation intentionally
-- fails closed if existing duplicate data needs investigation.

CREATE OR REPLACE FUNCTION ops.create_table_replace_staging(
    p_target text,
    p_staging text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, std, pit
AS $$
DECLARE
    v_schema text;
BEGIN
    IF p_target NOT IN ('std.security_lifecycle', 'pit.financial_asof') THEN
        RAISE EXCEPTION 'unsupported TABLE_REPLACE target: %', p_target;
    END IF;
    v_schema := split_part(p_target, '.', 1);
    IF p_staging !~ ('^' || v_schema || '\._staging_[a-z][a-z0-9_]*_[0-9a-f]{12}$') THEN
        RAISE EXCEPTION 'invalid TABLE_REPLACE staging name: %', p_staging;
    END IF;
    IF to_regclass(p_target) IS NULL THEN
        RAISE EXCEPTION 'TABLE_REPLACE target does not exist: %', p_target;
    END IF;
    IF to_regclass(p_staging) IS NOT NULL THEN
        RAISE EXCEPTION 'TABLE_REPLACE staging already exists: %', p_staging;
    END IF;

    -- LIKE INCLUDING INDEXES also copies primary/unique constraints. PostgreSQL
    -- chooses fresh names for the shadow-table indexes and constraints.
    EXECUTE format(
        'CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING INDEXES)',
        p_staging, p_target
    );
    EXECUTE format(
        'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE %s TO ashares_app',
        p_staging
    );
END;
$$;

ALTER FUNCTION ops.create_table_replace_staging(text, text) OWNER TO ashares;
REVOKE ALL ON FUNCTION ops.create_table_replace_staging(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.create_table_replace_staging(text, text) TO ashares_app;

-- Restore the expected physical key if a prior TABLE_REPLACE dropped it.
-- The catalog-only constraint attachment reuses the concurrently-built index.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS security_lifecycle_pkey
    ON std.security_lifecycle (lifecycle_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'std.security_lifecycle'::regclass
           AND contype = 'p'
    ) THEN
        ALTER TABLE std.security_lifecycle
            ADD CONSTRAINT security_lifecycle_pkey
            PRIMARY KEY USING INDEX security_lifecycle_pkey;
    END IF;
END;
$$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_security_lifecycle_code_date
    ON std.security_lifecycle (ts_code, effective_from, effective_to);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_security_lifecycle_effective_interval
    ON std.security_lifecycle (ts_code, effective_from, effective_to, source)
    NULLS NOT DISTINCT;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_security_lifecycle_source_lineage
    ON std.security_lifecycle (source_record_key, record_version, source_payload_hash);
