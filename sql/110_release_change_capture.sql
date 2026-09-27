-- Transactional change capture for bounded daily release-delta audits.
-- This migration creates small control-plane tables only; it never scans or
-- modifies Raw/Standard/PIT/Feature facts. Apply before enabling the opt-in
-- DATA_RELEASE_CHANGE_CAPTURE runtime flag.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15s';

CREATE TABLE IF NOT EXISTS ops.release_change_scope (
    scope_hash CHAR(64) PRIMARY KEY
        CHECK (scope_hash ~ '^[0-9a-f]{64}$'),
    scope JSONB NOT NULL CHECK (jsonb_typeof(scope) = 'object'),
    scope_precision VARCHAR(16) NOT NULL
        CHECK (scope_precision IN ('DECLARED', 'INFERRED')),
    delta_eligible BOOLEAN NOT NULL,
    full_audit_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (delta_eligible OR NULLIF(btrim(full_audit_reason), '') IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS ops.release_change_event (
    event_id BIGSERIAL NOT NULL UNIQUE,
    event_key CHAR(64) PRIMARY KEY
        CHECK (event_key ~ '^[0-9a-f]{64}$'),
    run_id VARCHAR(160) NOT NULL,
    job_name VARCHAR(160) NOT NULL,
    code_version VARCHAR(160) NOT NULL,
    pipeline_version VARCHAR(160) NOT NULL,
    target VARCHAR(200) NOT NULL,
    operation VARCHAR(40) NOT NULL,
    scope_hash CHAR(64) NOT NULL
        REFERENCES ops.release_change_scope(scope_hash),
    input_hash CHAR(64) NOT NULL
        CHECK (input_hash ~ '^[0-9a-f]{64}$'),
    input_row_count INTEGER NOT NULL CHECK (input_row_count >= 0),
    affected_rows BIGINT CHECK (affected_rows IS NULL OR affected_rows >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (target ~ '^(public|raw|std|pit|feature)\.[a-zA-Z_][a-zA-Z0-9_]*$'),
    CHECK (operation <> '')
);

CREATE INDEX IF NOT EXISTS idx_release_change_event_target_id
    ON ops.release_change_event (target, event_id);
CREATE INDEX IF NOT EXISTS idx_release_change_event_run_id
    ON ops.release_change_event (run_id);

CREATE OR REPLACE FUNCTION ops.reject_release_change_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'release change ledger is append-only';
END;
$$;

DROP TRIGGER IF EXISTS trg_release_change_scope_append_only
    ON ops.release_change_scope;
CREATE TRIGGER trg_release_change_scope_append_only
BEFORE UPDATE OR DELETE ON ops.release_change_scope
FOR EACH ROW EXECUTE FUNCTION ops.reject_release_change_mutation();

DROP TRIGGER IF EXISTS trg_release_change_event_append_only
    ON ops.release_change_event;
CREATE TRIGGER trg_release_change_event_append_only
BEFORE UPDATE OR DELETE ON ops.release_change_event
FOR EACH ROW EXECUTE FUNCTION ops.reject_release_change_mutation();

GRANT SELECT, INSERT ON ops.release_change_scope, ops.release_change_event
    TO raw_writer, standard_writer, pit_writer, feature_writer;
GRANT SELECT ON ops.release_change_scope, ops.release_change_event
    TO contract_publisher, research_reader;
GRANT USAGE, SELECT ON SEQUENCE ops.release_change_event_event_id_seq
    TO raw_writer, standard_writer, pit_writer, feature_writer;
REVOKE UPDATE, DELETE, TRUNCATE ON ops.release_change_scope, ops.release_change_event
    FROM PUBLIC, raw_writer, standard_writer, pit_writer, feature_writer,
         contract_publisher, research_reader;

COMMENT ON TABLE ops.release_change_scope IS
    'Content-addressed, deduplicated impact scopes for committed release input changes.';
COMMENT ON TABLE ops.release_change_event IS
    'Append-only per-transaction-chunk change evidence; an event is committed atomically with its fact write.';
COMMENT ON COLUMN ops.release_change_scope.delta_eligible IS
    'False means the scope is not provably bounded; downstream release must request a full audit.';
COMMENT ON COLUMN ops.release_change_event.affected_rows IS
    'Gateway-reported affected row count; NULL for publication operations where PostgreSQL does not expose a meaningful count.';

COMMIT;
