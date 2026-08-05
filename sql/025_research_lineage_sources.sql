-- Research lineage source metadata migration.
-- This changes Standard metadata only; no Raw table or Raw payload is modified.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE IF EXISTS std.security_lifecycle
    ADD COLUMN IF NOT EXISTS source_payload_hash CHAR(64);

CREATE INDEX IF NOT EXISTS idx_security_lifecycle_source_lineage
    ON std.security_lifecycle (source_record_key, record_version, source_payload_hash);
