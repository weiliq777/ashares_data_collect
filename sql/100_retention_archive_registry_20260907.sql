-- Reversible retention registry.
--
-- This migration registers archive candidates only.  It never deletes or
-- changes Snapshot, Dataset, lineage, Raw, Standard, PIT, or Feature facts.
-- Physical archive/delete operations must be implemented as a separate,
-- explicitly approved operation after restore validation.

BEGIN;

CREATE TABLE IF NOT EXISTS ops.retention_archive_registry (
    registry_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    object_type VARCHAR(64) NOT NULL
        CHECK (object_type IN ('SNAPSHOT', 'DATASET', 'ROLLBACK_BACKUP', 'COMPATIBILITY_TABLE')),
    object_name VARCHAR(256) NOT NULL,
    object_key VARCHAR(256) NOT NULL,
    source_status VARCHAR(64) NOT NULL,
    archive_state VARCHAR(32) NOT NULL DEFAULT 'CANDIDATE'
        CHECK (archive_state IN ('CANDIDATE', 'APPROVED', 'ARCHIVED', 'RESTORED', 'REJECTED')),
    candidate_reason TEXT NOT NULL,
    reference_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    audit_fingerprint CHAR(64) NOT NULL,
    candidate_since TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retention_until TIMESTAMPTZ,
    approved_by VARCHAR(160),
    approved_at TIMESTAMPTZ,
    archived_at TIMESTAMPTZ,
    restored_at TIMESTAMPTZ,
    notes TEXT,
    UNIQUE (object_type, object_name, object_key)
);

CREATE INDEX IF NOT EXISTS idx_retention_archive_registry_state
    ON ops.retention_archive_registry (archive_state, object_type, candidate_since);

CREATE INDEX IF NOT EXISTS idx_retention_archive_registry_object
    ON ops.retention_archive_registry (object_name, object_key);

-- The registry is auditable metadata.  Runtime readers may inspect it, while
-- only the controlled publisher role may register or approve candidates.
GRANT SELECT ON ops.retention_archive_registry TO research_reader;
GRANT SELECT, INSERT, UPDATE ON ops.retention_archive_registry TO contract_publisher;
REVOKE DELETE, TRUNCATE ON ops.retention_archive_registry
    FROM research_reader, contract_publisher;

-- Stale, unreferenced Snapshot manifests.  The reference checks are bounded
-- by the manifest's primary key and deliberately cover every current
-- consumer table; no snapshot fact rows are scanned or modified here.
INSERT INTO ops.retention_archive_registry (
    object_type, object_name, object_key, source_status, candidate_reason,
    reference_summary, source_state, audit_fingerprint, notes
)
SELECT
    'SNAPSHOT',
    'research.snapshot_manifest',
    m.snapshot_id::text,
    m.status,
    'stale incomplete snapshot with no Dataset/Run/Watchlist/Evidence reference',
    jsonb_build_object(
        'dataset_members', (SELECT COUNT(*) FROM backtest.dataset_member x WHERE x.research_snapshot_id=m.snapshot_id),
        'signals', (SELECT COUNT(*) FROM backtest.signal x WHERE x.research_snapshot_id=m.snapshot_id),
        'watchlist_members', (SELECT COUNT(*) FROM research.watchlist_member x WHERE x.snapshot_id=m.snapshot_id),
        'evidence_references', (SELECT COUNT(*) FROM research.evidence_reference x WHERE x.snapshot_id=m.snapshot_id),
        'candidates', (SELECT COUNT(*) FROM research.candidate x WHERE x.snapshot_id=m.snapshot_id),
        'candidate_runs', (SELECT COUNT(*) FROM research.candidate_run x WHERE x.snapshot_id=m.snapshot_id)
    ),
    jsonb_build_object(
        'lineage_storage_version', m.lineage_storage_version,
        'research_cutoff_date', m.research_cutoff_date,
        'created_at', m.created_at,
        'built_at', m.built_at,
        'failed_at', m.failed_at,
        'input_manifest_hash', m.input_manifest_hash,
        'quality_report_hash', m.quality_report_hash
    ),
    encode(digest(convert_to(jsonb_build_object(
        'snapshot_id', m.snapshot_id,
        'status', m.status,
        'lineage_storage_version', m.lineage_storage_version,
        'research_cutoff_date', m.research_cutoff_date,
        'input_manifest_hash', m.input_manifest_hash,
        'quality_report_hash', m.quality_report_hash
    )::text, 'UTF8'), 'sha256'), 'hex')::char(64),
    'Generated from the 2026-09-07 read-only retention reference audit'
FROM research.snapshot_manifest m
WHERE m.status IN ('DRAFT', 'BUILDING')
  AND m.created_at < CURRENT_TIMESTAMP - INTERVAL '24 hours'
  AND NOT EXISTS (SELECT 1 FROM backtest.dataset_member x WHERE x.research_snapshot_id=m.snapshot_id)
  AND NOT EXISTS (SELECT 1 FROM backtest.signal x WHERE x.research_snapshot_id=m.snapshot_id)
  AND NOT EXISTS (SELECT 1 FROM research.watchlist_member x WHERE x.snapshot_id=m.snapshot_id)
  AND NOT EXISTS (SELECT 1 FROM research.evidence_reference x WHERE x.snapshot_id=m.snapshot_id)
  AND NOT EXISTS (SELECT 1 FROM research.candidate x WHERE x.snapshot_id=m.snapshot_id)
  AND NOT EXISTS (SELECT 1 FROM research.candidate_run x WHERE x.snapshot_id=m.snapshot_id)
ON CONFLICT (object_type, object_name, object_key) DO NOTHING;

-- Validated/Frozen Datasets with no Backtest Run reference.  Dataset members
-- remain intact; this registry is only a reversible decision record.
INSERT INTO ops.retention_archive_registry (
    object_type, object_name, object_key, source_status, candidate_reason,
    reference_summary, source_state, audit_fingerprint, notes
)
SELECT
    'DATASET',
    'backtest.dataset_manifest',
    d.dataset_id::text,
    d.status,
    'validated/frozen Dataset with no Backtest Run reference',
    jsonb_build_object(
        'run_count', (SELECT COUNT(*) FROM backtest.run_manifest x WHERE x.dataset_id=d.dataset_id),
        'dataset_members', (SELECT COUNT(*) FROM backtest.dataset_member x WHERE x.dataset_id=d.dataset_id),
        'schema_version', d.schema_version,
        'data_baseline_tag', d.data_baseline_tag
    ),
    jsonb_build_object(
        'dataset_version', d.dataset_version,
        'dataset_name', d.dataset_name,
        'evaluation_start', d.evaluation_start,
        'evaluation_end', d.evaluation_end,
        'input_manifest_hash', d.input_manifest_hash,
        'rules_hash', d.rules_hash,
        'created_at', d.created_at,
        'frozen_at', d.frozen_at
    ),
    encode(digest(convert_to(jsonb_build_object(
        'dataset_id', d.dataset_id,
        'dataset_version', d.dataset_version,
        'status', d.status,
        'schema_version', d.schema_version,
        'input_manifest_hash', d.input_manifest_hash,
        'rules_hash', d.rules_hash
    )::text, 'UTF8'), 'sha256'), 'hex')::char(64),
    'Generated from the 2026-09-07 read-only retention reference audit'
FROM backtest.dataset_manifest d
WHERE d.status IN ('VALIDATED', 'FROZEN')
  AND NOT EXISTS (SELECT 1 FROM backtest.run_manifest x WHERE x.dataset_id=d.dataset_id)
ON CONFLICT (object_type, object_name, object_key) DO NOTHING;

COMMIT;
