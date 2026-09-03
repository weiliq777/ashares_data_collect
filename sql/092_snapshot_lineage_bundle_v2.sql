-- Research Snapshot lineage v2: compact, partitioned, source-addressed lineage.
--
-- This migration does not touch Raw/Standard/PIT/Feature data.  It adds a
-- physical representation for new Research Snapshot builds.  The logical
-- contract remains the same: every stock/date snapshot has all ten lineage
-- field groups, and every group resolves to a source record (including an
-- explicit :missing record when the upstream source is absent).
--
-- Apply with an administration role.  Application writes must still go
-- through ResearchSnapshotRepository -> DataAccessKernel.

BEGIN;

ALTER TABLE IF EXISTS research.snapshot_manifest
    ADD COLUMN IF NOT EXISTS lineage_storage_version VARCHAR(20)
    DEFAULT 'v1';

UPDATE research.snapshot_manifest
   SET lineage_storage_version = 'v1'
 WHERE lineage_storage_version IS NULL
   AND status <> 'FROZEN';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM research.snapshot_manifest
         WHERE lineage_storage_version IS NULL
    ) THEN
        RAISE EXCEPTION
            'cannot initialize lineage_storage_version on immutable FROZEN manifests';
    END IF;
END;
$$;

ALTER TABLE research.snapshot_manifest
    ALTER COLUMN lineage_storage_version SET DEFAULT 'v1';
ALTER TABLE research.snapshot_manifest
    ALTER COLUMN lineage_storage_version SET NOT NULL;
ALTER TABLE research.snapshot_manifest
    DROP CONSTRAINT IF EXISTS snapshot_manifest_lineage_storage_version_check;
ALTER TABLE research.snapshot_manifest
    ADD CONSTRAINT snapshot_manifest_lineage_storage_version_check
    CHECK (lineage_storage_version IN ('v1', 'v2'));

CREATE TABLE IF NOT EXISTS research.lineage_source_record (
    source_ref_id BIGSERIAL PRIMARY KEY,
    source_identity_hash BYTEA NOT NULL,
    source_table VARCHAR(120) NOT NULL,
    source_record_key VARCHAR(240) NOT NULL,
    source_record_version BIGINT,
    source_payload_hash CHAR(64),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_identity_hash)
);

COMMENT ON TABLE research.lineage_source_record IS
    '研究快照血缘来源字典：去重保存来源表、业务键、版本和内容哈希。';
COMMENT ON COLUMN research.lineage_source_record.source_identity_hash IS
    '来源身份哈希：由来源表、业务键、版本和内容哈希共同计算。';

CREATE TABLE IF NOT EXISTS research.snapshot_lineage_bundle (
    snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    research_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    universe_source_ref_id BIGINT NOT NULL REFERENCES research.lineage_source_record(source_ref_id),
    tradeability_source_ref_id BIGINT NOT NULL REFERENCES research.lineage_source_record(source_ref_id),
    price_source_ref_id BIGINT NOT NULL REFERENCES research.lineage_source_record(source_ref_id),
    valuation_source_ref_id BIGINT NOT NULL REFERENCES research.lineage_source_record(source_ref_id),
    financial_source_ref_id BIGINT NOT NULL REFERENCES research.lineage_source_record(source_ref_id),
    shareholder_return_source_ref_id BIGINT NOT NULL REFERENCES research.lineage_source_record(source_ref_id),
    lifecycle_source_ref_id BIGINT NOT NULL REFERENCES research.lineage_source_record(source_ref_id),
    instrument_source_ref_id BIGINT NOT NULL REFERENCES research.lineage_source_record(source_ref_id),
    market_facts_source_ref_id BIGINT NOT NULL REFERENCES research.lineage_source_record(source_ref_id),
    fundamental_history_source_ref_id BIGINT NOT NULL REFERENCES research.lineage_source_record(source_ref_id),
    source_bundle_hash CHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (research_date, snapshot_id, ts_code),
    CHECK (research_date IS NOT NULL),
    CHECK (source_bundle_hash ~ '^[0-9a-f]{64}$')
) PARTITION BY RANGE (research_date);

COMMENT ON TABLE research.snapshot_lineage_bundle IS
    '研究快照紧凑血缘包：每个快照日期和股票一行，十个字段组引用去重来源字典。';
COMMENT ON COLUMN research.snapshot_lineage_bundle.source_bundle_hash IS
    '十个逻辑血缘字段组来源编号的稳定哈希。';

-- Monthly partitions keep the high-density lineage table small enough for
-- vacuum, index maintenance, date pruning and bounded repairs.  Partitions
-- are provisioned ahead through 2035; the DEFAULT partition is a safety net
-- and must be drained into a named month before it becomes material.
DO $$
DECLARE
    month_start DATE := DATE '2021-08-01';
    month_end DATE;
    partition_name TEXT;
BEGIN
    WHILE month_start < DATE '2036-01-01' LOOP
        month_end := (month_start + INTERVAL '1 month')::date;
        partition_name := format(
            'snapshot_lineage_bundle_%s', to_char(month_start, 'YYYY_MM')
        );
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS research.%I PARTITION OF research.snapshot_lineage_bundle FOR VALUES FROM (%L) TO (%L)',
            partition_name, month_start, month_end
        );
        month_start := month_end;
    END LOOP;
END;
$$;

CREATE TABLE IF NOT EXISTS research.snapshot_lineage_bundle_default
    PARTITION OF research.snapshot_lineage_bundle DEFAULT;

-- Runtime access is deliberately limited to the two concrete tables needed by
-- the named Repository operation.  No DDL or broad schema privilege is
-- granted to the application role.
GRANT USAGE ON SCHEMA research TO ashares_app;
GRANT SELECT, INSERT ON research.lineage_source_record TO ashares_app;
GRANT USAGE, SELECT ON SEQUENCE research.lineage_source_record_source_ref_id_seq
    TO ashares_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON research.snapshot_lineage_bundle
    TO ashares_app;

-- The primary key already supports the common (research_date, snapshot_id,
-- ts_code) access path.  Do not add a second wide lookup index until a
-- representative EXPLAIN/actual benchmark proves it is needed.

-- Install statement-level guards.  A row-level manifest lookup here would
-- turn a bounded bundle insert into thousands of repeated probes, so the
-- transition-table guard is deliberately used for every lifecycle operation.
DROP TRIGGER IF EXISTS trg_research_snapshot_lineage_bundle_mutable
    ON research.snapshot_lineage_bundle;
DROP TRIGGER IF EXISTS trg_research_snapshot_lineage_bundle_mutable_stmt_ins
    ON research.snapshot_lineage_bundle;
DROP TRIGGER IF EXISTS trg_research_snapshot_lineage_bundle_mutable_stmt_upd
    ON research.snapshot_lineage_bundle;
DROP TRIGGER IF EXISTS trg_research_snapshot_lineage_bundle_mutable_stmt_del
    ON research.snapshot_lineage_bundle;
CREATE TRIGGER trg_research_snapshot_lineage_bundle_mutable_stmt_ins
AFTER INSERT ON research.snapshot_lineage_bundle
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION research.assert_snapshot_child_new_rows_mutable();
CREATE TRIGGER trg_research_snapshot_lineage_bundle_mutable_stmt_upd
AFTER UPDATE ON research.snapshot_lineage_bundle
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION research.assert_snapshot_child_old_new_rows_mutable();
CREATE TRIGGER trg_research_snapshot_lineage_bundle_mutable_stmt_del
AFTER DELETE ON research.snapshot_lineage_bundle
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION research.assert_snapshot_child_old_rows_mutable();

DROP TRIGGER IF EXISTS trg_research_snapshot_lineage_bundle_date_stmt_ins
    ON research.snapshot_lineage_bundle;
DROP TRIGGER IF EXISTS trg_research_snapshot_lineage_bundle_date_stmt_upd
    ON research.snapshot_lineage_bundle;
CREATE TRIGGER trg_research_snapshot_lineage_bundle_date_stmt_ins
AFTER INSERT ON research.snapshot_lineage_bundle
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION research.assert_snapshot_date_new_rows();
CREATE TRIGGER trg_research_snapshot_lineage_bundle_date_stmt_upd
AFTER UPDATE ON research.snapshot_lineage_bundle
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION research.assert_snapshot_date_new_rows();

-- A direct row-level trigger from an older partial migration would defeat the
-- statement-level design above; remove it by name as a defensive cleanup.
DROP TRIGGER IF EXISTS trg_research_snapshot_lineage_bundle_mutable
    ON research.snapshot_lineage_bundle;

COMMIT;
