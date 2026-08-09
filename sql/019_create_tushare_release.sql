-- Tushare 数据发布基线与增量控制。
-- 该表只记录数据发布点，不修改任何 *_raw 历史事实。
CREATE TABLE IF NOT EXISTS tushare_data_release (
    release_id BIGSERIAL PRIMARY KEY,
    release_tag VARCHAR(100) NOT NULL UNIQUE,
    baseline_cutoff_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'draft',
    quality_report_hash CHAR(64),
    quality_report JSONB,
    source_manifest JSONB,
    source_manifest_hash CHAR(64),
    schema_version VARCHAR(40),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    validated_at TIMESTAMP,
    CHECK (status IN ('draft', 'validated', 'superseded'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_tushare_validated_release
    ON tushare_data_release (status)
    WHERE status = 'validated';

CREATE INDEX IF NOT EXISTS idx_tushare_release_cutoff
    ON tushare_data_release (baseline_cutoff_date, status);
