-- Financial VIP incremental ingestion contract.
-- Legacy tushare_*_raw tables remain unchanged.  New VIP observations enter
-- the immutable version tables and are coupled to a durable change queue.

CREATE TABLE IF NOT EXISTS financial_change_queue (
    queue_id BIGSERIAL PRIMARY KEY,
    dataset VARCHAR(30) NOT NULL,
    business_key VARCHAR(180) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    report_date DATE,
    source_record_id BIGINT,
    batch_id VARCHAR(100) NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    available_date DATE,
    processed_through DATE,
    quarantine_resolved_by VARCHAR(160),
    quarantine_resolved_at TIMESTAMP,
    quarantine_resolution_reason TEXT,
    UNIQUE (dataset, business_key, payload_hash),
    CHECK (status IN (
        'PENDING', 'RUNNING', 'DONE', 'FAILED',
        'COVERAGE_BLOCKED', 'QUARANTINED', 'RESOLVED'
    ))
);

CREATE INDEX IF NOT EXISTS idx_financial_change_queue_pending
    ON financial_change_queue (status, dataset, ts_code, report_date);
CREATE INDEX IF NOT EXISTS idx_financial_change_queue_batch
    ON financial_change_queue (batch_id, dataset);

CREATE TABLE IF NOT EXISTS financial_ingest_anomaly (
    anomaly_id BIGSERIAL PRIMARY KEY,
    dataset VARCHAR(30) NOT NULL,
    api_name VARCHAR(60) NOT NULL,
    report_period DATE,
    batch_id VARCHAR(100),
    reason_code VARCHAR(60) NOT NULL,
    handling VARCHAR(20) NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    CHECK (handling IN ('WARNING', 'QUARANTINE'))
);

CREATE INDEX IF NOT EXISTS idx_financial_ingest_anomaly_lookup
    ON financial_ingest_anomaly (dataset, api_name, report_period, observed_at DESC);

-- One cutover watermark per immutable version store.  It is deliberately
-- keyed by record_id and batch_id rather than a timestamp so orphan repair is
-- deterministic at the migration boundary.
CREATE TABLE IF NOT EXISTS financial_version_cutover_watermark (
    dataset VARCHAR(30) PRIMARY KEY,
    record_id BIGINT NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO financial_version_cutover_watermark (dataset, record_id, batch_id)
SELECT 'income', COALESCE(MAX(record_id), 0), 'legacy-cutover-046'
  FROM tushare_financial_raw_version
 WHERE dataset = 'income'
ON CONFLICT (dataset) DO NOTHING;

INSERT INTO financial_version_cutover_watermark (dataset, record_id, batch_id)
SELECT 'balancesheet', COALESCE(MAX(record_id), 0), 'legacy-cutover-046'
  FROM tushare_financial_raw_version
 WHERE dataset = 'balancesheet'
ON CONFLICT (dataset) DO NOTHING;

INSERT INTO financial_version_cutover_watermark (dataset, record_id, batch_id)
SELECT 'cashflow', COALESCE(MAX(record_id), 0), 'legacy-cutover-046'
  FROM tushare_financial_raw_version
 WHERE dataset = 'cashflow'
ON CONFLICT (dataset) DO NOTHING;

INSERT INTO financial_version_cutover_watermark (dataset, record_id, batch_id)
SELECT 'fina_indicator', COALESCE(MAX(record_id), 0), 'legacy-cutover-046'
  FROM fina_indicator_raw_version
ON CONFLICT (dataset) DO NOTHING;
