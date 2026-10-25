-- Independent PIT feature for the balance-sheet debt-to-assets factor.
-- Existing company_financial_v2 rows and frozen snapshots remain unchanged.

CREATE TABLE IF NOT EXISTS feature.debt_to_assets_v1 (
    feature_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,
    data_available_timestamp TIMESTAMPTZ NOT NULL,
    source_observed_at TIMESTAMP,
    report_type VARCHAR(10),
    comp_type VARCHAR(10),
    end_type VARCHAR(10),
    total_liab DOUBLE PRECISION,
    total_assets DOUBLE PRECISION,
    debt_to_assets DOUBLE PRECISION,
    quality_status VARCHAR(30) NOT NULL,
    quality_reason VARCHAR(80),
    source_quality_status VARCHAR(30) NOT NULL,
    vintage_quality VARCHAR(50),
    source_record_key VARCHAR(180),
    record_version BIGINT,
    payload_hash CHAR(64),
    source_fingerprint CHAR(64) NOT NULL,
    feature_contract_version VARCHAR(50) NOT NULL DEFAULT 'debt-to-assets-v1',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, report_date, data_available_timestamp,
            source_record_key, record_version, payload_hash)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_debt_to_assets_v1_fingerprint
    ON feature.debt_to_assets_v1 (source_fingerprint);

CREATE INDEX IF NOT EXISTS idx_debt_to_assets_v1_asof
    ON feature.debt_to_assets_v1
       (ts_code, report_date DESC, data_available_timestamp DESC,
        record_version DESC);

ALTER TABLE research.stock_snapshot_daily
    ADD COLUMN IF NOT EXISTS debt_to_assets_quality_status VARCHAR(30),
    ADD COLUMN IF NOT EXISTS debt_to_assets_quality_reason VARCHAR(80),
    ADD COLUMN IF NOT EXISTS debt_to_assets_data_available_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS debt_to_assets_source_observed_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS debt_to_assets_vintage_quality VARCHAR(50),
    ADD COLUMN IF NOT EXISTS debt_to_assets_source_quality_status VARCHAR(30),
    ADD COLUMN IF NOT EXISTS debt_to_assets_source_record_key VARCHAR(180),
    ADD COLUMN IF NOT EXISTS debt_to_assets_source_record_version BIGINT,
    ADD COLUMN IF NOT EXISTS debt_to_assets_source_payload_hash CHAR(64),
    ADD COLUMN IF NOT EXISTS debt_to_assets_feature_contract_version VARCHAR(50),
    ADD COLUMN IF NOT EXISTS debt_to_assets_report_date DATE;

INSERT INTO ops.feature_registry
    (feature_name, feature_version, grain, lookback_rule, source_tables,
     code_version, status)
VALUES
    ('debt_to_assets_v1', 'v1', 'stock x balance-sheet source version',
     'Current balance-sheet total liabilities / total assets; PIT per source version',
     ARRAY['financial_balance_sheet_standard'], 'debt-to-assets-v1', 'ACTIVE')
ON CONFLICT (feature_name) DO UPDATE SET
    feature_version=EXCLUDED.feature_version,
    grain=EXCLUDED.grain,
    lookback_rule=EXCLUDED.lookback_rule,
    source_tables=EXCLUDED.source_tables,
    code_version=EXCLUDED.code_version,
    status=EXCLUDED.status;

GRANT SELECT, INSERT, UPDATE ON feature.debt_to_assets_v1 TO ashares_app;
GRANT USAGE, SELECT ON SEQUENCE feature.debt_to_assets_v1_feature_id_seq TO ashares_app;
