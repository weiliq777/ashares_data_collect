-- Append-only PIT interpretations for an existing immutable Standard fact.
-- Raw record_version remains the Raw record_id; projection identity is separate.

CREATE SCHEMA IF NOT EXISTS pit;

CREATE TABLE IF NOT EXISTS pit.financial_standard_projection_run (
    projection_version VARCHAR(100) PRIMARY KEY,
    manifest_sha256 CHAR(64) NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    candidate_sha256 CHAR(64) NOT NULL CHECK (candidate_sha256 ~ '^[0-9a-f]{64}$'),
    raw_bundle_sha256 CHAR(64) NOT NULL CHECK (raw_bundle_sha256 ~ '^[0-9a-f]{64}$'),
    projection_code_sha256 CHAR(64) NOT NULL CHECK (projection_code_sha256 ~ '^[0-9a-f]{64}$'),
    projection_rule_version VARCHAR(100) NOT NULL,
    base_projection_version VARCHAR(100) NOT NULL,
    expected_rows INTEGER NOT NULL CHECK (expected_rows > 0),
    expected_rows_by_source JSONB NOT NULL,
    expected_output_sha256 CHAR(64) NOT NULL
        CHECK (expected_output_sha256 ~ '^[0-9a-f]{64}$'),
    report_start DATE NOT NULL,
    report_end DATE NOT NULL,
    created_run_id VARCHAR(160) NOT NULL,
    execution_environment VARCHAR(20) NOT NULL
        CHECK (execution_environment IN ('TEST', 'PRODUCTION')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (report_start <= report_end)
);

CREATE TABLE IF NOT EXISTS pit.financial_standard_projection_batch (
    projection_version VARCHAR(100) NOT NULL
        REFERENCES pit.financial_standard_projection_run(projection_version),
    source_dataset VARCHAR(20) NOT NULL
        CHECK (source_dataset IN ('income', 'balance', 'cashflow')),
    batch_number INTEGER NOT NULL CHECK (batch_number > 0),
    first_standard_id BIGINT NOT NULL CHECK (first_standard_id > 0),
    last_standard_id BIGINT NOT NULL CHECK (last_standard_id >= first_standard_id),
    row_count INTEGER NOT NULL CHECK (row_count > 0),
    input_sha256 CHAR(64) NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    output_sha256 CHAR(64) NOT NULL CHECK (output_sha256 ~ '^[0-9a-f]{64}$'),
    created_run_id VARCHAR(160) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (projection_version, source_dataset, batch_number)
);

CREATE TABLE IF NOT EXISTS pit.financial_standard_projection (
    projection_version VARCHAR(100) NOT NULL
        REFERENCES pit.financial_standard_projection_run(projection_version),
    source_dataset VARCHAR(20) NOT NULL
        CHECK (source_dataset IN ('income', 'balance', 'cashflow')),
    batch_number INTEGER NOT NULL CHECK (batch_number > 0),
    standard_id BIGINT NOT NULL CHECK (standard_id > 0),
    source_standard_id BIGINT NOT NULL CHECK (source_standard_id > 0),
    raw_dataset VARCHAR(20) NOT NULL
        CHECK (raw_dataset IN ('income', 'balancesheet', 'cashflow')),
    raw_record_id BIGINT NOT NULL CHECK (raw_record_id > 0),
    raw_business_key VARCHAR(180) NOT NULL,
    raw_payload_sha256 CHAR(64) NOT NULL CHECK (raw_payload_sha256 ~ '^[0-9a-f]{64}$'),
    standard_source VARCHAR(30) NOT NULL,
    source_record_key VARCHAR(180) NOT NULL,
    source_record_version BIGINT NOT NULL CHECK (source_record_version > 0),
    standard_payload_sha256 CHAR(64) NOT NULL CHECK (standard_payload_sha256 ~ '^[0-9a-f]{64}$'),
    standard_content_sha256 CHAR(64) NOT NULL CHECK (standard_content_sha256 ~ '^[0-9a-f]{64}$'),
    ts_code VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,
    report_type VARCHAR(10),
    comp_type VARCHAR(10),
    projected_end_type VARCHAR(10),
    projected_announce_date DATE NOT NULL,
    data_available_timestamp TIMESTAMPTZ NOT NULL,
    vintage_quality VARCHAR(50) NOT NULL
        CHECK (vintage_quality = 'SOURCE_ANNOUNCEMENT_ASSUMED'),
    quality_status VARCHAR(30) NOT NULL,
    source_observed_at TIMESTAMP,
    projection_rule_version VARCHAR(100) NOT NULL,
    projection_content_sha256 CHAR(64) NOT NULL
        CHECK (projection_content_sha256 ~ '^[0-9a-f]{64}$'),
    created_run_id VARCHAR(160) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (projection_version, source_dataset, standard_id),
    UNIQUE (projection_version, raw_dataset, raw_record_id),
    FOREIGN KEY (projection_version, source_dataset, batch_number)
        REFERENCES pit.financial_standard_projection_batch
            (projection_version, source_dataset, batch_number),
    CHECK (
        (source_dataset = 'income' AND raw_dataset = 'income') OR
        (source_dataset = 'balance' AND raw_dataset = 'balancesheet') OR
        (source_dataset = 'cashflow' AND raw_dataset = 'cashflow')
    ),
    CHECK (source_record_version = raw_record_id),
    CHECK (data_available_timestamp =
        ((projected_announce_date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'))
);

CREATE INDEX IF NOT EXISTS idx_financial_standard_projection_asof
    ON pit.financial_standard_projection
       (projection_version, source_dataset, ts_code, report_date,
        data_available_timestamp, standard_id);

CREATE OR REPLACE FUNCTION pit.reject_financial_projection_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'financial PIT projection records are append-only';
END;
$$;

DROP TRIGGER IF EXISTS trg_financial_projection_run_immutable
    ON pit.financial_standard_projection_run;
CREATE TRIGGER trg_financial_projection_run_immutable
    BEFORE UPDATE OR DELETE ON pit.financial_standard_projection_run
    FOR EACH ROW EXECUTE FUNCTION pit.reject_financial_projection_mutation();

DROP TRIGGER IF EXISTS trg_financial_projection_batch_immutable
    ON pit.financial_standard_projection_batch;
CREATE TRIGGER trg_financial_projection_batch_immutable
    BEFORE UPDATE OR DELETE ON pit.financial_standard_projection_batch
    FOR EACH ROW EXECUTE FUNCTION pit.reject_financial_projection_mutation();

DROP TRIGGER IF EXISTS trg_financial_projection_row_immutable
    ON pit.financial_standard_projection;
CREATE TRIGGER trg_financial_projection_row_immutable
    BEFORE UPDATE OR DELETE ON pit.financial_standard_projection
    FOR EACH ROW EXECUTE FUNCTION pit.reject_financial_projection_mutation();

REVOKE UPDATE, DELETE, TRUNCATE ON
    pit.financial_standard_projection_run,
    pit.financial_standard_projection_batch,
    pit.financial_standard_projection
FROM ashares_app;
GRANT SELECT, INSERT ON
    pit.financial_standard_projection_run,
    pit.financial_standard_projection_batch,
    pit.financial_standard_projection
TO ashares_app;

ALTER TABLE feature.debt_to_assets_v1
    ADD COLUMN IF NOT EXISTS source_projection_version VARCHAR(100);

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA pit TO ashares_app;
