-- T-037: research-v1.2 correctness and reproducibility repairs.
-- This migration only extends derived/audit models.  It never modifies Raw facts.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Mature-window facts.  Existing v1/v1.1 rows remain readable; v1.2 rebuilds
-- these columns from Standard data.
ALTER TABLE feature.price_daily_v1
    ADD COLUMN IF NOT EXISTS ma20_observation_count INTEGER,
    ADD COLUMN IF NOT EXISTS ma60_observation_count INTEGER,
    ADD COLUMN IF NOT EXISTS ma120_observation_count INTEGER;

ALTER TABLE feature.valuation_daily_v1
    ADD COLUMN IF NOT EXISTS pe_window_start_date_3y DATE,
    ADD COLUMN IF NOT EXISTS pe_window_start_date_5y DATE,
    ADD COLUMN IF NOT EXISTS pe_observation_count_3y INTEGER,
    ADD COLUMN IF NOT EXISTS pe_observation_count_5y INTEGER,
    ADD COLUMN IF NOT EXISTS has_full_pe_3y_history BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS has_full_pe_5y_history BOOLEAN NOT NULL DEFAULT FALSE;

-- A source revision with the same announcement date must not be projected
-- backwards.  The first warehouse version uses the source announcement date;
-- later payload revisions are visible no earlier than their fetch date.
ALTER TABLE fina_indicator_standard
    ADD COLUMN IF NOT EXISTS version_available_date DATE;
ALTER TABLE financial_income_standard
    ADD COLUMN IF NOT EXISTS version_available_date DATE;
ALTER TABLE financial_balance_sheet_standard
    ADD COLUMN IF NOT EXISTS version_available_date DATE;
ALTER TABLE financial_cash_flow_standard
    ADD COLUMN IF NOT EXISTS version_available_date DATE;
ALTER TABLE pit.financial_asof
    ADD COLUMN IF NOT EXISTS version_available_date DATE;
ALTER TABLE feature.company_financial_v1
    ADD COLUMN IF NOT EXISTS version_available_date DATE;

CREATE INDEX IF NOT EXISTS idx_fina_indicator_standard_version_available
    ON fina_indicator_standard
       (ts_code, version_available_date, report_date, record_version DESC);
CREATE INDEX IF NOT EXISTS idx_financial_income_version_available
    ON financial_income_standard
       (stock_code, version_available_date, report_date, record_version DESC);
CREATE INDEX IF NOT EXISTS idx_financial_cashflow_version_available
    ON financial_cash_flow_standard
       (stock_code, version_available_date, report_date, record_version DESC);
CREATE INDEX IF NOT EXISTS idx_company_financial_v1_available
    ON feature.company_financial_v1
       (ts_code, version_available_date, report_date, record_version DESC);

-- Freeze the same-day research facts used by the candidate layer.  Daily
-- candidates must never join mutable daily_kline after a snapshot is FROZEN.
ALTER TABLE research.stock_snapshot_daily
    ADD COLUMN IF NOT EXISTS raw_open DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS raw_high DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS raw_low DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS raw_volume DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS raw_amount DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS up_limit DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS down_limit DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS available_observation_count INTEGER,
    ADD COLUMN IF NOT EXISTS ma20_observation_count INTEGER,
    ADD COLUMN IF NOT EXISTS ma60_observation_count INTEGER,
    ADD COLUMN IF NOT EXISTS ma120_observation_count INTEGER,
    ADD COLUMN IF NOT EXISTS valuation_observation_count INTEGER,
    ADD COLUMN IF NOT EXISTS pe_window_start_date_5y DATE,
    ADD COLUMN IF NOT EXISTS has_full_pe_5y_history BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS financial_version_available_date DATE,
    ADD COLUMN IF NOT EXISTS annual_report_count_5y INTEGER,
    ADD COLUMN IF NOT EXISTS roe_above_15_years_5y INTEGER,
    ADD COLUMN IF NOT EXISTS roe_5y_min DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS roe_5y_all_above_15 BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS ocf_report_count_3y INTEGER,
    ADD COLUMN IF NOT EXISTS ocf_positive_years_3y INTEGER,
    ADD COLUMN IF NOT EXISTS ocf_3y_all_positive BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS quarterly_report_count_4q INTEGER,
    ADD COLUMN IF NOT EXISTS q_sales_yoy_above_30_quarters_4q INTEGER,
    ADD COLUMN IF NOT EXISTS q_sales_yoy_4q_all_above_30 BOOLEAN NOT NULL DEFAULT FALSE;

-- Code archives are content addressed.  Paths alone are not reproducible.
ALTER TABLE research.snapshot_manifest
    ADD COLUMN IF NOT EXISTS code_archive_sha256 CHAR(64),
    ADD COLUMN IF NOT EXISTS code_archive_size_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS baseline_quality_report_hash CHAR(64);

ALTER TABLE backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS code_archive_sha256 CHAR(64),
    ADD COLUMN IF NOT EXISTS code_archive_size_bytes BIGINT;
ALTER TABLE backtest.run_manifest
    ADD COLUMN IF NOT EXISTS code_archive_sha256 CHAR(64),
    ADD COLUMN IF NOT EXISTS code_archive_size_bytes BIGINT;

CREATE OR REPLACE FUNCTION backtest.assert_code_archive_content_addressed()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.git_dirty AND NEW.status IN ('FROZEN','RUNNING','SUCCEEDED') AND (
        NEW.code_archive_path IS NULL OR NEW.code_archive_sha256 IS NULL
        OR NEW.code_archive_size_bytes IS NULL OR NEW.code_archive_size_bytes <= 0
    ) THEN
        RAISE EXCEPTION 'dirty code archive must be content-addressed';
    END IF;
    IF TG_OP='UPDATE' AND OLD.status IN ('RUNNING','VALIDATED')
       AND OLD.code_archive_sha256 IS NOT NULL
       AND NEW.status IS DISTINCT FROM OLD.status AND (
           NEW.code_archive_sha256 IS DISTINCT FROM OLD.code_archive_sha256
           OR NEW.code_archive_size_bytes IS DISTINCT FROM OLD.code_archive_size_bytes
       ) THEN
        RAISE EXCEPTION 'code archive identity cannot change during status transition';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dataset_code_archive_content ON backtest.dataset_manifest;
CREATE TRIGGER trg_dataset_code_archive_content
BEFORE INSERT OR UPDATE ON backtest.dataset_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_code_archive_content_addressed();
DROP TRIGGER IF EXISTS trg_run_code_archive_content ON backtest.run_manifest;
CREATE TRIGGER trg_run_code_archive_content
BEFORE INSERT OR UPDATE ON backtest.run_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_code_archive_content_addressed();

CREATE OR REPLACE FUNCTION research.assert_snapshot_status_transition()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'research snapshot manifest is append-only; DELETE is forbidden: %', OLD.snapshot_id;
    END IF;
    IF OLD.status = 'FROZEN' THEN
        RAISE EXCEPTION 'FROZEN research snapshot is immutable: %', OLD.snapshot_id;
    END IF;
    IF NEW.status <> OLD.status AND NOT (
        (OLD.status = 'DRAFT' AND NEW.status IN ('BUILDING','FAILED')) OR
        (OLD.status = 'FAILED' AND NEW.status IN ('BUILDING','FAILED')) OR
        (OLD.status = 'BUILDING' AND NEW.status IN ('VALIDATED','FAILED')) OR
        (OLD.status = 'VALIDATED' AND NEW.status = 'FROZEN')
    ) THEN
        RAISE EXCEPTION 'invalid research snapshot status transition: % -> %', OLD.status, NEW.status;
    END IF;
    IF NEW.status = 'FROZEN' THEN
        IF OLD.status <> 'VALIDATED' THEN
            RAISE EXCEPTION 'only VALIDATED snapshot can be FROZEN: %', NEW.snapshot_id;
        END IF;
        IF NEW.quality_report_hash IS NULL OR NEW.input_manifest_hash IS NULL THEN
            RAISE EXCEPTION 'FROZEN snapshot requires quality and input hashes: %', NEW.snapshot_id;
        END IF;
        IF NEW.schema_version = 'research-v1.2' THEN
            IF NEW.baseline_quality_report_hash IS NULL THEN
                RAISE EXCEPTION 'research-v1.2 requires a bound baseline quality hash: %', NEW.snapshot_id;
            END IF;
            IF NEW.git_dirty AND (
                NEW.code_archive_path IS NULL OR NEW.code_archive_sha256 IS NULL
                OR NEW.code_archive_size_bytes IS NULL OR NEW.code_archive_size_bytes <= 0
            ) THEN
                RAISE EXCEPTION 'dirty research-v1.2 code requires a content-addressed archive: %', NEW.snapshot_id;
            END IF;
        ELSIF NEW.git_dirty AND NEW.code_archive_path IS NULL THEN
            -- Grandfather the already-created v1/v1.1 contract without
            -- weakening the new v1.2 gate.
            RAISE EXCEPTION 'dirty code requires an explicit code archive: %', NEW.snapshot_id;
        END IF;
        NEW.frozen_at := CURRENT_TIMESTAMP;
    END IF;
    IF NEW.status = 'BUILDING' AND OLD.status IN ('DRAFT','FAILED') THEN
        NEW.built_at := CURRENT_TIMESTAMP;
    ELSIF NEW.status = 'VALIDATED' THEN
        NEW.validated_at := CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$;

-- Baseline hashes must be generated from the actual quality report.
ALTER TABLE ops.tushare_data_release
    ADD COLUMN IF NOT EXISTS quality_report JSONB;

-- New rule records carry their approval evidence in the database.  Existing
-- v2 rows are retained as immutable legacy audit records; the v1.2 flow uses
-- a newly imported approved rule version.
ALTER TABLE backtest.market_rule_effective
    ADD COLUMN IF NOT EXISTS approval_status VARCHAR(20),
    ADD COLUMN IF NOT EXISTS approved_by VARCHAR(120),
    ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS approval_hash CHAR(64);

ALTER TABLE backtest.dataset_rule_snapshot
    ADD COLUMN IF NOT EXISTS approval_status VARCHAR(20),
    ADD COLUMN IF NOT EXISTS approved_by VARCHAR(120),
    ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS approval_hash CHAR(64);

CREATE OR REPLACE FUNCTION backtest.assert_approved_rule_insert()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.approval_status IS DISTINCT FROM 'APPROVED'
       OR NULLIF(BTRIM(COALESCE(NEW.approved_by, '')), '') IS NULL
       OR NEW.approved_at IS NULL OR NEW.approval_hash IS NULL
       OR length(BTRIM(NEW.approval_hash)) <> 64 THEN
        RAISE EXCEPTION 'new market/fee rule requires persisted APPROVED metadata: %', NEW.rule_version;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_market_rule_requires_approval ON backtest.market_rule_effective;
CREATE TRIGGER trg_market_rule_requires_approval
BEFORE INSERT ON backtest.market_rule_effective
FOR EACH ROW EXECUTE FUNCTION backtest.assert_approved_rule_insert();

-- Candidate idempotency binds the complete immutable input, not only runtime
-- parameters or a portfolio identifier.
ALTER TABLE research.candidate_run
    ADD COLUMN IF NOT EXISTS portfolio_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS portfolio_state_hash CHAR(64),
    ADD COLUMN IF NOT EXISTS observation_input_hash CHAR(64),
    ADD COLUMN IF NOT EXISTS rule_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS rule_snapshot_hash CHAR(64);

ALTER TABLE research.candidate_run
    DROP CONSTRAINT IF EXISTS candidate_run_snapshot_id_run_type_strategy_id_strategy_ver_key;
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_run_complete_input
    ON research.candidate_run
       (snapshot_id, run_type, strategy_id, strategy_version, input_hash);

-- Preserve every stock_basic response version.  The legacy Raw table remains
-- first-seen only and is never overwritten.
CREATE TABLE IF NOT EXISTS tushare_stock_basic_raw_version (
    record_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    business_key VARCHAR(80) NOT NULL,
    payload JSONB NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    batch_id VARCHAR(80) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (business_key, payload_hash)
);
CREATE INDEX IF NOT EXISTS idx_stock_basic_raw_version_lookup
    ON tushare_stock_basic_raw_version (ts_code, fetched_at DESC, record_id DESC);
