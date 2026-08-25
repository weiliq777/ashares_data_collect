-- 阶段三 Research 快照基础模型。
-- 快照只读取已验收的 PIT/Feature；本迁移不修改任何 Raw/Standard/PIT/Feature。

CREATE SCHEMA IF NOT EXISTS research;

CREATE TABLE IF NOT EXISTS research.snapshot_manifest (
    snapshot_id UUID PRIMARY KEY,
    research_cutoff_date DATE NOT NULL,
    data_baseline_tag VARCHAR(100) NOT NULL REFERENCES ops.tushare_data_release(release_tag),
    schema_version VARCHAR(40) NOT NULL,
    code_version VARCHAR(180) NOT NULL,
    git_commit CHAR(40),
    git_dirty BOOLEAN NOT NULL DEFAULT FALSE,
    diff_hash CHAR(64),
    code_archive_path TEXT,
    code_archive_sha256 CHAR(64),
    code_archive_size_bytes BIGINT,
    quality_report_hash CHAR(64),
    baseline_quality_report_hash CHAR(64),
    baseline_source_manifest_hash CHAR(64),
    quality_report JSONB,
    input_manifest_hash CHAR(64),
    status VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    built_at TIMESTAMP,
    validated_at TIMESTAMP,
    frozen_at TIMESTAMP,
    failed_at TIMESTAMP,
    failure_reason TEXT,
    notes TEXT,
    CHECK (status IN ('DRAFT','BUILDING','VALIDATED','FROZEN','FAILED')),
    CHECK (git_dirty = FALSE OR diff_hash IS NOT NULL OR code_archive_path IS NOT NULL),
    CHECK (status <> 'FROZEN' OR quality_report_hash IS NOT NULL),
    CHECK (status <> 'FROZEN' OR input_manifest_hash IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_research_snapshot_manifest_cutoff
    ON research.snapshot_manifest (research_cutoff_date, status);

-- 同一数据基线、Schema 和截止日只能有一个快照；多日编排任务可安全续跑，
-- 旧版 research-v1 与新版 research-v1.1 可以并存，但同一版本不能重复创建。
CREATE UNIQUE INDEX IF NOT EXISTS uq_research_snapshot_cutoff_baseline_schema
    ON research.snapshot_manifest (research_cutoff_date, data_baseline_tag, schema_version);

CREATE TABLE IF NOT EXISTS research.snapshot_input (
    snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    schema_name VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    selection_predicate TEXT NOT NULL,
    selected_columns TEXT[] NOT NULL,
    hash_algorithm VARCHAR(20) NOT NULL DEFAULT 'sha256',
    row_count BIGINT NOT NULL,
    min_date DATE,
    max_date DATE,
    content_hash CHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (snapshot_id, schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS research.stock_snapshot_daily (
    snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    research_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    symbol VARCHAR(20),
    name TEXT,
    market VARCHAR(30),
    list_status VARCHAR(5),
    list_date DATE,
    delist_date DATE,
    is_listed_asof BOOLEAN,
    is_st_asof BOOLEAN,
    is_suspended_asof BOOLEAN,
    eligible_by_market BOOLEAN,
    is_tradeable BOOLEAN,
    can_buy BOOLEAN,
    can_sell BOOLEAN,
    limit_status VARCHAR(20),
    raw_open DOUBLE PRECISION,
    raw_high DOUBLE PRECISION,
    raw_low DOUBLE PRECISION,
    raw_close DOUBLE PRECISION,
    raw_volume DOUBLE PRECISION,
    raw_amount DOUBLE PRECISION,
    up_limit DOUBLE PRECISION,
    down_limit DOUBLE PRECISION,
    adj_close_base DOUBLE PRECISION,
    return_1d DOUBLE PRECISION,
    return_20d DOUBLE PRECISION,
    return_60d DOUBLE PRECISION,
    ma20 DOUBLE PRECISION,
    ma60 DOUBLE PRECISION,
    ma120 DOUBLE PRECISION,
    volume_ratio_20d DOUBLE PRECISION,
    available_observation_count INTEGER,
    ma20_observation_count INTEGER,
    ma60_observation_count INTEGER,
    ma120_observation_count INTEGER,
    pe_ttm DOUBLE PRECISION,
    pb DOUBLE PRECISION,
    ps_ttm DOUBLE PRECISION,
    dv_ratio DOUBLE PRECISION,
    total_mv DOUBLE PRECISION,
    circ_mv DOUBLE PRECISION,
    pe_pct_3y DOUBLE PRECISION,
    pe_pct_5y DOUBLE PRECISION,
    pb_pct_3y DOUBLE PRECISION,
    pb_pct_5y DOUBLE PRECISION,
    is_pe_meaningful BOOLEAN,
    valuation_observation_count INTEGER,
    pe_window_start_date_5y DATE,
    has_full_pe_5y_history BOOLEAN NOT NULL DEFAULT FALSE,
    report_date DATE,
    effective_announce_date DATE,
    financial_version_available_date DATE,
    financial_record_version BIGINT,
    financial_source_record_key VARCHAR(180),
    financial_payload_hash CHAR(64),
    financial_vintage_quality VARCHAR(50),
    financial_source_observed_at TIMESTAMP,
    roe DOUBLE PRECISION,
    roa DOUBLE PRECISION,
    debt_to_assets DOUBLE PRECISION,
    revenue_yoy DOUBLE PRECISION,
    tr_yoy DOUBLE PRECISION,
    q_sales_yoy DOUBLE PRECISION,
    netprofit_yoy DOUBLE PRECISION,
    operating_cashflow DOUBLE PRECISION,
    net_profit DOUBLE PRECISION,
    cash_div_12m DOUBLE PRECISION,
    dividend_event_count_3y INTEGER,
    dividend_year_count_3y INTEGER,
    annual_report_count_5y INTEGER,
    roe_above_15_years_5y INTEGER,
    roe_5y_min DOUBLE PRECISION,
    roe_5y_all_above_15 BOOLEAN NOT NULL DEFAULT FALSE,
    ocf_report_count_3y INTEGER,
    ocf_positive_years_3y INTEGER,
    ocf_3y_all_positive BOOLEAN NOT NULL DEFAULT FALSE,
    quarterly_report_count_4q INTEGER,
    q_sales_yoy_above_30_quarters_4q INTEGER,
    q_sales_yoy_4q_all_above_30 BOOLEAN NOT NULL DEFAULT FALSE,
    instrument_source_record_key VARCHAR(180),
    instrument_source_record_version BIGINT,
    instrument_source_payload_hash CHAR(64),
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (snapshot_id, research_date, ts_code),
    CHECK (research_date IS NOT NULL)
);

ALTER TABLE IF EXISTS research.stock_snapshot_daily
    ADD COLUMN IF NOT EXISTS eligible_by_market BOOLEAN;

CREATE INDEX IF NOT EXISTS idx_research_stock_snapshot_code
    ON research.stock_snapshot_daily (ts_code, research_date, snapshot_id);

CREATE TABLE IF NOT EXISTS research.snapshot_lineage (
    lineage_id BIGSERIAL PRIMARY KEY,
    snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    research_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    field_group VARCHAR(40) NOT NULL,
    source_table VARCHAR(120) NOT NULL,
    source_record_key VARCHAR(240) NOT NULL,
    source_record_version BIGINT,
    source_payload_hash CHAR(64),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (snapshot_id, research_date, ts_code, field_group, source_table, source_record_key)
);

CREATE INDEX IF NOT EXISTS idx_research_snapshot_lineage_lookup
    ON research.snapshot_lineage (snapshot_id, research_date, ts_code, field_group);

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
        IF NEW.git_dirty AND NEW.code_archive_path IS NULL THEN
            RAISE EXCEPTION 'dirty code requires an explicit code archive before freezing: %', NEW.snapshot_id;
        END IF;
        IF NEW.quality_report_hash IS NULL OR NEW.input_manifest_hash IS NULL THEN
            RAISE EXCEPTION 'FROZEN snapshot requires quality and input hashes: %', NEW.snapshot_id;
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

DROP TRIGGER IF EXISTS trg_research_snapshot_status ON research.snapshot_manifest;
CREATE TRIGGER trg_research_snapshot_status
BEFORE UPDATE OR DELETE ON research.snapshot_manifest
FOR EACH ROW EXECUTE FUNCTION research.assert_snapshot_status_transition();

CREATE OR REPLACE FUNCTION research.assert_snapshot_child_mutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE current_status VARCHAR(20);
BEGIN
    SELECT status INTO current_status FROM research.snapshot_manifest
    WHERE snapshot_id = CASE WHEN TG_OP = 'DELETE' THEN OLD.snapshot_id ELSE NEW.snapshot_id END;
    IF current_status = 'FROZEN' THEN
        RAISE EXCEPTION 'FROZEN research snapshot child is immutable: %', CASE WHEN TG_OP = 'DELETE' THEN OLD.snapshot_id ELSE NEW.snapshot_id END;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION research.assert_snapshot_date()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE expected_date DATE;
BEGIN
    SELECT research_cutoff_date INTO expected_date
    FROM research.snapshot_manifest WHERE snapshot_id = NEW.snapshot_id;
    IF expected_date IS NULL OR NEW.research_date <> expected_date THEN
        RAISE EXCEPTION 'snapshot research_date must equal manifest cutoff: % <> %', NEW.research_date, expected_date;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_research_snapshot_input_mutable ON research.snapshot_input;
CREATE TRIGGER trg_research_snapshot_input_mutable
BEFORE INSERT OR UPDATE OR DELETE ON research.snapshot_input
FOR EACH ROW EXECUTE FUNCTION research.assert_snapshot_child_mutable();

DROP TRIGGER IF EXISTS trg_research_stock_snapshot_mutable ON research.stock_snapshot_daily;
CREATE TRIGGER trg_research_stock_snapshot_mutable
BEFORE INSERT OR UPDATE OR DELETE ON research.stock_snapshot_daily
FOR EACH ROW EXECUTE FUNCTION research.assert_snapshot_child_mutable();

DROP TRIGGER IF EXISTS trg_research_stock_snapshot_date ON research.stock_snapshot_daily;
CREATE TRIGGER trg_research_stock_snapshot_date
BEFORE INSERT OR UPDATE ON research.stock_snapshot_daily
FOR EACH ROW EXECUTE FUNCTION research.assert_snapshot_date();

DROP TRIGGER IF EXISTS trg_research_snapshot_lineage_mutable ON research.snapshot_lineage;
CREATE TRIGGER trg_research_snapshot_lineage_mutable
BEFORE INSERT OR UPDATE OR DELETE ON research.snapshot_lineage
FOR EACH ROW EXECUTE FUNCTION research.assert_snapshot_child_mutable();
