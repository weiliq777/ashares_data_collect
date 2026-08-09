-- fina_indicator 的不可变版本层与标准层。
-- 不修改 tushare_fina_indicator_raw；该表作为历史快照继续保留。

CREATE TABLE IF NOT EXISTS fina_indicator_raw_version (
    record_id BIGSERIAL PRIMARY KEY,
    business_key VARCHAR(180) NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    ann_date DATE,
    f_ann_date DATE,
    end_date DATE,
    report_type VARCHAR(10),
    comp_type VARCHAR(10),
    end_type VARCHAR(10),
    update_flag VARCHAR(10),
    payload JSONB NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    batch_id VARCHAR(80) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (business_key, payload_hash)
);

CREATE INDEX IF NOT EXISTS idx_fina_indicator_version_lookup
    ON fina_indicator_raw_version (ts_code, end_date, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_fina_indicator_version_announce
    ON fina_indicator_raw_version (ts_code, ann_date, f_ann_date);

CREATE TABLE IF NOT EXISTS fina_indicator_standard (
    indicator_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,
    ann_date DATE,
    f_ann_date DATE,
    effective_announce_date DATE,
    version_available_date DATE,
    vintage_quality VARCHAR(50),
    source_observed_at TIMESTAMP,
    report_type VARCHAR(10),
    comp_type VARCHAR(10),
    end_type VARCHAR(10),
    update_flag VARCHAR(10),
    roe DOUBLE PRECISION,
    roa DOUBLE PRECISION,
    debt_to_assets DOUBLE PRECISION,
    grossprofit_margin DOUBLE PRECISION,
    netprofit_margin DOUBLE PRECISION,
    ocf_to_or DOUBLE PRECISION,
    eps DOUBLE PRECISION,
    bps DOUBLE PRECISION,
    or_yoy DOUBLE PRECISION,
    tr_yoy DOUBLE PRECISION,
    q_sales_yoy DOUBLE PRECISION,
    netprofit_yoy DOUBLE PRECISION,
    source VARCHAR(30) NOT NULL,
    source_record_key VARCHAR(180) NOT NULL,
    record_version BIGINT NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source, source_record_key, record_version, payload_hash)
);

CREATE INDEX IF NOT EXISTS idx_fina_indicator_standard_lookup
    ON fina_indicator_standard (ts_code, report_date, effective_announce_date);
CREATE INDEX IF NOT EXISTS idx_fina_indicator_standard_pit
    ON fina_indicator_standard (ts_code, effective_announce_date, report_date, record_version DESC);
