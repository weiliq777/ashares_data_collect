-- 阶段二：证券生命周期、历史状态、PIT 与 Feature 模型。
-- 既有 public tushare_*_raw 表保留不动；本迁移只新增第二阶段对象。

CREATE TABLE IF NOT EXISTS tushare_stock_st_raw (
    record_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    type VARCHAR(20),
    payload JSONB NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    batch_id VARCHAR(80) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, trade_date, type, payload_hash)
);
CREATE INDEX IF NOT EXISTS idx_tushare_stock_st_raw_date
    ON tushare_stock_st_raw (trade_date, ts_code);

CREATE TABLE IF NOT EXISTS tushare_namechange_raw (
    record_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    start_date DATE,
    end_date DATE,
    ann_date DATE,
    payload JSONB NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    batch_id VARCHAR(80) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, payload_hash)
);
CREATE INDEX IF NOT EXISTS idx_tushare_namechange_raw_code_date
    ON tushare_namechange_raw (ts_code, start_date, end_date);

CREATE TABLE IF NOT EXISTS tushare_suspend_d_raw (
    record_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    suspend_type VARCHAR(10),
    payload JSONB NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    batch_id VARCHAR(80) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, trade_date, suspend_type, payload_hash)
);
CREATE INDEX IF NOT EXISTS idx_tushare_suspend_d_raw_date
    ON tushare_suspend_d_raw (trade_date, ts_code);

CREATE TABLE IF NOT EXISTS tushare_stk_limit_raw (
    record_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    payload JSONB NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    batch_id VARCHAR(80) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, trade_date, payload_hash)
);
CREATE INDEX IF NOT EXISTS idx_tushare_stk_limit_raw_date
    ON tushare_stk_limit_raw (trade_date, ts_code);

CREATE TABLE IF NOT EXISTS tushare_dividend_raw (
    record_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    end_date DATE,
    ann_date DATE,
    div_proc VARCHAR(20),
    record_date DATE,
    ex_date DATE,
    payload JSONB NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    batch_id VARCHAR(80) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, payload_hash)
);
CREATE INDEX IF NOT EXISTS idx_tushare_dividend_raw_code_date
    ON tushare_dividend_raw (ts_code, ex_date, ann_date);

CREATE TABLE IF NOT EXISTS tushare_index_daily_raw (
    record_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    payload JSONB NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    batch_id VARCHAR(80) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, trade_date, payload_hash)
);
CREATE INDEX IF NOT EXISTS idx_tushare_index_daily_raw_date
    ON tushare_index_daily_raw (ts_code, trade_date);

CREATE SCHEMA IF NOT EXISTS std;
CREATE SCHEMA IF NOT EXISTS pit;
CREATE SCHEMA IF NOT EXISTS feature;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE INDEX IF NOT EXISTS idx_stock_financial_indicator_asof
    ON public.stock_financial_indicator (stock_code, announce_date, report_date, fetched_at DESC);

CREATE TABLE IF NOT EXISTS std.security_lifecycle (
    lifecycle_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    symbol VARCHAR(20),
    name TEXT,
    exchange VARCHAR(10),
    market VARCHAR(30),
    list_status VARCHAR(5),
    list_date DATE,
    delist_date DATE,
    effective_from DATE NOT NULL,
    effective_to DATE,
    source VARCHAR(30) NOT NULL,
    source_record_key VARCHAR(180) NOT NULL,
    record_version BIGINT NOT NULL DEFAULT 0,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, effective_from, effective_to, source)
);
CREATE INDEX IF NOT EXISTS idx_security_lifecycle_code_date
    ON std.security_lifecycle (ts_code, effective_from, effective_to);

CREATE TABLE IF NOT EXISTS std.security_status_daily (
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    is_listed BOOLEAN,
    is_st BOOLEAN,
    is_suspended BOOLEAN,
    is_tradeable BOOLEAN,
    limit_status VARCHAR(20),
    up_limit DOUBLE PRECISION,
    down_limit DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    status_reason TEXT,
    source VARCHAR(30) NOT NULL,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, ts_code)
);
CREATE INDEX IF NOT EXISTS idx_security_status_daily_code_date
    ON std.security_status_daily (ts_code, trade_date);

CREATE TABLE IF NOT EXISTS std.dividend_event (
    dividend_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    report_date DATE,
    announce_date DATE,
    implementation_status VARCHAR(20),
    stock_div DOUBLE PRECISION,
    stock_bonus_rate DOUBLE PRECISION,
    stock_conversion_rate DOUBLE PRECISION,
    cash_div DOUBLE PRECISION,
    cash_div_tax DOUBLE PRECISION,
    record_date DATE,
    ex_date DATE,
    pay_date DATE,
    div_list_date DATE,
    implementation_announce_date DATE,
    source VARCHAR(30) NOT NULL,
    source_record_key VARCHAR(180) NOT NULL,
    record_version BIGINT NOT NULL DEFAULT 0,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source, source_record_key, record_version)
);
CREATE INDEX IF NOT EXISTS idx_dividend_event_code_ex_date
    ON std.dividend_event (ts_code, ex_date);

CREATE TABLE IF NOT EXISTS std.index_daily_tushare (
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    pre_close DOUBLE PRECISION,
    change DOUBLE PRECISION,
    pct_chg DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    amount DOUBLE PRECISION,
    source VARCHAR(30) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date, source)
);

CREATE TABLE IF NOT EXISTS std.quarantine_record (
    quarantine_id BIGSERIAL PRIMARY KEY,
    source_table VARCHAR(100) NOT NULL,
    source_record_key VARCHAR(180),
    quality_status VARCHAR(30) NOT NULL,
    reason_code VARCHAR(80) NOT NULL,
    details JSONB,
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolution_status VARCHAR(30) NOT NULL DEFAULT 'OPEN'
);

CREATE TABLE IF NOT EXISTS pit.universe_daily (
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    is_listed_asof BOOLEAN,
    is_st_asof BOOLEAN,
    is_suspended_asof BOOLEAN,
    has_price_history BOOLEAN,
    days_since_listing INTEGER,
    eligible_by_market BOOLEAN,
    exclusion_reason TEXT,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    source_asof_date DATE,
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, ts_code)
);
CREATE INDEX IF NOT EXISTS idx_pit_universe_daily_code_date
    ON pit.universe_daily (ts_code, trade_date);

CREATE TABLE IF NOT EXISTS pit.security_tradeability_daily (
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    is_tradeable BOOLEAN,
    can_buy BOOLEAN,
    can_sell BOOLEAN,
    is_suspended BOOLEAN,
    limit_status VARCHAR(20),
    blocked_reason TEXT,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    source_asof_date DATE,
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, ts_code)
);
CREATE INDEX IF NOT EXISTS idx_pit_tradeability_code_date
    ON pit.security_tradeability_daily (ts_code, trade_date);

CREATE TABLE IF NOT EXISTS pit.financial_asof (
    decision_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    report_date DATE,
    effective_announce_date DATE,
    ann_date DATE,
    f_ann_date DATE,
    report_type VARCHAR(10),
    comp_type VARCHAR(10),
    record_version BIGINT,
    source_record_key VARCHAR(180),
    roe DOUBLE PRECISION,
    roa DOUBLE PRECISION,
    debt_to_assets DOUBLE PRECISION,
    revenue_yoy DOUBLE PRECISION,
    netprofit_yoy DOUBLE PRECISION,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (decision_date, ts_code)
);
CREATE INDEX IF NOT EXISTS idx_pit_financial_asof_code_date
    ON pit.financial_asof (ts_code, decision_date);

CREATE TABLE IF NOT EXISTS feature.price_daily_v1 (
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    raw_close DOUBLE PRECISION,
    adj_close_base DOUBLE PRECISION,
    return_1d DOUBLE PRECISION,
    return_20d DOUBLE PRECISION,
    return_60d DOUBLE PRECISION,
    ma20 DOUBLE PRECISION,
    ma60 DOUBLE PRECISION,
    ma120 DOUBLE PRECISION,
    volatility_20d DOUBLE PRECISION,
    amount_ma20 DOUBLE PRECISION,
    volume_ratio_20d DOUBLE PRECISION,
    available_observation_count INTEGER,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    feature_set_version VARCHAR(30) NOT NULL DEFAULT 'price_daily_v1',
    source_asof_date DATE,
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_feature_price_daily_v1_date
    ON feature.price_daily_v1 (trade_date, ts_code);

CREATE TABLE IF NOT EXISTS feature.valuation_daily_v1 (
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
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
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    feature_set_version VARCHAR(30) NOT NULL DEFAULT 'valuation_daily_v1',
    source_asof_date DATE,
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date)
);

CREATE TABLE IF NOT EXISTS feature.company_financial_v1 (
    feature_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,
    effective_announce_date DATE,
    ann_date DATE,
    f_ann_date DATE,
    roe DOUBLE PRECISION,
    roa DOUBLE PRECISION,
    debt_to_assets DOUBLE PRECISION,
    revenue_yoy DOUBLE PRECISION,
    netprofit_yoy DOUBLE PRECISION,
    operating_cashflow DOUBLE PRECISION,
    net_profit DOUBLE PRECISION,
    operating_cashflow_to_profit DOUBLE PRECISION,
    grossprofit_margin DOUBLE PRECISION,
    netprofit_margin DOUBLE PRECISION,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    feature_set_version VARCHAR(30) NOT NULL DEFAULT 'company_financial_v1',
    source_record_key VARCHAR(180),
    record_version BIGINT NOT NULL DEFAULT 0,
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, report_date, source_record_key, record_version)
);

CREATE TABLE IF NOT EXISTS feature.shareholder_return_v1 (
    feature_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    asof_date DATE NOT NULL,
    cash_div_12m DOUBLE PRECISION,
    dividend_event_count_3y INTEGER,
    dividend_year_count_3y INTEGER,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    feature_set_version VARCHAR(30) NOT NULL DEFAULT 'shareholder_return_v1',
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, asof_date)
);

CREATE TABLE IF NOT EXISTS ops.feature_registry (
    feature_name VARCHAR(100) PRIMARY KEY,
    feature_version VARCHAR(30) NOT NULL,
    grain VARCHAR(100) NOT NULL,
    lookback_rule TEXT,
    source_tables TEXT[] NOT NULL,
    code_version VARCHAR(80),
    status VARCHAR(30) NOT NULL DEFAULT 'ACTIVE',
    registered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
