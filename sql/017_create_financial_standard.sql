-- 财务 Raw 版本和三张 Standard 财务事实表。
CREATE TABLE IF NOT EXISTS tushare_financial_raw_version (
    record_id BIGSERIAL PRIMARY KEY,
    dataset VARCHAR(30) NOT NULL,
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
    UNIQUE (dataset, business_key, payload_hash)
);

CREATE INDEX IF NOT EXISTS idx_financial_raw_version_lookup
    ON tushare_financial_raw_version (dataset, ts_code, end_date, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_financial_raw_version_announce
    ON tushare_financial_raw_version (dataset, ann_date, f_ann_date);

CREATE TABLE IF NOT EXISTS financial_income_standard (
    financial_id BIGSERIAL PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,
    ann_date DATE,
    f_ann_date DATE,
    announce_date DATE,
    report_type VARCHAR(10),
    comp_type VARCHAR(10),
    end_type VARCHAR(10),
    update_flag VARCHAR(10),
    revenue DOUBLE PRECISION,
    total_revenue DOUBLE PRECISION,
    oper_cost DOUBLE PRECISION,
    total_cogs DOUBLE PRECISION,
    operate_profit DOUBLE PRECISION,
    total_profit DOUBLE PRECISION,
    income_tax DOUBLE PRECISION,
    net_profit DOUBLE PRECISION,
    n_income_attr_p DOUBLE PRECISION,
    basic_eps DOUBLE PRECISION,
    diluted_eps DOUBLE PRECISION,
    source VARCHAR(30) NOT NULL,
    source_record_key VARCHAR(180),
    record_version BIGINT NOT NULL DEFAULT 0,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS financial_balance_sheet_standard (
    financial_id BIGSERIAL PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,
    ann_date DATE,
    f_ann_date DATE,
    announce_date DATE,
    report_type VARCHAR(10),
    comp_type VARCHAR(10),
    end_type VARCHAR(10),
    update_flag VARCHAR(10),
    total_assets DOUBLE PRECISION,
    total_liab DOUBLE PRECISION,
    total_hldr_eqy_exc_min_int DOUBLE PRECISION,
    total_hldr_eqy_inc_min_int DOUBLE PRECISION,
    cash DOUBLE PRECISION,
    accounts_receiv DOUBLE PRECISION,
    inventories DOUBLE PRECISION,
    goodwill DOUBLE PRECISION,
    total_cur_assets DOUBLE PRECISION,
    total_cur_liab DOUBLE PRECISION,
    st_borr DOUBLE PRECISION,
    lt_borr DOUBLE PRECISION,
    source VARCHAR(30) NOT NULL,
    source_record_key VARCHAR(180),
    record_version BIGINT NOT NULL DEFAULT 0,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS financial_cash_flow_standard (
    financial_id BIGSERIAL PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,
    ann_date DATE,
    f_ann_date DATE,
    announce_date DATE,
    report_type VARCHAR(10),
    comp_type VARCHAR(10),
    end_type VARCHAR(10),
    update_flag VARCHAR(10),
    n_cashflow_act DOUBLE PRECISION,
    n_cashflow_inv_act DOUBLE PRECISION,
    n_cash_flows_fnc_act DOUBLE PRECISION,
    c_fr_sale_sg DOUBLE PRECISION,
    c_paid_goods_s DOUBLE PRECISION,
    c_paid_for_taxes DOUBLE PRECISION,
    c_pay_acq_const_fiolta DOUBLE PRECISION,
    c_recp_cap_contrib DOUBLE PRECISION,
    c_recp_borrow DOUBLE PRECISION,
    c_pay_debt DOUBLE PRECISION,
    n_incr_cash_cash_equ DOUBLE PRECISION,
    c_cash_equ_beg_period DOUBLE PRECISION,
    c_cash_equ_end_period DOUBLE PRECISION,
    free_cashflow DOUBLE PRECISION,
    net_profit DOUBLE PRECISION,
    source VARCHAR(30) NOT NULL,
    source_record_key VARCHAR(180),
    record_version BIGINT NOT NULL DEFAULT 0,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_financial_income_lookup ON financial_income_standard (stock_code, report_date, announce_date);
CREATE INDEX IF NOT EXISTS idx_financial_balance_lookup ON financial_balance_sheet_standard (stock_code, report_date, announce_date);
CREATE INDEX IF NOT EXISTS idx_financial_cashflow_lookup ON financial_cash_flow_standard (stock_code, report_date, announce_date);
CREATE INDEX IF NOT EXISTS idx_financial_income_pit ON financial_income_standard (stock_code, announce_date);
CREATE INDEX IF NOT EXISTS idx_financial_balance_pit ON financial_balance_sheet_standard (stock_code, announce_date);
CREATE INDEX IF NOT EXISTS idx_financial_cashflow_pit ON financial_cash_flow_standard (stock_code, announce_date);
CREATE UNIQUE INDEX IF NOT EXISTS uq_financial_income_business
    ON financial_income_standard (source, source_record_key, record_version);
CREATE UNIQUE INDEX IF NOT EXISTS uq_financial_balance_business
    ON financial_balance_sheet_standard (source, source_record_key, record_version);
CREATE UNIQUE INDEX IF NOT EXISTS uq_financial_cashflow_business
    ON financial_cash_flow_standard (source, source_record_key, record_version);
