CREATE TABLE IF NOT EXISTS stock_basic_info (
    stock_code VARCHAR(20) PRIMARY KEY,
    symbol VARCHAR(20), name TEXT, area TEXT, industry TEXT, market TEXT,
    list_date DATE, delist_date DATE, list_status VARCHAR(5), source VARCHAR(30) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tushare_stock_basic_raw (
    ts_code VARCHAR(20) PRIMARY KEY, payload JSONB NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS valuation_daily (
    stock_code VARCHAR(20) NOT NULL, trade_date DATE NOT NULL,
    turnover_rate DOUBLE PRECISION, pe DOUBLE PRECISION, pe_ttm DOUBLE PRECISION,
    pb DOUBLE PRECISION, ps DOUBLE PRECISION, ps_ttm DOUBLE PRECISION,
    dv_ratio DOUBLE PRECISION, total_mv DOUBLE PRECISION, circ_mv DOUBLE PRECISION,
    source VARCHAR(30) NOT NULL, fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_code, trade_date, source)
);

CREATE TABLE IF NOT EXISTS stock_financial_indicator (
    stock_code VARCHAR(20) NOT NULL, report_date DATE NOT NULL, announce_date DATE,
    roe DOUBLE PRECISION, roa DOUBLE PRECISION, debt_to_assets DOUBLE PRECISION,
    grossprofit_margin DOUBLE PRECISION, netprofit_margin DOUBLE PRECISION,
    ocf_to_or DOUBLE PRECISION, revenue_yoy DOUBLE PRECISION, netprofit_yoy DOUBLE PRECISION,
    eps DOUBLE PRECISION, bps DOUBLE PRECISION, source VARCHAR(30) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_code, report_date, announce_date, source)
);

CREATE TABLE IF NOT EXISTS tushare_income_raw (
    ts_code VARCHAR(20) NOT NULL, ann_date DATE, end_date DATE, payload JSONB NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, ann_date, end_date)
);
CREATE TABLE IF NOT EXISTS tushare_balancesheet_raw (
    ts_code VARCHAR(20) NOT NULL, ann_date DATE, end_date DATE, payload JSONB NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, ann_date, end_date)
);
CREATE TABLE IF NOT EXISTS tushare_cashflow_raw (
    ts_code VARCHAR(20) NOT NULL, ann_date DATE, end_date DATE, payload JSONB NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, ann_date, end_date)
);
CREATE TABLE IF NOT EXISTS tushare_fina_indicator_raw (
    ts_code VARCHAR(20) NOT NULL, ann_date DATE, end_date DATE, payload JSONB NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, ann_date, end_date)
);
