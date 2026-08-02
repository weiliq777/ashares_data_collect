CREATE TABLE IF NOT EXISTS tushare_daily_raw (
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    payload JSONB NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date, source)
);

CREATE TABLE IF NOT EXISTS tushare_daily_basic_raw (
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    payload JSONB NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date, source)
);

CREATE TABLE IF NOT EXISTS tushare_adj_factor_raw (
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    payload JSONB NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date, source)
);

CREATE TABLE IF NOT EXISTS tushare_trade_cal_raw (
    exchange VARCHAR(10) NOT NULL,
    cal_date DATE NOT NULL,
    payload JSONB NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (exchange, cal_date, source)
);
