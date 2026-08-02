CREATE TABLE IF NOT EXISTS trading_calendar (
    exchange VARCHAR(10) NOT NULL,
    trade_date DATE NOT NULL,
    is_open BOOLEAN NOT NULL,
    pretrade_date DATE,
    source VARCHAR(30) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (exchange, trade_date, source)
);

CREATE TABLE IF NOT EXISTS adj_factor_daily (
    stock_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    adj_factor DOUBLE PRECISION,
    source VARCHAR(30) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_code, trade_date, source)
);

CREATE TABLE IF NOT EXISTS tushare_history_checkpoint (
    dataset VARCHAR(50) NOT NULL,
    checkpoint_key VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    rows_written BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset, checkpoint_key)
);
