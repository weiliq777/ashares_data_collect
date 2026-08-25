-- Physical-migration pilot: standard stock master data.
-- The public source remains available for rollback while dual-write is active.

CREATE SCHEMA IF NOT EXISTS std;

CREATE TABLE IF NOT EXISTS std.stock_basic_info (
    stock_code VARCHAR(20) NOT NULL,
    symbol VARCHAR(20),
    name TEXT,
    area TEXT,
    industry TEXT,
    market TEXT,
    list_date DATE,
    delist_date DATE,
    list_status VARCHAR(5),
    source VARCHAR(30) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_code)
);

CREATE INDEX IF NOT EXISTS idx_std_stock_basic_market_status
    ON std.stock_basic_info (market, list_status, stock_code);

GRANT SELECT, INSERT, UPDATE, DELETE ON std.stock_basic_info TO ashares_app;
