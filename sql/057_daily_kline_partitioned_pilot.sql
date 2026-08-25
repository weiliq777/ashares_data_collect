-- Partitioned Standard target for daily_kline.
-- The public source remains intact until the full copy and validation finish.

CREATE SCHEMA IF NOT EXISTS std;

CREATE TABLE IF NOT EXISTS std.daily_kline (
    stock_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    amount DOUBLE PRECISION,
    PRIMARY KEY (stock_code, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE TABLE IF NOT EXISTS std.daily_kline_2016
    PARTITION OF std.daily_kline FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
CREATE TABLE IF NOT EXISTS std.daily_kline_2017
    PARTITION OF std.daily_kline FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE IF NOT EXISTS std.daily_kline_2018
    PARTITION OF std.daily_kline FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE IF NOT EXISTS std.daily_kline_2019
    PARTITION OF std.daily_kline FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE IF NOT EXISTS std.daily_kline_2020
    PARTITION OF std.daily_kline FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE IF NOT EXISTS std.daily_kline_2021
    PARTITION OF std.daily_kline FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE IF NOT EXISTS std.daily_kline_2022
    PARTITION OF std.daily_kline FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE IF NOT EXISTS std.daily_kline_2023
    PARTITION OF std.daily_kline FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE IF NOT EXISTS std.daily_kline_2024
    PARTITION OF std.daily_kline FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS std.daily_kline_2025
    PARTITION OF std.daily_kline FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS std.daily_kline_2026
    PARTITION OF std.daily_kline FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE IF NOT EXISTS std.daily_kline_2027
    PARTITION OF std.daily_kline FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE IF NOT EXISTS std.daily_kline_default
    PARTITION OF std.daily_kline DEFAULT;

CREATE INDEX IF NOT EXISTS idx_std_daily_kline_trade_date
    ON std.daily_kline (trade_date, stock_code);

GRANT SELECT, INSERT, UPDATE, DELETE ON std.daily_kline TO ashares_app;
