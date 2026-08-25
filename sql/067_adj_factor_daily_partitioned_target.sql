-- Partitioned Standard target for the adjusted-factor fact table.
-- public.adj_factor_daily remains the immutable migration source until the
-- bounded copy, validation, consumer switch, and physical cutover complete.
CREATE SCHEMA IF NOT EXISTS std;

CREATE TABLE IF NOT EXISTS std.adj_factor_daily (
    stock_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    adj_factor DOUBLE PRECISION,
    source VARCHAR(30) NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    PRIMARY KEY (stock_code, trade_date, source)
) PARTITION BY RANGE (trade_date);

CREATE TABLE IF NOT EXISTS std.adj_factor_daily_2016 PARTITION OF std.adj_factor_daily FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
CREATE TABLE IF NOT EXISTS std.adj_factor_daily_2017 PARTITION OF std.adj_factor_daily FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE IF NOT EXISTS std.adj_factor_daily_2018 PARTITION OF std.adj_factor_daily FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE IF NOT EXISTS std.adj_factor_daily_2019 PARTITION OF std.adj_factor_daily FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE IF NOT EXISTS std.adj_factor_daily_2020 PARTITION OF std.adj_factor_daily FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE IF NOT EXISTS std.adj_factor_daily_2021 PARTITION OF std.adj_factor_daily FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE IF NOT EXISTS std.adj_factor_daily_2022 PARTITION OF std.adj_factor_daily FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE IF NOT EXISTS std.adj_factor_daily_2023 PARTITION OF std.adj_factor_daily FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE IF NOT EXISTS std.adj_factor_daily_2024 PARTITION OF std.adj_factor_daily FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS std.adj_factor_daily_2025 PARTITION OF std.adj_factor_daily FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS std.adj_factor_daily_2026 PARTITION OF std.adj_factor_daily FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE IF NOT EXISTS std.adj_factor_daily_2027 PARTITION OF std.adj_factor_daily FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
CREATE TABLE IF NOT EXISTS std.adj_factor_daily_default PARTITION OF std.adj_factor_daily DEFAULT;

CREATE INDEX IF NOT EXISTS idx_std_adj_factor_daily_trade_date
    ON std.adj_factor_daily (trade_date, stock_code);
GRANT SELECT, INSERT, UPDATE, DELETE ON std.adj_factor_daily TO ashares_app;
