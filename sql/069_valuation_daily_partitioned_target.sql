-- Partitioned Standard target for the valuation fact table.
CREATE SCHEMA IF NOT EXISTS std;

CREATE TABLE IF NOT EXISTS std.valuation_daily (
    stock_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    turnover_rate DOUBLE PRECISION, pe DOUBLE PRECISION, pe_ttm DOUBLE PRECISION,
    pb DOUBLE PRECISION, ps DOUBLE PRECISION, ps_ttm DOUBLE PRECISION,
    dv_ratio DOUBLE PRECISION, total_mv DOUBLE PRECISION, circ_mv DOUBLE PRECISION,
    source VARCHAR(30) NOT NULL, fetched_at TIMESTAMP NOT NULL,
    pe_ttm_vendor DOUBLE PRECISION, ps_ttm_vendor DOUBLE PRECISION, pb_vendor DOUBLE PRECISION,
    dv_ttm DOUBLE PRECISION, dv_ttm_vendor DOUBLE PRECISION, dv_ttm_source VARCHAR(40),
    source_observed_at TIMESTAMPTZ, data_available_timestamp TIMESTAMPTZ,
    PRIMARY KEY (stock_code, trade_date, source)
) PARTITION BY RANGE (trade_date);

CREATE TABLE IF NOT EXISTS std.valuation_daily_2016 PARTITION OF std.valuation_daily FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
CREATE TABLE IF NOT EXISTS std.valuation_daily_2017 PARTITION OF std.valuation_daily FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE IF NOT EXISTS std.valuation_daily_2018 PARTITION OF std.valuation_daily FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE IF NOT EXISTS std.valuation_daily_2019 PARTITION OF std.valuation_daily FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE IF NOT EXISTS std.valuation_daily_2020 PARTITION OF std.valuation_daily FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE IF NOT EXISTS std.valuation_daily_2021 PARTITION OF std.valuation_daily FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE IF NOT EXISTS std.valuation_daily_2022 PARTITION OF std.valuation_daily FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE IF NOT EXISTS std.valuation_daily_2023 PARTITION OF std.valuation_daily FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE IF NOT EXISTS std.valuation_daily_2024 PARTITION OF std.valuation_daily FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS std.valuation_daily_2025 PARTITION OF std.valuation_daily FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS std.valuation_daily_2026 PARTITION OF std.valuation_daily FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE IF NOT EXISTS std.valuation_daily_2027 PARTITION OF std.valuation_daily FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
CREATE TABLE IF NOT EXISTS std.valuation_daily_default PARTITION OF std.valuation_daily DEFAULT;
CREATE INDEX IF NOT EXISTS idx_std_valuation_daily_trade_date ON std.valuation_daily (trade_date, stock_code);
GRANT SELECT, INSERT, UPDATE, DELETE ON std.valuation_daily TO ashares_app;
