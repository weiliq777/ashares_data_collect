-- Keep stock-partitioned Raw Version projections index-backed.
-- This is intentionally additive and does not alter any source facts.
CREATE INDEX IF NOT EXISTS idx_tushare_market_raw_version_dataset_code_date
    ON tushare_market_raw_version (dataset, ts_code, trade_date, fetched_at DESC);
