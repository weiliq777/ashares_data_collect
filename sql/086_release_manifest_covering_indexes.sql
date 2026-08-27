-- Release source-manifest aggregates must be index-backed.
-- CREATE INDEX CONCURRENTLY keeps production reads/writes available.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_market_raw_version_manifest_cover
    ON public.tushare_market_raw_version (dataset, trade_date)
    INCLUDE (record_id, fetched_at);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_raw_manifest_cover
    ON public.tushare_daily_raw (trade_date)
    INCLUDE (fetched_at);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_basic_raw_manifest_cover
    ON public.tushare_daily_basic_raw (trade_date)
    INCLUDE (fetched_at);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_adj_factor_raw_manifest_cover
    ON public.tushare_adj_factor_raw (trade_date)
    INCLUDE (fetched_at);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trade_cal_raw_manifest_cover
    ON public.tushare_trade_cal_raw (cal_date)
    INCLUDE (fetched_at);
