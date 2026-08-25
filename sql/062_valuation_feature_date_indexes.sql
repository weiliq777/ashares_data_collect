-- Keep date-window quality checks and incremental inspection bounded.
-- This migration must be run outside an explicit transaction because the
-- indexes are built CONCURRENTLY on a production-sized feature table.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_valuation_daily_v1_date
    ON feature.valuation_daily_v1 (trade_date, ts_code);

-- 061 defines this index as well; repeat it idempotently here so deployments
-- that started after 061 still receive the quality-window access path.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_valuation_daily_v1_quality_window
    ON feature.valuation_daily_v1 (trade_date, quality_status);

