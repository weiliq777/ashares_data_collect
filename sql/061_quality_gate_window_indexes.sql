-- Keep the production quality gate bounded by its date window while avoiding
-- heap reads for quality_status on JSON-heavy feature/PIT tables.
-- Run outside an explicit transaction; these are additive CONCURRENTLY builds.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_universe_daily_quality_window
    ON pit.universe_daily (trade_date, quality_status);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tradeability_quality_window
    ON pit.security_tradeability_daily (trade_date, quality_status);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_daily_v2_quality_window
    ON feature.price_daily_v2 (trade_date, quality_status);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_valuation_daily_v1_quality_window
    ON feature.valuation_daily_v1 (trade_date, quality_status);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_technical_daily_v1_quality_window
    ON feature.technical_daily_v1 (trade_date, quality_status);
