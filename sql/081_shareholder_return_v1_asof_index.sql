-- Date-first lookup index for daily shareholder-return snapshot inputs.
-- The existing unique key starts with ts_code and cannot serve asof_date scans.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shareholder_return_v1_asof_ts_code
    ON feature.shareholder_return_v1 (asof_date, ts_code);
