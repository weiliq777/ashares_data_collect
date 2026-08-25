-- Research Snapshot v2 schema completion.
-- The deployed partitioned parent must contain the valuation window columns
-- consumed by the Foundation Snapshot repository.
BEGIN;

ALTER TABLE research.stock_snapshot_daily
    ADD COLUMN IF NOT EXISTS dv_ttm_source VARCHAR(40),
    ADD COLUMN IF NOT EXISTS dv_window_start DATE,
    ADD COLUMN IF NOT EXISTS dv_window_end DATE;

COMMIT;
