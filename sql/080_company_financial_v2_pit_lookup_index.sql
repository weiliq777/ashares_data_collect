-- PIT lookup index for Foundation V2 snapshot input manifests.
-- CONCURRENTLY keeps the production table readable while the index builds.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_company_financial_v2_pit_latest
    ON feature.company_financial_v2
       (ts_code, report_date DESC, data_available_timestamp DESC, feature_id DESC);
