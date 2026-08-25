-- Bound the per-page version-to-queue lookup used by the financial VIP
-- Repository. Run outside an explicit transaction because CONCURRENTLY is
-- intentional: this is additive and must not block production reads/writes.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_financial_raw_version_batch_dataset_hash
    ON tushare_financial_raw_version (batch_id, dataset, payload_hash);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fina_indicator_version_batch_hash
    ON fina_indicator_raw_version (batch_id, payload_hash);
