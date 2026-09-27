-- Add durable ownership and lease metadata to the financial change queue.
-- Existing RUNNING rows without a lease are reclaimable after a one-hour
-- compatibility grace period; no Raw or derived data is rewritten here.

DO $$
BEGIN
    IF to_regclass('ops.financial_change_queue') IS NULL THEN
        RAISE EXCEPTION
            'financial queue lease migration requires ops.financial_change_queue';
    END IF;
END;
$$;

ALTER TABLE ops.financial_change_queue
    ADD COLUMN IF NOT EXISTS claim_run_id VARCHAR(160),
    ADD COLUMN IF NOT EXISTS claim_owner_id VARCHAR(160),
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_financial_change_queue_lease
    ON ops.financial_change_queue (status, lease_expires_at, queue_id);

COMMENT ON COLUMN ops.financial_change_queue.claim_run_id IS
    'Stable execution ID of the current RUNNING lease; retained across scheduler retries.';
COMMENT ON COLUMN ops.financial_change_queue.claim_owner_id IS
    'Unique subprocess owner of the current RUNNING lease; cleared on completion or recovery.';
COMMENT ON COLUMN ops.financial_change_queue.lease_expires_at IS
    'Database timestamp after which a RUNNING claim may be safely reclaimed.';
