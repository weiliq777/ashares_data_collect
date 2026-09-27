-- Audited recovery and resolution for financial queue quarantine rows.
-- No row is automatically approved by this migration; only the typed
-- Repository administrative operations may transition QUARANTINED rows.

DO $$
BEGIN
    IF to_regclass('ops.financial_change_queue') IS NULL THEN
        RAISE EXCEPTION
            'quarantine resolution migration requires ops.financial_change_queue';
    END IF;
END $$;

ALTER TABLE ops.financial_change_queue
    ADD COLUMN IF NOT EXISTS quarantine_resolved_by VARCHAR(160),
    ADD COLUMN IF NOT EXISTS quarantine_resolved_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS quarantine_resolution_reason TEXT;

ALTER TABLE ops.financial_change_queue
    DROP CONSTRAINT IF EXISTS financial_change_queue_status_check;

ALTER TABLE ops.financial_change_queue
    ADD CONSTRAINT financial_change_queue_status_check CHECK (status IN (
        'PENDING', 'RUNNING', 'DONE', 'FAILED',
        'COVERAGE_BLOCKED', 'QUARANTINED', 'RESOLVED'
    ));

COMMENT ON COLUMN ops.financial_change_queue.quarantine_resolved_by IS
    'Audited operator or service identity that approved requeue or resolution.';
COMMENT ON COLUMN ops.financial_change_queue.quarantine_resolved_at IS
    'Timestamp of the audited quarantine transition.';
COMMENT ON COLUMN ops.financial_change_queue.quarantine_resolution_reason IS
    'Required explanation for the audited quarantine transition.';
