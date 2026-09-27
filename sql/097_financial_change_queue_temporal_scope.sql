-- Financial change queue temporal scope and processing provenance.
--
-- A queue row is eligible for a consumer only when the source version was
-- available on or before that consumer's output_end.  report_date is the
-- reporting period and is not a safe substitute for that availability date.
-- Existing rows are backfilled from their immutable Raw version by
-- source_record_id.  Rows that cannot be resolved remain NULL and are
-- deliberately fail-closed in the consumer.

DO $$
BEGIN
    IF to_regclass('ops.financial_change_queue') IS NULL THEN
        RAISE EXCEPTION
            'temporal scope migration requires ops.financial_change_queue';
    END IF;
END $$;

ALTER TABLE ops.financial_change_queue
    ADD COLUMN IF NOT EXISTS available_date DATE,
    ADD COLUMN IF NOT EXISTS processed_through DATE;

UPDATE ops.financial_change_queue q
   SET available_date = COALESCE(v.f_ann_date, v.ann_date, q.report_date)
  FROM public.tushare_financial_raw_version v
 WHERE q.available_date IS NULL
   AND q.dataset IN ('income', 'balancesheet', 'cashflow')
   AND q.source_record_id = v.record_id;

UPDATE ops.financial_change_queue q
   SET available_date = COALESCE(v.f_ann_date, v.ann_date, q.report_date)
  FROM public.fina_indicator_raw_version v
 WHERE q.available_date IS NULL
   AND q.dataset = 'fina_indicator'
   AND q.source_record_id = v.record_id;

CREATE INDEX IF NOT EXISTS idx_financial_change_queue_temporal_claim
    ON ops.financial_change_queue (status, available_date, queue_id);

COMMENT ON COLUMN ops.financial_change_queue.available_date IS
    'Business availability date used to scope a consumer output_end; not fetched_at.';
COMMENT ON COLUMN ops.financial_change_queue.processed_through IS
    'Consumer output cutoff used when this queue row was marked DONE.';
