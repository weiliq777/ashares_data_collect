-- Move financial ingestion control tables from public (公共模式) to ops
-- (运维模式). SET SCHEMA is catalog-only: rows and indexes are not copied.
DO $$
BEGIN
    IF to_regclass('public.financial_change_queue') IS NOT NULL
       AND to_regclass('ops.financial_change_queue') IS NULL THEN
        ALTER TABLE public.financial_change_queue SET SCHEMA ops;
    END IF;
    IF to_regclass('public.financial_ingest_anomaly') IS NOT NULL
       AND to_regclass('ops.financial_ingest_anomaly') IS NULL THEN
        ALTER TABLE public.financial_ingest_anomaly SET SCHEMA ops;
    END IF;
    IF to_regclass('public.financial_version_cutover_watermark') IS NOT NULL
       AND to_regclass('ops.financial_version_cutover_watermark') IS NULL THEN
        ALTER TABLE public.financial_version_cutover_watermark SET SCHEMA ops;
    END IF;
END
$$;

ALTER TABLE IF EXISTS ops.financial_change_queue OWNER TO ashares;
ALTER TABLE IF EXISTS ops.financial_ingest_anomaly OWNER TO ashares;
ALTER TABLE IF EXISTS ops.financial_version_cutover_watermark OWNER TO ashares;
