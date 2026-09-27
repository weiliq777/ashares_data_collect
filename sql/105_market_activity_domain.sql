-- Market Activity Observation domain (市场成交活跃度观察域)
--
-- This migration only creates the persistence model.  It does not backfill
-- historical dates and it does not alter Raw/Standard/PIT/Feature data.
-- Application writes must use Repository -> DataAccessKernel -> Gateway.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS observation;

CREATE TABLE IF NOT EXISTS observation.market_activity_snapshot (
    activity_snapshot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    activity_date_start DATE NOT NULL,
    activity_date_end DATE NOT NULL,
    methodology_hash CHAR(64) NOT NULL,
    data_baseline_id VARCHAR(100) NOT NULL,
    input_manifest_hash CHAR(64),
    contract_hash CHAR(64) NOT NULL,
    calculation_run_id VARCHAR(160) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
    quality_report_hash CHAR(64),
    quality_report JSONB,
    code_identity JSONB NOT NULL DEFAULT '{}'::jsonb,
    row_count BIGINT NOT NULL DEFAULT 0,
    stock_count BIGINT NOT NULL DEFAULT 0,
    date_count INTEGER NOT NULL DEFAULT 0,
    total_input_row_count BIGINT NOT NULL DEFAULT 0,
    target_row_count BIGINT NOT NULL DEFAULT 0,
    invalid_target_row_count BIGINT NOT NULL DEFAULT 0,
    invalid_history_row_count BIGINT NOT NULL DEFAULT 0,
    insufficient_history_stock_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    built_at TIMESTAMPTZ,
    validated_at TIMESTAMPTZ,
    frozen_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    failure_reason TEXT,
    CONSTRAINT market_activity_snapshot_dates
        CHECK (activity_date_end >= activity_date_start),
    CONSTRAINT market_activity_snapshot_status
        CHECK (status IN ('DRAFT','BUILDING','VALIDATED','FROZEN',
                         'FAILED','SUPERSEDED','CORRUPTED')),
    CONSTRAINT market_activity_snapshot_methodology_hash
        CHECK (length(btrim(methodology_hash::text)) = 64),
    CONSTRAINT market_activity_snapshot_contract_hash
        CHECK (length(btrim(contract_hash::text)) = 64),
    CONSTRAINT market_activity_snapshot_counts_nonnegative
        CHECK (row_count >= 0 AND stock_count >= 0 AND date_count >= 0
               AND total_input_row_count >= 0 AND target_row_count >= 0
               AND invalid_target_row_count >= 0
               AND invalid_history_row_count >= 0
               AND insufficient_history_stock_count >= 0),
    CONSTRAINT market_activity_snapshot_frozen_fields
        CHECK (status <> 'FROZEN'
               OR (input_manifest_hash IS NOT NULL
                   AND quality_report_hash IS NOT NULL
                   AND frozen_at IS NOT NULL
                   AND length(btrim(calculation_run_id)) > 0)),
    CONSTRAINT market_activity_snapshot_baseline_fk
        FOREIGN KEY (data_baseline_id)
        REFERENCES ops.tushare_data_release(release_tag)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_market_activity_snapshot_identity
    ON observation.market_activity_snapshot
       (activity_date_start, activity_date_end, methodology_hash, data_baseline_id);
CREATE INDEX IF NOT EXISTS idx_market_activity_snapshot_status_date
    ON observation.market_activity_snapshot
       (status, activity_date_start, activity_date_end);

CREATE TABLE IF NOT EXISTS observation.market_activity_daily (
    activity_daily_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    activity_snapshot_id UUID NOT NULL
        REFERENCES observation.market_activity_snapshot(activity_snapshot_id),
    trade_date DATE NOT NULL,
    methodology_hash CHAR(64) NOT NULL,
    data_baseline_id VARCHAR(100) NOT NULL,
    top_n INTEGER NOT NULL,
    total_volume DOUBLE PRECISION NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    top_n_volume DOUBLE PRECISION NOT NULL,
    top_n_amount DOUBLE PRECISION NOT NULL,
    top_n_volume_share DOUBLE PRECISION,
    top_n_amount_share DOUBLE PRECISION,
    input_row_count BIGINT NOT NULL,
    target_row_count BIGINT NOT NULL,
    invalid_target_row_count BIGINT NOT NULL,
    invalid_history_row_count BIGINT NOT NULL,
    insufficient_history_stock_count BIGINT NOT NULL,
    quality_status VARCHAR(30) NOT NULL,
    input_manifest_hash CHAR(64) NOT NULL,
    activity_hash CHAR(64) NOT NULL,
    calculation_run_id VARCHAR(160) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT market_activity_daily_identity
        UNIQUE (trade_date, methodology_hash, data_baseline_id),
    CONSTRAINT market_activity_daily_hashes
        CHECK (length(btrim(input_manifest_hash::text)) = 64
               AND length(btrim(activity_hash::text)) = 64),
    CONSTRAINT market_activity_daily_quality
        CHECK (quality_status IN ('VALID','PARTIAL','PARTIAL_HISTORY','INVALID','FAILED')),
    CONSTRAINT market_activity_daily_values_nonnegative
        CHECK (top_n > 0 AND total_volume >= 0 AND total_amount >= 0
               AND top_n_volume >= 0 AND top_n_amount >= 0
               AND input_row_count >= 0 AND target_row_count >= 0
               AND invalid_target_row_count >= 0 AND invalid_history_row_count >= 0
               AND insufficient_history_stock_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_market_activity_daily_snapshot_date
    ON observation.market_activity_daily (activity_snapshot_id, trade_date);

CREATE TABLE IF NOT EXISTS observation.stock_activity_daily (
    stock_activity_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    activity_snapshot_id UUID NOT NULL
        REFERENCES observation.market_activity_snapshot(activity_snapshot_id),
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    methodology_hash CHAR(64) NOT NULL,
    data_baseline_id VARCHAR(100) NOT NULL,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    amount_share DOUBLE PRECISION,
    volume_share DOUBLE PRECISION,
    amount_rank INTEGER NOT NULL,
    volume_rank INTEGER NOT NULL,
    amount_avg_previous DOUBLE PRECISION,
    amount_ratio_previous DOUBLE PRECISION,
    volume_avg_previous DOUBLE PRECISION,
    volume_ratio_previous DOUBLE PRECISION,
    history_observation_count INTEGER NOT NULL,
    reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    calculation_status VARCHAR(30) NOT NULL,
    quality_status VARCHAR(30) NOT NULL,
    input_manifest_hash CHAR(64) NOT NULL,
    activity_hash CHAR(64) NOT NULL,
    calculation_run_id VARCHAR(160) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT stock_activity_daily_identity
        UNIQUE (trade_date, ts_code, methodology_hash, data_baseline_id),
    CONSTRAINT stock_activity_daily_hashes
        CHECK (length(btrim(input_manifest_hash::text)) = 64
               AND length(btrim(activity_hash::text)) = 64),
    CONSTRAINT stock_activity_daily_status
        CHECK (calculation_status IN ('VALID','INSUFFICIENT_HISTORY','INVALID')
               AND quality_status IN ('VALID','PARTIAL','PARTIAL_HISTORY','INVALID','FAILED')),
    CONSTRAINT stock_activity_daily_values_nonnegative
        CHECK (volume >= 0 AND amount >= 0 AND amount_rank > 0
               AND volume_rank > 0 AND history_observation_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_stock_activity_daily_snapshot_date
    ON observation.stock_activity_daily (activity_snapshot_id, trade_date, ts_code);
CREATE INDEX IF NOT EXISTS idx_stock_activity_daily_date_amount
    ON observation.stock_activity_daily (trade_date, amount_rank, ts_code);

CREATE OR REPLACE FUNCTION observation.enforce_market_activity_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    parent_methodology CHAR(64);
    parent_baseline VARCHAR(100);
    parent_run_id VARCHAR(160);
    parent_start DATE;
    parent_end DATE;
BEGIN
    SELECT methodology_hash, data_baseline_id, calculation_run_id,
           activity_date_start, activity_date_end
      INTO parent_methodology, parent_baseline, parent_run_id,
           parent_start, parent_end
      FROM observation.market_activity_snapshot
     WHERE activity_snapshot_id = NEW.activity_snapshot_id;

    IF parent_methodology IS NULL THEN
        RAISE EXCEPTION 'Market activity snapshot % does not exist',
            NEW.activity_snapshot_id;
    END IF;
    IF parent_methodology <> NEW.methodology_hash
       OR parent_baseline <> NEW.data_baseline_id
       OR parent_run_id <> NEW.calculation_run_id THEN
        RAISE EXCEPTION 'Market activity row identity does not match its snapshot';
    END IF;
    IF NEW.trade_date < parent_start OR NEW.trade_date > parent_end THEN
        RAISE EXCEPTION 'Market activity date % is outside snapshot range %..%',
            NEW.trade_date, parent_start, parent_end;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION observation.enforce_market_activity_frozen_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    parent_status TEXT;
    parent_id UUID;
BEGIN
    IF TG_TABLE_NAME = 'market_activity_snapshot' THEN
        IF TG_OP IN ('UPDATE', 'DELETE') AND OLD.status IN
           ('FROZEN', 'SUPERSEDED', 'CORRUPTED') THEN
            RAISE EXCEPTION 'Market activity snapshot % is %, row is immutable',
                OLD.activity_snapshot_id, OLD.status;
        END IF;
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;

    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        parent_id := OLD.activity_snapshot_id;
    ELSE
        parent_id := NEW.activity_snapshot_id;
    END IF;
    SELECT status INTO parent_status
      FROM observation.market_activity_snapshot
     WHERE activity_snapshot_id = parent_id;
    IF parent_status IN ('FROZEN', 'SUPERSEDED', 'CORRUPTED') THEN
        RAISE EXCEPTION 'Market activity snapshot % is %, row is immutable',
            parent_id, parent_status;
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

DROP TRIGGER IF EXISTS trg_market_activity_snapshot_immutable
    ON observation.market_activity_snapshot;
CREATE TRIGGER trg_market_activity_snapshot_immutable
BEFORE UPDATE OR DELETE ON observation.market_activity_snapshot
FOR EACH ROW EXECUTE FUNCTION observation.enforce_market_activity_frozen_immutable();

DROP TRIGGER IF EXISTS trg_market_activity_daily_identity
    ON observation.market_activity_daily;
CREATE TRIGGER trg_market_activity_daily_identity
BEFORE INSERT OR UPDATE ON observation.market_activity_daily
FOR EACH ROW EXECUTE FUNCTION observation.enforce_market_activity_identity();

DROP TRIGGER IF EXISTS trg_market_activity_daily_immutable
    ON observation.market_activity_daily;
CREATE TRIGGER trg_market_activity_daily_immutable
BEFORE INSERT OR UPDATE OR DELETE ON observation.market_activity_daily
FOR EACH ROW EXECUTE FUNCTION observation.enforce_market_activity_frozen_immutable();

DROP TRIGGER IF EXISTS trg_stock_activity_daily_identity
    ON observation.stock_activity_daily;
CREATE TRIGGER trg_stock_activity_daily_identity
BEFORE INSERT OR UPDATE ON observation.stock_activity_daily
FOR EACH ROW EXECUTE FUNCTION observation.enforce_market_activity_identity();

DROP TRIGGER IF EXISTS trg_stock_activity_daily_immutable
    ON observation.stock_activity_daily;
CREATE TRIGGER trg_stock_activity_daily_immutable
BEFORE INSERT OR UPDATE OR DELETE ON observation.stock_activity_daily
FOR EACH ROW EXECUTE FUNCTION observation.enforce_market_activity_frozen_immutable();

GRANT USAGE ON SCHEMA observation TO ashares_app, research_reader;
GRANT SELECT, INSERT, UPDATE
    ON observation.market_activity_snapshot,
       observation.market_activity_daily,
       observation.stock_activity_daily
    TO ashares_app;
GRANT SELECT
    ON observation.market_activity_snapshot,
       observation.market_activity_daily,
       observation.stock_activity_daily
    TO research_reader;

COMMIT;
