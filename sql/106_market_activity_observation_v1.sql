-- Market Activity Observation V1 (成交活动观察 V1)
--
-- Reuses the canonical Observation snapshot/stock/tag tables.  This avoids a
-- second parallel Observation domain while keeping the Market Activity input
-- explicit and auditable.  It does not rebuild or alter Raw/Standard/PIT/
-- Feature/Market Activity facts.

BEGIN;

ALTER TABLE observation.snapshot
    ADD COLUMN IF NOT EXISTS source_activity_snapshot_id UUID;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'observation_snapshot_activity_source_fk'
           AND conrelid = 'observation.snapshot'::regclass
    ) THEN
        ALTER TABLE observation.snapshot
            ADD CONSTRAINT observation_snapshot_activity_source_fk
            FOREIGN KEY (source_activity_snapshot_id)
            REFERENCES observation.market_activity_snapshot(activity_snapshot_id);
    END IF;
END;
$$;

-- The old four-column identity remains valid for legacy snapshots but would
-- prevent two immutable Observation results sourced from different frozen
-- Market Activity snapshots.  The source snapshot is part of the new identity.
DROP INDEX IF EXISTS observation.uq_observation_snapshot_identity;
CREATE UNIQUE INDEX IF NOT EXISTS uq_observation_snapshot_identity_v2
    ON observation.snapshot
       (observation_date_start, observation_date_end, methodology_hash,
        data_baseline_id, source_activity_snapshot_id);

ALTER TABLE observation.stock_anomaly_daily
    ADD COLUMN IF NOT EXISTS source_activity_snapshot_id UUID,
    ADD COLUMN IF NOT EXISTS activity_volume_ratio_previous DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS activity_amount_ratio_previous DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS activity_amount_share DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS activity_volume_share DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS activity_volume_rank INTEGER,
    ADD COLUMN IF NOT EXISTS activity_amount_rank INTEGER,
    ADD COLUMN IF NOT EXISTS activity_history_observation_count INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'stock_anomaly_activity_source_fk'
           AND conrelid = 'observation.stock_anomaly_daily'::regclass
    ) THEN
        ALTER TABLE observation.stock_anomaly_daily
            ADD CONSTRAINT stock_anomaly_activity_source_fk
            FOREIGN KEY (source_activity_snapshot_id)
            REFERENCES observation.market_activity_snapshot(activity_snapshot_id);
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_observation_stock_activity_source
    ON observation.stock_anomaly_daily
       (source_activity_snapshot_id, observation_date, ts_code);

ALTER TABLE observation.stock_anomaly_tag
    DROP CONSTRAINT IF EXISTS stock_anomaly_tag_code;
ALTER TABLE observation.stock_anomaly_tag
    ADD CONSTRAINT stock_anomaly_tag_code CHECK
        (tag_code IN (
            'VOLUME_EXPANSION', 'TURNOVER_EXPANSION', 'LOW_POSITION',
            'SECTOR_CONFIRMATION', 'PRICE_DIVERGENCE', 'PERSISTENT_ANOMALY',
            'MARKET_RELATIVE', 'ABNORMAL_VOLUME', 'ABNORMAL_AMOUNT',
            'SUDDEN_ACTIVITY', 'HIGH_AMOUNT_SHARE'
        ));

CREATE OR REPLACE FUNCTION observation.enforce_activity_source_frozen()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    source_status TEXT;
BEGIN
    IF NEW.source_activity_snapshot_id IS NOT NULL THEN
        SELECT status
          INTO source_status
          FROM observation.market_activity_snapshot
         WHERE activity_snapshot_id = NEW.source_activity_snapshot_id;
        IF source_status IS NULL THEN
            RAISE EXCEPTION 'Market Activity source snapshot % does not exist',
                NEW.source_activity_snapshot_id;
        END IF;
        IF source_status <> 'FROZEN' THEN
            RAISE EXCEPTION 'Market Activity source snapshot % is %, Observation requires FROZEN',
                NEW.source_activity_snapshot_id, source_status;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION observation.enforce_observation_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    parent_methodology CHAR(64);
    parent_baseline VARCHAR(100);
    parent_run_id VARCHAR(160);
    parent_start DATE;
    parent_end DATE;
    parent_activity_snapshot UUID;
BEGIN
    SELECT methodology_hash, data_baseline_id, calculation_run_id,
           observation_date_start, observation_date_end,
           source_activity_snapshot_id
      INTO parent_methodology, parent_baseline, parent_run_id,
           parent_start, parent_end, parent_activity_snapshot
      FROM observation.snapshot
     WHERE snapshot_id = NEW.snapshot_id;
    IF parent_methodology IS NULL THEN
        RAISE EXCEPTION 'Observation snapshot % does not exist', NEW.snapshot_id;
    END IF;
    IF parent_methodology <> NEW.methodology_hash
       OR parent_baseline <> NEW.data_baseline_id
       OR parent_run_id <> NEW.calculation_run_id THEN
        RAISE EXCEPTION 'Observation row identity does not match its snapshot: %',
            NEW.observation_id;
    END IF;
    IF NEW.observation_date < parent_start OR NEW.observation_date > parent_end THEN
        RAISE EXCEPTION 'Observation date % is outside snapshot % range %..%',
            NEW.observation_date, NEW.snapshot_id, parent_start, parent_end;
    END IF;
    IF NEW.source_activity_snapshot_id IS DISTINCT FROM parent_activity_snapshot THEN
        RAISE EXCEPTION 'Observation Activity source does not match its snapshot: %',
            NEW.observation_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_observation_activity_source_frozen
    ON observation.snapshot;
CREATE TRIGGER trg_observation_activity_source_frozen
BEFORE INSERT OR UPDATE ON observation.snapshot
FOR EACH ROW EXECUTE FUNCTION observation.enforce_activity_source_frozen();

COMMENT ON COLUMN observation.snapshot.source_activity_snapshot_id IS
    'Frozen Market Activity source snapshot for Market Activity Observation V1';
COMMENT ON COLUMN observation.stock_anomaly_daily.source_activity_snapshot_id IS
    'Frozen Market Activity source snapshot used by this observation row';
COMMENT ON COLUMN observation.stock_anomaly_daily.activity_volume_ratio_previous IS
    'Target volume divided by the preceding available Market Activity history average';
COMMENT ON COLUMN observation.stock_anomaly_daily.activity_amount_ratio_previous IS
    'Target amount divided by the preceding available Market Activity history average';

COMMIT;
