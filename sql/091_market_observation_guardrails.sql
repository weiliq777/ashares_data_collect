-- Market Anomaly Observation guardrails (市场异常观察域防线)
--
-- 090 已经在部分环境执行过。本迁移只做加法和幂等修复：补齐运行身份、
-- 把冻结保护扩展到子表 INSERT，并让 FROZEN 快照必须具备完整审计字段。
-- 不触碰 Raw/Standard/PIT/Feature/Research Snapshot 的既有数据。

BEGIN;

ALTER TABLE observation.snapshot
    ADD COLUMN IF NOT EXISTS calculation_run_id VARCHAR(160);

ALTER TABLE observation.stock_anomaly_daily
    ADD COLUMN IF NOT EXISTS calculation_run_id VARCHAR(160);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM observation.snapshot
         WHERE calculation_run_id IS NULL OR btrim(calculation_run_id) = ''
    ) THEN
        RAISE EXCEPTION 'observation.snapshot contains rows without calculation_run_id';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM observation.stock_anomaly_daily
         WHERE calculation_run_id IS NULL OR btrim(calculation_run_id) = ''
    ) THEN
        RAISE EXCEPTION 'observation.stock_anomaly_daily contains rows without calculation_run_id';
    END IF;
END;
$$;

ALTER TABLE observation.snapshot
    ALTER COLUMN calculation_run_id SET NOT NULL;
ALTER TABLE observation.stock_anomaly_daily
    ALTER COLUMN calculation_run_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'observation.snapshot'::regclass
           AND conname = 'observation_snapshot_frozen_fields'
    ) THEN
        ALTER TABLE observation.snapshot
            ADD CONSTRAINT observation_snapshot_frozen_fields
            CHECK (
                status <> 'FROZEN'
                OR (
                    input_manifest_hash IS NOT NULL
                    AND quality_report_hash IS NOT NULL
                    AND frozen_at IS NOT NULL
                    AND calculation_run_id IS NOT NULL
                    AND length(btrim(calculation_run_id)) > 0
                )
            ) NOT VALID;
    END IF;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'observation.stock_anomaly_daily'::regclass
           AND conname = 'stock_anomaly_sector_status_consistency'
    ) THEN
        ALTER TABLE observation.stock_anomaly_daily
            ADD CONSTRAINT stock_anomaly_sector_status_consistency
            CHECK (
                sector_confirmation_status <> 'CONFIRMED'
                OR sector_data_status = 'CONFIRMED'
            ) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'observation.stock_anomaly_daily'::regclass
           AND conname = 'stock_anomaly_sector_unknown_consistency'
    ) THEN
        ALTER TABLE observation.stock_anomaly_daily
            ADD CONSTRAINT stock_anomaly_sector_unknown_consistency
            CHECK (
                sector_data_status IN ('CONFIRMED','DAILY_CAPTURED')
                OR sector_confirmation_status = 'UNKNOWN'
            ) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'observation.stock_sector_confirmation'::regclass
           AND conname = 'stock_sector_confirmation_status_consistency'
    ) THEN
        ALTER TABLE observation.stock_sector_confirmation
            ADD CONSTRAINT stock_sector_confirmation_status_consistency
            CHECK (
                sector_confirmation_status <> 'CONFIRMED'
                OR sector_data_status = 'CONFIRMED'
            ) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'observation.stock_sector_confirmation'::regclass
           AND conname = 'stock_sector_confirmation_unknown_consistency'
    ) THEN
        ALTER TABLE observation.stock_sector_confirmation
            ADD CONSTRAINT stock_sector_confirmation_unknown_consistency
            CHECK (
                sector_data_status IN ('CONFIRMED','DAILY_CAPTURED')
                OR sector_confirmation_status = 'UNKNOWN'
            ) NOT VALID;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION observation.enforce_frozen_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    row_data JSONB;
    snapshot_status TEXT;
    snapshot_id_text TEXT;
    observation_id_text TEXT;
BEGIN
    -- UPDATE checks OLD first so a row cannot be moved out of a frozen
    -- snapshot. DELETE only has OLD; INSERT only has NEW.
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        row_data := to_jsonb(OLD);
        snapshot_id_text := row_data->>'snapshot_id';
        observation_id_text := row_data->>'observation_id';
        IF TG_TABLE_SCHEMA = 'observation' AND TG_TABLE_NAME = 'snapshot' THEN
            snapshot_status := row_data->>'status';
        ELSE
            IF snapshot_id_text IS NULL AND observation_id_text IS NOT NULL THEN
                SELECT d.snapshot_id::text INTO snapshot_id_text
                  FROM observation.stock_anomaly_daily d
                 WHERE d.observation_id = observation_id_text::uuid;
            END IF;
            SELECT s.status INTO snapshot_status
              FROM observation.snapshot s
             WHERE s.snapshot_id = snapshot_id_text::uuid;
        END IF;
        IF snapshot_status IN ('FROZEN', 'SUPERSEDED', 'CORRUPTED') THEN
            RAISE EXCEPTION 'observation snapshot % is %, % is immutable',
                COALESCE(snapshot_id_text, observation_id_text),
                snapshot_status, TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
        END IF;
    END IF;

    IF TG_OP IN ('INSERT', 'UPDATE')
       AND NOT (TG_TABLE_SCHEMA = 'observation' AND TG_TABLE_NAME = 'snapshot') THEN
        row_data := to_jsonb(NEW);
        snapshot_id_text := row_data->>'snapshot_id';
        observation_id_text := row_data->>'observation_id';
        IF snapshot_id_text IS NULL AND observation_id_text IS NOT NULL THEN
            SELECT d.snapshot_id::text INTO snapshot_id_text
              FROM observation.stock_anomaly_daily d
             WHERE d.observation_id = observation_id_text::uuid;
        END IF;
        SELECT s.status INTO snapshot_status
          FROM observation.snapshot s
         WHERE s.snapshot_id = snapshot_id_text::uuid;
        IF snapshot_status IN ('FROZEN', 'SUPERSEDED', 'CORRUPTED') THEN
            RAISE EXCEPTION 'observation snapshot % is %, % is immutable',
                COALESCE(snapshot_id_text, observation_id_text),
                snapshot_status, TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
        END IF;
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
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
BEGIN
    SELECT methodology_hash, data_baseline_id, calculation_run_id,
           observation_date_start, observation_date_end
      INTO parent_methodology, parent_baseline, parent_run_id,
           parent_start, parent_end
      FROM observation.snapshot
     WHERE snapshot_id = NEW.snapshot_id;
    IF parent_methodology IS NULL THEN
        RAISE EXCEPTION 'Observation snapshot % does not exist', NEW.snapshot_id;
    END IF;
    IF parent_methodology <> NEW.methodology_hash
       OR parent_baseline <> NEW.data_baseline_id
       OR parent_run_id <> NEW.calculation_run_id THEN
        RAISE EXCEPTION 'Observation row identity does not match its snapshot: %', NEW.observation_id;
    END IF;
    IF NEW.observation_date < parent_start OR NEW.observation_date > parent_end THEN
        RAISE EXCEPTION 'Observation date % is outside snapshot % range %..%',
            NEW.observation_date, NEW.snapshot_id, parent_start, parent_end;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION observation.enforce_observation_date_scope()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    parent_start DATE;
    parent_end DATE;
BEGIN
    SELECT observation_date_start, observation_date_end
      INTO parent_start, parent_end
      FROM observation.snapshot
     WHERE snapshot_id = NEW.snapshot_id;
    IF parent_start IS NULL THEN
        RAISE EXCEPTION 'Observation snapshot % does not exist', NEW.snapshot_id;
    END IF;
    IF NEW.observation_date < parent_start OR NEW.observation_date > parent_end THEN
        RAISE EXCEPTION 'Observation date % is outside snapshot % range %..%',
            NEW.observation_date, NEW.snapshot_id, parent_start, parent_end;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION observation.clear_snapshot_date_chunk(
    p_snapshot_id UUID,
    p_observation_date DATE,
    p_ts_codes TEXT[],
    p_clear_sector BOOLEAN
)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    snapshot_status TEXT;
    snapshot_start DATE;
    snapshot_end DATE;
    deleted_count BIGINT := 0;
    current_count BIGINT;
BEGIN
    IF p_snapshot_id IS NULL OR p_observation_date IS NULL
       OR p_ts_codes IS NULL OR cardinality(p_ts_codes) = 0 THEN
        RAISE EXCEPTION 'Observation cleanup requires snapshot, date and codes';
    END IF;
    IF cardinality(p_ts_codes) > 10000 THEN
        RAISE EXCEPTION 'Observation cleanup code scope exceeds 10000';
    END IF;

    SELECT status, observation_date_start, observation_date_end
      INTO snapshot_status, snapshot_start, snapshot_end
      FROM observation.snapshot
     WHERE snapshot_id = p_snapshot_id;
    IF snapshot_status IS NULL THEN
        RAISE EXCEPTION 'Observation snapshot % does not exist', p_snapshot_id;
    END IF;
    IF snapshot_status IN ('FROZEN', 'SUPERSEDED', 'CORRUPTED') THEN
        RAISE EXCEPTION 'Observation snapshot % is %, cleanup is forbidden',
            p_snapshot_id, snapshot_status;
    END IF;
    IF p_observation_date < snapshot_start OR p_observation_date > snapshot_end THEN
        RAISE EXCEPTION 'Observation date % is outside snapshot % range %..%',
            p_observation_date, p_snapshot_id, snapshot_start, snapshot_end;
    END IF;

    DELETE FROM observation.stock_anomaly_tag t
     USING observation.stock_anomaly_daily d
     WHERE t.observation_id = d.observation_id
       AND d.snapshot_id = p_snapshot_id
       AND d.observation_date = p_observation_date
       AND d.ts_code = ANY (p_ts_codes);
    GET DIAGNOSTICS current_count = ROW_COUNT;
    deleted_count := deleted_count + current_count;

    DELETE FROM observation.stock_sector_confirmation c
     USING observation.stock_anomaly_daily d
     WHERE c.observation_id = d.observation_id
       AND d.snapshot_id = p_snapshot_id
       AND d.observation_date = p_observation_date
       AND d.ts_code = ANY (p_ts_codes);
    GET DIAGNOSTICS current_count = ROW_COUNT;
    deleted_count := deleted_count + current_count;

    DELETE FROM observation.stock_anomaly_daily
     WHERE snapshot_id = p_snapshot_id
       AND observation_date = p_observation_date
       AND ts_code = ANY (p_ts_codes);
    GET DIAGNOSTICS current_count = ROW_COUNT;
    deleted_count := deleted_count + current_count;

    IF p_clear_sector IS TRUE THEN
        DELETE FROM observation.sector_anomaly_daily
         WHERE snapshot_id = p_snapshot_id
           AND observation_date = p_observation_date;
        GET DIAGNOSTICS current_count = ROW_COUNT;
        deleted_count := deleted_count + current_count;
    END IF;

    RETURN deleted_count;
END;
$$;

CREATE OR REPLACE FUNCTION observation.clear_snapshot_date(
    p_snapshot_id UUID,
    p_observation_date DATE,
    p_ts_codes TEXT[]
)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    RETURN observation.clear_snapshot_date_chunk(
        p_snapshot_id, p_observation_date, p_ts_codes, TRUE
    );
END;
$$;

REVOKE ALL ON FUNCTION observation.clear_snapshot_date_chunk(UUID, DATE, TEXT[], BOOLEAN) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION observation.clear_snapshot_date_chunk(UUID, DATE, TEXT[], BOOLEAN) TO ashares_app;
REVOKE ALL ON FUNCTION observation.clear_snapshot_date(UUID, DATE, TEXT[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION observation.clear_snapshot_date(UUID, DATE, TEXT[]) TO ashares_app;

DROP TRIGGER IF EXISTS trg_observation_stock_frozen ON observation.stock_anomaly_daily;
CREATE TRIGGER trg_observation_stock_frozen
BEFORE INSERT OR UPDATE OR DELETE ON observation.stock_anomaly_daily
FOR EACH ROW EXECUTE FUNCTION observation.enforce_frozen_immutable();

DROP TRIGGER IF EXISTS trg_observation_stock_identity ON observation.stock_anomaly_daily;
CREATE TRIGGER trg_observation_stock_identity
BEFORE INSERT OR UPDATE ON observation.stock_anomaly_daily
FOR EACH ROW EXECUTE FUNCTION observation.enforce_observation_identity();

DROP TRIGGER IF EXISTS trg_observation_tag_frozen ON observation.stock_anomaly_tag;
CREATE TRIGGER trg_observation_tag_frozen
BEFORE INSERT OR UPDATE OR DELETE ON observation.stock_anomaly_tag
FOR EACH ROW EXECUTE FUNCTION observation.enforce_frozen_immutable();

DROP TRIGGER IF EXISTS trg_observation_sector_frozen ON observation.sector_anomaly_daily;
CREATE TRIGGER trg_observation_sector_frozen
BEFORE INSERT OR UPDATE OR DELETE ON observation.sector_anomaly_daily
FOR EACH ROW EXECUTE FUNCTION observation.enforce_frozen_immutable();

DROP TRIGGER IF EXISTS trg_observation_sector_scope ON observation.sector_anomaly_daily;
CREATE TRIGGER trg_observation_sector_scope
BEFORE INSERT OR UPDATE ON observation.sector_anomaly_daily
FOR EACH ROW EXECUTE FUNCTION observation.enforce_observation_date_scope();

DROP TRIGGER IF EXISTS trg_observation_confirmation_frozen ON observation.stock_sector_confirmation;
CREATE TRIGGER trg_observation_confirmation_frozen
BEFORE INSERT OR UPDATE OR DELETE ON observation.stock_sector_confirmation
FOR EACH ROW EXECUTE FUNCTION observation.enforce_frozen_immutable();

DROP TRIGGER IF EXISTS trg_observation_event_frozen ON observation.event_link;
CREATE TRIGGER trg_observation_event_frozen
BEFORE INSERT OR UPDATE OR DELETE ON observation.event_link
FOR EACH ROW EXECUTE FUNCTION observation.enforce_frozen_immutable();

COMMIT;
