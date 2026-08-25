-- Data Foundation architecture contract completion.
-- Additive only: this migration creates the target code/data contracts.  It
-- does not copy legacy research/backtest data; physical cutover is separate.

CREATE SCHEMA IF NOT EXISTS contract;
CREATE SCHEMA IF NOT EXISTS feature;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS feature.feature_registry (
    feature_name TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    business_name TEXT NOT NULL,
    definition TEXT NOT NULL,
    grain TEXT NOT NULL,
    source_datasets TEXT[] NOT NULL,
    dependency_features TEXT[] NOT NULL DEFAULT '{}',
    lookback_rule TEXT NOT NULL,
    asof_rule TEXT NOT NULL,
    adjustment_rule TEXT NOT NULL,
    impact_rule JSONB NOT NULL,
    quality_rule JSONB NOT NULL,
    code_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE'
        CHECK (status IN ('ACTIVE','DEPRECATED','RETIRED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (feature_name, feature_version)
);

CREATE TABLE IF NOT EXISTS ops.source_registry (
    source_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    api_name TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    business_key TEXT[] NOT NULL,
    frequency TEXT NOT NULL,
    supports_incremental BOOLEAN NOT NULL,
    supports_backfill BOOLEAN NOT NULL,
    available_time_rule TEXT NOT NULL,
    rate_limit JSONB NOT NULL DEFAULT '{}',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.change_event (
    event_id UUID PRIMARY KEY,
    dataset TEXT NOT NULL,
    business_key TEXT NOT NULL,
    ts_code TEXT,
    effective_date DATE,
    old_hash CHAR(64),
    new_hash CHAR(64) NOT NULL,
    source_run_id TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    change_type TEXT NOT NULL
        CHECK (change_type IN ('INSERT','CORRECTION','DELETE')),
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','RESOLVED','APPLIED','FAILED')),
    UNIQUE (dataset, business_key, new_hash)
);
CREATE INDEX IF NOT EXISTS idx_change_event_pending
    ON ops.change_event (status, dataset, effective_date, ts_code);

CREATE OR REPLACE FUNCTION ops.reject_update_delete()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; % is forbidden', TG_TABLE_NAME, TG_OP;
END $$;

DROP TRIGGER IF EXISTS trg_feature_registry_append_only ON feature.feature_registry;
CREATE TRIGGER trg_feature_registry_append_only
BEFORE UPDATE OR DELETE ON feature.feature_registry
FOR EACH ROW EXECUTE FUNCTION ops.reject_update_delete();

CREATE OR REPLACE FUNCTION ops.guard_change_event_update()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'change_event is auditable and cannot be deleted';
    END IF;
    IF (to_jsonb(NEW) - 'status') IS DISTINCT FROM (to_jsonb(OLD) - 'status') THEN
        RAISE EXCEPTION 'only change_event.status may be updated';
    END IF;
    IF NEW.status NOT IN ('RESOLVED','APPLIED','FAILED') OR OLD.status <> 'PENDING' THEN
        RAISE EXCEPTION 'invalid change_event status transition: % -> %', OLD.status, NEW.status;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_change_event_guard ON ops.change_event;
CREATE TRIGGER trg_change_event_guard
BEFORE UPDATE OR DELETE ON ops.change_event
FOR EACH ROW EXECUTE FUNCTION ops.guard_change_event_update();

DO $$
DECLARE relation_name TEXT;
DECLARE trigger_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'public.tushare_financial_raw_version',
        'public.fina_indicator_raw_version',
        'public.tushare_market_raw_version'
    ] LOOP
        IF to_regclass(relation_name) IS NOT NULL THEN
            trigger_name := 'trg_' || replace(split_part(relation_name, '.', 2), '.', '_') || '_append_only';
            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', trigger_name, relation_name);
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %s '
                'FOR EACH ROW EXECUTE FUNCTION ops.reject_update_delete()',
                trigger_name, relation_name
            );
        END IF;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS ops.run_manifest (
    run_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    code_commit TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    scope JSONB NOT NULL,
    input_manifest_hash CHAR(64),
    output_manifest_hash CHAR(64),
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    rows_read BIGINT NOT NULL DEFAULT 0 CHECK (rows_read >= 0),
    rows_written BIGINT NOT NULL DEFAULT 0 CHECK (rows_written >= 0),
    rows_rejected BIGINT NOT NULL DEFAULT 0 CHECK (rows_rejected >= 0),
    status TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
    error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (status = 'RUNNING' OR end_time IS NOT NULL),
    CHECK (status <> 'FAILED' OR error IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS ops.data_quality_result (
    quality_result_id UUID PRIMARY KEY,
    run_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    target TEXT NOT NULL,
    scope JSONB NOT NULL,
    checks JSONB NOT NULL,
    block_count INTEGER NOT NULL CHECK (block_count >= 0),
    report_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_quality_result_run
    ON ops.data_quality_result (run_id, target, created_at);

DROP TRIGGER IF EXISTS trg_quality_result_append_only ON ops.data_quality_result;
CREATE TRIGGER trg_quality_result_append_only
BEFORE UPDATE OR DELETE ON ops.data_quality_result
FOR EACH ROW EXECUTE FUNCTION ops.reject_update_delete();

CREATE OR REPLACE FUNCTION ops.touch_trading_day_build()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := CURRENT_TIMESTAMP;
    IF NEW.final_status = 'READY' AND (
        NEW.completed_at IS NULL
        OR NEW.raw_status <> 'READY'
        OR NEW.standard_status <> 'READY'
        OR NEW.pit_status <> 'READY'
        OR NEW.feature_status <> 'READY'
        OR NEW.quality_status <> 'READY'
        OR NEW.snapshot_status <> 'READY'
    ) THEN
        RAISE EXCEPTION 'READY requires every foundation stage READY and completed_at';
    END IF;
    RETURN NEW;
END $$;

CREATE TABLE IF NOT EXISTS contract.snapshot_manifest (
    snapshot_id UUID PRIMARY KEY,
    research_date DATE NOT NULL,
    data_baseline TEXT NOT NULL,
    contract_version TEXT NOT NULL REFERENCES contract.contract_version(contract_version),
    feature_version TEXT NOT NULL,
    pit_version TEXT NOT NULL,
    code_version TEXT NOT NULL,
    input_manifest_hash CHAR(64),
    quality_report_hash CHAR(64),
    manifest_hash CHAR(64),
    status TEXT NOT NULL DEFAULT 'DRAFT'
        CHECK (status IN ('DRAFT','BUILDING','VALIDATED','FROZEN','FAILED')),
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    validated_at TIMESTAMPTZ,
    frozen_at TIMESTAMPTZ,
    CHECK (status <> 'FROZEN' OR (
        input_manifest_hash IS NOT NULL AND quality_report_hash IS NOT NULL
        AND manifest_hash IS NOT NULL AND frozen_at IS NOT NULL
    ))
);

CREATE TABLE IF NOT EXISTS contract.stock_snapshot_daily (
    snapshot_id UUID NOT NULL REFERENCES contract.snapshot_manifest(snapshot_id),
    research_date DATE NOT NULL,
    ts_code TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    market TEXT,
    is_listed_asof BOOLEAN,
    is_st_asof BOOLEAN,
    is_suspended_asof BOOLEAN,
    is_tradeable BOOLEAN,
    can_buy BOOLEAN,
    can_sell BOOLEAN,
    limit_status TEXT,
    raw_open DOUBLE PRECISION,
    raw_high DOUBLE PRECISION,
    raw_low DOUBLE PRECISION,
    raw_close DOUBLE PRECISION,
    raw_volume DOUBLE PRECISION,
    raw_amount DOUBLE PRECISION,
    asof_close DOUBLE PRECISION,
    return_1d DOUBLE PRECISION,
    return_20d DOUBLE PRECISION,
    return_60d DOUBLE PRECISION,
    ma20 DOUBLE PRECISION,
    ma60 DOUBLE PRECISION,
    ma120 DOUBLE PRECISION,
    volume_ratio_20d DOUBLE PRECISION,
    volatility_20d DOUBLE PRECISION,
    pe_ttm DOUBLE PRECISION,
    pb DOUBLE PRECISION,
    ps_ttm DOUBLE PRECISION,
    dv_ratio DOUBLE PRECISION,
    total_mv DOUBLE PRECISION,
    circ_mv DOUBLE PRECISION,
    pe_pct_3y DOUBLE PRECISION,
    pe_pct_5y DOUBLE PRECISION,
    pb_pct_3y DOUBLE PRECISION,
    pb_pct_5y DOUBLE PRECISION,
    report_date DATE,
    effective_announce_date DATE,
    financial_version_available_date DATE,
    financial_record_version BIGINT,
    roe DOUBLE PRECISION,
    roa DOUBLE PRECISION,
    debt_to_assets DOUBLE PRECISION,
    revenue_yoy DOUBLE PRECISION,
    netprofit_yoy DOUBLE PRECISION,
    operating_cashflow DOUBLE PRECISION,
    net_profit DOUBLE PRECISION,
    cash_div_12m DOUBLE PRECISION,
    quality_status TEXT NOT NULL,
    row_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (snapshot_id, research_date, ts_code)
) PARTITION BY RANGE (research_date);

CREATE TABLE IF NOT EXISTS contract.dataset_release (
    dataset_id UUID PRIMARY KEY,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    trading_dates DATE[] NOT NULL,
    snapshot_ids UUID[] NOT NULL,
    feature_version TEXT NOT NULL,
    pit_version TEXT NOT NULL,
    data_baseline TEXT NOT NULL,
    contract_version TEXT NOT NULL REFERENCES contract.contract_version(contract_version),
    manifest_hash CHAR(64),
    quality_report_hash CHAR(64),
    status TEXT NOT NULL DEFAULT 'DRAFT'
        CHECK (status IN ('DRAFT','BUILDING','VALIDATED','FROZEN','FAILED')),
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    validated_at TIMESTAMPTZ,
    frozen_at TIMESTAMPTZ,
    CHECK (start_date <= end_date),
    CHECK (status <> 'FROZEN' OR (
        manifest_hash IS NOT NULL AND quality_report_hash IS NOT NULL
        AND frozen_at IS NOT NULL
    ))
);

CREATE OR REPLACE FUNCTION contract.guard_frozen_product()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'contract products are append-only; DELETE is forbidden';
    END IF;
    IF OLD.status = 'FROZEN' THEN
        RAISE EXCEPTION 'FROZEN contract product is immutable';
    END IF;
    IF NEW.status NOT IN (
        CASE OLD.status
            WHEN 'DRAFT' THEN 'BUILDING'
            WHEN 'BUILDING' THEN 'VALIDATED'
            WHEN 'VALIDATED' THEN 'FROZEN'
            WHEN 'FAILED' THEN 'BUILDING'
            ELSE OLD.status
        END,
        'FAILED'
    ) THEN
        RAISE EXCEPTION 'invalid contract product state transition: % -> %', OLD.status, NEW.status;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_contract_snapshot_frozen ON contract.snapshot_manifest;
CREATE TRIGGER trg_contract_snapshot_frozen
BEFORE UPDATE OR DELETE ON contract.snapshot_manifest
FOR EACH ROW EXECUTE FUNCTION contract.guard_frozen_product();

DROP TRIGGER IF EXISTS trg_contract_dataset_frozen ON contract.dataset_release;
CREATE TRIGGER trg_contract_dataset_frozen
BEFORE UPDATE OR DELETE ON contract.dataset_release
FOR EACH ROW EXECUTE FUNCTION contract.guard_frozen_product();

CREATE OR REPLACE FUNCTION contract.guard_snapshot_rows()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE product_status TEXT;
BEGIN
    SELECT status INTO product_status FROM contract.snapshot_manifest
     WHERE snapshot_id = COALESCE(NEW.snapshot_id, OLD.snapshot_id);
    IF product_status = 'FROZEN' THEN
        RAISE EXCEPTION 'rows of a FROZEN snapshot are immutable';
    END IF;
    RETURN COALESCE(NEW, OLD);
END $$;

DROP TRIGGER IF EXISTS trg_contract_snapshot_rows ON contract.stock_snapshot_daily;
CREATE TRIGGER trg_contract_snapshot_rows
BEFORE INSERT OR UPDATE OR DELETE ON contract.stock_snapshot_daily
FOR EACH ROW EXECUTE FUNCTION contract.guard_snapshot_rows();

-- Role capabilities stay split even when deployment uses one LOGIN account.
-- The administrative superuser is deliberately not part of runtime grants.
GRANT SELECT, INSERT ON feature.feature_registry TO feature_writer;
REVOKE UPDATE, DELETE, TRUNCATE ON feature.feature_registry FROM feature_writer;
GRANT SELECT, INSERT, UPDATE ON ops.source_registry TO raw_writer;
GRANT SELECT, INSERT, UPDATE ON ops.change_event TO raw_writer, standard_writer,
    pit_writer, feature_writer;
GRANT SELECT, INSERT, UPDATE ON ops.run_manifest TO raw_writer, standard_writer,
    pit_writer, feature_writer, contract_publisher;
GRANT SELECT, INSERT ON ops.data_quality_result TO raw_writer, standard_writer,
    pit_writer, feature_writer, contract_publisher;
REVOKE UPDATE, DELETE, TRUNCATE ON ops.data_quality_result FROM raw_writer,
    standard_writer, pit_writer, feature_writer, contract_publisher;

GRANT SELECT, INSERT, UPDATE ON contract.snapshot_manifest,
    contract.dataset_release TO contract_publisher;
GRANT SELECT, INSERT ON contract.stock_snapshot_daily TO contract_publisher;
REVOKE DELETE, TRUNCATE ON contract.snapshot_manifest,
    contract.dataset_release, contract.stock_snapshot_daily FROM contract_publisher;

REVOKE ALL ON ALL TABLES IN SCHEMA research FROM research_reader;
GRANT USAGE ON SCHEMA contract, mart TO research_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA contract, mart TO research_reader;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA contract, mart
    FROM research_reader;
