-- Data Foundation architecture split: Stage 1 boundary and governance objects.
--
-- This migration is additive and idempotent.  It deliberately does not move
-- the existing public/research fact tables: that physical move needs a
-- dual-read/dual-write cutover and is a separate, reversible migration.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS contract;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS backtest;

-- A contract version is append-only.  A correction creates a new version;
-- consumers never observe an in-place rewrite of a published contract.
CREATE TABLE IF NOT EXISTS contract.contract_version (
    contract_version VARCHAR(50) PRIMARY KEY,
    status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
    release_notes TEXT NOT NULL DEFAULT '',
    manifest_hash CHAR(64),
    released_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (status IN ('DRAFT', 'ACTIVE', 'RETIRED')),
    CHECK (manifest_hash IS NULL OR manifest_hash ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS contract.data_dictionary (
    contract_version VARCHAR(50) NOT NULL
        REFERENCES contract.contract_version(contract_version),
    schema_name VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    column_name VARCHAR(63) NOT NULL,
    business_name VARCHAR(240) NOT NULL,
    definition TEXT NOT NULL,
    source TEXT NOT NULL,
    quality_rule TEXT NOT NULL DEFAULT '',
    data_type VARCHAR(120),
    is_nullable BOOLEAN NOT NULL DEFAULT TRUE,
    is_key BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (contract_version, schema_name, table_name, column_name)
);

CREATE INDEX IF NOT EXISTS idx_contract_dictionary_table
    ON contract.data_dictionary (schema_name, table_name, contract_version);

CREATE OR REPLACE FUNCTION contract.reject_immutable_metadata_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; publish a new version instead',
        TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
END;
$$;

DROP TRIGGER IF EXISTS trg_contract_version_append_only
    ON contract.contract_version;
CREATE TRIGGER trg_contract_version_append_only
BEFORE UPDATE OR DELETE ON contract.contract_version
FOR EACH ROW EXECUTE FUNCTION contract.reject_immutable_metadata_change();

DROP TRIGGER IF EXISTS trg_contract_dictionary_append_only
    ON contract.data_dictionary;
CREATE TRIGGER trg_contract_dictionary_append_only
BEFORE UPDATE OR DELETE ON contract.data_dictionary
FOR EACH ROW EXECUTE FUNCTION contract.reject_immutable_metadata_change();

-- Application-owned strategy parameters.  The foundation never interprets
-- the JSON; it only stores the immutable, versioned application input.
CREATE TABLE IF NOT EXISTS backtest.app_strategy_version (
    strategy_id VARCHAR(80) NOT NULL,
    strategy_version VARCHAR(40) NOT NULL,
    strategy_name VARCHAR(120) NOT NULL,
    parameters JSONB NOT NULL,
    parameters_hash CHAR(64) NOT NULL,
    approval_status VARCHAR(16) NOT NULL DEFAULT 'DRAFT',
    approved_by VARCHAR(120),
    approved_at TIMESTAMPTZ,
    evidence_hash CHAR(64),
    code_version VARCHAR(180),
    effective_from DATE,
    effective_to DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (strategy_id, strategy_version),
    UNIQUE (strategy_name, strategy_version),
    CHECK (approval_status IN ('DRAFT', 'MIGRATED', 'APPROVED', 'RETIRED')),
    CHECK (effective_to IS NULL OR effective_from IS NULL OR effective_to >= effective_from),
    CHECK (parameters_hash ~ '^[0-9a-f]{64}$'),
    CHECK (evidence_hash IS NULL OR evidence_hash ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS backtest.app_execution_params (
    execution_params_version VARCHAR(100) NOT NULL,
    fee_rule_version VARCHAR(80) NOT NULL,
    market VARCHAR(30) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    commission_rate DOUBLE PRECISION NOT NULL CHECK (commission_rate >= 0),
    min_commission DOUBLE PRECISION NOT NULL CHECK (min_commission >= 0),
    slippage_bps DOUBLE PRECISION NOT NULL CHECK (slippage_bps >= 0),
    gap_stress_pct DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (gap_stress_pct >= 0 AND gap_stress_pct <= 1),
    max_volume_participation_pct DOUBLE PRECISION NOT NULL DEFAULT 1 CHECK (max_volume_participation_pct > 0 AND max_volume_participation_pct <= 1),
    liquidity_policy VARCHAR(80) NOT NULL DEFAULT 'reject_full_order',
    execution_model VARCHAR(80) NOT NULL DEFAULT 'bar_open_close',
    parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    approval_status VARCHAR(16) NOT NULL DEFAULT 'DRAFT',
    approved_by VARCHAR(120),
    approved_at TIMESTAMPTZ,
    evidence_hash CHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (execution_params_version, market, effective_from),
    CHECK (effective_to IS NULL OR effective_to >= effective_from),
    CHECK (approval_status IN ('DRAFT', 'MIGRATED', 'APPROVED', 'RETIRED')),
    CHECK (evidence_hash IS NULL OR evidence_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_app_execution_params_lookup
    ON backtest.app_execution_params (market, effective_from, effective_to);

DROP TRIGGER IF EXISTS trg_app_strategy_version_append_only
    ON backtest.app_strategy_version;
CREATE TRIGGER trg_app_strategy_version_append_only
BEFORE UPDATE OR DELETE ON backtest.app_strategy_version
FOR EACH ROW EXECUTE FUNCTION contract.reject_immutable_metadata_change();

DROP TRIGGER IF EXISTS trg_app_execution_params_append_only
    ON backtest.app_execution_params;
CREATE TRIGGER trg_app_execution_params_append_only
BEFORE UPDATE OR DELETE ON backtest.app_execution_params
FOR EACH ROW EXECUTE FUNCTION contract.reject_immutable_metadata_change();

-- Dataset Ready state machine.  A READY row is the only row downstream jobs
-- may use as a publication prerequisite; partial/failed rows remain evidence.
CREATE TABLE IF NOT EXISTS ops.trading_day_build (
    trade_date DATE NOT NULL,
    pipeline_version VARCHAR(80) NOT NULL,
    run_id VARCHAR(160) NOT NULL,
    raw_status VARCHAR(16) NOT NULL DEFAULT 'BUILDING',
    standard_status VARCHAR(16) NOT NULL DEFAULT 'BUILDING',
    pit_status VARCHAR(16) NOT NULL DEFAULT 'BUILDING',
    feature_status VARCHAR(16) NOT NULL DEFAULT 'BUILDING',
    quality_status VARCHAR(16) NOT NULL DEFAULT 'BUILDING',
    snapshot_status VARCHAR(16) NOT NULL DEFAULT 'BUILDING',
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    error_code VARCHAR(80),
    error_message TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    final_status VARCHAR(16) NOT NULL DEFAULT 'BUILDING',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, pipeline_version),
    CHECK (raw_status IN ('READY', 'PARTIAL', 'FAILED', 'STALE', 'BUILDING')),
    CHECK (standard_status IN ('READY', 'PARTIAL', 'FAILED', 'STALE', 'BUILDING')),
    CHECK (pit_status IN ('READY', 'PARTIAL', 'FAILED', 'STALE', 'BUILDING')),
    CHECK (feature_status IN ('READY', 'PARTIAL', 'FAILED', 'STALE', 'BUILDING')),
    CHECK (quality_status IN ('READY', 'PARTIAL', 'FAILED', 'STALE', 'BUILDING')),
    CHECK (snapshot_status IN ('READY', 'PARTIAL', 'FAILED', 'STALE', 'BUILDING')),
    CHECK (final_status IN ('READY', 'PARTIAL', 'FAILED', 'STALE', 'BUILDING')),
    CHECK ((final_status = 'READY' AND completed_at IS NOT NULL) OR final_status <> 'READY')
);

CREATE INDEX IF NOT EXISTS idx_trading_day_build_status
    ON ops.trading_day_build (trade_date, final_status, updated_at DESC);

CREATE OR REPLACE FUNCTION ops.touch_trading_day_build()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := CURRENT_TIMESTAMP;
    IF NEW.final_status = 'READY' AND NEW.completed_at IS NULL THEN
        RAISE EXCEPTION 'READY trading day build requires completed_at';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_trading_day_build_guard
    ON ops.trading_day_build;
CREATE TRIGGER trg_trading_day_build_guard
BEFORE INSERT OR UPDATE ON ops.trading_day_build
FOR EACH ROW EXECUTE FUNCTION ops.touch_trading_day_build();

-- Seed the first contract identity.  It is metadata only and does not alter
-- any existing fact or snapshot rows.
INSERT INTO contract.contract_version (contract_version, status, release_notes)
VALUES ('foundation-contract-v1.0.0', 'ACTIVE', 'Stage 1 governance baseline')
ON CONFLICT (contract_version) DO NOTHING;

-- Migrate existing immutable strategy definitions into the new application
-- boundary when the legacy table is present.  No legacy row is modified.
DO $$
BEGIN
    IF to_regclass('backtest.strategy_definition') IS NOT NULL THEN
        INSERT INTO backtest.app_strategy_version (
            strategy_id, strategy_version, strategy_name, parameters,
            parameters_hash, approval_status, approved_by, evidence_hash,
            code_version
        )
        SELECT strategy_id, strategy_version, strategy_name, parameters,
               parameters_hash, 'MIGRATED', 'migration:backtest.strategy_definition',
               parameters_hash, code_version
          FROM backtest.strategy_definition
        ON CONFLICT (strategy_id, strategy_version) DO NOTHING;
    END IF;
END
$$;

-- Split execution assumptions out of the mixed legacy market-rule table.
-- Tax and transfer-rate facts are intentionally not copied into this table;
-- they remain market/legal facts and must later move to foundation rule tables.
DO $$
BEGIN
    IF to_regclass('backtest.market_rule_effective') IS NOT NULL THEN
        INSERT INTO backtest.app_execution_params (
            execution_params_version, fee_rule_version, market,
            effective_from, effective_to, commission_rate, min_commission,
            slippage_bps, gap_stress_pct, max_volume_participation_pct,
            liquidity_policy, parameters, approval_status, approved_by,
            approved_at, evidence_hash
        )
        SELECT fee_rule_version || ':' || effective_from::text,
               fee_rule_version, market, effective_from, effective_to,
               commission_rate, min_commission, slippage_bps,
               COALESCE((parameters ->> 'gap_stress_pct')::double precision, 0),
               COALESCE((parameters ->> 'max_volume_participation_pct')::double precision, 1),
               COALESCE(parameters ->> 'liquidity_policy', 'reject_full_order'),
               parameters, COALESCE(approval_status, 'MIGRATED'), approved_by,
               approved_at, approval_hash
          FROM backtest.market_rule_effective
        ON CONFLICT (execution_params_version, market, effective_from) DO NOTHING;
    END IF;
END
$$;

-- Initial core dictionary.  Further feature columns are added as part of the
-- contract publication workflow; this seed is intentionally explicit so no
-- column receives a fabricated business definition from introspection.
INSERT INTO contract.data_dictionary (
    contract_version, schema_name, table_name, column_name,
    business_name, definition, source, quality_rule, data_type, is_nullable, is_key
)
VALUES
    ('foundation-contract-v1.0.0', 'std', 'daily_kline', 'stock_code',
     '证券代码', '标准化证券代码，唯一标识一只证券', 'Tushare daily / standard mapping', '非空；代码格式已标准化', 'varchar', FALSE, TRUE),
    ('foundation-contract-v1.0.0', 'std', 'daily_kline', 'trade_date',
     '交易日期', '该行情记录对应的交易日', 'Tushare daily', '非空；必须为交易日', 'date', FALSE, TRUE),
    ('foundation-contract-v1.0.0', 'feature', 'price_daily_v2', 'ts_code',
     '证券代码', '特征记录对应的标准化证券代码', 'std.daily_kline', '非空；可关联证券主数据', 'varchar', FALSE, TRUE),
    ('foundation-contract-v1.0.0', 'feature', 'price_daily_v2', 'trade_date',
     '交易日期', '特征计算的截面交易日', 'std.daily_kline', '非空；不得晚于数据可见日', 'date', FALSE, TRUE),
    ('foundation-contract-v1.0.0', 'feature', 'price_daily_v2', 'ma20',
     '20日均线', '最近20个交易日复权收盘价均值', '派生计算：feature pipeline', '有效观察不足时为空；不得使用未来数据', 'double precision', TRUE, FALSE),
    ('foundation-contract-v1.0.0', 'feature', 'price_daily_v2', 'ma60',
     '60日均线', '最近60个交易日复权收盘价均值', '派生计算：feature pipeline', '有效观察不足时为空；不得使用未来数据', 'double precision', TRUE, FALSE),
    ('foundation-contract-v1.0.0', 'ops', 'trading_day_build', 'final_status',
     '交易日最终构建状态', 'Raw、Standard、PIT、Feature、Quality、Snapshot 阶段汇总状态', '数据基座流水线', '仅允许 READY/PARTIAL/FAILED/STALE/BUILDING', 'varchar', FALSE, FALSE),
    ('foundation-contract-v1.0.0', 'contract', 'data_dictionary', 'business_name',
     '字段业务名称', '对外契约字段的中文业务含义', '契约发布流程', '非空；不得使用无意义占位文本', 'varchar', FALSE, FALSE)
ON CONFLICT (contract_version, schema_name, table_name, column_name) DO NOTHING;

-- Database-side grants for the current single application login.  The
-- administrative ashares role remains the owner/superuser and is not used by
-- jobs; the application receives only the new boundary privileges.
GRANT USAGE ON SCHEMA contract, ops, backtest, portfolio TO ashares_app;
GRANT SELECT ON contract.contract_version, contract.data_dictionary TO ashares_app;
GRANT SELECT, INSERT, UPDATE ON ops.trading_day_build TO ashares_app;
GRANT SELECT, INSERT ON backtest.app_strategy_version, backtest.app_execution_params TO ashares_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA backtest, portfolio TO ashares_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA contract, ops, backtest, portfolio TO ashares_app;
REVOKE UPDATE, DELETE, TRUNCATE ON contract.contract_version, contract.data_dictionary FROM ashares_app;
REVOKE UPDATE, DELETE, TRUNCATE ON backtest.app_strategy_version, backtest.app_execution_params FROM ashares_app;
