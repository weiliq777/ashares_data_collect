-- Market Anomaly Observation Domain（市场异常观察域）正式模型。
--
-- 该迁移只创建/扩展结构，不启动历史回填。所有业务写入仍必须经过
-- Repository（仓储） -> DataAccessKernel（数据访问内核）。UUID 业务身份由
-- Repository 确定性生成；因此本模型不依赖一条可被重复消费的全局序列。

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS observation;
CREATE SCHEMA IF NOT EXISTS evaluation;

-- Standard（标准层）自由流通换手率/股本：Tushare daily_basic 的 free_share
-- 原单位为“万股”，标准层统一保存为“股”；turnover_rate_f 保持供应商百分比口径。
ALTER TABLE std.valuation_daily
    ADD COLUMN IF NOT EXISTS turnover_rate_f DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS free_share DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS free_share_unit VARCHAR(20) NOT NULL DEFAULT 'shares',
    ADD COLUMN IF NOT EXISTS quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    ADD COLUMN IF NOT EXISTS source_asof_date DATE;

ALTER TABLE std.valuation_daily
    DROP CONSTRAINT IF EXISTS valuation_daily_free_share_nonnegative,
    ADD CONSTRAINT valuation_daily_free_share_nonnegative
        CHECK (free_share IS NULL OR free_share >= 0) NOT VALID,
    DROP CONSTRAINT IF EXISTS valuation_daily_turnover_rate_f_nonnegative,
    ADD CONSTRAINT valuation_daily_turnover_rate_f_nonnegative
        CHECK (turnover_rate_f IS NULL OR turnover_rate_f >= 0) NOT VALID;

-- std.valuation_daily 已有分区父表索引和 (stock_code, trade_date, source) 唯一索引；
-- 不再额外创建同前缀索引，避免对历史分区触发一次不必要的全量索引构建。

-- Feature（特征层）价格双坐标。asof_close 继续保留为历史兼容字段；新契约消费
-- 明确使用 adj_close_base（只处理机械公司行为）和 total_return_price（含现金分红）。
ALTER TABLE feature.price_adjustment_factor_asof
    ADD COLUMN IF NOT EXISTS adj_adjustment_factor DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS adj_close_base DOUBLE PRECISION;

ALTER TABLE feature.price_daily_v2
    ADD COLUMN IF NOT EXISTS adj_close_base DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS total_return_price DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS price_coordinate_contract_version VARCHAR(40);

-- feature.price_daily_v2 已有 (ts_code, trade_date) 主键索引；不重复创建。

-- Optional PIT（时点）行业输入。当前没有可靠的五年历史时点行业来源时，表可以为空；
-- 个股 Observation（观察）仍可运行并把行业状态记录为 UNKNOWN（未知）。
CREATE TABLE IF NOT EXISTS pit.industry_membership_history (
    membership_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    industry_system VARCHAR(30) NOT NULL,
    industry_level SMALLINT NOT NULL,
    industry_code VARCHAR(40) NOT NULL,
    industry_name VARCHAR(160),
    effective_from DATE NOT NULL,
    effective_to DATE,
    data_available_timestamp TIMESTAMPTZ,
    source_asof_date DATE,
    source VARCHAR(30) NOT NULL,
    source_record_key VARCHAR(180) NOT NULL,
    record_version BIGINT NOT NULL DEFAULT 0,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT industry_membership_history_dates
        CHECK (effective_to IS NULL OR effective_to >= effective_from),
    CONSTRAINT industry_membership_history_source_key
        UNIQUE (source, source_record_key, record_version)
);

-- BIGSERIAL（大整数序列）通常会自动创建该序列；显式固化 ownership（归属）和默认值，
-- 使部分建库、恢复或历史半成品场景也不会留下“表存在但序列缺失”的状态。
CREATE SEQUENCE IF NOT EXISTS pit.industry_membership_history_membership_id_seq;
ALTER SEQUENCE pit.industry_membership_history_membership_id_seq
    OWNED BY pit.industry_membership_history.membership_id;
ALTER TABLE pit.industry_membership_history
    ALTER COLUMN membership_id SET DEFAULT nextval('pit.industry_membership_history_membership_id_seq'::regclass);

CREATE INDEX IF NOT EXISTS idx_pit_industry_membership_history_lookup
    ON pit.industry_membership_history (ts_code, industry_system, industry_level, effective_from, effective_to);

CREATE TABLE IF NOT EXISTS pit.industry_membership_daily (
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    industry_system VARCHAR(30) NOT NULL,
    industry_level SMALLINT NOT NULL,
    industry_code VARCHAR(40) NOT NULL,
    industry_name VARCHAR(160),
    source_asof_date DATE,
    data_available_timestamp TIMESTAMPTZ,
    source_record_key VARCHAR(180),
    record_version BIGINT,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, ts_code, industry_system, industry_level)
);

CREATE INDEX IF NOT EXISTS idx_pit_industry_membership_daily_code_date
    ON pit.industry_membership_daily (ts_code, trade_date);
CREATE INDEX IF NOT EXISTS idx_pit_industry_membership_daily_sector_date
    ON pit.industry_membership_daily (trade_date, industry_system, industry_level, industry_code);

CREATE TABLE IF NOT EXISTS observation.snapshot (
    snapshot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    observation_date_start DATE NOT NULL,
    observation_date_end DATE NOT NULL,
    methodology_hash CHAR(64) NOT NULL,
    data_baseline_id VARCHAR(100) NOT NULL,
    input_manifest_hash CHAR(64),
    observation_policy_id VARCHAR(120) NOT NULL,
    contract_hash CHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
    quality_report_hash CHAR(64),
    quality_report JSONB,
    code_identity JSONB NOT NULL DEFAULT '{}'::jsonb,
    row_count BIGINT NOT NULL DEFAULT 0,
    stock_count BIGINT NOT NULL DEFAULT 0,
    sector_count BIGINT NOT NULL DEFAULT 0,
    date_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    built_at TIMESTAMPTZ,
    validated_at TIMESTAMPTZ,
    frozen_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    failure_reason TEXT,
    CONSTRAINT observation_snapshot_dates CHECK (observation_date_end >= observation_date_start),
    CONSTRAINT observation_snapshot_status CHECK
        (status IN ('DRAFT','BUILDING','VALIDATED','FROZEN','FAILED','SUPERSEDED','CORRUPTED')),
    CONSTRAINT observation_snapshot_methodology_hash CHECK (length(btrim(methodology_hash::text)) = 64),
    CONSTRAINT observation_snapshot_contract_hash CHECK (length(btrim(contract_hash::text)) = 64),
    CONSTRAINT observation_snapshot_baseline_fk
        FOREIGN KEY (data_baseline_id) REFERENCES ops.tushare_data_release(release_tag)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_observation_snapshot_identity
    ON observation.snapshot (observation_date_start, observation_date_end, methodology_hash, data_baseline_id);
CREATE INDEX IF NOT EXISTS idx_observation_snapshot_status_date
    ON observation.snapshot (status, observation_date_start, observation_date_end);

CREATE TABLE IF NOT EXISTS observation.stock_anomaly_daily (
    observation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id UUID NOT NULL REFERENCES observation.snapshot(snapshot_id),
    observation_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    methodology_hash CHAR(64) NOT NULL,
    data_baseline_id VARCHAR(100) NOT NULL,
    amount_ratio_5_60 DOUBLE PRECISION,
    amount_ratio_20_60 DOUBLE PRECISION,
    turnover_ratio_5_60 DOUBLE PRECISION,
    turnover_ratio_20_60 DOUBLE PRECISION,
    amount_zscore_60 DOUBLE PRECISION,
    amount_single_ratio_60 DOUBLE PRECISION,
    abnormal_days_20 INTEGER,
    return_1d DOUBLE PRECISION,
    return_5d DOUBLE PRECISION,
    return_20d DOUBLE PRECISION,
    price_position_52w DOUBLE PRECISION,
    distance_ma20 DOUBLE PRECISION,
    distance_ma60 DOUBLE PRECISION,
    max_drawdown_20d DOUBLE PRECISION,
    up_volume_ratio_20 DOUBLE PRECISION,
    down_volume_ratio_20 DOUBLE PRECISION,
    up_down_volume_ratio DOUBLE PRECISION,
    price_response_efficiency_20 DOUBLE PRECISION,
    anomaly_flag BOOLEAN,
    anomaly_streak INTEGER,
    anomaly_frequency_20 DOUBLE PRECISION,
    days_since_first_anomaly INTEGER,
    relative_return_20d DOUBLE PRECISION,
    relative_volume_zscore DOUBLE PRECISION,
    trading_anomaly_score DOUBLE PRECISION,
    price_structure_score DOUBLE PRECISION,
    persistence_score DOUBLE PRECISION,
    sector_confirmation_score DOUBLE PRECISION,
    risk_score DOUBLE PRECISION,
    observation_priority DOUBLE PRECISION,
    primary_state VARCHAR(40),
    state_reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    sector_data_status VARCHAR(20) NOT NULL DEFAULT 'MISSING',
    sector_confirmation_status VARCHAR(20) NOT NULL DEFAULT 'UNKNOWN',
    calculation_status VARCHAR(30) NOT NULL DEFAULT 'PARTIAL',
    quality_status VARCHAR(30) NOT NULL DEFAULT 'WARNING',
    missing_reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    input_row_count INTEGER NOT NULL DEFAULT 0,
    input_manifest_hash CHAR(64) NOT NULL,
    observation_hash CHAR(64) NOT NULL,
    calculation_run_id VARCHAR(160) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT stock_anomaly_status CHECK
        (calculation_status IN ('VALID','PARTIAL','INSUFFICIENT_HISTORY','INVALID')),
    CONSTRAINT stock_anomaly_sector_data_status CHECK
        (sector_data_status IN ('CONFIRMED','DAILY_CAPTURED','MISSING','INVALID')),
    CONSTRAINT stock_anomaly_sector_confirmation_status CHECK
        (sector_confirmation_status IN ('CONFIRMED','NOT_CONFIRMED','UNKNOWN')),
    CONSTRAINT stock_anomaly_hashes CHECK
        (length(btrim(input_manifest_hash::text)) = 64 AND length(btrim(observation_hash::text)) = 64),
    CONSTRAINT stock_anomaly_business_identity
        UNIQUE (observation_date, ts_code, methodology_hash, data_baseline_id)
);

CREATE INDEX IF NOT EXISTS idx_observation_stock_date_code
    ON observation.stock_anomaly_daily (observation_date, ts_code);
CREATE INDEX IF NOT EXISTS idx_observation_stock_snapshot
    ON observation.stock_anomaly_daily (snapshot_id, observation_date, ts_code);
CREATE INDEX IF NOT EXISTS idx_observation_stock_state
    ON observation.stock_anomaly_daily (observation_date, primary_state);

CREATE TABLE IF NOT EXISTS observation.stock_anomaly_tag (
    tag_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    observation_id UUID NOT NULL REFERENCES observation.stock_anomaly_daily(observation_id),
    tag_code VARCHAR(40) NOT NULL,
    tag_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT stock_anomaly_tag_code CHECK
        (tag_code IN ('VOLUME_EXPANSION','TURNOVER_EXPANSION','LOW_POSITION','SECTOR_CONFIRMATION',
                      'PRICE_DIVERGENCE','PERSISTENT_ANOMALY','MARKET_RELATIVE')),
    CONSTRAINT stock_anomaly_tag_identity UNIQUE (observation_id, tag_code)
);
CREATE INDEX IF NOT EXISTS idx_observation_stock_tag_observation
    ON observation.stock_anomaly_tag (observation_id, tag_code);

CREATE TABLE IF NOT EXISTS observation.sector_anomaly_daily (
    sector_observation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id UUID NOT NULL REFERENCES observation.snapshot(snapshot_id),
    observation_date DATE NOT NULL,
    industry_system VARCHAR(30) NOT NULL,
    industry_level SMALLINT NOT NULL,
    industry_code VARCHAR(40) NOT NULL,
    industry_name VARCHAR(160),
    eligible_stock_count INTEGER NOT NULL DEFAULT 0,
    anomaly_stock_count INTEGER NOT NULL DEFAULT 0,
    new_anomaly_count INTEGER NOT NULL DEFAULT 0,
    persistent_anomaly_count INTEGER NOT NULL DEFAULT 0,
    previous_anomaly_count INTEGER NOT NULL DEFAULT 0,
    exited_anomaly_count INTEGER NOT NULL DEFAULT 0,
    sector_anomaly_breadth DOUBLE PRECISION,
    sector_new_anomaly_ratio DOUBLE PRECISION,
    sector_persistence DOUBLE PRECISION,
    sector_data_status VARCHAR(20) NOT NULL DEFAULT 'MISSING',
    quality_status VARCHAR(30) NOT NULL DEFAULT 'WARNING',
    input_manifest_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT sector_anomaly_data_status CHECK
        (sector_data_status IN ('CONFIRMED','DAILY_CAPTURED','MISSING','INVALID')),
    CONSTRAINT sector_anomaly_counts_nonnegative CHECK
        (eligible_stock_count >= 0 AND anomaly_stock_count >= 0 AND new_anomaly_count >= 0
         AND persistent_anomaly_count >= 0 AND previous_anomaly_count >= 0 AND exited_anomaly_count >= 0),
    CONSTRAINT sector_anomaly_identity UNIQUE
        (snapshot_id, observation_date, industry_system, industry_level, industry_code)
);
CREATE INDEX IF NOT EXISTS idx_observation_sector_date_code
    ON observation.sector_anomaly_daily (observation_date, industry_system, industry_level, industry_code);

CREATE TABLE IF NOT EXISTS observation.stock_sector_confirmation (
    confirmation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    observation_id UUID NOT NULL REFERENCES observation.stock_anomaly_daily(observation_id),
    industry_system VARCHAR(30),
    industry_level SMALLINT,
    industry_code VARCHAR(40),
    industry_name VARCHAR(160),
    sector_data_status VARCHAR(20) NOT NULL,
    sector_confirmation_status VARCHAR(20) NOT NULL,
    sector_confirmation_score DOUBLE PRECISION,
    sector_reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    input_manifest_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT stock_sector_data_status CHECK
        (sector_data_status IN ('CONFIRMED','DAILY_CAPTURED','MISSING','INVALID')),
    CONSTRAINT stock_sector_confirmation_status CHECK
        (sector_confirmation_status IN ('CONFIRMED','NOT_CONFIRMED','UNKNOWN')),
    CONSTRAINT stock_sector_confirmation_identity
        UNIQUE (observation_id),
    CONSTRAINT stock_sector_confirmation_dimensions
        UNIQUE (observation_id, industry_system, industry_level, industry_code)
);
CREATE INDEX IF NOT EXISTS idx_observation_sector_confirmation_observation
    ON observation.stock_sector_confirmation (observation_id);

CREATE TABLE IF NOT EXISTS observation.event_link (
    event_link_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    observation_id UUID NOT NULL REFERENCES observation.stock_anomaly_daily(observation_id),
    event_id VARCHAR(180),
    event_type VARCHAR(80) NOT NULL,
    event_visible_at TIMESTAMPTZ NOT NULL,
    effective_date DATE,
    source_record_id VARCHAR(180) NOT NULL,
    source_hash CHAR(64) NOT NULL,
    evidence_role VARCHAR(30) NOT NULL DEFAULT 'AS_OF_EVIDENCE',
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT observation_event_role CHECK (evidence_role = 'AS_OF_EVIDENCE'),
    CONSTRAINT observation_event_identity UNIQUE (observation_id, source_record_id, source_hash)
);
CREATE INDEX IF NOT EXISTS idx_observation_event_observation
    ON observation.event_link (observation_id, event_visible_at);

CREATE TABLE IF NOT EXISTS evaluation.retrospective_event_link (
    retrospective_link_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    observation_id UUID NOT NULL REFERENCES observation.stock_anomaly_daily(observation_id),
    event_id VARCHAR(180),
    event_type VARCHAR(80) NOT NULL,
    event_visible_at TIMESTAMPTZ NOT NULL,
    effective_date DATE,
    source_record_id VARCHAR(180) NOT NULL,
    source_hash CHAR(64) NOT NULL,
    evidence_role VARCHAR(30) NOT NULL DEFAULT 'RETROSPECTIVE_CAUSE',
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT retrospective_event_role CHECK (evidence_role = 'RETROSPECTIVE_CAUSE'),
    CONSTRAINT retrospective_event_identity UNIQUE (observation_id, source_record_id, source_hash)
);
CREATE INDEX IF NOT EXISTS idx_evaluation_retrospective_observation
    ON evaluation.retrospective_event_link (observation_id, event_visible_at);

CREATE TABLE IF NOT EXISTS evaluation.market_anomaly_outcome (
    outcome_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    observation_id UUID NOT NULL REFERENCES observation.stock_anomaly_daily(observation_id),
    evaluation_methodology_hash CHAR(64) NOT NULL,
    maturity_status VARCHAR(30) NOT NULL DEFAULT 'PENDING_MATURITY',
    input_manifest_hash CHAR(64) NOT NULL,
    future_price_return_5d DOUBLE PRECISION,
    future_price_return_10d DOUBLE PRECISION,
    future_price_return_20d DOUBLE PRECISION,
    future_price_return_60d DOUBLE PRECISION,
    future_price_return_120d DOUBLE PRECISION,
    future_total_return_5d DOUBLE PRECISION,
    future_total_return_10d DOUBLE PRECISION,
    future_total_return_20d DOUBLE PRECISION,
    future_total_return_60d DOUBLE PRECISION,
    future_total_return_120d DOUBLE PRECISION,
    excess_return_5d DOUBLE PRECISION,
    excess_return_10d DOUBLE PRECISION,
    excess_return_20d DOUBLE PRECISION,
    excess_return_60d DOUBLE PRECISION,
    excess_return_120d DOUBLE PRECISION,
    max_drawdown_5d DOUBLE PRECISION,
    max_drawdown_10d DOUBLE PRECISION,
    max_drawdown_20d DOUBLE PRECISION,
    max_drawdown_60d DOUBLE PRECISION,
    max_drawdown_120d DOUBLE PRECISION,
    max_favorable_excursion_5d DOUBLE PRECISION,
    max_favorable_excursion_10d DOUBLE PRECISION,
    max_favorable_excursion_20d DOUBLE PRECISION,
    max_favorable_excursion_60d DOUBLE PRECISION,
    max_favorable_excursion_120d DOUBLE PRECISION,
    max_adverse_excursion_5d DOUBLE PRECISION,
    max_adverse_excursion_10d DOUBLE PRECISION,
    max_adverse_excursion_20d DOUBLE PRECISION,
    max_adverse_excursion_60d DOUBLE PRECISION,
    max_adverse_excursion_120d DOUBLE PRECISION,
    volatility_5d DOUBLE PRECISION,
    volatility_10d DOUBLE PRECISION,
    volatility_20d DOUBLE PRECISION,
    volatility_60d DOUBLE PRECISION,
    volatility_120d DOUBLE PRECISION,
    best_return_5d DOUBLE PRECISION,
    best_return_10d DOUBLE PRECISION,
    best_return_20d DOUBLE PRECISION,
    best_return_60d DOUBLE PRECISION,
    best_return_120d DOUBLE PRECISION,
    worst_return_5d DOUBLE PRECISION,
    worst_return_10d DOUBLE PRECISION,
    worst_return_20d DOUBLE PRECISION,
    worst_return_60d DOUBLE PRECISION,
    worst_return_120d DOUBLE PRECISION,
    benchmark_code VARCHAR(40),
    outcome_hash CHAR(64) NOT NULL,
    calculation_run_id VARCHAR(160) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT outcome_maturity_status CHECK
        (maturity_status IN ('PENDING_MATURITY','MATURED','INVALID_INPUT')),
    CONSTRAINT outcome_identity UNIQUE (observation_id, evaluation_methodology_hash)
);
CREATE INDEX IF NOT EXISTS idx_evaluation_outcome_observation
    ON evaluation.market_anomaly_outcome (observation_id, evaluation_methodology_hash);
CREATE INDEX IF NOT EXISTS idx_evaluation_outcome_maturity
    ON evaluation.market_anomaly_outcome (maturity_status, created_at);

CREATE TABLE IF NOT EXISTS evaluation.scenario_result (
    scenario_result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    observation_id UUID NOT NULL REFERENCES observation.stock_anomaly_daily(observation_id),
    scenario_methodology_hash CHAR(64) NOT NULL,
    entry_timestamp TIMESTAMPTZ,
    entry_price DOUBLE PRECISION,
    exit_timestamp TIMESTAMPTZ,
    exit_price DOUBLE PRECISION,
    holding_days INTEGER,
    fee_bps DOUBLE PRECISION,
    slippage_bps DOUBLE PRECISION,
    stop_loss DOUBLE PRECISION,
    take_profit DOUBLE PRECISION,
    hit_target_before_stop BOOLEAN,
    result_status VARCHAR(30) NOT NULL,
    result_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    scenario_hash CHAR(64) NOT NULL,
    calculation_run_id VARCHAR(160) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT scenario_identity UNIQUE (observation_id, scenario_methodology_hash)
);
CREATE INDEX IF NOT EXISTS idx_evaluation_scenario_observation
    ON evaluation.scenario_result (observation_id, scenario_methodology_hash);

-- FROZEN（冻结）观察的数据库兜底：主表及观察证据/行业证据不可更新或删除。
-- Evaluation（评估）表故意不挂此触发器，允许未来追加成熟 Outcome（后验结果）和
-- RETROSPECTIVE_CAUSE（事后原因），但这些写入不可能修改观察主体。
CREATE OR REPLACE FUNCTION observation.enforce_frozen_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    snapshot_status TEXT;
    old_row JSONB;
    snapshot_id_text TEXT;
    observation_id_text TEXT;
BEGIN
    IF TG_TABLE_SCHEMA = 'observation' AND TG_TABLE_NAME = 'snapshot' THEN
        old_row := to_jsonb(OLD);
        snapshot_id_text := old_row->>'snapshot_id';
        snapshot_status := old_row->>'status';
    ELSE
        old_row := to_jsonb(OLD);
        snapshot_id_text := old_row->>'snapshot_id';
        observation_id_text := old_row->>'observation_id';
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
        RAISE EXCEPTION '观察快照 % 已是 %，禁止修改或删除 %',
            COALESCE(snapshot_id_text, observation_id_text),
            snapshot_status, TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
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
BEGIN
    IF TG_TABLE_SCHEMA = 'observation' AND TG_TABLE_NAME = 'stock_anomaly_daily' THEN
        SELECT methodology_hash, data_baseline_id
          INTO parent_methodology, parent_baseline
          FROM observation.snapshot
         WHERE snapshot_id = NEW.snapshot_id;
        IF parent_methodology IS NULL OR parent_methodology <> NEW.methodology_hash
           OR parent_baseline <> NEW.data_baseline_id THEN
            RAISE EXCEPTION '观察主体身份与快照身份不一致: %', NEW.observation_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION observation.enforce_asof_evidence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    observed_date DATE;
BEGIN
    SELECT observation_date INTO observed_date
      FROM observation.stock_anomaly_daily
     WHERE observation_id = NEW.observation_id;
    IF (NEW.event_visible_at AT TIME ZONE 'Asia/Shanghai')::date > observed_date THEN
        RAISE EXCEPTION 'AS_OF_EVIDENCE 的可见时间晚于观察日: % > %', NEW.event_visible_at, observed_date;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION evaluation.enforce_retrospective_evidence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    observed_date DATE;
BEGIN
    SELECT observation_date INTO observed_date
      FROM observation.stock_anomaly_daily
     WHERE observation_id = NEW.observation_id;
    IF (NEW.event_visible_at AT TIME ZONE 'Asia/Shanghai')::date <= observed_date THEN
        RAISE EXCEPTION 'RETROSPECTIVE_CAUSE 的可见时间必须晚于观察日: % <= %', NEW.event_visible_at, observed_date;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_observation_snapshot_frozen ON observation.snapshot;
CREATE TRIGGER trg_observation_snapshot_frozen
BEFORE UPDATE OR DELETE ON observation.snapshot
FOR EACH ROW EXECUTE FUNCTION observation.enforce_frozen_immutable();

DROP TRIGGER IF EXISTS trg_observation_stock_frozen ON observation.stock_anomaly_daily;
CREATE TRIGGER trg_observation_stock_frozen
BEFORE UPDATE OR DELETE ON observation.stock_anomaly_daily
FOR EACH ROW EXECUTE FUNCTION observation.enforce_frozen_immutable();

DROP TRIGGER IF EXISTS trg_observation_stock_identity ON observation.stock_anomaly_daily;
CREATE TRIGGER trg_observation_stock_identity
BEFORE INSERT OR UPDATE ON observation.stock_anomaly_daily
FOR EACH ROW EXECUTE FUNCTION observation.enforce_observation_identity();

DROP TRIGGER IF EXISTS trg_observation_tag_frozen ON observation.stock_anomaly_tag;
CREATE TRIGGER trg_observation_tag_frozen
BEFORE UPDATE OR DELETE ON observation.stock_anomaly_tag
FOR EACH ROW EXECUTE FUNCTION observation.enforce_frozen_immutable();

DROP TRIGGER IF EXISTS trg_observation_sector_frozen ON observation.sector_anomaly_daily;
CREATE TRIGGER trg_observation_sector_frozen
BEFORE UPDATE OR DELETE ON observation.sector_anomaly_daily
FOR EACH ROW EXECUTE FUNCTION observation.enforce_frozen_immutable();

DROP TRIGGER IF EXISTS trg_observation_confirmation_frozen ON observation.stock_sector_confirmation;
CREATE TRIGGER trg_observation_confirmation_frozen
BEFORE UPDATE OR DELETE ON observation.stock_sector_confirmation
FOR EACH ROW EXECUTE FUNCTION observation.enforce_frozen_immutable();

DROP TRIGGER IF EXISTS trg_observation_event_frozen ON observation.event_link;
CREATE TRIGGER trg_observation_event_frozen
BEFORE UPDATE OR DELETE ON observation.event_link
FOR EACH ROW EXECUTE FUNCTION observation.enforce_frozen_immutable();

DROP TRIGGER IF EXISTS trg_observation_event_asof ON observation.event_link;
CREATE TRIGGER trg_observation_event_asof
BEFORE INSERT OR UPDATE ON observation.event_link
FOR EACH ROW EXECUTE FUNCTION observation.enforce_asof_evidence();

DROP TRIGGER IF EXISTS trg_retrospective_event_time ON evaluation.retrospective_event_link;
CREATE TRIGGER trg_retrospective_event_time
BEFORE INSERT OR UPDATE ON evaluation.retrospective_event_link
FOR EACH ROW EXECUTE FUNCTION evaluation.enforce_retrospective_evidence();

COMMENT ON TABLE observation.snapshot IS '市场异常观察发布批次表：保存方法论、数据基线、输入清单和生命周期。';
COMMENT ON TABLE observation.stock_anomaly_daily IS '个股异常观察日表：保存当日可见指标、标签依据和主状态。';
COMMENT ON TABLE observation.stock_anomaly_tag IS '个股异常标签表：一条观察可有多个独立标签。';
COMMENT ON TABLE observation.sector_anomaly_daily IS '行业异常聚合表：使用观察日 PIT 股票池分母。';
COMMENT ON TABLE observation.stock_sector_confirmation IS '个股行业确认表：行业可选增强证据，不改变个股主状态。';
COMMENT ON TABLE observation.event_link IS '观察日当时可见事件证据表：仅允许 AS_OF_EVIDENCE。';
COMMENT ON TABLE evaluation.retrospective_event_link IS '事后事件关联表：仅允许 RETROSPECTIVE_CAUSE，不能回写观察。';
COMMENT ON TABLE evaluation.market_anomaly_outcome IS '市场异常后验结果表：只保存未来客观路径表现。';
COMMENT ON TABLE evaluation.scenario_result IS '策略场景结果表：保存执行、费用、止盈止损等假设。';
COMMENT ON COLUMN std.valuation_daily.turnover_rate_f IS '自由流通换手率：Tushare daily_basic 百分比口径。';
COMMENT ON COLUMN std.valuation_daily.free_share IS '自由流通股本：标准层统一单位为股。';
COMMENT ON COLUMN feature.price_daily_v2.adj_close_base IS '底座复权收盘价：只处理机械公司行为，不含现金分红收益。';
COMMENT ON COLUMN feature.price_daily_v2.total_return_price IS '总回报价格：按 PIT 可见公司行为包含现金分红再投资。';

-- 应用账号可写入未冻结观察和评估结果，但不给删除权限；研究读账号只读。
GRANT USAGE ON SCHEMA observation, evaluation TO ashares_app, research_reader;
GRANT USAGE ON SCHEMA pit TO ashares_app, research_reader;
GRANT SELECT, INSERT, UPDATE ON
    pit.industry_membership_history,
    pit.industry_membership_daily
    TO ashares_app;
REVOKE DELETE, TRUNCATE, REFERENCES, TRIGGER ON
    pit.industry_membership_history,
    pit.industry_membership_daily
    FROM ashares_app;
GRANT SELECT ON
    pit.industry_membership_history,
    pit.industry_membership_daily
    TO research_reader;
GRANT SELECT, INSERT, UPDATE ON
    observation.snapshot,
    observation.stock_anomaly_daily,
    observation.stock_anomaly_tag,
    observation.sector_anomaly_daily,
    observation.stock_sector_confirmation,
    observation.event_link,
    evaluation.retrospective_event_link,
    evaluation.market_anomaly_outcome,
    evaluation.scenario_result
    TO ashares_app;
REVOKE DELETE, TRUNCATE, REFERENCES, TRIGGER ON
    observation.snapshot,
    observation.stock_anomaly_daily,
    observation.stock_anomaly_tag,
    observation.sector_anomaly_daily,
    observation.stock_sector_confirmation,
    observation.event_link,
    evaluation.retrospective_event_link,
    evaluation.market_anomaly_outcome,
    evaluation.scenario_result
    FROM ashares_app;
GRANT SELECT ON
    observation.snapshot,
    observation.stock_anomaly_daily,
    observation.stock_anomaly_tag,
    observation.sector_anomaly_daily,
    observation.stock_sector_confirmation,
    observation.event_link,
    evaluation.retrospective_event_link,
    evaluation.market_anomaly_outcome,
    evaluation.scenario_result
    TO research_reader;
GRANT USAGE, SELECT ON SEQUENCE pit.industry_membership_history_membership_id_seq TO pit_writer;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE pit.industry_membership_history_membership_id_seq TO ashares_app;
