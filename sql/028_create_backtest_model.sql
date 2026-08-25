-- 阶段三 Backtest v1：不可变 Dataset、Run Manifest、日线撮合事件和绩效。
-- 本迁移只创建 backtest 对象，不修改任何 Raw/Standard/PIT/Feature/Research 数据。

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS backtest;
ALTER TABLE IF EXISTS backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS rules_hash CHAR(64);

-- 一个 Dataset 可以引用多个单日 FROZEN Research Snapshot。
CREATE TABLE IF NOT EXISTS backtest.dataset_manifest (
    dataset_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_version VARCHAR(40) NOT NULL,
    dataset_name VARCHAR(160) NOT NULL,
    data_baseline_tag VARCHAR(100) NOT NULL REFERENCES ops.tushare_data_release(release_tag),
    baseline_quality_report_hash CHAR(64),
    baseline_source_manifest_hash CHAR(64),
    schema_version VARCHAR(40) NOT NULL,
    adapter_version VARCHAR(40) NOT NULL,
    feature_version VARCHAR(80) NOT NULL,
    price_adjustment_mode VARCHAR(40) NOT NULL DEFAULT 'adj_close_base_for_features',
    warmup_start DATE NOT NULL,
    evaluation_start DATE NOT NULL,
    evaluation_end DATE NOT NULL,
    out_of_sample_start DATE,
    calendar_rule_version VARCHAR(80) NOT NULL,
    market_rule_version VARCHAR(80) NOT NULL,
    fee_rule_version VARCHAR(80) NOT NULL,
    calendar_hash CHAR(64),
    rules_hash CHAR(64),
    execution_hash CHAR(64),
    corporate_action_hash CHAR(64),
    benchmark_code VARCHAR(20) NOT NULL DEFAULT '000300.SH',
    benchmark_hash CHAR(64),
    input_manifest_hash CHAR(64),
    selection_predicate TEXT NOT NULL DEFAULT 'dataset_member research_date range; frozen snapshot membership',
    selected_columns JSONB NOT NULL DEFAULT '[]'::jsonb,
    hash_algorithm VARCHAR(20) NOT NULL DEFAULT 'sha256',
    availability_rules JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
    quality_report JSONB,
    quality_report_hash CHAR(64),
    code_version VARCHAR(180),
    git_commit CHAR(40),
    git_dirty BOOLEAN NOT NULL DEFAULT FALSE,
    diff_hash CHAR(64),
    code_archive_path TEXT,
    code_archive_sha256 CHAR(64),
    code_archive_size_bytes BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    built_at TIMESTAMP,
    validated_at TIMESTAMP,
    frozen_at TIMESTAMP,
    failed_at TIMESTAMP,
    failure_reason TEXT,
    notes TEXT,
    CHECK (evaluation_start <= evaluation_end),
    CHECK (warmup_start <= evaluation_start),
    CHECK (price_adjustment_mode IN ('adj_close_base_for_features')),
    CHECK (status IN ('DRAFT','BUILDING','VALIDATED','FROZEN','FAILED')),
    CHECK (git_dirty = FALSE OR diff_hash IS NOT NULL OR code_archive_path IS NOT NULL),
    CHECK (status <> 'FROZEN' OR input_manifest_hash IS NOT NULL),
    CHECK (status <> 'FROZEN' OR quality_report_hash IS NOT NULL),
    CONSTRAINT backtest_dataset_baseline_quality_hash_check
        CHECK (status <> 'FROZEN' OR baseline_quality_report_hash IS NOT NULL)
);

ALTER TABLE IF EXISTS backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS baseline_quality_report_hash CHAR(64);
ALTER TABLE IF EXISTS backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS selection_predicate TEXT NOT NULL DEFAULT 'dataset_member research_date range; frozen snapshot membership';
ALTER TABLE IF EXISTS backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS selected_columns JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE IF EXISTS backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS hash_algorithm VARCHAR(20) NOT NULL DEFAULT 'sha256';
ALTER TABLE IF EXISTS backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS availability_rules JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'backtest.dataset_manifest'::regclass
           AND conname = 'backtest_dataset_baseline_quality_hash_check'
    ) THEN
        ALTER TABLE backtest.dataset_manifest
            ADD CONSTRAINT backtest_dataset_baseline_quality_hash_check
            CHECK (status <> 'FROZEN' OR baseline_quality_report_hash IS NOT NULL);
    END IF;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_backtest_dataset_name_version
    ON backtest.dataset_manifest (dataset_name, dataset_version);
CREATE INDEX IF NOT EXISTS idx_backtest_dataset_status_dates
    ON backtest.dataset_manifest (status, evaluation_start, evaluation_end);

CREATE TABLE IF NOT EXISTS backtest.dataset_member (
    dataset_id UUID NOT NULL REFERENCES backtest.dataset_manifest(dataset_id),
    research_snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    research_date DATE NOT NULL,
    member_role VARCHAR(20) NOT NULL,
    snapshot_input_hash CHAR(64) NOT NULL,
    observation_row_count BIGINT NOT NULL DEFAULT 0,
    execution_row_count BIGINT NOT NULL DEFAULT 0,
    execution_hash CHAR(64),
    corporate_action_hash CHAR(64),
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, research_date),
    UNIQUE (dataset_id, research_snapshot_id),
    CHECK (member_role IN ('WARMUP','EVALUATION')),
    CHECK (quality_status IN ('UNVERIFIED','VALID','WARNING','INVALID','MISSING_SOURCE'))
);

CREATE INDEX IF NOT EXISTS idx_backtest_dataset_member_snapshot
    ON backtest.dataset_member (research_snapshot_id, research_date);

-- 冻结后的实际执行行情物料。指标仍来自对应 FROZEN Research Snapshot。
CREATE TABLE IF NOT EXISTS backtest.dataset_execution_bar (
    dataset_id UUID NOT NULL REFERENCES backtest.dataset_manifest(dataset_id),
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    market VARCHAR(30),
    raw_open DOUBLE PRECISION,
    raw_high DOUBLE PRECISION,
    raw_low DOUBLE PRECISION,
    raw_close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    amount DOUBLE PRECISION,
    up_limit DOUBLE PRECISION,
    down_limit DOUBLE PRECISION,
    is_suspended BOOLEAN,
    can_buy BOOLEAN,
    can_sell BOOLEAN,
    limit_status VARCHAR(20),
    source_record_key VARCHAR(180) NOT NULL,
    source_record_version BIGINT,
    source_payload_hash CHAR(64),
    limit_source_record_id BIGINT,
    limit_source_payload_hash CHAR(64),
    suspend_source_record_id BIGINT,
    suspend_source_payload_hash CHAR(64),
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, trade_date, ts_code),
    CHECK (quality_status IN ('UNVERIFIED','VALID','WARNING','INVALID','MISSING_SOURCE'))
);

CREATE INDEX IF NOT EXISTS idx_backtest_execution_bar_date_code
    ON backtest.dataset_execution_bar (dataset_id, trade_date, ts_code);
ALTER TABLE IF EXISTS backtest.dataset_execution_bar
    ADD COLUMN IF NOT EXISTS market VARCHAR(30);

CREATE TABLE IF NOT EXISTS backtest.dataset_calendar (
    dataset_id UUID NOT NULL REFERENCES backtest.dataset_manifest(dataset_id),
    trade_date DATE NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    is_open BOOLEAN NOT NULL,
    pretrade_date DATE,
    source_record_key VARCHAR(180) NOT NULL,
    source_record_version BIGINT,
    source_payload_hash CHAR(64),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, exchange, trade_date)
);

CREATE TABLE IF NOT EXISTS backtest.dataset_dividend_event (
    dataset_id UUID NOT NULL REFERENCES backtest.dataset_manifest(dataset_id),
    event_id BIGSERIAL,
    ts_code VARCHAR(20) NOT NULL,
    report_date DATE,
    announce_date DATE,
    implementation_status VARCHAR(20) NOT NULL,
    stock_div DOUBLE PRECISION,
    stock_bonus_rate DOUBLE PRECISION,
    stock_conversion_rate DOUBLE PRECISION,
    cash_div DOUBLE PRECISION,
    cash_div_tax DOUBLE PRECISION,
    record_date DATE,
    ex_date DATE,
    pay_date DATE,
    div_list_date DATE,
    implementation_announce_date DATE,
    source_record_key VARCHAR(180) NOT NULL,
    source_record_version BIGINT,
    source_payload_hash CHAR(64),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, event_id),
    UNIQUE (dataset_id, ts_code, source_record_key, source_record_version)
);

CREATE INDEX IF NOT EXISTS idx_backtest_dividend_event_date
    ON backtest.dataset_dividend_event (dataset_id, ex_date, ts_code);

-- 规则/费用也必须随 Dataset 冻结，不能只保存一个可变版本名。
CREATE TABLE IF NOT EXISTS backtest.dataset_rule_snapshot (
    dataset_id UUID NOT NULL REFERENCES backtest.dataset_manifest(dataset_id),
    rule_version VARCHAR(80) NOT NULL,
    market_rule_version VARCHAR(80) NOT NULL DEFAULT 'cn-a-share-v2',
    fee_rule_version VARCHAR(80) NOT NULL DEFAULT 'research-fee-assumption-v3',
    effective_from DATE NOT NULL,
    effective_to DATE,
    market VARCHAR(30) NOT NULL,
    min_buy_lot INTEGER NOT NULL,
    allow_sell_odd_lot BOOLEAN NOT NULL,
    t_plus_one BOOLEAN NOT NULL,
    price_tick DOUBLE PRECISION NOT NULL,
    cash_settlement_days INTEGER NOT NULL DEFAULT 0,
    limit_pct DOUBLE PRECISION,
    commission_rate DOUBLE PRECISION NOT NULL,
    min_commission DOUBLE PRECISION NOT NULL,
    stamp_duty_sell_rate DOUBLE PRECISION NOT NULL,
    transfer_fee_rate DOUBLE PRECISION NOT NULL,
    slippage_bps DOUBLE PRECISION NOT NULL,
    parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    approval_status VARCHAR(20),
    approved_by VARCHAR(120),
    approved_at TIMESTAMPTZ,
    approval_hash CHAR(64),
    rule_hash CHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, rule_version, effective_from, market),
    CHECK (effective_to IS NULL OR effective_to >= effective_from),
    CHECK (cash_settlement_days >= 0)
);

ALTER TABLE IF EXISTS backtest.dataset_rule_snapshot
    ADD COLUMN IF NOT EXISTS cash_settlement_days INTEGER NOT NULL DEFAULT 0;
ALTER TABLE IF EXISTS backtest.dataset_rule_snapshot
    ADD COLUMN IF NOT EXISTS market_rule_version VARCHAR(80) NOT NULL DEFAULT 'cn-a-share-v2';
ALTER TABLE IF EXISTS backtest.dataset_rule_snapshot
    ADD COLUMN IF NOT EXISTS fee_rule_version VARCHAR(80) NOT NULL DEFAULT 'research-fee-assumption-v3';

CREATE INDEX IF NOT EXISTS idx_backtest_dataset_rule_lookup
    ON backtest.dataset_rule_snapshot (dataset_id, market, effective_from, effective_to);

-- 交易规则和费用是数据集的一部分，不允许在引擎中隐式使用当前规则。
CREATE TABLE IF NOT EXISTS backtest.market_rule_effective (
    rule_version VARCHAR(80) NOT NULL,
    market_rule_version VARCHAR(80) NOT NULL DEFAULT 'cn-a-share-v2',
    fee_rule_version VARCHAR(80) NOT NULL DEFAULT 'research-fee-assumption-v3',
    effective_from DATE NOT NULL,
    effective_to DATE,
    market VARCHAR(30) NOT NULL,
    min_buy_lot INTEGER NOT NULL DEFAULT 100 CHECK (min_buy_lot > 0),
    allow_sell_odd_lot BOOLEAN NOT NULL DEFAULT TRUE,
    t_plus_one BOOLEAN NOT NULL DEFAULT TRUE,
    price_tick DOUBLE PRECISION NOT NULL DEFAULT 0.01 CHECK (price_tick > 0),
    cash_settlement_days INTEGER NOT NULL DEFAULT 0 CHECK (cash_settlement_days >= 0),
    limit_pct DOUBLE PRECISION,
    commission_rate DOUBLE PRECISION NOT NULL,
    min_commission DOUBLE PRECISION NOT NULL,
    stamp_duty_sell_rate DOUBLE PRECISION NOT NULL,
    transfer_fee_rate DOUBLE PRECISION NOT NULL,
    slippage_bps DOUBLE PRECISION NOT NULL DEFAULT 0,
    parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    approval_status VARCHAR(20),
    approved_by VARCHAR(120),
    approved_at TIMESTAMPTZ,
    approval_hash CHAR(64),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (rule_version, effective_from, market),
    CHECK (effective_to IS NULL OR effective_to >= effective_from),
    CHECK (commission_rate >= 0 AND min_commission >= 0),
    CHECK (stamp_duty_sell_rate >= 0 AND transfer_fee_rate >= 0 AND slippage_bps >= 0)
);

CREATE INDEX IF NOT EXISTS idx_backtest_market_rule_lookup
    ON backtest.market_rule_effective (market, effective_from, effective_to);

ALTER TABLE IF EXISTS backtest.market_rule_effective
    ADD COLUMN IF NOT EXISTS cash_settlement_days INTEGER NOT NULL DEFAULT 0;
ALTER TABLE IF EXISTS backtest.market_rule_effective
    ADD COLUMN IF NOT EXISTS market_rule_version VARCHAR(80) NOT NULL DEFAULT 'cn-a-share-v2';
ALTER TABLE IF EXISTS backtest.market_rule_effective
    ADD COLUMN IF NOT EXISTS fee_rule_version VARCHAR(80) NOT NULL DEFAULT 'research-fee-assumption-v3';

CREATE TABLE IF NOT EXISTS backtest.strategy_definition (
    strategy_id VARCHAR(80) PRIMARY KEY,
    strategy_name VARCHAR(120) NOT NULL,
    strategy_version VARCHAR(40) NOT NULL,
    description TEXT,
    parameters JSONB NOT NULL,
    parameters_hash CHAR(64) NOT NULL,
    code_version VARCHAR(180),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (strategy_name, strategy_version)
);

CREATE OR REPLACE FUNCTION backtest.assert_strategy_immutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'backtest strategy definition is append-only: %',
        CASE WHEN TG_OP = 'DELETE' THEN OLD.strategy_id ELSE NEW.strategy_id END;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_strategy_immutable ON backtest.strategy_definition;
CREATE TRIGGER trg_backtest_strategy_immutable
BEFORE UPDATE OR DELETE ON backtest.strategy_definition
FOR EACH ROW EXECUTE FUNCTION backtest.assert_strategy_immutable();

CREATE TABLE IF NOT EXISTS backtest.run_manifest (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL REFERENCES backtest.dataset_manifest(dataset_id),
    strategy_id VARCHAR(80) NOT NULL REFERENCES backtest.strategy_definition(strategy_id),
    run_version VARCHAR(40) NOT NULL,
    initial_cash NUMERIC(20,6) NOT NULL CHECK (initial_cash > 0),
    max_positions INTEGER CHECK (max_positions IS NULL OR max_positions > 0),
    max_single_position_pct DOUBLE PRECISION,
    max_portfolio_risk_pct DOUBLE PRECISION,
    reserve_cash_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
    market_rule_version VARCHAR(80) NOT NULL,
    fee_rule_version VARCHAR(80) NOT NULL,
    execution_model_version VARCHAR(80) NOT NULL,
    random_seed BIGINT,
    parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    parameters_hash CHAR(64),
    input_hash CHAR(64),
    code_version VARCHAR(180),
    git_commit CHAR(40),
    git_dirty BOOLEAN NOT NULL DEFAULT FALSE,
    diff_hash CHAR(64),
    code_archive_path TEXT,
    code_archive_sha256 CHAR(64),
    code_archive_size_bytes BIGINT,
    status VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    failure_reason TEXT,
    CHECK (status IN ('DRAFT','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    CHECK (max_single_position_pct IS NULL OR max_single_position_pct > 0),
    CHECK (max_portfolio_risk_pct IS NULL OR max_portfolio_risk_pct > 0),
    CHECK (reserve_cash_pct >= 0 AND reserve_cash_pct < 1),
    CHECK (git_dirty = FALSE OR diff_hash IS NOT NULL OR code_archive_path IS NOT NULL)
);

ALTER TABLE IF EXISTS backtest.run_manifest
    ADD COLUMN IF NOT EXISTS code_archive_path TEXT;
ALTER TABLE IF EXISTS backtest.run_manifest
    ADD COLUMN IF NOT EXISTS parameters_hash CHAR(64);

CREATE UNIQUE INDEX IF NOT EXISTS uq_backtest_run_version
    ON backtest.run_manifest (dataset_id, strategy_id, run_version);

CREATE TABLE IF NOT EXISTS backtest.signal (
    signal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES backtest.run_manifest(run_id),
    research_snapshot_id UUID REFERENCES research.snapshot_manifest(snapshot_id),
    ts_code VARCHAR(20) NOT NULL,
    signal_date DATE NOT NULL,
    decision_time TIMESTAMP,
    intended_execution_date DATE NOT NULL,
    direction VARCHAR(10) NOT NULL CHECK (direction IN ('BUY','SELL','HOLD')),
    indicator_price DOUBLE PRECISION,
    raw_reference_price DOUBLE PRECISION,
    stop_loss_price_raw DOUBLE PRECISION,
    signal_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    reason JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(20) NOT NULL DEFAULT 'CREATED',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, ts_code, signal_date, direction)
);

CREATE INDEX IF NOT EXISTS idx_backtest_signal_execution
    ON backtest.signal (run_id, intended_execution_date, ts_code);

ALTER TABLE IF EXISTS backtest.signal
    ADD COLUMN IF NOT EXISTS signal_score DOUBLE PRECISION NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS backtest.order (
    order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES backtest.run_manifest(run_id),
    signal_id UUID REFERENCES backtest.signal(signal_id),
    ts_code VARCHAR(20) NOT NULL,
    decision_date DATE NOT NULL,
    order_date DATE NOT NULL,
    expire_date DATE NOT NULL,
    direction VARCHAR(10) NOT NULL CHECK (direction IN ('BUY','SELL')),
    planned_shares INTEGER NOT NULL DEFAULT 0 CHECK (planned_shares >= 0),
    order_shares INTEGER NOT NULL CHECK (order_shares >= 0),
    reference_price DOUBLE PRECISION,
    stop_loss_price_raw DOUBLE PRECISION,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    reject_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, signal_id),
    CHECK (status IN ('PENDING','FILLED','REJECTED','EXPIRED','CANCELLED','PARTIAL')),
    CHECK (status = 'REJECTED' OR order_shares > 0)
);

CREATE INDEX IF NOT EXISTS idx_backtest_order_execution
    ON backtest.order (run_id, order_date, status, ts_code);

ALTER TABLE IF EXISTS backtest.order
    ADD COLUMN IF NOT EXISTS planned_shares INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS liquidity_reference_date DATE;

CREATE TABLE IF NOT EXISTS backtest.fill (
    fill_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES backtest.run_manifest(run_id),
    order_id UUID NOT NULL REFERENCES backtest.order(order_id),
    ts_code VARCHAR(20) NOT NULL,
    fill_date DATE NOT NULL,
    actual_fill_time TIMESTAMP,
    direction VARCHAR(10) NOT NULL CHECK (direction IN ('BUY','SELL')),
    fill_price DOUBLE PRECISION NOT NULL CHECK (fill_price > 0),
    fill_shares INTEGER NOT NULL CHECK (fill_shares > 0),
    commission NUMERIC(20,6) NOT NULL DEFAULT 0,
    stamp_duty NUMERIC(20,6) NOT NULL DEFAULT 0,
    transfer_fee NUMERIC(20,6) NOT NULL DEFAULT 0,
    slippage_cost NUMERIC(20,6) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (order_id, fill_date, fill_shares, fill_price)
);

CREATE TABLE IF NOT EXISTS backtest.position_lot (
    lot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES backtest.run_manifest(run_id),
    ts_code VARCHAR(20) NOT NULL,
    open_fill_id UUID REFERENCES backtest.fill(fill_id),
    open_date DATE NOT NULL,
    available_sell_date DATE NOT NULL,
    open_shares INTEGER NOT NULL CHECK (open_shares > 0),
    remaining_shares INTEGER NOT NULL CHECK (remaining_shares >= 0 AND remaining_shares <= open_shares),
    cost_price DOUBLE PRECISION NOT NULL CHECK (cost_price >= 0),
    stop_loss_price_raw DOUBLE PRECISION,
    close_date DATE,
    close_fill_id UUID REFERENCES backtest.fill(fill_id),
    opening_event_type VARCHAR(30) NOT NULL DEFAULT 'FILL',
    opening_event_key VARCHAR(180),
    accrued_cash_dividend_per_share NUMERIC(20,8) NOT NULL DEFAULT 0,
    CONSTRAINT position_lot_opening_event_type_check
        CHECK (opening_event_type IN ('FILL', 'CORPORATE_ACTION')),
    CONSTRAINT position_lot_opening_event_provenance_check
        CHECK ((opening_event_type = 'FILL' AND open_fill_id IS NOT NULL)
           OR (opening_event_type = 'CORPORATE_ACTION' AND opening_event_key IS NOT NULL)),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE IF EXISTS backtest.position_lot
    ALTER COLUMN open_fill_id DROP NOT NULL;
ALTER TABLE IF EXISTS backtest.position_lot
    ADD COLUMN IF NOT EXISTS opening_event_type VARCHAR(30) NOT NULL DEFAULT 'FILL';
ALTER TABLE IF EXISTS backtest.position_lot
    ADD COLUMN IF NOT EXISTS opening_event_key VARCHAR(180);
ALTER TABLE IF EXISTS backtest.position_lot
    ADD COLUMN IF NOT EXISTS accrued_cash_dividend_per_share NUMERIC(20,8) NOT NULL DEFAULT 0;
DO $$
BEGIN
    ALTER TABLE backtest.position_lot DROP CONSTRAINT IF EXISTS position_lot_cost_price_check;
    ALTER TABLE backtest.position_lot DROP CONSTRAINT IF EXISTS position_lot_cost_price_nonnegative;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'backtest.position_lot'::regclass
           AND conname = 'position_lot_cost_price_nonnegative'
    ) THEN
        ALTER TABLE backtest.position_lot
            ADD CONSTRAINT position_lot_cost_price_nonnegative CHECK (cost_price >= 0);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'backtest.position_lot'::regclass
           AND conname = 'position_lot_opening_event_type_check'
    ) THEN
        ALTER TABLE backtest.position_lot
            ADD CONSTRAINT position_lot_opening_event_type_check
            CHECK (opening_event_type IN ('FILL', 'CORPORATE_ACTION'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'backtest.position_lot'::regclass
           AND conname = 'position_lot_opening_event_provenance_check'
    ) THEN
        ALTER TABLE backtest.position_lot
            ADD CONSTRAINT position_lot_opening_event_provenance_check
            CHECK ((opening_event_type = 'FILL' AND open_fill_id IS NOT NULL)
                OR (opening_event_type = 'CORPORATE_ACTION' AND opening_event_key IS NOT NULL));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_backtest_position_lot_open
    ON backtest.position_lot (run_id, ts_code, open_date, remaining_shares);

CREATE TABLE IF NOT EXISTS backtest.position_daily (
    run_id UUID NOT NULL REFERENCES backtest.run_manifest(run_id),
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    hold_shares INTEGER NOT NULL CHECK (hold_shares >= 0),
    average_cost DOUBLE PRECISION,
    raw_close DOUBLE PRECISION,
    market_value NUMERIC(20,6),
    unrealized_pnl NUMERIC(20,6),
    valuation_status VARCHAR(30) NOT NULL DEFAULT 'VALID',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, trade_date, ts_code)
);

CREATE TABLE IF NOT EXISTS backtest.cash_ledger (
    ledger_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES backtest.run_manifest(run_id),
    trade_date DATE NOT NULL,
    event_type VARCHAR(30) NOT NULL,
    amount NUMERIC(20,6) NOT NULL,
    balance NUMERIC(20,6) NOT NULL,
    available_date DATE NOT NULL,
    reference_id UUID,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (balance >= 0)
);

CREATE INDEX IF NOT EXISTS idx_backtest_cash_ledger_run_date
    ON backtest.cash_ledger (run_id, trade_date, ledger_id);

CREATE TABLE IF NOT EXISTS backtest.performance_daily (
    run_id UUID NOT NULL REFERENCES backtest.run_manifest(run_id),
    trade_date DATE NOT NULL,
    cash_balance NUMERIC(20,6) NOT NULL,
    position_value NUMERIC(20,6) NOT NULL,
    cash_receivable NUMERIC(20,6) NOT NULL DEFAULT 0,
    stock_receivable_value NUMERIC(20,6) NOT NULL DEFAULT 0,
    receivable_total NUMERIC(20,6) NOT NULL DEFAULT 0,
    total_equity NUMERIC(20,6) NOT NULL,
    daily_return DOUBLE PRECISION,
    cumulative_return DOUBLE PRECISION,
    drawdown DOUBLE PRECISION,
    turnover NUMERIC(20,6) NOT NULL DEFAULT 0,
    valuation_status VARCHAR(30) NOT NULL DEFAULT 'VALID',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, trade_date)
);

CREATE TABLE IF NOT EXISTS backtest.metric (
    run_id UUID NOT NULL REFERENCES backtest.run_manifest(run_id),
    metric_name VARCHAR(100) NOT NULL,
    metric_value DOUBLE PRECISION,
    metric_text TEXT,
    parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, metric_name)
);

-- Dataset/Run 状态约束。FROZEN Dataset 和 SUCCEEDED Run 只能读取，不能回改。
CREATE OR REPLACE FUNCTION backtest.assert_dataset_manifest_state()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'backtest dataset manifest is append-only: %', OLD.dataset_id;
    END IF;
    IF OLD.status = 'FROZEN' THEN
        RAISE EXCEPTION 'FROZEN backtest dataset is immutable: %', OLD.dataset_id;
    END IF;
    IF OLD.status = 'VALIDATED' AND NEW.status = 'VALIDATED' THEN
        RAISE EXCEPTION 'VALIDATED backtest dataset is immutable before freeze: %', OLD.dataset_id;
    END IF;
    IF OLD.status = 'VALIDATED' AND NEW.status = 'FROZEN' THEN
        -- VALIDATED -> FROZEN 只允许发布状态、冻结时间和代码血缘字段变化。
        -- 不能借最终冻结之机改写输入范围、哈希、质量报告或规则版本。
        IF NEW.dataset_id IS DISTINCT FROM OLD.dataset_id
           OR NEW.dataset_version IS DISTINCT FROM OLD.dataset_version
           OR NEW.dataset_name IS DISTINCT FROM OLD.dataset_name
           OR NEW.data_baseline_tag IS DISTINCT FROM OLD.data_baseline_tag
           OR NEW.baseline_quality_report_hash IS DISTINCT FROM OLD.baseline_quality_report_hash
           OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
           OR NEW.adapter_version IS DISTINCT FROM OLD.adapter_version
           OR NEW.feature_version IS DISTINCT FROM OLD.feature_version
           OR NEW.price_adjustment_mode IS DISTINCT FROM OLD.price_adjustment_mode
           OR NEW.warmup_start IS DISTINCT FROM OLD.warmup_start
           OR NEW.evaluation_start IS DISTINCT FROM OLD.evaluation_start
           OR NEW.evaluation_end IS DISTINCT FROM OLD.evaluation_end
           OR NEW.calendar_rule_version IS DISTINCT FROM OLD.calendar_rule_version
           OR NEW.market_rule_version IS DISTINCT FROM OLD.market_rule_version
           OR NEW.fee_rule_version IS DISTINCT FROM OLD.fee_rule_version
           OR NEW.calendar_hash IS DISTINCT FROM OLD.calendar_hash
           OR NEW.rules_hash IS DISTINCT FROM OLD.rules_hash
           OR NEW.execution_hash IS DISTINCT FROM OLD.execution_hash
           OR NEW.corporate_action_hash IS DISTINCT FROM OLD.corporate_action_hash
           OR NEW.input_manifest_hash IS DISTINCT FROM OLD.input_manifest_hash
           OR NEW.selection_predicate IS DISTINCT FROM OLD.selection_predicate
           OR NEW.selected_columns IS DISTINCT FROM OLD.selected_columns
           OR NEW.hash_algorithm IS DISTINCT FROM OLD.hash_algorithm
           OR NEW.availability_rules IS DISTINCT FROM OLD.availability_rules
           OR NEW.quality_report IS DISTINCT FROM OLD.quality_report
           OR NEW.quality_report_hash IS DISTINCT FROM OLD.quality_report_hash
           OR NEW.created_at IS DISTINCT FROM OLD.created_at
           OR NEW.built_at IS DISTINCT FROM OLD.built_at
           OR NEW.validated_at IS DISTINCT FROM OLD.validated_at
           OR NEW.failed_at IS DISTINCT FROM OLD.failed_at
           OR NEW.failure_reason IS DISTINCT FROM OLD.failure_reason
           OR NEW.notes IS DISTINCT FROM OLD.notes THEN
            RAISE EXCEPTION 'VALIDATED Dataset 冻结时只允许状态/冻结时间/代码血缘字段变化: %', OLD.dataset_id;
        END IF;
    END IF;
    IF NEW.status <> OLD.status AND NOT (
        (OLD.status = 'DRAFT' AND NEW.status IN ('BUILDING','FAILED')) OR
        (OLD.status = 'FAILED' AND NEW.status IN ('BUILDING','FAILED')) OR
        (OLD.status = 'BUILDING' AND NEW.status IN ('VALIDATED','FAILED')) OR
        (OLD.status = 'VALIDATED' AND NEW.status = 'FROZEN')
    ) THEN
        RAISE EXCEPTION 'invalid backtest dataset status transition: % -> %', OLD.status, NEW.status;
    END IF;
    IF NEW.status = 'FROZEN' THEN
        IF OLD.status <> 'VALIDATED' OR NEW.input_manifest_hash IS NULL
           OR NEW.quality_report_hash IS NULL
           OR NEW.baseline_quality_report_hash IS NULL THEN
            RAISE EXCEPTION 'only validated dataset with hashes can be frozen: %', NEW.dataset_id;
        END IF;
        IF NEW.git_dirty AND NEW.code_archive_path IS NULL THEN
            RAISE EXCEPTION 'dirty code requires an explicit code archive before freezing: %', NEW.dataset_id;
        END IF;
        NEW.frozen_at := CURRENT_TIMESTAMP;
    ELSIF NEW.status = 'BUILDING' AND OLD.status IN ('DRAFT','FAILED') THEN
        NEW.built_at := CURRENT_TIMESTAMP;
    ELSIF NEW.status = 'VALIDATED' THEN
        NEW.validated_at := CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_dataset_manifest_state ON backtest.dataset_manifest;
CREATE TRIGGER trg_backtest_dataset_manifest_state
BEFORE UPDATE OR DELETE ON backtest.dataset_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_manifest_state();

-- Dataset 必须绑定创建时读取到的 validated 基线质量报告哈希，不能只绑定标签名。
CREATE OR REPLACE FUNCTION backtest.assert_dataset_baseline_binding()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    release_status VARCHAR(20);
    release_quality_hash CHAR(64);
BEGIN
    SELECT status, quality_report_hash
      INTO release_status, release_quality_hash
      FROM ops.tushare_data_release
     WHERE release_tag = NEW.data_baseline_tag;
    IF NEW.baseline_quality_report_hash IS NULL
       OR (release_status = 'validated'
           AND NEW.baseline_quality_report_hash IS DISTINCT FROM release_quality_hash)
       OR (NEW.status IN ('BUILDING', 'VALIDATED', 'FROZEN')
           AND release_status IS DISTINCT FROM 'validated') THEN
        RAISE EXCEPTION
            'Dataset baseline quality hash does not match validated release: %',
            NEW.data_baseline_tag;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_dataset_baseline_binding ON backtest.dataset_manifest;
CREATE TRIGGER trg_backtest_dataset_baseline_binding
BEFORE INSERT OR UPDATE ON backtest.dataset_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_baseline_binding();

CREATE OR REPLACE FUNCTION backtest.assert_dataset_child_mutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE dataset_status VARCHAR(20); dataset_id_value UUID;
BEGIN
    dataset_id_value := CASE WHEN TG_OP = 'DELETE' THEN OLD.dataset_id ELSE NEW.dataset_id END;
    SELECT status INTO dataset_status FROM backtest.dataset_manifest
    WHERE dataset_id = dataset_id_value;
    IF dataset_status IS NULL THEN
        RAISE EXCEPTION 'missing backtest dataset: %', dataset_id_value;
    END IF;
    IF dataset_status IN ('VALIDATED', 'FROZEN') THEN
        RAISE EXCEPTION 'validated/frozen backtest dataset child is immutable: %', dataset_id_value;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_dataset_member_mutable ON backtest.dataset_member;
CREATE TRIGGER trg_backtest_dataset_member_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.dataset_member
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_child_mutable();

DROP TRIGGER IF EXISTS trg_backtest_execution_bar_mutable ON backtest.dataset_execution_bar;
CREATE TRIGGER trg_backtest_execution_bar_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.dataset_execution_bar
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_child_mutable();

DROP TRIGGER IF EXISTS trg_backtest_calendar_mutable ON backtest.dataset_calendar;
CREATE TRIGGER trg_backtest_calendar_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.dataset_calendar
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_child_mutable();

DROP TRIGGER IF EXISTS trg_backtest_dividend_mutable ON backtest.dataset_dividend_event;
CREATE TRIGGER trg_backtest_dividend_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.dataset_dividend_event
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_child_mutable();

DROP TRIGGER IF EXISTS trg_backtest_rule_snapshot_mutable ON backtest.dataset_rule_snapshot;
CREATE TRIGGER trg_backtest_rule_snapshot_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.dataset_rule_snapshot
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_child_mutable();

CREATE OR REPLACE FUNCTION backtest.assert_run_manifest_state()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    dataset_status VARCHAR(20);
    dataset_input_hash CHAR(64);
    dataset_market_rule_version VARCHAR(80);
    dataset_fee_rule_version VARCHAR(80);
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.status IN ('RUNNING','SUCCEEDED') THEN
            RAISE EXCEPTION 'running/succeeded backtest run cannot be deleted: %', OLD.run_id;
        END IF;
        RETURN OLD;
    END IF;
    IF OLD.status = 'SUCCEEDED' THEN
        RAISE EXCEPTION 'succeeded backtest run is immutable: %', OLD.run_id;
    END IF;
    IF OLD.status = 'RUNNING' AND NEW.status IN ('SUCCEEDED', 'FAILED', 'CANCELLED') THEN
        -- 运行结束只能写入终态、结束时间和失败原因，运行输入本身不可改写。
         IF NEW.run_id IS DISTINCT FROM OLD.run_id
           OR NEW.dataset_id IS DISTINCT FROM OLD.dataset_id
           OR NEW.strategy_id IS DISTINCT FROM OLD.strategy_id
           OR NEW.run_version IS DISTINCT FROM OLD.run_version
           OR NEW.initial_cash IS DISTINCT FROM OLD.initial_cash
           OR NEW.max_positions IS DISTINCT FROM OLD.max_positions
           OR NEW.max_single_position_pct IS DISTINCT FROM OLD.max_single_position_pct
           OR NEW.max_portfolio_risk_pct IS DISTINCT FROM OLD.max_portfolio_risk_pct
           OR NEW.reserve_cash_pct IS DISTINCT FROM OLD.reserve_cash_pct
           OR NEW.market_rule_version IS DISTINCT FROM OLD.market_rule_version
           OR NEW.fee_rule_version IS DISTINCT FROM OLD.fee_rule_version
           OR NEW.execution_model_version IS DISTINCT FROM OLD.execution_model_version
           OR NEW.random_seed IS DISTINCT FROM OLD.random_seed
           OR NEW.parameters IS DISTINCT FROM OLD.parameters
           OR NEW.parameters_hash IS DISTINCT FROM OLD.parameters_hash
           OR NEW.input_hash IS DISTINCT FROM OLD.input_hash
           OR NEW.code_version IS DISTINCT FROM OLD.code_version
           OR NEW.git_commit IS DISTINCT FROM OLD.git_commit
           OR NEW.git_dirty IS DISTINCT FROM OLD.git_dirty
           OR NEW.diff_hash IS DISTINCT FROM OLD.diff_hash
           OR NEW.code_archive_path IS DISTINCT FROM OLD.code_archive_path
           OR NEW.created_at IS DISTINCT FROM OLD.created_at
           OR NEW.started_at IS DISTINCT FROM OLD.started_at THEN
            RAISE EXCEPTION 'RUNNING 回测结束时不允许改写运行输入: %', OLD.run_id;
        END IF;
    END IF;
    IF NEW.status <> OLD.status AND NOT (
        (OLD.status = 'DRAFT' AND NEW.status IN ('RUNNING','FAILED','CANCELLED')) OR
        (OLD.status = 'RUNNING' AND NEW.status IN ('SUCCEEDED','FAILED','CANCELLED')) OR
        (OLD.status = 'FAILED' AND NEW.status = 'RUNNING')
    ) THEN
        RAISE EXCEPTION 'invalid backtest run status transition: % -> %', OLD.status, NEW.status;
    END IF;
    IF NEW.status IN ('RUNNING','SUCCEEDED') THEN
        SELECT status, input_manifest_hash, market_rule_version, fee_rule_version
          INTO dataset_status, dataset_input_hash,
               dataset_market_rule_version, dataset_fee_rule_version
          FROM backtest.dataset_manifest
         WHERE dataset_id = NEW.dataset_id;
        IF dataset_status IS DISTINCT FROM 'FROZEN' THEN
            RAISE EXCEPTION 'formal backtest run requires FROZEN Dataset: % (%)',
                NEW.dataset_id, dataset_status;
        END IF;
        IF NEW.input_hash IS NULL OR NEW.parameters_hash IS NULL THEN
            RAISE EXCEPTION 'formal backtest run requires input and parameters hashes: %', NEW.run_id;
        END IF;
        IF NEW.input_hash IS DISTINCT FROM dataset_input_hash THEN
            RAISE EXCEPTION 'Run input_hash must equal Dataset input_manifest_hash: %', NEW.run_id;
        END IF;
        IF NEW.market_rule_version IS DISTINCT FROM dataset_market_rule_version
           OR NEW.fee_rule_version IS DISTINCT FROM dataset_fee_rule_version THEN
            RAISE EXCEPTION 'Run rule versions must equal Dataset rule versions: %', NEW.run_id;
        END IF;
        IF NEW.git_dirty AND NEW.code_archive_path IS NULL THEN
            RAISE EXCEPTION 'dirty code requires an explicit code archive before running: %', NEW.run_id;
        END IF;
    END IF;
    IF NEW.status = 'RUNNING' AND OLD.status IN ('DRAFT','FAILED') THEN
        NEW.started_at := CURRENT_TIMESTAMP;
    ELSIF NEW.status IN ('SUCCEEDED','FAILED','CANCELLED') AND OLD.status = 'RUNNING' THEN
        NEW.finished_at := CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_run_manifest_state ON backtest.run_manifest;
CREATE TRIGGER trg_backtest_run_manifest_state
BEFORE UPDATE OR DELETE ON backtest.run_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_run_manifest_state();

CREATE OR REPLACE FUNCTION backtest.assert_run_child_mutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE run_status VARCHAR(20); run_id_value UUID;
BEGIN
    run_id_value := CASE WHEN TG_OP = 'DELETE' THEN OLD.run_id ELSE NEW.run_id END;
    SELECT status INTO run_status FROM backtest.run_manifest WHERE run_id = run_id_value;
    IF run_status IS NULL THEN
        RAISE EXCEPTION 'missing backtest run: %', run_id_value;
    END IF;
    IF run_status <> 'RUNNING' THEN
        RAISE EXCEPTION 'backtest run child rows are writable only while RUNNING: % (%)',
            run_id_value, run_status;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_signal_mutable ON backtest.signal;
CREATE TRIGGER trg_backtest_signal_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.signal
FOR EACH ROW EXECUTE FUNCTION backtest.assert_run_child_mutable();

DROP TRIGGER IF EXISTS trg_backtest_order_mutable ON backtest.order;
CREATE TRIGGER trg_backtest_order_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.order
FOR EACH ROW EXECUTE FUNCTION backtest.assert_run_child_mutable();

DROP TRIGGER IF EXISTS trg_backtest_fill_mutable ON backtest.fill;
CREATE TRIGGER trg_backtest_fill_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.fill
FOR EACH ROW EXECUTE FUNCTION backtest.assert_run_child_mutable();

DROP TRIGGER IF EXISTS trg_backtest_position_lot_mutable ON backtest.position_lot;
CREATE TRIGGER trg_backtest_position_lot_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.position_lot
FOR EACH ROW EXECUTE FUNCTION backtest.assert_run_child_mutable();

DROP TRIGGER IF EXISTS trg_backtest_position_daily_mutable ON backtest.position_daily;
CREATE TRIGGER trg_backtest_position_daily_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.position_daily
FOR EACH ROW EXECUTE FUNCTION backtest.assert_run_child_mutable();

DROP TRIGGER IF EXISTS trg_backtest_cash_ledger_mutable ON backtest.cash_ledger;
CREATE TRIGGER trg_backtest_cash_ledger_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.cash_ledger
FOR EACH ROW EXECUTE FUNCTION backtest.assert_run_child_mutable();

DROP TRIGGER IF EXISTS trg_backtest_performance_daily_mutable ON backtest.performance_daily;
CREATE TRIGGER trg_backtest_performance_daily_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.performance_daily
FOR EACH ROW EXECUTE FUNCTION backtest.assert_run_child_mutable();

DROP TRIGGER IF EXISTS trg_backtest_metric_mutable ON backtest.metric;
CREATE TRIGGER trg_backtest_metric_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.metric
FOR EACH ROW EXECUTE FUNCTION backtest.assert_run_child_mutable();
