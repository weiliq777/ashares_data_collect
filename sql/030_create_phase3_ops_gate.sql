-- 阶段三运行门禁：市场覆盖率与熔断记录。
-- 只记录同步任务的质量判断，不修改或删除任何 Raw/Standard/PIT/Feature 数据。

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.market_data_quality_gate (
    gate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset VARCHAR(40) NOT NULL,
    trade_date DATE NOT NULL,
    actual_rows INTEGER NOT NULL,
    expected_rows NUMERIC(12, 2),
    baseline_days INTEGER NOT NULL DEFAULT 0,
    threshold_pct NUMERIC(8, 6) NOT NULL DEFAULT 0.01,
    drop_pct NUMERIC(8, 6),
    status VARCHAR(24) NOT NULL,
    sample_mode BOOLEAN NOT NULL DEFAULT FALSE,
    batch_id VARCHAR(80) NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset, trade_date, batch_id),
    CHECK (actual_rows >= 0),
    CHECK (baseline_days >= 0),
    CHECK (threshold_pct >= 0 AND threshold_pct < 1),
    CHECK (status IN ('PASS', 'BLOCKED', 'NO_BASELINE', 'SKIPPED_SAMPLE'))
);

CREATE INDEX IF NOT EXISTS idx_market_data_quality_gate_date
    ON ops.market_data_quality_gate (trade_date, dataset, created_at DESC);

CREATE OR REPLACE FUNCTION ops.assert_market_data_quality_gate_append_only()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        RAISE EXCEPTION 'ops.market_data_quality_gate 只允许追加，禁止 UPDATE/DELETE';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_market_data_quality_gate_append_only
    ON ops.market_data_quality_gate;
CREATE TRIGGER trg_market_data_quality_gate_append_only
    BEFORE UPDATE OR DELETE ON ops.market_data_quality_gate
    FOR EACH ROW EXECUTE FUNCTION ops.assert_market_data_quality_gate_append_only();

-- 生效规则也是版本化输入事实：修正规则必须新增 rule_version，不能覆盖历史。
CREATE OR REPLACE FUNCTION backtest.assert_market_rule_append_only()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'backtest.market_rule_effective 只允许追加，修正必须新增 rule_version';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_market_rule_effective_append_only
    ON backtest.market_rule_effective;
CREATE TRIGGER trg_market_rule_effective_append_only
    BEFORE UPDATE OR DELETE ON backtest.market_rule_effective
    FOR EACH ROW EXECUTE FUNCTION backtest.assert_market_rule_append_only();

-- 同一市场/市场规则/费用规则的有效日期区间不得重叠；ALL 与具体市场仍可并存。
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = 'backtest'
          AND r.relname = 'market_rule_effective'
          AND c.conname = 'market_rule_effective_no_overlap'
    ) THEN
        ALTER TABLE backtest.market_rule_effective
            ADD CONSTRAINT market_rule_effective_no_overlap
            EXCLUDE USING gist (
                market WITH =,
                market_rule_version WITH =,
                fee_rule_version WITH =,
                daterange(effective_from, effective_to, '[]') WITH &&
            );
    END IF;
END $$;

DO $$
BEGIN
    ALTER TABLE backtest.market_rule_effective
        ADD CONSTRAINT market_rule_effective_provenance_check
        CHECK (parameters ? 'provenance');
EXCEPTION WHEN duplicate_object THEN
    NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE backtest.dataset_rule_snapshot
        ADD CONSTRAINT dataset_rule_snapshot_provenance_check
        CHECK (parameters ? 'provenance');
EXCEPTION WHEN duplicate_object THEN
    NULL;
END $$;
