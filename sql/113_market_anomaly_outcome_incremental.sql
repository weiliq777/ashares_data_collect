-- Market Anomaly Outcome (市场异常后验结果) incremental maturity contract.
-- This is metadata-only for existing rows. Do not run a historical replay here.
-- Run outside an explicit transaction because the replacement indexes are
-- built CONCURRENTLY; the existing unique constraint stays in place until the
-- new revision-aware index is ready.

ALTER TABLE evaluation.market_anomaly_outcome
    ADD COLUMN IF NOT EXISTS anchor_adj_close_base DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS anchor_total_return_price DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS anchor_benchmark_close DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS next_evaluation_date DATE,
    ADD COLUMN IF NOT EXISTS horizon_status JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS input_provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS missing_reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS outcome_revision INTEGER NOT NULL DEFAULT 1;

ALTER TABLE evaluation.market_anomaly_outcome
    DROP CONSTRAINT IF EXISTS outcome_maturity_status,
    ADD CONSTRAINT outcome_maturity_status
        CHECK (maturity_status IN (
            'PENDING_MATURITY', 'MATURED', 'INCOMPLETE', 'INVALID_INPUT'
        )) NOT VALID;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid='evaluation.market_anomaly_outcome'::regclass
           AND conname='outcome_revision_positive'
    ) THEN
        ALTER TABLE evaluation.market_anomaly_outcome
            ADD CONSTRAINT outcome_revision_positive
            CHECK (outcome_revision > 0) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid='evaluation.market_anomaly_outcome'::regclass
           AND conname='outcome_anchor_prices_positive'
    ) THEN
        ALTER TABLE evaluation.market_anomaly_outcome
            ADD CONSTRAINT outcome_anchor_prices_positive
            CHECK (
                (anchor_adj_close_base IS NULL OR anchor_adj_close_base > 0)
                AND (anchor_total_return_price IS NULL OR anchor_total_return_price > 0)
                AND (anchor_benchmark_close IS NULL OR anchor_benchmark_close > 0)
            ) NOT VALID;
    END IF;
END;
$$;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_evaluation_outcome_revision
    ON evaluation.market_anomaly_outcome
       (observation_id, evaluation_methodology_hash, outcome_revision);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid='evaluation.market_anomaly_outcome'::regclass
           AND conname='outcome_identity'
    ) THEN
        ALTER TABLE evaluation.market_anomaly_outcome
            DROP CONSTRAINT outcome_identity;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid='evaluation.market_anomaly_outcome'::regclass
           AND conname='outcome_identity_revision'
    ) THEN
        ALTER TABLE evaluation.market_anomaly_outcome
            ADD CONSTRAINT outcome_identity_revision
            UNIQUE USING INDEX uq_evaluation_outcome_revision;
    END IF;
END;
$$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_evaluation_outcome_next_due
    ON evaluation.market_anomaly_outcome
       (evaluation_methodology_hash, next_evaluation_date, outcome_id)
    WHERE maturity_status='PENDING_MATURITY'
      AND next_evaluation_date IS NOT NULL;

COMMENT ON COLUMN evaluation.market_anomaly_outcome.anchor_adj_close_base
    IS '观察日的不可变底座复权价格锚点，由首次质量门禁通过时捕获。';
COMMENT ON COLUMN evaluation.market_anomaly_outcome.anchor_total_return_price
    IS '观察日的不可变总回报价格锚点，由首次质量门禁通过时捕获。';
COMMENT ON COLUMN evaluation.market_anomaly_outcome.anchor_benchmark_close
    IS '观察日基准指数收盘价锚点，绑定当前后验结果输入清单。';
COMMENT ON COLUMN evaluation.market_anomaly_outcome.next_evaluation_date
    IS '下一个应评估日期；待成熟部分索引使用，不扫描历史结果。';
COMMENT ON COLUMN evaluation.market_anomaly_outcome.horizon_status
    IS '各交易期限独立的目标日期、成熟状态和缺失原因。';
COMMENT ON COLUMN evaluation.market_anomaly_outcome.input_provenance
    IS '观察锚点与各期限输入的来源、质量和稳定哈希摘要。';
COMMENT ON COLUMN evaluation.market_anomaly_outcome.outcome_revision
    IS '显式来源修订号；普通重试保持不变，旧修订保留。';

GRANT SELECT, INSERT, UPDATE ON evaluation.market_anomaly_outcome TO ashares_app;
GRANT SELECT ON evaluation.market_anomaly_outcome TO research_reader;
