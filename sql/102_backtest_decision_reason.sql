-- 回测逐日决策解释字段。
-- 该迁移只扩展现有 backtest.performance_daily，不改变策略、数据集或历史结果。
BEGIN;

ALTER TABLE backtest.performance_daily
    ADD COLUMN IF NOT EXISTS decision_reason JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN backtest.performance_daily.decision_reason IS
    '每个评价日的有界决策解释：信号日的门禁统计/动作，非信号日的明确无动作原因；不参与交易决策';

COMMIT;
