-- Research v1.3 使用 feature.price_daily_v2 的时点总回报指标。
-- 只扩展 Snapshot 字段，不修改/删除已有快照数据。

ALTER TABLE research.stock_snapshot_daily
    ADD COLUMN IF NOT EXISTS price_feature_version VARCHAR(40),
    ADD COLUMN IF NOT EXISTS price_adjustment_mode VARCHAR(80);

CREATE INDEX IF NOT EXISTS idx_research_snapshot_price_feature_version
    ON research.stock_snapshot_daily (snapshot_id, price_feature_version);
