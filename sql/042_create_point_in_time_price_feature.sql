-- 阶段三：时点公司行为调整价格 Feature v2。
-- 只新增派生表，不修改/删除任何 Raw、Standard、PIT 或既有 v1 Feature。
-- v2 使用 ex_date <= trade_date 的“实施”公司行为构造时点总回报序列，
-- 避免把研究截止日之后的公司行为带入历史技术指标。

CREATE TABLE IF NOT EXISTS feature.price_adjustment_factor_asof (
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    asof_adjustment_factor DOUBLE PRECISION,
    asof_close DOUBLE PRECISION,
    event_count INTEGER NOT NULL DEFAULT 0,
    source_event_max_ex_date DATE,
    adjustment_mode VARCHAR(50) NOT NULL DEFAULT 'point_in_time_total_return',
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    source_asof_date DATE,
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_price_adjustment_factor_asof_date
    ON feature.price_adjustment_factor_asof (trade_date, ts_code);

CREATE TABLE IF NOT EXISTS feature.price_daily_v2 (
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    raw_close DOUBLE PRECISION,
    asof_close DOUBLE PRECISION,
    asof_adjustment_factor DOUBLE PRECISION,
    return_1d DOUBLE PRECISION,
    return_20d DOUBLE PRECISION,
    return_60d DOUBLE PRECISION,
    ma20 DOUBLE PRECISION,
    ma60 DOUBLE PRECISION,
    ma120 DOUBLE PRECISION,
    volatility_20d DOUBLE PRECISION,
    amount_ma20 DOUBLE PRECISION,
    volume_ratio_20d DOUBLE PRECISION,
    available_observation_count INTEGER,
    ma20_observation_count INTEGER,
    ma60_observation_count INTEGER,
    ma120_observation_count INTEGER,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    feature_set_version VARCHAR(30) NOT NULL DEFAULT 'price_daily_v2',
    adjustment_mode VARCHAR(50) NOT NULL DEFAULT 'point_in_time_total_return',
    source_asof_date DATE,
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_feature_price_daily_v2_date
    ON feature.price_daily_v2 (trade_date, ts_code);
