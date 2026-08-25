-- Feature contract for reusable technical indicators previously calculated in jobs/wave3.py.
CREATE TABLE IF NOT EXISTS feature.technical_daily_v1 (
    stock_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    wr10 DOUBLE PRECISION,
    wr20 DOUBLE PRECISION,
    rsi9 DOUBLE PRECISION,
    k DOUBLE PRECISION,
    d DOUBLE PRECISION,
    j DOUBLE PRECISION,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'UNVERIFIED',
    feature_contract_version VARCHAR(50) NOT NULL DEFAULT 'technical_daily_v1',
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_technical_daily_v1_date
    ON feature.technical_daily_v1 (trade_date, stock_code);
