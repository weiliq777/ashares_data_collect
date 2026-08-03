-- 每次抓取的不可变版本记录。现有 *_raw 表继续保留首次业务记录，禁止更新或删除。
CREATE TABLE IF NOT EXISTS tushare_market_raw_version (
    record_id BIGSERIAL PRIMARY KEY,
    dataset VARCHAR(40) NOT NULL,
    business_key VARCHAR(120) NOT NULL,
    ts_code VARCHAR(20),
    trade_date DATE,
    exchange VARCHAR(10),
    cal_date DATE,
    payload JSONB NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'tushare',
    batch_id VARCHAR(80) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset, business_key, payload_hash)
);

CREATE INDEX IF NOT EXISTS idx_tushare_raw_version_dataset_date
    ON tushare_market_raw_version (dataset, trade_date, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_tushare_raw_version_business
    ON tushare_market_raw_version (dataset, business_key, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_tushare_raw_version_batch
    ON tushare_market_raw_version (batch_id);
