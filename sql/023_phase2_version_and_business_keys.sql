-- 阶段二 T-013：财务 Standard 血缘与分红业务事件键。
-- 只新增派生层字段/索引，不修改或删除任何 Raw 数据。

ALTER TABLE financial_income_standard ADD COLUMN IF NOT EXISTS payload_hash CHAR(64);
ALTER TABLE financial_balance_sheet_standard ADD COLUMN IF NOT EXISTS payload_hash CHAR(64);
ALTER TABLE financial_cash_flow_standard ADD COLUMN IF NOT EXISTS payload_hash CHAR(64);

ALTER TABLE std.dividend_event ADD COLUMN IF NOT EXISTS business_key VARCHAR(180);
ALTER TABLE std.dividend_event ADD COLUMN IF NOT EXISTS payload_hash CHAR(64);

CREATE INDEX IF NOT EXISTS idx_financial_income_payload_hash
    ON financial_income_standard (payload_hash);
CREATE INDEX IF NOT EXISTS idx_financial_balance_payload_hash
    ON financial_balance_sheet_standard (payload_hash);
CREATE INDEX IF NOT EXISTS idx_financial_cashflow_payload_hash
    ON financial_cash_flow_standard (payload_hash);
CREATE INDEX IF NOT EXISTS idx_dividend_event_business_key
    ON std.dividend_event (business_key);

