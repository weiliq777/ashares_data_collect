-- Financial PIT/Feature columns for explicit revenue metric semantics and lineage.
ALTER TABLE pit.financial_asof
    ADD COLUMN IF NOT EXISTS or_yoy DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS tr_yoy DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS q_sales_yoy DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS payload_hash CHAR(64);

ALTER TABLE feature.company_financial_v1
    ADD COLUMN IF NOT EXISTS or_yoy DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS tr_yoy DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS q_sales_yoy DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS payload_hash CHAR(64);
