-- Tushare stock_basic daily industry capture (每日行业名称观测)
-- The endpoint returns a current industry name, not a stable industry code or
-- an official effective date. Preserve the observed name without inventing a
-- code; rows remain DAILY_CAPTURED and are not accepted as validated PIT data.

BEGIN;

ALTER TABLE pit.industry_membership_daily
    ALTER COLUMN industry_code DROP NOT NULL;

COMMENT ON COLUMN pit.industry_membership_daily.industry_code IS
    '行业代码；Tushare stock_basic 仅提供行业名称，因此每日观测允许为空，禁止伪造代码。';
COMMENT ON COLUMN pit.industry_membership_daily.industry_system IS
    '行业分类来源/体系；TUSHARE_STOCK_BASIC 表示供应商当前名称观测，不代表官方生效区间。';
COMMENT ON COLUMN pit.industry_membership_daily.quality_status IS
    'DAILY_CAPTURED 为仓库每日观察证据，不能直接作为经验证 PIT；UNKNOWN 表示接口未提供行业名称。';

COMMIT;
