-- Safe retention cleanup performed after a read-only dependency audit.
--
-- This migration deliberately does not delete formal Raw, Standard, PIT,
-- Feature, Research, lineage, or rollback backup data.  It only removes
-- generated smoke-test relations with no inbound foreign keys and updates a
-- compatibility view whose old source was preventing legacy daily-kline
-- retirement.

CREATE OR REPLACE VIEW mart.v_price_feature AS
 SELECT p.ts_code,
    p.trade_date,
    p.raw_close,
    p.asof_close,
    p.ma20,
    p.ma60,
    p.ma120,
    p.volume_ratio_20d,
    p.volatility_20d,
    v.pe_ttm,
    v.pe_pct_5y,
    v.pb_pct_5y,
    v.is_pe_meaningful,
    v.valuation_observation_count,
    p.quality_status AS price_quality_status,
    v.quality_status AS valuation_quality_status,
    p.feature_set_version AS price_feature_version,
    v.feature_set_version AS valuation_feature_version,
    p.source_asof_date AS price_source_asof_date,
    v.source_asof_date AS valuation_source_asof_date,
    v.feature_contract_version,
    k.open AS raw_open,
    k.high AS raw_high,
    k.low AS raw_low,
    k.amount,
    p.amount_ma20
   FROM feature.price_daily_v2 p
     LEFT JOIN std.daily_kline k
       ON k.stock_code::text = p.ts_code::text
      AND k.trade_date = p.trade_date
     LEFT JOIN feature.valuation_daily_v1 v
       ON v.ts_code::text = p.ts_code::text
      AND v.trade_date = p.trade_date;

DROP TABLE IF EXISTS feature.company_financial_v2_smoke_61367278809e;
DROP TABLE IF EXISTS feature.company_financial_v2_smoke_a09297afe4f3;
DROP TABLE IF EXISTS feature.company_financial_v2_smoke_ad50d1bcfcce;
DROP TABLE IF EXISTS feature.company_financial_v2_smoke_d008f20322c8;
DROP TABLE IF EXISTS feature.company_financial_v2_smoke_e67a5414f359;
DROP TABLE IF EXISTS feature.valuation_daily_v1_smoke_61367278809e;
DROP TABLE IF EXISTS feature.valuation_daily_v1_smoke_a09297afe4f3;
DROP TABLE IF EXISTS feature.valuation_daily_v1_smoke_ad50d1bcfcce;
DROP TABLE IF EXISTS feature.valuation_daily_v1_smoke_d008f20322c8;
DROP TABLE IF EXISTS feature.valuation_daily_v1_smoke_e67a5414f359;
