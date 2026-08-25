-- Stage 2: application-facing mart contract views.
--
-- This migration is metadata-only: it creates a read-only projection over
-- existing feature tables.  It does not copy, update, delete, or rewrite
-- feature/snapshot rows.  Consumers must query the view through ReadGateway
-- with a bounded WorkScope.

CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE VIEW mart.v_price_feature AS
SELECT
    p.ts_code,
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
FROM feature.price_daily_v2 AS p
LEFT JOIN std.daily_kline AS k
       ON k.stock_code = p.ts_code
      AND k.trade_date = p.trade_date
LEFT JOIN feature.valuation_daily_v1 AS v
       ON v.ts_code = p.ts_code
      AND v.trade_date = p.trade_date;

GRANT USAGE ON SCHEMA mart TO ashares_app;
GRANT SELECT ON mart.v_price_feature TO ashares_app;
