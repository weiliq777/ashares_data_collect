-- Production quality invariants are expected to return zero rows. Scanning
-- multi-million-row feature/PIT tables for every release gets slower as
-- healthy data grows. Partial indexes contain only violating rows, so normal
-- writes add no entries and the exact fail-closed predicates stay cheap.
-- Run outside an explicit transaction because these builds are CONCURRENTLY.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_daily_v2_immature_violation
    ON feature.price_daily_v2 (trade_date)
    WHERE (ma20 IS NOT NULL AND COALESCE(ma20_observation_count, 0) < 20)
       OR (ma60 IS NOT NULL AND COALESCE(ma60_observation_count, 0) < 60)
       OR (ma120 IS NOT NULL AND COALESCE(ma120_observation_count, 0) < 120);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_valuation_daily_v1_immature_violation
    ON feature.valuation_daily_v1 (trade_date)
    WHERE pe_pct_5y IS NOT NULL
      AND (
          NOT COALESCE(has_full_pe_5y_history, FALSE)
          OR COALESCE(pe_observation_count_5y, 0) < 1000
          OR pe_window_start_date_5y IS NULL
          OR trade_date - pe_window_start_date_5y < 1794
      );

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_financial_asof_future_violation
    ON pit.financial_asof (decision_date)
    WHERE (effective_announce_date IS NOT NULL AND effective_announce_date > decision_date)
       OR (version_available_date IS NOT NULL AND version_available_date > decision_date);
