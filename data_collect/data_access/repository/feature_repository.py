"""FeatureRepository：特征领域仓储。

迁移自 tushare_phase2_features.rebuild_price_v2（分批 scoped merge）。
"""
from __future__ import annotations

from datetime import timedelta

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.repository.base import BaseRepository

_PRICE_V2_ADJUSTMENT_SQL = """DELETE FROM feature.price_adjustment_factor_asof WHERE ts_code=ANY(%s) AND trade_date BETWEEN %s AND %s;
WITH events AS (
        SELECT ts_code, ex_date,
               COALESCE(SUM(cash_div), 0.0) AS cash_div,
               COALESCE(SUM(stock_div), 0.0) AS stock_rate,
               COUNT(*)::integer AS event_count
          FROM std.dividend_event
         WHERE implementation_status = '实施'
           AND ex_date IS NOT NULL
           AND COALESCE(implementation_announce_date, announce_date,
                        report_date, ex_date) <= ex_date
           AND ts_code=ANY(%s)
         GROUP BY ts_code, ex_date
    ), raw AS (
        SELECT d.stock_code AS ts_code, d.trade_date, d.close AS raw_close,
               d.amount, d.volume,
               LAG(d.close) OVER w AS prev_close,
               FIRST_VALUE(d.close) OVER w AS anchor_close,
               COALESCE(e.cash_div, 0.0) AS cash_div,
               COALESCE(e.stock_rate, 0.0) AS stock_rate,
               COALESCE(e.event_count, 0) AS event_count,
               e.ex_date AS event_ex_date
          FROM std.daily_kline d
          LEFT JOIN events e
            ON e.ts_code = d.stock_code AND e.ex_date = d.trade_date
         WHERE d.stock_code=ANY(%s) AND d.trade_date<=%s
         WINDOW w AS (PARTITION BY d.stock_code ORDER BY d.trade_date
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ), growth AS (
        SELECT *,
               CASE
                 WHEN raw_close IS NULL OR raw_close <= 0 THEN NULL
                 WHEN prev_close IS NULL THEN 1.0
                 WHEN prev_close <= 0 THEN NULL
                 WHEN event_count > 0
                   THEN (raw_close * (1.0 + stock_rate) + cash_div) / prev_close
                 ELSE raw_close / prev_close
               END AS growth_factor
          FROM raw
    ), indexed AS (
        SELECT *,
               SUM(CASE WHEN growth_factor > 0 THEN LN(growth_factor) END)
                 OVER (PARTITION BY ts_code ORDER BY trade_date
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS log_index,
               COUNT(*) FILTER (WHERE growth_factor IS NULL OR growth_factor <= 0)
                 OVER (PARTITION BY ts_code ORDER BY trade_date
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS invalid_count,
               MAX(event_ex_date) OVER (PARTITION BY ts_code ORDER BY trade_date
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_event_ex_date
          FROM growth
    ), adjusted AS (
        SELECT ts_code, trade_date, raw_close, amount, volume,
               CASE WHEN invalid_count = 0 AND log_index IS NOT NULL
                    THEN anchor_close * EXP(log_index) END AS asof_close,
               event_count, max_event_ex_date
          FROM indexed
    )
    INSERT INTO feature.price_adjustment_factor_asof
        (ts_code, trade_date, asof_adjustment_factor, asof_close,
         event_count, source_event_max_ex_date, adjustment_mode,
         quality_status, source_asof_date)
    SELECT ts_code, trade_date,
           asof_close / NULLIF(raw_close, 0), asof_close,
           event_count, max_event_ex_date, 'point_in_time_total_return',
           CASE WHEN asof_close IS NULL THEN 'MISSING_SOURCE'
                WHEN raw_close IS NULL OR raw_close <= 0 THEN 'WARNING'
                ELSE 'VALID' END,
           trade_date
      FROM adjusted
      WHERE trade_date BETWEEN %s AND %s
    ON CONFLICT (ts_code, trade_date) DO UPDATE SET
        asof_adjustment_factor=EXCLUDED.asof_adjustment_factor,
        asof_close=EXCLUDED.asof_close, event_count=EXCLUDED.event_count,
        source_event_max_ex_date=EXCLUDED.source_event_max_ex_date,
        adjustment_mode=EXCLUDED.adjustment_mode,
        quality_status=EXCLUDED.quality_status,
        source_asof_date=EXCLUDED.source_asof_date,
        calculated_at=CURRENT_TIMESTAMP
"""

_PRICE_V2_FEATURE_SQL = """DELETE FROM feature.price_daily_v2 WHERE ts_code=ANY(%s) AND trade_date BETWEEN %s AND %s;
WITH base AS (
        SELECT d.stock_code AS ts_code, d.trade_date, d.close AS raw_close,
               d.amount, d.volume, a.asof_close,
               a.asof_adjustment_factor
          FROM std.daily_kline d
          JOIN feature.price_adjustment_factor_asof a
            ON a.ts_code = d.stock_code AND a.trade_date = d.trade_date
         WHERE d.stock_code=ANY(%s) AND d.trade_date<=%s
    ), returns AS (
        SELECT *,
            asof_close / NULLIF(LAG(asof_close, 1) OVER w, 0) - 1 AS return_1d,
            asof_close / NULLIF(LAG(asof_close, 20) OVER w, 0) - 1 AS return_20d,
            asof_close / NULLIF(LAG(asof_close, 60) OVER w, 0) - 1 AS return_60d
          FROM base
          WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
    ), metrics AS (
        SELECT *,
            AVG(asof_close) OVER (PARTITION BY ts_code ORDER BY trade_date
                                  ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20_raw,
            AVG(asof_close) OVER (PARTITION BY ts_code ORDER BY trade_date
                                  ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60_raw,
            AVG(asof_close) OVER (PARTITION BY ts_code ORDER BY trade_date
                                  ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120_raw,
            STDDEV_SAMP(return_1d) OVER (PARTITION BY ts_code ORDER BY trade_date
                                         ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS volatility_20d_raw,
            AVG(amount) OVER (PARTITION BY ts_code ORDER BY trade_date
                              ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS amount_ma20_raw,
            AVG(volume) OVER (PARTITION BY ts_code ORDER BY trade_date
                              ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS volume_ma20_raw,
            COUNT(asof_close) OVER (PARTITION BY ts_code ORDER BY trade_date
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS available_observation_count,
            COUNT(asof_close) OVER (PARTITION BY ts_code ORDER BY trade_date
                                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20_observation_count,
            COUNT(asof_close) OVER (PARTITION BY ts_code ORDER BY trade_date
                                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60_observation_count,
            COUNT(asof_close) OVER (PARTITION BY ts_code ORDER BY trade_date
                                    ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120_observation_count
          FROM returns
    )
    INSERT INTO feature.price_daily_v2
        (ts_code, trade_date, raw_close, asof_close, asof_adjustment_factor,
         return_1d, return_20d, return_60d, ma20, ma60, ma120,
         volatility_20d, amount_ma20, volume_ratio_20d,
         available_observation_count, ma20_observation_count,
         ma60_observation_count, ma120_observation_count, quality_status,
         feature_set_version, adjustment_mode, source_asof_date)
    SELECT ts_code, trade_date, raw_close, asof_close, asof_adjustment_factor,
           return_1d, return_20d, return_60d,
           CASE WHEN ma20_observation_count = 20 THEN ma20_raw END,
           CASE WHEN ma60_observation_count = 60 THEN ma60_raw END,
           CASE WHEN ma120_observation_count = 120 THEN ma120_raw END,
           CASE WHEN ma20_observation_count = 20 THEN volatility_20d_raw END,
           CASE WHEN ma20_observation_count = 20 THEN amount_ma20_raw END,
           CASE WHEN ma20_observation_count = 20
                THEN volume / NULLIF(volume_ma20_raw, 0) END,
           available_observation_count, ma20_observation_count,
           ma60_observation_count, ma120_observation_count,
           CASE WHEN asof_close IS NULL THEN 'MISSING_SOURCE'
                WHEN raw_close IS NULL OR raw_close <= 0 THEN 'WARNING'
                WHEN ma60_observation_count < 60 THEN 'WARNING'
                ELSE 'VALID' END,
           'price_daily_v2', 'point_in_time_total_return', trade_date
      FROM metrics
      WHERE trade_date BETWEEN %s AND %s
    ON CONFLICT (ts_code, trade_date) DO UPDATE SET
        raw_close=EXCLUDED.raw_close, asof_close=EXCLUDED.asof_close,
        asof_adjustment_factor=EXCLUDED.asof_adjustment_factor,
        return_1d=EXCLUDED.return_1d, return_20d=EXCLUDED.return_20d,
        return_60d=EXCLUDED.return_60d, ma20=EXCLUDED.ma20,
        ma60=EXCLUDED.ma60, ma120=EXCLUDED.ma120,
        volatility_20d=EXCLUDED.volatility_20d,
        amount_ma20=EXCLUDED.amount_ma20,
        volume_ratio_20d=EXCLUDED.volume_ratio_20d,
        available_observation_count=EXCLUDED.available_observation_count,
        ma20_observation_count=EXCLUDED.ma20_observation_count,
        ma60_observation_count=EXCLUDED.ma60_observation_count,
        ma120_observation_count=EXCLUDED.ma120_observation_count,
        quality_status=EXCLUDED.quality_status,
        feature_set_version=EXCLUDED.feature_set_version,
        adjustment_mode=EXCLUDED.adjustment_mode,
        source_asof_date=EXCLUDED.source_asof_date,
        calculated_at=CURRENT_TIMESTAMP
"""

# Bounded production path.  The previous implementation calculated every
# historical row up to ``date_to`` for each code batch.  That was correct but
# made a one-day run scan the entire price history repeatedly.  This path uses
# one persisted adjustment seed immediately before the rebuild window, then
# reads only the affected range.  Corporate-action corrections still rebuild
# from the earliest affected ex-date through the requested end date; only the
# rolling feature warm-up is bounded.
_PRICE_V2_ADJUSTMENT_BOUNDED_SQL = """DELETE FROM feature.price_adjustment_factor_asof WHERE ts_code=ANY(%s) AND trade_date BETWEEN %s AND %s;
WITH seed AS (
    SELECT DISTINCT ON (d.stock_code)
           d.stock_code AS ts_code, d.trade_date AS seed_date,
           d.close AS seed_raw_close, a.asof_close AS seed_asof_close,
           a.source_event_max_ex_date AS seed_max_event
      FROM std.daily_kline d
      LEFT JOIN feature.price_adjustment_factor_asof a
        ON a.ts_code=d.stock_code AND a.trade_date=d.trade_date
     WHERE d.stock_code=ANY(%s) AND d.trade_date < %s
     ORDER BY d.stock_code, d.trade_date DESC
), events AS (
    SELECT ts_code, ex_date,
           COALESCE(SUM(cash_div), 0.0) AS cash_div,
           COALESCE(SUM(stock_div), 0.0) AS stock_rate,
           COUNT(*)::integer AS event_count
      FROM std.dividend_event
     WHERE implementation_status = '瀹炴柦' AND ex_date IS NOT NULL
       AND COALESCE(implementation_announce_date, announce_date,
                    report_date, ex_date) <= ex_date
       AND ts_code=ANY(%s) AND ex_date <= %s
     GROUP BY ts_code, ex_date
), raw AS (
    SELECT d.stock_code AS ts_code, d.trade_date, d.close AS raw_close,
           d.amount, d.volume, s.seed_asof_close, s.seed_max_event,
           s.seed_date,
           LAG(d.close) OVER w AS prev_close,
           ROW_NUMBER() OVER w AS row_no,
           COALESCE(e.cash_div, 0.0) AS cash_div,
           COALESCE(e.stock_rate, 0.0) AS stock_rate,
           COALESCE(e.event_count, 0) AS event_count,
           e.ex_date AS event_ex_date
      FROM std.daily_kline d
      LEFT JOIN seed s ON s.ts_code=d.stock_code
      LEFT JOIN events e ON e.ts_code=d.stock_code AND e.ex_date=d.trade_date
     WHERE d.stock_code=ANY(%s)
       AND d.trade_date >= COALESCE(s.seed_date, %s)
       AND d.trade_date <= %s
     WINDOW w AS (PARTITION BY d.stock_code ORDER BY d.trade_date)
), growth AS (
    SELECT *, CASE
        WHEN row_no=1 THEN 1.0
        WHEN raw_close IS NULL OR raw_close <= 0 OR prev_close IS NULL OR prev_close <= 0 THEN NULL
        WHEN event_count > 0 THEN (raw_close * (1.0 + stock_rate) + cash_div) / prev_close
        ELSE raw_close / prev_close END AS growth_factor
      FROM raw
), indexed AS (
    SELECT *,
           SUM(CASE WHEN growth_factor > 0 THEN LN(growth_factor) ELSE 0 END)
             OVER (PARTITION BY ts_code ORDER BY trade_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS log_index,
           COUNT(*) FILTER (WHERE growth_factor IS NULL OR growth_factor <= 0)
             OVER (PARTITION BY ts_code ORDER BY trade_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS invalid_count,
           MAX(event_ex_date) OVER (PARTITION BY ts_code ORDER BY trade_date
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS local_max_event
      FROM growth
), adjusted AS (
    SELECT ts_code, trade_date, raw_close, amount, volume, event_count,
           CASE WHEN invalid_count=0
                THEN COALESCE(MAX(seed_asof_close) OVER (PARTITION BY ts_code),
                              FIRST_VALUE(raw_close) OVER (PARTITION BY ts_code ORDER BY trade_date))
                     * EXP(log_index) END AS asof_close,
           GREATEST(MAX(seed_max_event) OVER (PARTITION BY ts_code), local_max_event) AS max_event
      FROM indexed
)
INSERT INTO feature.price_adjustment_factor_asof
    (ts_code, trade_date, asof_adjustment_factor, asof_close,
     event_count, source_event_max_ex_date, adjustment_mode,
     quality_status, source_asof_date)
SELECT ts_code, trade_date, asof_close/NULLIF(raw_close,0), asof_close,
       event_count, max_event, 'point_in_time_total_return',
       CASE WHEN asof_close IS NULL THEN 'MISSING_SOURCE'
            WHEN raw_close IS NULL OR raw_close <= 0 THEN 'WARNING' ELSE 'VALID' END,
       trade_date
  FROM adjusted WHERE trade_date BETWEEN %s AND %s
ON CONFLICT (ts_code, trade_date) DO UPDATE SET
    asof_adjustment_factor=EXCLUDED.asof_adjustment_factor,
    asof_close=EXCLUDED.asof_close, event_count=EXCLUDED.event_count,
    source_event_max_ex_date=EXCLUDED.source_event_max_ex_date,
    adjustment_mode=EXCLUDED.adjustment_mode, quality_status=EXCLUDED.quality_status,
    source_asof_date=EXCLUDED.source_asof_date, calculated_at=CURRENT_TIMESTAMP
"""

_PRICE_V2_FEATURE_BOUNDED_SQL = """DELETE FROM feature.price_daily_v2 WHERE ts_code=ANY(%s) AND trade_date BETWEEN %s AND %s;
WITH prior AS (
    SELECT ts_code, COALESCE(MAX(available_observation_count),0) AS prior_available
      FROM feature.price_daily_v2
     WHERE ts_code=ANY(%s) AND trade_date < %s
     GROUP BY ts_code
), base AS (
    SELECT d.stock_code AS ts_code, d.trade_date, d.close AS raw_close,
           d.amount, d.volume, a.asof_close, a.asof_adjustment_factor
      FROM std.daily_kline d
      JOIN feature.price_adjustment_factor_asof a
        ON a.ts_code=d.stock_code AND a.trade_date=d.trade_date
     WHERE d.stock_code=ANY(%s)
       AND d.trade_date >= (%s::date - INTERVAL '400 days')
       AND d.trade_date <= %s
), returns AS (
    SELECT *, asof_close/NULLIF(LAG(asof_close,1) OVER w,0)-1 AS return_1d,
           asof_close/NULLIF(LAG(asof_close,20) OVER w,0)-1 AS return_20d,
           asof_close/NULLIF(LAG(asof_close,60) OVER w,0)-1 AS return_60d
      FROM base WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
), metrics AS (
    SELECT returns.*, AVG(returns.asof_close) OVER w20 AS ma20_raw,
           AVG(asof_close) OVER w60 AS ma60_raw,
           AVG(asof_close) OVER w120 AS ma120_raw,
           STDDEV_SAMP(return_1d) OVER w20 AS volatility_20d_raw,
           AVG(amount) OVER w20 AS amount_ma20_raw,
           AVG(volume) OVER w20 AS volume_ma20_raw,
           COALESCE(prior.prior_available,0) +
             COUNT(asof_close) OVER (PARTITION BY returns.ts_code ORDER BY trade_date
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS available_observation_count,
           COUNT(asof_close) OVER w20 AS ma20_observation_count,
           COUNT(asof_close) OVER w60 AS ma60_observation_count,
           COUNT(asof_close) OVER w120 AS ma120_observation_count
      FROM returns LEFT JOIN prior ON prior.ts_code=returns.ts_code
      WINDOW w20 AS (PARTITION BY returns.ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
             w60 AS (PARTITION BY returns.ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
             w120 AS (PARTITION BY returns.ts_code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
)
INSERT INTO feature.price_daily_v2
    (ts_code,trade_date,raw_close,asof_close,asof_adjustment_factor,
     return_1d,return_20d,return_60d,ma20,ma60,ma120,volatility_20d,
     amount_ma20,volume_ratio_20d,available_observation_count,
     ma20_observation_count,ma60_observation_count,ma120_observation_count,
     quality_status,feature_set_version,adjustment_mode,source_asof_date)
SELECT metrics.ts_code,metrics.trade_date,metrics.raw_close,metrics.asof_close,
       metrics.asof_adjustment_factor,metrics.return_1d,metrics.return_20d,
       metrics.return_60d,
       CASE WHEN metrics.ma20_observation_count=20 THEN metrics.ma20_raw END,
       CASE WHEN metrics.ma60_observation_count=60 THEN metrics.ma60_raw END,
       CASE WHEN metrics.ma120_observation_count=120 THEN metrics.ma120_raw END,
       CASE WHEN metrics.ma20_observation_count=20 THEN metrics.volatility_20d_raw END,
       CASE WHEN metrics.ma20_observation_count=20 THEN metrics.amount_ma20_raw END,
       CASE WHEN metrics.ma20_observation_count=20 THEN metrics.volume/NULLIF(metrics.volume_ma20_raw,0) END,
       metrics.available_observation_count,metrics.ma20_observation_count,
       metrics.ma60_observation_count,metrics.ma120_observation_count,
       CASE WHEN metrics.asof_close IS NULL THEN 'MISSING_SOURCE'
            WHEN metrics.raw_close IS NULL OR metrics.raw_close<=0 THEN 'WARNING'
            WHEN metrics.ma60_observation_count<60 THEN 'WARNING' ELSE 'VALID' END,
       'price_daily_v2','point_in_time_total_return',metrics.trade_date
  FROM metrics WHERE metrics.trade_date BETWEEN %s AND %s
ON CONFLICT (ts_code,trade_date) DO UPDATE SET
    raw_close=EXCLUDED.raw_close,asof_close=EXCLUDED.asof_close,
    asof_adjustment_factor=EXCLUDED.asof_adjustment_factor,
    return_1d=EXCLUDED.return_1d,return_20d=EXCLUDED.return_20d,
    return_60d=EXCLUDED.return_60d,ma20=EXCLUDED.ma20,ma60=EXCLUDED.ma60,
    ma120=EXCLUDED.ma120,volatility_20d=EXCLUDED.volatility_20d,
    amount_ma20=EXCLUDED.amount_ma20,volume_ratio_20d=EXCLUDED.volume_ratio_20d,
    available_observation_count=EXCLUDED.available_observation_count,
    ma20_observation_count=EXCLUDED.ma20_observation_count,
    ma60_observation_count=EXCLUDED.ma60_observation_count,
    ma120_observation_count=EXCLUDED.ma120_observation_count,
    quality_status=EXCLUDED.quality_status,feature_set_version=EXCLUDED.feature_set_version,
    adjustment_mode=EXCLUDED.adjustment_mode,source_asof_date=EXCLUDED.source_asof_date,
    calculated_at=CURRENT_TIMESTAMP
"""


class FeatureRepository(BaseRepository):
    def rebuild_price_v2(self, scope: WorkScope, context: RunContext, codes=None) -> int:
        """重建价格特征（复权因子 + price_daily_v2），按股票代码分批 scoped merge。"""
        if not isinstance(scope, WorkScope):
            raise TypeError("rebuild_price_v2 必须携带 WorkScope")
        if scope.date_from is None or scope.date_to is None:
            raise ValueError("rebuild_price_v2 需要 scope.date_from/date_to")
        codes = list(codes or scope.codes or ())
        if not codes:
            raise ValueError("rebuild_price_v2 需要 codes 或 scope.codes")
        total = 0
        for batch in self._batches(codes):
            # params 顺序：DELETE price_daily_v2(3) + DELETE adj_factor(3) + event_sql(5) + feature_sql(4)
            adjustment_params = (
                batch, scope.date_from, scope.date_to,
                batch, scope.date_from,
                batch, scope.date_to,
                batch, scope.date_from, scope.date_to,
                scope.date_from, scope.date_to,
            )
            feature_params = (
                batch, scope.date_from, scope.date_to,
                batch, scope.date_from,
                batch, scope.date_from - timedelta(days=400), scope.date_to,
                scope.date_from, scope.date_to,
            )
            adjustment_plan = SqlPlan(
                operation=Operation.REBUILD,
                target="feature.price_adjustment_factor_asof",
                scope=scope,
                expected_mode="SET_BASED",
                estimated_rows=0,
                allow_insert_select=True,
                allow_delete=True,
            )
            feature_plan = SqlPlan(
                operation=Operation.REBUILD,
                target="feature.price_daily_v2",
                scope=scope,
                expected_mode="SET_BASED",
                estimated_rows=0,
                allow_insert_select=True,
                allow_delete=True,
            )
            self.kernel.rebuild.run_scoped_merge(
                sql=_PRICE_V2_ADJUSTMENT_BOUNDED_SQL,
                plan=adjustment_plan,
                context=context,
                params=adjustment_params,
            )
            total += self.kernel.rebuild.run_scoped_merge(
                sql=_PRICE_V2_FEATURE_BOUNDED_SQL,
                plan=feature_plan,
                context=context,
                params=feature_params,
            )
        return total

    @staticmethod
    def _batches(values, size=50):
        for i in range(0, len(values), size):
            yield list(values[i : i + size])

    def load_codes_in_window(self, scope: WorkScope, context: RunContext) -> list:
        """读日期窗口内的股票代码列表（受控读，Python 去重）。"""
        rows = self.kernel.read.query(
            context=context,
            source="std.daily_kline",
            columns=["stock_code"],
            where={
                "trade_date__gte": scope.date_from,
                "trade_date__lte": scope.date_to,
            },
            scope=scope,
        )
        return sorted({r[0] for r in rows})

    def load_price_date_range(self, context: RunContext) -> tuple | None:
        """读取行情日期边界；Job 不直接拿 Kernel。"""
        return self._load_date_range(context, "std.daily_kline")

    def load_valuation_date_range(self, context: RunContext) -> tuple | None:
        return self._load_date_range(context, "std.valuation_daily")

    def _load_date_range(self, context: RunContext, source: str) -> tuple | None:
        from datetime import date as _date

        row = self.kernel.read.scalar(
            context=context,
            source=source,
            aggregate="MIN(trade_date), MAX(trade_date)",
            where={"trade_date__gte": _date(1990, 1, 1)},
            scope=WorkScope(date_from=_date(1990, 1, 1)),
        )
        if not row or not row[0] or not row[1]:
            return None
        return (
            row[0] if isinstance(row[0], _date) else _date.fromisoformat(str(row[0])),
            row[1] if isinstance(row[1], _date) else _date.fromisoformat(str(row[1])),
        )



# ---- 迁移自 tushare_phase2_features 的其余特征函数 ----

_COMPANY_FINANCIAL_SQL = """INSERT INTO feature.company_financial_v1
        (ts_code, report_date, effective_announce_date, version_available_date,
         vintage_quality, source_observed_at, ann_date, f_ann_date, roe, roa,
         debt_to_assets, revenue_yoy, or_yoy, tr_yoy, q_sales_yoy, netprofit_yoy,
         operating_cashflow, net_profit,
         operating_cashflow_to_profit, grossprofit_margin, netprofit_margin,
         quality_status, source_record_key, record_version, payload_hash)
    WITH events AS (
        SELECT ts_code, report_date, version_available_date
          FROM std.fina_indicator_standard WHERE version_available_date IS NOT NULL
        UNION
        SELECT stock_code, report_date, version_available_date
          FROM std.financial_income_standard WHERE version_available_date IS NOT NULL
        UNION
        SELECT stock_code, report_date, version_available_date
          FROM std.financial_cash_flow_standard WHERE version_available_date IS NOT NULL
    ), combined AS (
        SELECT e.ts_code, e.report_date, e.version_available_date,
               f.effective_announce_date, f.ann_date, f.f_ann_date,
               f.roe, f.roa, f.debt_to_assets, f.or_yoy, f.tr_yoy,
               f.q_sales_yoy, f.netprofit_yoy, f.grossprofit_margin,
               f.netprofit_margin, f.source_record_key AS fina_key,
               f.record_version AS fina_version, f.payload_hash AS fina_hash,
               f.vintage_quality AS fina_vintage_quality,
               f.source_observed_at AS fina_source_observed_at,
               cf.n_cashflow_act, cf.source_record_key AS cashflow_key,
               cf.announce_date AS cashflow_announce_date,
               cf.record_version AS cashflow_version, cf.payload_hash AS cashflow_hash,
               cf.vintage_quality AS cashflow_vintage_quality,
               cf.source_observed_at AS cashflow_source_observed_at,
               inc.net_profit, inc.source_record_key AS income_key,
               inc.announce_date AS income_announce_date,
               inc.record_version AS income_version, inc.payload_hash AS income_hash,
               inc.vintage_quality AS income_vintage_quality,
               inc.source_observed_at AS income_source_observed_at
        FROM events e
        JOIN LATERAL (
            SELECT f.* FROM std.fina_indicator_standard f
             WHERE f.ts_code=e.ts_code AND f.report_date=e.report_date
               AND f.version_available_date <= e.version_available_date
             ORDER BY f.version_available_date DESC, f.record_version DESC,
                      f.indicator_id DESC LIMIT 1
        ) f ON TRUE
        LEFT JOIN LATERAL (
            SELECT c.* FROM std.financial_cash_flow_standard c
             WHERE c.stock_code=e.ts_code AND c.report_date=e.report_date
               AND c.version_available_date <= e.version_available_date
             ORDER BY c.version_available_date DESC, c.record_version DESC,
                      c.financial_id DESC LIMIT 1
        ) cf ON TRUE
        LEFT JOIN LATERAL (
            SELECT i.* FROM std.financial_income_standard i
             WHERE i.stock_code=e.ts_code AND i.report_date=e.report_date
               AND i.version_available_date <= e.version_available_date
             ORDER BY i.version_available_date DESC, i.record_version DESC,
                      i.financial_id DESC LIMIT 1
        ) inc ON TRUE
    ), versioned AS (
        SELECT c.*,
               ROW_NUMBER() OVER (
                   PARTITION BY ts_code, report_date ORDER BY version_available_date,
                            fina_version, income_version NULLS FIRST,
                            cashflow_version NULLS FIRST
               ) AS combined_version,
               encode(digest(convert_to(jsonb_build_object(
                   'fina_key', fina_key, 'fina_version', fina_version,
                   'fina_hash', fina_hash, 'income_key', income_key,
                   'income_version', income_version, 'income_hash', income_hash,
                   'cashflow_key', cashflow_key,
                   'cashflow_version', cashflow_version,
                   'cashflow_hash', cashflow_hash
               )::text, 'UTF8'), 'sha256'), 'hex') AS combined_hash
        FROM combined c
    )
    SELECT ts_code, report_date,
           GREATEST(effective_announce_date, cashflow_announce_date,
                    income_announce_date),
           version_available_date,
           CASE WHEN 'WAREHOUSE_OBSERVED_ONLY' = ANY(ARRAY[
                        fina_vintage_quality, cashflow_vintage_quality,
                        income_vintage_quality])
                THEN 'WAREHOUSE_OBSERVED_ONLY'
                ELSE 'SOURCE_ANNOUNCEMENT_ASSUMED' END,
           GREATEST(fina_source_observed_at, cashflow_source_observed_at,
                    income_source_observed_at),
           ann_date, f_ann_date,
           NULLIF(roe, 'NaN'::double precision),
           NULLIF(roa, 'NaN'::double precision),
           NULLIF(debt_to_assets, 'NaN'::double precision),
           NULLIF(or_yoy, 'NaN'::double precision),
           NULLIF(or_yoy, 'NaN'::double precision),
           NULLIF(tr_yoy, 'NaN'::double precision),
           NULLIF(q_sales_yoy, 'NaN'::double precision),
           NULLIF(netprofit_yoy, 'NaN'::double precision),
           NULLIF(n_cashflow_act, 'NaN'::double precision),
           NULLIF(net_profit, 'NaN'::double precision),
           NULLIF(n_cashflow_act, 'NaN'::double precision)
             / NULLIF(NULLIF(net_profit, 'NaN'::double precision), 0),
           NULLIF(grossprofit_margin, 'NaN'::double precision),
           NULLIF(netprofit_margin, 'NaN'::double precision),
           CASE WHEN effective_announce_date IS NULL THEN 'MISSING_SOURCE'
                WHEN 'WAREHOUSE_OBSERVED_ONLY' = ANY(ARRAY[
                     fina_vintage_quality, cashflow_vintage_quality,
                     income_vintage_quality]) THEN 'WARNING'
                WHEN roe IS NULL OR debt_to_assets IS NULL
                  OR n_cashflow_act IS NULL OR net_profit IS NULL
                THEN 'WARNING' ELSE 'VALID' END,
           'combined:' || combined_hash, combined_version, combined_hash
    FROM versioned
    ON CONFLICT (ts_code, report_date, source_record_key, record_version) DO UPDATE SET
      effective_announce_date=EXCLUDED.effective_announce_date, roe=EXCLUDED.roe,
      version_available_date=EXCLUDED.version_available_date,
      vintage_quality=EXCLUDED.vintage_quality,
      source_observed_at=EXCLUDED.source_observed_at,
      roa=EXCLUDED.roa, debt_to_assets=EXCLUDED.debt_to_assets,
      revenue_yoy=EXCLUDED.revenue_yoy, or_yoy=EXCLUDED.or_yoy,
      tr_yoy=EXCLUDED.tr_yoy, q_sales_yoy=EXCLUDED.q_sales_yoy,
      netprofit_yoy=EXCLUDED.netprofit_yoy, payload_hash=EXCLUDED.payload_hash,
      operating_cashflow=EXCLUDED.operating_cashflow, net_profit=EXCLUDED.net_profit,
      operating_cashflow_to_profit=EXCLUDED.operating_cashflow_to_profit,
      quality_status=EXCLUDED.quality_status, calculated_at=CURRENT_TIMESTAMP
"""

_SHAREHOLDER_RETURN_SQL = """DELETE FROM feature.shareholder_return_v1 WHERE ts_code=ANY(%s) AND asof_date BETWEEN %s AND %s;
INSERT INTO feature.shareholder_return_v1
        (ts_code, asof_date, cash_div_12m, dividend_event_count_3y,
         dividend_year_count_3y, quality_status)
    SELECT d.stock_code, d.trade_date,
           COALESCE(SUM(e.cash_div) FILTER (WHERE e.ex_date > d.trade_date - INTERVAL '365 days'), 0), COUNT(e.dividend_id),
           COUNT(DISTINCT EXTRACT(YEAR FROM e.ex_date)),
           CASE WHEN COUNT(e.dividend_id) = 0 THEN 'WARNING' ELSE 'VALID' END
    FROM std.daily_kline d
    LEFT JOIN std.dividend_event e
      ON e.ts_code=d.stock_code AND e.ex_date > d.trade_date - INTERVAL '3 years'
     AND e.ex_date <= d.trade_date
     AND e.implementation_status = '实施'
    WHERE d.stock_code=ANY(%s) AND d.trade_date BETWEEN %s AND %s
    GROUP BY d.stock_code, d.trade_date
    ON CONFLICT (ts_code, asof_date) DO UPDATE SET
      cash_div_12m=EXCLUDED.cash_div_12m,
      dividend_event_count_3y=EXCLUDED.dividend_event_count_3y,
      dividend_year_count_3y=EXCLUDED.dividend_year_count_3y,
      quality_status=EXCLUDED.quality_status, calculated_at=CURRENT_TIMESTAMP
"""

class _FeatureRepositoryMore:
    def rebuild_company_financial(self, context: RunContext) -> int:
        """重建财务特征（纯 upsert，无 TRUNCATE——原实现 TRUNCATE 冗余且危险）。"""
        plan = SqlPlan(
            operation=Operation.REBUILD, target="feature.company_financial_v1",
            scope=WorkScope(), expected_mode="SET_BASED",
            estimated_rows=200_000, allow_insert_select=True,
        )
        return self.kernel.rebuild.run_insert_select(
            sql=_COMPANY_FINANCIAL_SQL, plan=plan, context=context
        )

    def rebuild_shareholder_return(self, scope: WorkScope, context: RunContext, codes=None) -> int:
        """重建股东回报特征（按代码分批 scoped merge）。"""
        if scope.date_from is None or scope.date_to is None:
            raise ValueError("rebuild_shareholder_return 需要 scope.date_from/date_to")
        codes = list(codes or scope.codes or ())
        if not codes:
            raise ValueError("rebuild_shareholder_return 需要 codes")
        total = 0
        for batch in self._batches(codes):
            params = (batch, scope.date_from, scope.date_to, batch, scope.date_from, scope.date_to)
            plan = SqlPlan(
                operation=Operation.REBUILD, target="feature.shareholder_return_v1",
                scope=scope, expected_mode="SET_BASED",
                estimated_rows=0, allow_insert_select=True, allow_delete=True,
            )
            total += self.kernel.rebuild.run_scoped_merge(
                sql=_SHAREHOLDER_RETURN_SQL, plan=plan, context=context, params=params
            )
        return total

    def register_features(self, context: RunContext) -> int:
        rows = [
            {"feature_name": "price_daily", "feature_version": "v2",
             "business_name": "时点复权价格特征", "definition": "仅回放已实施公司行动的总收益价格坐标",
             "grain": "stock x trade_date", "source_datasets": ["std.daily_kline", "std.dividend_event"],
             "dependency_features": [], "lookback_rule": "120 trading days",
             "asof_rule": "ex_date <= trade_date", "adjustment_rule": "point-in-time total return",
             "impact_rule": {"from": "effective_date", "forward_trading_days": 120},
             "quality_rule": {"required": ["raw_close", "asof_close"]},
             "code_version": "phase3", "status": "ACTIVE"},
            {"feature_name": "valuation_daily", "feature_version": "v1",
             "business_name": "估值历史分位", "definition": "正值估值观测的三年与五年滚动分位",
             "grain": "stock x trade_date", "source_datasets": ["valuation_daily"],
             "dependency_features": [], "lookback_rule": "5 calendar years",
             "asof_rule": "observation_date <= trade_date", "adjustment_rule": "none",
             "impact_rule": {"from": "effective_date", "forward_calendar_years": 5},
             "quality_rule": {"minimum_observations": 1},
             "code_version": "phase2", "status": "ACTIVE"},
            {"feature_name": "company_financial", "feature_version": "v1",
             "business_name": "公司财务时点特征", "definition": "按公告可见日选取财务版本",
             "grain": "stock x report_date x version",
             "source_datasets": ["std.fina_indicator_standard", "std.financial_income_standard", "std.financial_cash_flow_standard"],
             "dependency_features": [], "lookback_rule": "latest visible version",
             "asof_rule": "version_available_date <= asof_date", "adjustment_rule": "restatement creates a new version",
             "impact_rule": {"from": "effective_date", "rebuild": ["pit", "feature", "snapshot"]},
             "quality_rule": {"pit_safety": True},
             "code_version": "phase2", "status": "ACTIVE"},
            {"feature_name": "shareholder_return", "feature_version": "v1",
             "business_name": "股东回报特征", "definition": "已实施现金分红的滚动统计",
             "grain": "stock x asof_date", "source_datasets": ["std.dividend_event"],
             "dependency_features": [], "lookback_rule": "3 calendar years",
             "asof_rule": "ex_date <= asof_date", "adjustment_rule": "none",
             "impact_rule": {"from": "effective_date", "forward_calendar_years": 3},
             "quality_rule": {"nonnegative": ["cash_div_12m"]},
             "code_version": "phase2", "status": "ACTIVE"},
        ]
        total = 0
        from data_collect.data_access.repository.foundation_metadata_repository import FoundationMetadataRepository
        registry = FoundationMetadataRepository(self.kernel)
        for row in rows:
            total += registry.register_feature_version(context, row)
        return total


# 把方法合并进 FeatureRepository（简单 mixin 方式）
FeatureRepository.rebuild_company_financial = _FeatureRepositoryMore.rebuild_company_financial
FeatureRepository.rebuild_shareholder_return = _FeatureRepositoryMore.rebuild_shareholder_return
FeatureRepository.register_features = _FeatureRepositoryMore.register_features


_PRICE_V1_SCOPED_MERGE_SQL = """DELETE FROM feature.price_daily_v1 WHERE ts_code=ANY(%s) AND trade_date BETWEEN %s AND %s;
INSERT INTO feature.price_daily_v1
        (ts_code, trade_date, raw_close, adj_close_base, return_1d, return_20d,
         return_60d, ma20, ma60, ma120, volatility_20d, amount_ma20,
         volume_ratio_20d, available_observation_count, ma20_observation_count,
         ma60_observation_count, ma120_observation_count, quality_status, source_asof_date)
    WITH base AS (
        SELECT d.stock_code AS ts_code, d.trade_date, d.close AS raw_close,
               d.amount, d.volume, a.adj_factor,
               CASE WHEN a.adj_factor IS NOT NULL
                    THEN d.close * a.adj_factor END AS adj_close_base
        FROM std.daily_kline d
        LEFT JOIN LATERAL (
            SELECT adj_factor
            FROM std.adj_factor_daily a
            WHERE a.stock_code=d.stock_code AND a.trade_date=d.trade_date
            ORDER BY a.source DESC, a.fetched_at DESC
            LIMIT 1
        ) a ON TRUE
        WHERE d.stock_code=ANY(%s) AND d.trade_date<=%s
    ), returns AS (
        SELECT *,
            adj_close_base / NULLIF(LAG(adj_close_base, 1) OVER w, 0) - 1 AS return_1d,
            adj_close_base / NULLIF(LAG(adj_close_base, 20) OVER w, 0) - 1 AS return_20d,
            adj_close_base / NULLIF(LAG(adj_close_base, 60) OVER w, 0) - 1 AS return_60d
        FROM base
        WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
    ), metrics AS (
        SELECT *,
            AVG(adj_close_base) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20_raw,
            AVG(adj_close_base) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60_raw,
            AVG(adj_close_base) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120_raw,
            STDDEV_SAMP(return_1d)
                OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS volatility_20d_raw,
            AVG(amount) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS amount_ma20_raw,
            AVG(volume) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS volume_ma20_raw,
            COUNT(adj_close_base) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS available_observation_count,
            COUNT(adj_close_base) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20_observation_count,
            COUNT(adj_close_base) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60_observation_count,
            COUNT(adj_close_base) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120_observation_count,
            COUNT(return_1d) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS return_observation_count_20
        FROM returns
    )
    SELECT ts_code, trade_date, raw_close, adj_close_base, return_1d, return_20d,
           return_60d,
           CASE WHEN ma20_observation_count = 20 THEN ma20_raw END,
           CASE WHEN ma60_observation_count = 60 THEN ma60_raw END,
           CASE WHEN ma120_observation_count = 120 THEN ma120_raw END,
           CASE WHEN return_observation_count_20 = 20 THEN volatility_20d_raw END,
           CASE WHEN ma20_observation_count = 20 THEN amount_ma20_raw END,
           CASE WHEN ma20_observation_count = 20
                THEN volume / NULLIF(volume_ma20_raw, 0) END,
           available_observation_count, ma20_observation_count,
           ma60_observation_count, ma120_observation_count,
           CASE WHEN adj_factor IS NULL THEN 'MISSING_SOURCE'
                WHEN raw_close IS NULL OR raw_close <= 0 THEN 'WARNING'
                WHEN ma60_observation_count < 60 THEN 'WARNING'
                ELSE 'VALID' END,
           trade_date
    FROM metrics
    WHERE trade_date BETWEEN %s AND %s
    ON CONFLICT (ts_code, trade_date) DO UPDATE SET
      raw_close=EXCLUDED.raw_close, adj_close_base=EXCLUDED.adj_close_base,
      return_1d=EXCLUDED.return_1d, return_20d=EXCLUDED.return_20d,
      return_60d=EXCLUDED.return_60d, ma20=EXCLUDED.ma20, ma60=EXCLUDED.ma60,
      ma120=EXCLUDED.ma120, volatility_20d=EXCLUDED.volatility_20d,
      amount_ma20=EXCLUDED.amount_ma20, volume_ratio_20d=EXCLUDED.volume_ratio_20d,
      available_observation_count=EXCLUDED.available_observation_count,
      ma20_observation_count=EXCLUDED.ma20_observation_count,
      ma60_observation_count=EXCLUDED.ma60_observation_count,
      ma120_observation_count=EXCLUDED.ma120_observation_count,
      quality_status=EXCLUDED.quality_status, source_asof_date=EXCLUDED.source_asof_date,
      calculated_at=CURRENT_TIMESTAMP
"""


class _FeatureRepositoryPriceV1:
    def rebuild_price(self, scope: WorkScope, context: RunContext, codes=None) -> int:
        """重建价格特征 v1（遗留兼容：旧 adj_factor 复权，仅供历史对照）。"""
        if scope.date_from is None or scope.date_to is None:
            raise ValueError("rebuild_price 需要 scope.date_from/date_to")
        codes = list(codes or scope.codes or ())
        if not codes:
            raise ValueError("rebuild_price 需要 codes")
        total = 0
        for batch in self._batches(codes):
            params = (
                batch, scope.date_from, scope.date_to,
                batch, scope.date_to, scope.date_from, scope.date_to,
            )
            plan = SqlPlan(
                operation=Operation.REBUILD, target="feature.price_daily_v1",
                scope=scope, expected_mode="SET_BASED",
                estimated_rows=0, allow_insert_select=True, allow_delete=True,
            )
            total += self.kernel.rebuild.run_scoped_merge(
                sql=_PRICE_V1_SCOPED_MERGE_SQL, plan=plan, context=context, params=params
            )
        return total


FeatureRepository.rebuild_price = _FeatureRepositoryPriceV1.rebuild_price
