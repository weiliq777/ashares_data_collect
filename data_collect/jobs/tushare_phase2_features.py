"""阶段二 Feature v1 计算任务。

只读取 PostgreSQL Raw/Standard/PIT，不访问 Tushare；目标表可重复重建。
"""
from __future__ import annotations

import argparse
import bisect
import logging
import math
from collections import deque
from datetime import date, timedelta

from data_collect.utils.db import get_connection, require_psycopg2


logger = logging.getLogger("tushare_phase2_features")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())


def _execute(sql: str) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        affected = cur.rowcount
        conn.commit()
    return affected


def rebuild_price() -> int:
    _execute("TRUNCATE TABLE feature.price_daily_v1")
    sql = """
    INSERT INTO feature.price_daily_v1
        (ts_code, trade_date, raw_close, adj_close_base, return_1d, return_20d,
         return_60d, ma20, ma60, ma120, volatility_20d, amount_ma20,
         volume_ratio_20d, available_observation_count, quality_status, source_asof_date)
    WITH base AS (
        SELECT d.stock_code AS ts_code, d.trade_date, d.close AS raw_close,
               d.amount, d.volume, a.adj_factor,
               CASE WHEN a.adj_factor IS NOT NULL
                    THEN d.close * a.adj_factor END AS adj_close_base
        FROM daily_kline d
        LEFT JOIN LATERAL (
            SELECT adj_factor
            FROM adj_factor_daily a
            WHERE a.stock_code=d.stock_code AND a.trade_date=d.trade_date
            ORDER BY a.source DESC, a.fetched_at DESC
            LIMIT 1
        ) a ON TRUE
    ), returns AS (
        SELECT *,
            adj_close_base / NULLIF(LAG(adj_close_base, 1) OVER w, 0) - 1 AS return_1d,
            adj_close_base / NULLIF(LAG(adj_close_base, 20) OVER w, 0) - 1 AS return_20d,
            adj_close_base / NULLIF(LAG(adj_close_base, 60) OVER w, 0) - 1 AS return_60d
        FROM base
        WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
    ), metrics AS (
        SELECT *,
            AVG(adj_close_base) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
            AVG(adj_close_base) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
            AVG(adj_close_base) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120,
            STDDEV_SAMP(return_1d)
                OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS volatility_20d,
            AVG(amount) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS amount_ma20,
            volume / NULLIF(AVG(volume) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 0) AS volume_ratio_20d,
            COUNT(adj_close_base) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS available_observation_count
        FROM returns
    )
    SELECT ts_code, trade_date, raw_close, adj_close_base, return_1d, return_20d,
           return_60d, ma20, ma60, ma120, volatility_20d, amount_ma20,
           volume_ratio_20d, available_observation_count,
           CASE WHEN adj_factor IS NULL THEN 'MISSING_SOURCE'
                WHEN raw_close IS NULL OR raw_close <= 0 THEN 'WARNING'
                ELSE 'VALID' END,
           trade_date
    FROM metrics
    ON CONFLICT (ts_code, trade_date) DO UPDATE SET
      raw_close=EXCLUDED.raw_close, adj_close_base=EXCLUDED.adj_close_base,
      return_1d=EXCLUDED.return_1d, return_20d=EXCLUDED.return_20d,
      return_60d=EXCLUDED.return_60d, ma20=EXCLUDED.ma20, ma60=EXCLUDED.ma60,
      ma120=EXCLUDED.ma120, volatility_20d=EXCLUDED.volatility_20d,
      amount_ma20=EXCLUDED.amount_ma20, volume_ratio_20d=EXCLUDED.volume_ratio_20d,
      available_observation_count=EXCLUDED.available_observation_count,
      quality_status=EXCLUDED.quality_status, source_asof_date=EXCLUDED.source_asof_date,
      calculated_at=CURRENT_TIMESTAMP
    """
    return _execute(sql)


def _insert_valuation_rows(rows: list[tuple]) -> int:
    if not rows:
        return 0
    _, execute_values = require_psycopg2()
    columns = (
        "ts_code", "trade_date", "pe_ttm", "pb", "ps_ttm", "dv_ratio",
        "total_mv", "circ_mv", "pe_pct_3y", "pe_pct_5y", "pb_pct_3y",
        "pb_pct_5y", "is_pe_meaningful", "valuation_observation_count",
        "quality_status", "source_asof_date",
    )
    sql = f'''INSERT INTO feature.valuation_daily_v1 ({", ".join(columns)}) VALUES %s
              ON CONFLICT (ts_code, trade_date) DO UPDATE SET
              pe_ttm=EXCLUDED.pe_ttm, pb=EXCLUDED.pb, ps_ttm=EXCLUDED.ps_ttm,
              dv_ratio=EXCLUDED.dv_ratio, total_mv=EXCLUDED.total_mv, circ_mv=EXCLUDED.circ_mv,
              pe_pct_3y=EXCLUDED.pe_pct_3y, pe_pct_5y=EXCLUDED.pe_pct_5y,
              pb_pct_3y=EXCLUDED.pb_pct_3y, pb_pct_5y=EXCLUDED.pb_pct_5y,
              is_pe_meaningful=EXCLUDED.is_pe_meaningful,
              valuation_observation_count=EXCLUDED.valuation_observation_count,
              quality_status=EXCLUDED.quality_status, source_asof_date=EXCLUDED.source_asof_date,
              calculated_at=CURRENT_TIMESTAMP'''
    with get_connection() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=5000)
        inserted = cur.rowcount
        conn.commit()
    return inserted


def _rank(sorted_values: list[float], value) -> float | None:
    if value is None or value <= 0 or not sorted_values:
        return None
    return bisect.bisect_right(sorted_values, float(value)) / len(sorted_values)


def _finite(value):
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def rebuild_valuation() -> int:
    _execute("TRUNCATE TABLE feature.valuation_daily_v1")
    read_conn = get_connection()
    write_conn = get_connection()
    total = 0
    try:
        with read_conn.cursor(name="phase2_valuation_stream") as cur:
            cur.itersize = 5000
            cur.execute(
                """SELECT stock_code, trade_date, pe_ttm, pb, ps_ttm, dv_ratio, total_mv, circ_mv
                   FROM valuation_daily ORDER BY stock_code, trade_date"""
            )
            current_code = None
            q3: deque[tuple[date, float | None, float | None]] = deque()
            q5: deque[tuple[date, float | None, float | None]] = deque()
            pe3: list[float] = []
            pe5: list[float] = []
            pb3: list[float] = []
            pb5: list[float] = []
            rows: list[tuple] = []

            def reset(code):
                nonlocal current_code, q3, q5, pe3, pe5, pb3, pb5
                current_code = code
                q3, q5 = deque(), deque()
                pe3, pe5, pb3, pb5 = [], [], [], []

            def remove_item(item, pe_values, pb_values):
                _, pe_value, pb_value = item
                if pe_value is not None and pe_value > 0:
                    index = bisect.bisect_left(pe_values, pe_value)
                    if index < len(pe_values):
                        pe_values.pop(index)
                if pb_value is not None and pb_value > 0:
                    index = bisect.bisect_left(pb_values, pb_value)
                    if index < len(pb_values):
                        pb_values.pop(index)

            while True:
                batch = cur.fetchmany(5000)
                if not batch:
                    break
                for code, trade_date, pe_value, pb_value, ps_ttm, dv_ratio, total_mv, circ_mv in batch:
                    if code != current_code:
                        reset(code)
                    cutoff3 = trade_date - timedelta(days=365 * 3)
                    cutoff5 = trade_date - timedelta(days=365 * 5)
                    while q3 and q3[0][0] < cutoff3:
                        remove_item(q3.popleft(), pe3, pb3)
                    while q5 and q5[0][0] < cutoff5:
                        remove_item(q5.popleft(), pe5, pb5)
                    pe_value = _finite(pe_value)
                    pb_value = _finite(pb_value)
                    ps_ttm = _finite(ps_ttm)
                    dv_ratio = _finite(dv_ratio)
                    total_mv = _finite(total_mv)
                    circ_mv = _finite(circ_mv)
                    pe_valid = pe_value is not None and pe_value > 0
                    pb_valid = pb_value is not None and pb_value > 0
                    item = (trade_date, pe_value if pe_valid else None, pb_value if pb_valid else None)
                    q3.append(item); q5.append(item)
                    if pe_valid:
                        bisect.insort(pe3, float(pe_value)); bisect.insort(pe5, float(pe_value))
                    if pb_valid:
                        bisect.insort(pb3, float(pb_value)); bisect.insort(pb5, float(pb_value))
                    quality = "VALID" if pe_valid or pb_valid else "WARNING"
                    rows.append((
                        code, trade_date, pe_value, pb_value, ps_ttm, dv_ratio, total_mv, circ_mv,
                        _rank(pe3, pe_value), _rank(pe5, pe_value), _rank(pb3, pb_value), _rank(pb5, pb_value),
                        pe_valid, len(pe5), quality, trade_date,
                    ))
                    if len(rows) >= 5000:
                        total += _insert_valuation_rows(rows)
                        rows.clear()
                if total and total % 500000 < 5000:
                    logger.info("valuation feature progress=%s", total)
            total += _insert_valuation_rows(rows)
    finally:
        read_conn.close()
        write_conn.close()
    return total


def rebuild_company_financial() -> int:
    _execute("TRUNCATE TABLE feature.company_financial_v1 RESTART IDENTITY")
    sql = """
    INSERT INTO feature.company_financial_v1
        (ts_code, report_date, effective_announce_date, ann_date, f_ann_date, roe, roa,
         debt_to_assets, revenue_yoy, netprofit_yoy, operating_cashflow, net_profit,
         operating_cashflow_to_profit, grossprofit_margin, netprofit_margin,
         quality_status, source_record_key, record_version)
    SELECT f.stock_code, f.report_date, f.announce_date, f.announce_date, NULL,
           NULLIF(f.roe, 'NaN'::double precision),
           NULLIF(f.roa, 'NaN'::double precision),
           NULLIF(f.debt_to_assets, 'NaN'::double precision),
           NULLIF(f.revenue_yoy, 'NaN'::double precision),
           NULLIF(f.netprofit_yoy, 'NaN'::double precision),
           NULLIF(cf.n_cashflow_act, 'NaN'::double precision),
           NULLIF(inc.net_profit, 'NaN'::double precision),
           NULLIF(cf.n_cashflow_act, 'NaN'::double precision)
             / NULLIF(NULLIF(inc.net_profit, 'NaN'::double precision), 0),
           NULLIF(f.grossprofit_margin, 'NaN'::double precision),
           NULLIF(f.netprofit_margin, 'NaN'::double precision),
           CASE WHEN f.announce_date IS NULL THEN 'MISSING_SOURCE'
                WHEN f.roe = 'NaN'::double precision OR f.debt_to_assets = 'NaN'::double precision
                  OR cf.n_cashflow_act = 'NaN'::double precision OR inc.net_profit = 'NaN'::double precision
                THEN 'WARNING' ELSE 'VALID' END,
           f.stock_code || ':' || f.report_date || ':' || COALESCE(f.announce_date::text, ''), 0
    FROM stock_financial_indicator f
    LEFT JOIN LATERAL (
        SELECT n_cashflow_act FROM financial_cash_flow_standard c
        WHERE c.stock_code=f.stock_code AND c.report_date=f.report_date
          AND (f.announce_date IS NULL OR c.announce_date <= f.announce_date)
        ORDER BY c.announce_date DESC NULLS LAST, c.financial_id DESC LIMIT 1
    ) cf ON TRUE
    LEFT JOIN LATERAL (
        SELECT net_profit FROM financial_income_standard i
        WHERE i.stock_code=f.stock_code AND i.report_date=f.report_date
          AND (f.announce_date IS NULL OR i.announce_date <= f.announce_date)
        ORDER BY i.announce_date DESC NULLS LAST, i.financial_id DESC LIMIT 1
    ) inc ON TRUE
    ON CONFLICT (ts_code, report_date, source_record_key, record_version) DO UPDATE SET
      effective_announce_date=EXCLUDED.effective_announce_date, roe=EXCLUDED.roe,
      roa=EXCLUDED.roa, debt_to_assets=EXCLUDED.debt_to_assets,
      revenue_yoy=EXCLUDED.revenue_yoy, netprofit_yoy=EXCLUDED.netprofit_yoy,
      operating_cashflow=EXCLUDED.operating_cashflow, net_profit=EXCLUDED.net_profit,
      operating_cashflow_to_profit=EXCLUDED.operating_cashflow_to_profit,
      quality_status=EXCLUDED.quality_status, calculated_at=CURRENT_TIMESTAMP
    """
    return _execute(sql)


def rebuild_shareholder_return() -> int:
    _execute("TRUNCATE TABLE feature.shareholder_return_v1 RESTART IDENTITY")
    sql = """
    INSERT INTO feature.shareholder_return_v1
        (ts_code, asof_date, cash_div_12m, dividend_event_count_3y,
         dividend_year_count_3y, quality_status)
    SELECT d.stock_code, d.trade_date,
           COALESCE(SUM(e.cash_div) FILTER (WHERE e.ex_date > d.trade_date - INTERVAL '365 days'), 0), COUNT(e.dividend_id),
           COUNT(DISTINCT EXTRACT(YEAR FROM e.ex_date)),
           CASE WHEN COUNT(e.dividend_id) = 0 THEN 'WARNING' ELSE 'VALID' END
    FROM daily_kline d
    LEFT JOIN std.dividend_event e
      ON e.ts_code=d.stock_code AND e.ex_date > d.trade_date - INTERVAL '3 years'
     AND e.ex_date <= d.trade_date
    GROUP BY d.stock_code, d.trade_date
    ON CONFLICT (ts_code, asof_date) DO UPDATE SET
      cash_div_12m=EXCLUDED.cash_div_12m,
      dividend_event_count_3y=EXCLUDED.dividend_event_count_3y,
      dividend_year_count_3y=EXCLUDED.dividend_year_count_3y,
      quality_status=EXCLUDED.quality_status, calculated_at=CURRENT_TIMESTAMP
    """
    return _execute(sql)


def register_features() -> int:
    sql = """
    INSERT INTO ops.feature_registry (feature_name, feature_version, grain, lookback_rule, source_tables, code_version)
    VALUES
      ('price_daily', 'v1', 'stock x trade_date', 'rows before current trade_date only', ARRAY['daily_kline','adj_factor_daily'], 'phase2'),
      ('valuation_daily', 'v1', 'stock x trade_date', 'rolling 3y/5y positive observations', ARRAY['valuation_daily'], 'phase2'),
      ('company_financial', 'v1', 'stock x report_date/version', 'announce_date controls availability', ARRAY['stock_financial_indicator','financial_income_standard','financial_cash_flow_standard'], 'phase2'),
      ('shareholder_return', 'v1', 'stock x asof_date', 'ex_date <= asof_date', ARRAY['std.dividend_event'], 'phase2')
    ON CONFLICT (feature_name) DO UPDATE SET
      feature_version=EXCLUDED.feature_version, grain=EXCLUDED.grain,
      lookback_rule=EXCLUDED.lookback_rule, source_tables=EXCLUDED.source_tables,
      code_version=EXCLUDED.code_version, registered_at=CURRENT_TIMESTAMP
    """
    return _execute(sql)


def run(task: str = "all") -> dict[str, int]:
    tasks = {
        "price": rebuild_price,
        "valuation": rebuild_valuation,
        "company_financial": rebuild_company_financial,
        "shareholder_return": rebuild_shareholder_return,
        "registry": register_features,
    }
    selected = list(tasks) if task == "all" else [task]
    if any(name not in tasks for name in selected):
        raise ValueError(f"不支持的 task: {task}")
    result = {}
    for name in selected:
        logger.info("phase2 feature task started=%s", name)
        result[name] = tasks[name]()
        logger.info("phase2 feature task finished=%s affected=%s", name, result[name])
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="阶段二 Feature v1 构建")
    parser.add_argument("--task", choices=["all", "price", "valuation", "company_financial", "shareholder_return", "registry"], default="all")
    args = parser.parse_args()
    print(run(args.task))
