"""阶段二 Feature v1 计算任务。

只读取 PostgreSQL Raw/Standard/PIT，不访问 Tushare；目标表可重复重建。
"""
from __future__ import annotations

import argparse
import bisect
import logging
import math
import uuid
from collections import deque
from datetime import date, timedelta

from data_collect.utils.db import get_connection, require_psycopg2


logger = logging.getLogger("tushare_phase2_features")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())


# A percentile labelled "3y/5y" must cover a real calendar window as well as
# enough observations.  The count thresholds tolerate suspensions and missing
# PE values while preventing a few days of history from masquerading as years.
MIN_PE_OBSERVATIONS_3Y = 600
MIN_PE_OBSERVATIONS_5Y = 1000
MIN_CALENDAR_SPAN_DAYS_3Y = 365 * 3 - 31
MIN_CALENDAR_SPAN_DAYS_5Y = 365 * 5 - 31


def _execute(sql: str) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        affected = cur.rowcount
        conn.commit()
    return affected


def rebuild_price() -> int:
    sql = """
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
    return _execute("TRUNCATE TABLE feature.price_daily_v1;\n" + sql)


def rebuild_price_v2() -> int:
    """Build point-in-time total-return price features.

    ``adj_factor_daily`` is a final warehouse value and does not carry an
    announcement vintage.  v2 therefore does not use it.  It replays only
    implemented corporate actions on their ex-date and constructs a total
    return coordinate from raw closes.  Raw OHLC remains the execution
    coordinate; this table is for indicators only.
    """
    event_sql = """
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
          FROM daily_kline d
          LEFT JOIN events e
            ON e.ts_code = d.stock_code AND e.ex_date = d.trade_date
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
    ON CONFLICT (ts_code, trade_date) DO UPDATE SET
        asof_adjustment_factor=EXCLUDED.asof_adjustment_factor,
        asof_close=EXCLUDED.asof_close, event_count=EXCLUDED.event_count,
        source_event_max_ex_date=EXCLUDED.source_event_max_ex_date,
        adjustment_mode=EXCLUDED.adjustment_mode,
        quality_status=EXCLUDED.quality_status,
        source_asof_date=EXCLUDED.source_asof_date,
        calculated_at=CURRENT_TIMESTAMP
    """
    feature_sql = """
    WITH base AS (
        SELECT d.stock_code AS ts_code, d.trade_date, d.close AS raw_close,
               d.amount, d.volume, a.asof_close,
               a.asof_adjustment_factor
          FROM daily_kline d
          JOIN feature.price_adjustment_factor_asof a
            ON a.ts_code = d.stock_code AND a.trade_date = d.trade_date
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
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE feature.price_adjustment_factor_asof, feature.price_daily_v2")
        cur.execute(event_sql)
        factor_rows = cur.rowcount
        cur.execute(feature_sql)
        feature_rows = cur.rowcount
        conn.commit()
    return int(feature_rows or 0)


def _insert_valuation_rows(rows: list[tuple], conn, target_table: str) -> int:
    if not rows:
        return 0
    _, execute_values = require_psycopg2()
    columns = (
        "ts_code", "trade_date", "pe_ttm", "pb", "ps_ttm", "dv_ratio",
        "total_mv", "circ_mv", "pe_pct_3y", "pe_pct_5y", "pb_pct_3y",
        "pb_pct_5y", "is_pe_meaningful", "valuation_observation_count",
        "pe_window_start_date_3y", "pe_window_start_date_5y",
        "pe_observation_count_3y", "pe_observation_count_5y",
        "has_full_pe_3y_history", "has_full_pe_5y_history",
        "quality_status", "source_asof_date",
    )
    sql = f'''INSERT INTO {target_table} ({", ".join(columns)}) VALUES %s
              ON CONFLICT (ts_code, trade_date) DO UPDATE SET
              pe_ttm=EXCLUDED.pe_ttm, pb=EXCLUDED.pb, ps_ttm=EXCLUDED.ps_ttm,
              dv_ratio=EXCLUDED.dv_ratio, total_mv=EXCLUDED.total_mv, circ_mv=EXCLUDED.circ_mv,
              pe_pct_3y=EXCLUDED.pe_pct_3y, pe_pct_5y=EXCLUDED.pe_pct_5y,
              pb_pct_3y=EXCLUDED.pb_pct_3y, pb_pct_5y=EXCLUDED.pb_pct_5y,
              is_pe_meaningful=EXCLUDED.is_pe_meaningful,
              valuation_observation_count=EXCLUDED.valuation_observation_count,
              pe_window_start_date_3y=EXCLUDED.pe_window_start_date_3y,
              pe_window_start_date_5y=EXCLUDED.pe_window_start_date_5y,
              pe_observation_count_3y=EXCLUDED.pe_observation_count_3y,
              pe_observation_count_5y=EXCLUDED.pe_observation_count_5y,
              has_full_pe_3y_history=EXCLUDED.has_full_pe_3y_history,
              has_full_pe_5y_history=EXCLUDED.has_full_pe_5y_history,
              quality_status=EXCLUDED.quality_status, source_asof_date=EXCLUDED.source_asof_date,
              calculated_at=CURRENT_TIMESTAMP'''
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=5000)
        inserted = cur.rowcount
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
    read_conn = get_connection()
    write_conn = get_connection()
    staging_table = f"feature.valuation_daily_v1_build_{uuid.uuid4().hex[:12]}"
    total = 0
    try:
        with write_conn.cursor() as write_cur:
            write_cur.execute(
                f"CREATE UNLOGGED TABLE {staging_table} "
                "(LIKE feature.valuation_daily_v1 INCLUDING ALL)"
            )
        write_conn.commit()
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
            pe_dates3: deque[date] = deque()
            pe_dates5: deque[date] = deque()
            pb_dates3: deque[date] = deque()
            pb_dates5: deque[date] = deque()
            rows: list[tuple] = []

            def reset(code):
                nonlocal current_code, q3, q5, pe3, pe5, pb3, pb5
                nonlocal pe_dates3, pe_dates5, pb_dates3, pb_dates5
                current_code = code
                q3, q5 = deque(), deque()
                pe3, pe5, pb3, pb5 = [], [], [], []
                pe_dates3, pe_dates5, pb_dates3, pb_dates5 = deque(), deque(), deque(), deque()

            def remove_item(item, pe_values, pb_values, pe_dates, pb_dates):
                item_date, pe_value, pb_value = item
                if pe_value is not None and pe_value > 0:
                    index = bisect.bisect_left(pe_values, pe_value)
                    if index < len(pe_values):
                        pe_values.pop(index)
                    if pe_dates and pe_dates[0] == item_date:
                        pe_dates.popleft()
                if pb_value is not None and pb_value > 0:
                    index = bisect.bisect_left(pb_values, pb_value)
                    if index < len(pb_values):
                        pb_values.pop(index)
                    if pb_dates and pb_dates[0] == item_date:
                        pb_dates.popleft()

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
                        remove_item(q3.popleft(), pe3, pb3, pe_dates3, pb_dates3)
                    while q5 and q5[0][0] < cutoff5:
                        remove_item(q5.popleft(), pe5, pb5, pe_dates5, pb_dates5)
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
                        pe_dates3.append(trade_date); pe_dates5.append(trade_date)
                    if pb_valid:
                        bisect.insort(pb3, float(pb_value)); bisect.insort(pb5, float(pb_value))
                        pb_dates3.append(trade_date); pb_dates5.append(trade_date)
                    full_pe3 = bool(
                        len(pe3) >= MIN_PE_OBSERVATIONS_3Y and pe_dates3
                        and (trade_date - pe_dates3[0]).days >= MIN_CALENDAR_SPAN_DAYS_3Y
                    )
                    full_pe5 = bool(
                        len(pe5) >= MIN_PE_OBSERVATIONS_5Y and pe_dates5
                        and (trade_date - pe_dates5[0]).days >= MIN_CALENDAR_SPAN_DAYS_5Y
                    )
                    full_pb3 = bool(
                        len(pb3) >= MIN_PE_OBSERVATIONS_3Y and pb_dates3
                        and (trade_date - pb_dates3[0]).days >= MIN_CALENDAR_SPAN_DAYS_3Y
                    )
                    full_pb5 = bool(
                        len(pb5) >= MIN_PE_OBSERVATIONS_5Y and pb_dates5
                        and (trade_date - pb_dates5[0]).days >= MIN_CALENDAR_SPAN_DAYS_5Y
                    )
                    quality = "VALID" if full_pe5 else "WARNING"
                    rows.append((
                        code, trade_date, pe_value, pb_value, ps_ttm, dv_ratio, total_mv, circ_mv,
                        _rank(pe3, pe_value) if full_pe3 else None,
                        _rank(pe5, pe_value) if full_pe5 else None,
                        _rank(pb3, pb_value) if full_pb3 else None,
                        _rank(pb5, pb_value) if full_pb5 else None,
                        pe_valid, len(pe5),
                        pe_dates3[0] if pe_dates3 else None,
                        pe_dates5[0] if pe_dates5 else None,
                        len(pe3), len(pe5), full_pe3, full_pe5,
                        quality, trade_date,
                    ))
                    if len(rows) >= 5000:
                        total += _insert_valuation_rows(rows, write_conn, staging_table)
                        write_conn.commit()
                        rows.clear()
                if total and total % 500000 < 5000:
                    logger.info("valuation feature progress=%s", total)
            total += _insert_valuation_rows(rows, write_conn, staging_table)
            write_conn.commit()
        with write_conn.cursor() as write_cur:
            write_cur.execute("LOCK TABLE feature.valuation_daily_v1 IN ACCESS EXCLUSIVE MODE")
            write_cur.execute("TRUNCATE TABLE feature.valuation_daily_v1")
            write_cur.execute(
                f"INSERT INTO feature.valuation_daily_v1 SELECT * FROM {staging_table}"
            )
            write_cur.execute(f"DROP TABLE {staging_table}")
        write_conn.commit()
    except Exception:
        write_conn.rollback()
        try:
            with write_conn.cursor() as write_cur:
                write_cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
            write_conn.commit()
        except Exception:
            write_conn.rollback()
        raise
    finally:
        read_conn.close()
        write_conn.close()
    return total


def rebuild_company_financial() -> int:
    sql = """
    INSERT INTO feature.company_financial_v1
        (ts_code, report_date, effective_announce_date, version_available_date,
         vintage_quality, source_observed_at, ann_date, f_ann_date, roe, roa,
         debt_to_assets, revenue_yoy, or_yoy, tr_yoy, q_sales_yoy, netprofit_yoy,
         operating_cashflow, net_profit,
         operating_cashflow_to_profit, grossprofit_margin, netprofit_margin,
         quality_status, source_record_key, record_version, payload_hash)
    WITH events AS (
        SELECT ts_code, report_date, version_available_date
          FROM fina_indicator_standard WHERE version_available_date IS NOT NULL
        UNION
        SELECT stock_code, report_date, version_available_date
          FROM financial_income_standard WHERE version_available_date IS NOT NULL
        UNION
        SELECT stock_code, report_date, version_available_date
          FROM financial_cash_flow_standard WHERE version_available_date IS NOT NULL
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
            SELECT f.* FROM fina_indicator_standard f
             WHERE f.ts_code=e.ts_code AND f.report_date=e.report_date
               AND f.version_available_date <= e.version_available_date
             ORDER BY f.version_available_date DESC, f.record_version DESC,
                      f.indicator_id DESC LIMIT 1
        ) f ON TRUE
        LEFT JOIN LATERAL (
            SELECT c.* FROM financial_cash_flow_standard c
             WHERE c.stock_code=e.ts_code AND c.report_date=e.report_date
               AND c.version_available_date <= e.version_available_date
             ORDER BY c.version_available_date DESC, c.record_version DESC,
                      c.financial_id DESC LIMIT 1
        ) cf ON TRUE
        LEFT JOIN LATERAL (
            SELECT i.* FROM financial_income_standard i
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
    return _execute("TRUNCATE TABLE feature.company_financial_v1 RESTART IDENTITY;\n" + sql)


def rebuild_shareholder_return() -> int:
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
     AND e.implementation_status = '实施'
    GROUP BY d.stock_code, d.trade_date
    ON CONFLICT (ts_code, asof_date) DO UPDATE SET
      cash_div_12m=EXCLUDED.cash_div_12m,
      dividend_event_count_3y=EXCLUDED.dividend_event_count_3y,
      dividend_year_count_3y=EXCLUDED.dividend_year_count_3y,
      quality_status=EXCLUDED.quality_status, calculated_at=CURRENT_TIMESTAMP
    """
    return _execute("TRUNCATE TABLE feature.shareholder_return_v1 RESTART IDENTITY;\n" + sql)


def register_features() -> int:
    sql = """
    INSERT INTO ops.feature_registry (feature_name, feature_version, grain, lookback_rule, source_tables, code_version)
    VALUES
      ('price_daily_legacy', 'v1', 'stock x trade_date', 'legacy warehouse adj_factor series; read-only compatibility', ARRAY['daily_kline','adj_factor_daily'], 'phase2'),
      ('price_daily', 'v2', 'stock x trade_date', 'implemented corporate actions with ex_date <= trade_date; point-in-time total-return coordinate', ARRAY['daily_kline','std.dividend_event'], 'phase3'),
      ('valuation_daily', 'v1', 'stock x trade_date', 'rolling 3y/5y positive observations', ARRAY['valuation_daily'], 'phase2'),
      ('company_financial', 'v1', 'stock x report_date/version', 'effective_announce_date controls availability', ARRAY['fina_indicator_standard','financial_income_standard','financial_cash_flow_standard'], 'phase2'),
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
        "price_v2": rebuild_price_v2,
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
    parser.add_argument("--task", choices=["all", "price", "price_v2", "valuation", "company_financial", "shareholder_return", "registry"], default="all")
    args = parser.parse_args()
    print(run(args.task))
