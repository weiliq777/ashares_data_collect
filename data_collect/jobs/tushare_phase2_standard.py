"""阶段二 Standard、历史状态和 PIT 构建任务。

本任务只读取既有 Raw/Standard 并写入可重建的 std/pit 派生表，不访问 Tushare。
所有目标表均可安全重跑；不会修改或删除任何 ``*_raw`` 表。
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime

from data_collect.utils.db import get_connection


logger = logging.getLogger("tushare_phase2_standard")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())


def _execute(sql: str, params=None) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        affected = cur.rowcount
        conn.commit()
    return affected


def rebuild_lifecycle() -> int:
    # namechange 是名称历史的权威来源；同一证券同一开始日只保留最新 Raw 快照。
    # 开放 end_date 用下一次 start_date - 1 天推导，避免与下一段名称重叠。
    lifecycle_sql = """
    INSERT INTO std.security_lifecycle
        (ts_code, symbol, name, exchange, market, list_status, list_date, delist_date,
         effective_from, effective_to, source, source_record_key, record_version,
         source_payload_hash, quality_status)
    WITH ranked AS (
        SELECT DISTINCT ON (n.ts_code, n.start_date)
               n.ts_code, n.start_date, n.end_date, n.payload->>'name' AS name,
               n.payload_hash, n.source, n.fetched_at, n.record_id
        FROM tushare_namechange_raw n
        WHERE n.start_date IS NOT NULL
        ORDER BY n.ts_code, n.start_date, n.fetched_at DESC, n.record_id DESC
    ), chained AS (
        SELECT r.*, LEAD(r.start_date) OVER (PARTITION BY r.ts_code ORDER BY r.start_date) AS next_start
        FROM ranked r
    ), named AS (
        SELECT c.ts_code, b.symbol, c.name, split_part(c.ts_code, '.', 2) AS exchange,
               b.market, b.list_status, b.list_date, b.delist_date,
               GREATEST(c.start_date, COALESCE(b.list_date, c.start_date)) AS effective_from,
               NULLIF(LEAST(COALESCE(c.end_date, DATE '9999-12-31'),
                            COALESCE(c.next_start - 1, DATE '9999-12-31'),
                            COALESCE(b.delist_date, DATE '9999-12-31')),
                      DATE '9999-12-31') AS effective_to,
               c.source, c.ts_code || ':namechange:' || c.payload_hash AS source_record_key,
               c.record_id AS record_version, c.payload_hash AS source_payload_hash,
               CASE WHEN c.next_start IS NULL AND c.end_date IS NULL THEN 'VALID' ELSE 'VALID' END AS quality_status,
               c.fetched_at
        FROM chained c
        LEFT JOIN stock_basic_info b ON b.stock_code = c.ts_code
    ), fallback AS (
        SELECT b.stock_code AS ts_code, b.symbol, b.name, split_part(b.stock_code, '.', 2),
               b.market, b.list_status, b.list_date, b.delist_date,
               COALESCE(b.list_date, DATE '1990-01-01'), b.delist_date,
               b.source, b.stock_code || ':stock_basic',
               1::bigint,
               CASE WHEN r.payload IS NULL THEN NULL
                    ELSE encode(digest(convert_to(r.payload::text, 'UTF8'), 'sha256'), 'hex') END,
               CASE WHEN b.list_date IS NULL THEN 'WARNING' ELSE 'VALID' END, b.fetched_at
        FROM stock_basic_info b
        LEFT JOIN tushare_stock_basic_raw r ON r.ts_code = b.stock_code
        WHERE NOT EXISTS (
            SELECT 1 FROM tushare_namechange_raw n
            WHERE n.ts_code=b.stock_code AND n.start_date IS NOT NULL
        )
    ), all_rows AS (
        SELECT * FROM named WHERE effective_from <= COALESCE(effective_to, DATE '9999-12-31')
        UNION ALL SELECT * FROM fallback
    )
    SELECT ts_code, symbol, name, exchange, market, list_status, list_date, delist_date,
           effective_from, effective_to, source, source_record_key, record_version,
           source_payload_hash, quality_status
    FROM all_rows
    ON CONFLICT DO NOTHING
    """
    return _execute("TRUNCATE TABLE std.security_lifecycle RESTART IDENTITY;\n" + lifecycle_sql)


def rebuild_status_daily() -> int:
    sql = """
    INSERT INTO std.security_status_daily
        (trade_date, ts_code, is_listed, is_st, is_suspended, is_tradeable,
         limit_status, up_limit, down_limit, close_price, status_reason,
         source, quality_status)
    WITH dates AS (
        SELECT cal_date AS trade_date
        FROM tushare_trade_cal_raw
        WHERE exchange = 'SSE' AND (payload->>'is_open')::int = 1
    ), universe AS (
        SELECT d.trade_date, b.stock_code, b.market, b.list_date, b.delist_date
        FROM dates d
        JOIN stock_basic_info b
          ON COALESCE(b.list_date, DATE '1900-01-01') <= d.trade_date
         AND (b.delist_date IS NULL OR b.delist_date >= d.trade_date)
    ), limits AS (
        SELECT DISTINCT ON (ts_code, trade_date)
               ts_code, trade_date,
               (payload->>'up_limit')::double precision AS up_limit,
               (payload->>'down_limit')::double precision AS down_limit,
               fetched_at, record_id
        FROM tushare_stk_limit_raw
        ORDER BY ts_code, trade_date, fetched_at DESC, record_id DESC
    ), flags AS (
        SELECT u.*,
               EXISTS (
                   SELECT 1 FROM tushare_stock_st_raw st
                   WHERE st.ts_code = u.stock_code AND st.trade_date = u.trade_date
               ) AS is_st,
               EXISTS (
                   SELECT 1 FROM tushare_suspend_d_raw sd
                   WHERE sd.ts_code = u.stock_code AND sd.trade_date = u.trade_date
                     AND sd.suspend_type = 'S'
               ) AS is_suspended,
               d.close AS close_price,
               l.up_limit,
               l.down_limit
        FROM universe u
        LEFT JOIN daily_kline d
          ON d.stock_code = u.stock_code AND d.trade_date = u.trade_date
        LEFT JOIN limits l
          ON l.ts_code = u.stock_code AND l.trade_date = u.trade_date
    )
    SELECT trade_date, stock_code, TRUE, is_st, is_suspended,
           (close_price IS NOT NULL AND close_price > 0 AND NOT is_suspended) AS is_tradeable,
           CASE
             WHEN close_price IS NULL OR close_price <= 0 THEN 'NO_PRICE'
             WHEN up_limit IS NOT NULL AND close_price >= up_limit - 0.0001 THEN 'UP_LIMIT'
             WHEN down_limit IS NOT NULL AND close_price <= down_limit + 0.0001 THEN 'DOWN_LIMIT'
             ELSE 'NONE'
           END,
           up_limit, down_limit, close_price,
           NULLIF(concat_ws(',',
               CASE WHEN is_st THEN 'ST' END,
               CASE WHEN is_suspended THEN 'SUSPENDED' END,
               CASE WHEN close_price IS NULL OR close_price <= 0 THEN 'NO_PRICE' END,
               CASE WHEN up_limit IS NOT NULL AND close_price >= up_limit - 0.0001 THEN 'UP_LIMIT' END,
               CASE WHEN down_limit IS NOT NULL AND close_price <= down_limit + 0.0001 THEN 'DOWN_LIMIT' END
           ), '') AS status_reason,
           'tushare',
           CASE WHEN close_price IS NULL THEN 'MISSING_SOURCE'
                WHEN close_price <= 0 THEN 'WARNING'
                WHEN is_suspended OR is_st THEN 'WARNING' ELSE 'VALID' END
    FROM flags
    ON CONFLICT (trade_date, ts_code) DO UPDATE SET
       is_listed=EXCLUDED.is_listed, is_st=EXCLUDED.is_st,
       is_suspended=EXCLUDED.is_suspended, is_tradeable=EXCLUDED.is_tradeable,
       limit_status=EXCLUDED.limit_status, up_limit=EXCLUDED.up_limit,
       down_limit=EXCLUDED.down_limit, close_price=EXCLUDED.close_price,
       status_reason=EXCLUDED.status_reason, quality_status=EXCLUDED.quality_status,
       fetched_at=CURRENT_TIMESTAMP
    """
    return _execute("TRUNCATE TABLE std.security_status_daily;\n" + sql)


def rebuild_universe() -> int:
    sql = """
    INSERT INTO pit.universe_daily
        (trade_date, ts_code, is_listed_asof, is_st_asof, is_suspended_asof,
         has_price_history, days_since_listing, eligible_by_market, exclusion_reason,
         quality_status, source_asof_date)
    SELECT s.trade_date, s.ts_code, s.is_listed, s.is_st, s.is_suspended,
           (d.stock_code IS NOT NULL),
           CASE WHEN b.list_date IS NULL THEN NULL ELSE s.trade_date - b.list_date END,
           (b.market IN ('主板','创业板','科创板','北交所')),
           NULLIF(concat_ws(',',
               CASE WHEN b.market NOT IN ('主板','创业板','科创板','北交所') OR b.market IS NULL THEN 'MARKET' END,
               CASE WHEN s.is_st THEN 'ST' END,
               CASE WHEN NOT s.is_listed THEN 'NOT_LISTED' END
           ), ''),
           s.quality_status, s.trade_date
    FROM std.security_status_daily s
    LEFT JOIN stock_basic_info b ON b.stock_code = s.ts_code
    LEFT JOIN daily_kline d ON d.stock_code = s.ts_code AND d.trade_date = s.trade_date
    ON CONFLICT (trade_date, ts_code) DO UPDATE SET
      is_listed_asof=EXCLUDED.is_listed_asof, is_st_asof=EXCLUDED.is_st_asof,
      is_suspended_asof=EXCLUDED.is_suspended_asof, has_price_history=EXCLUDED.has_price_history,
      days_since_listing=EXCLUDED.days_since_listing, eligible_by_market=EXCLUDED.eligible_by_market,
      exclusion_reason=EXCLUDED.exclusion_reason, quality_status=EXCLUDED.quality_status,
      source_asof_date=EXCLUDED.source_asof_date, calculated_at=CURRENT_TIMESTAMP
    """
    return _execute("TRUNCATE TABLE pit.universe_daily;\n" + sql)


def rebuild_tradeability() -> int:
    sql = """
    INSERT INTO pit.security_tradeability_daily
        (trade_date, ts_code, is_tradeable, can_buy, can_sell, is_suspended,
         limit_status, blocked_reason, quality_status, source_asof_date)
    SELECT trade_date, ts_code, is_tradeable,
           is_tradeable,
           is_tradeable,
           is_suspended, limit_status,
           NULLIF(concat_ws(',',
             CASE WHEN NOT is_tradeable THEN 'NOT_TRADEABLE' END
           ), ''),
           quality_status, trade_date
    FROM std.security_status_daily
    ON CONFLICT (trade_date, ts_code) DO UPDATE SET
      is_tradeable=EXCLUDED.is_tradeable, can_buy=EXCLUDED.can_buy,
      can_sell=EXCLUDED.can_sell, is_suspended=EXCLUDED.is_suspended,
      limit_status=EXCLUDED.limit_status, blocked_reason=EXCLUDED.blocked_reason,
      quality_status=EXCLUDED.quality_status, source_asof_date=EXCLUDED.source_asof_date,
      calculated_at=CURRENT_TIMESTAMP
    """
    return _execute("TRUNCATE TABLE pit.security_tradeability_daily;\n" + sql)


def rebuild_dividend() -> int:
    sql = """
    INSERT INTO std.dividend_event
        (ts_code, report_date, announce_date, implementation_status, stock_div,
         stock_bonus_rate, stock_conversion_rate, cash_div, cash_div_tax,
         record_date, ex_date, pay_date, div_list_date, implementation_announce_date,
        source, source_record_key, record_version, business_key, payload_hash, quality_status)
    SELECT ts_code, end_date, ann_date, div_proc,
           NULLIF(payload->>'stk_div', '')::double precision,
           NULLIF(payload->>'stk_bo_rate', '')::double precision,
           NULLIF(payload->>'stk_co_rate', '')::double precision,
           NULLIF(payload->>'cash_div', '')::double precision,
           NULLIF(payload->>'cash_div_tax', '')::double precision,
           record_date, ex_date,
           to_date(NULLIF(payload->>'pay_date', ''), 'YYYYMMDD'),
           to_date(NULLIF(payload->>'div_listdate', ''), 'YYYYMMDD'),
           to_date(NULLIF(payload->>'imp_ann_date', ''), 'YYYYMMDD'),
           source, business_key, record_id, business_key, payload_hash,
           CASE WHEN div_proc = '实施' AND ex_date IS NULL THEN 'WARNING' ELSE 'VALID' END
    FROM (
      SELECT DISTINCT ON (ts_code, end_date, div_proc, record_date, ex_date, (payload->>'imp_ann_date'))
             record_id, ts_code, end_date, ann_date, div_proc, record_date, ex_date,
             payload, source, payload_hash,
             ts_code || ':' || COALESCE(end_date::text,'') || ':' ||
             COALESCE(div_proc,'') || ':' || COALESCE(record_date::text,'') || ':' ||
             COALESCE(ex_date::text,'') || ':' || COALESCE((payload->>'imp_ann_date'),'') AS business_key
      FROM tushare_dividend_raw
      ORDER BY ts_code, end_date, div_proc, record_date, ex_date, (payload->>'imp_ann_date'),
               fetched_at DESC, record_id DESC
    ) d
    ON CONFLICT (source, source_record_key, record_version) DO UPDATE SET
      report_date=EXCLUDED.report_date, announce_date=EXCLUDED.announce_date,
      implementation_status=EXCLUDED.implementation_status, stock_div=EXCLUDED.stock_div,
      cash_div=EXCLUDED.cash_div, cash_div_tax=EXCLUDED.cash_div_tax,
      record_date=EXCLUDED.record_date, ex_date=EXCLUDED.ex_date,
      pay_date=EXCLUDED.pay_date, div_list_date=EXCLUDED.div_list_date,
      implementation_announce_date=EXCLUDED.implementation_announce_date,
      business_key=EXCLUDED.business_key, payload_hash=EXCLUDED.payload_hash,
      quality_status=EXCLUDED.quality_status, fetched_at=CURRENT_TIMESTAMP
    """
    return _execute("TRUNCATE TABLE std.dividend_event RESTART IDENTITY;\n" + sql)


def rebuild_index_daily() -> int:
    sql = """
    INSERT INTO std.index_daily_tushare
        (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg,
         volume, amount, source)
    SELECT ts_code, trade_date,
           NULLIF(payload->>'open', '')::double precision,
           NULLIF(payload->>'high', '')::double precision,
           NULLIF(payload->>'low', '')::double precision,
           NULLIF(payload->>'close', '')::double precision,
           NULLIF(payload->>'pre_close', '')::double precision,
           NULLIF(payload->>'change', '')::double precision,
           NULLIF(payload->>'pct_chg', '')::double precision,
           NULLIF(payload->>'vol', '')::double precision * 100,
           NULLIF(payload->>'amount', '')::double precision * 1000,
           source
    FROM tushare_index_daily_raw
    ON CONFLICT (ts_code, trade_date, source) DO UPDATE SET
      open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
      pre_close=EXCLUDED.pre_close, change=EXCLUDED.change, pct_chg=EXCLUDED.pct_chg,
      volume=EXCLUDED.volume, amount=EXCLUDED.amount, fetched_at=CURRENT_TIMESTAMP
    """
    return _execute("TRUNCATE TABLE std.index_daily_tushare;\n" + sql)


def rebuild_financial_asof() -> int:
    sql = """
    INSERT INTO pit.financial_asof
        (decision_date, ts_code, report_date, effective_announce_date,
         version_available_date, ann_date,
         f_ann_date, report_type, comp_type, record_version, source_record_key,
         roe, roa, debt_to_assets, revenue_yoy, netprofit_yoy, or_yoy, tr_yoy,
         q_sales_yoy, payload_hash, vintage_quality, source_observed_at,
         quality_status)
    SELECT f.trade_date, f.ts_code, f.report_date, f.effective_announce_date,
        f.version_available_date, f.ann_date, f.f_ann_date, f.report_type,
        f.comp_type, f.record_version,
        f.source_record_key, f.roe, f.roa, f.debt_to_assets, f.or_yoy,
        f.netprofit_yoy, f.or_yoy, f.tr_yoy, f.q_sales_yoy, f.payload_hash,
        f.vintage_quality, f.source_observed_at,
        f.quality_status
    FROM (
        SELECT d.trade_date, i.ts_code, i.report_date, i.effective_announce_date,
               i.version_available_date,
               i.ann_date, i.f_ann_date, i.report_type, i.comp_type, i.record_version,
               i.source_record_key, i.roe, i.roa, i.debt_to_assets, i.or_yoy,
               i.tr_yoy, i.q_sales_yoy, i.netprofit_yoy, i.payload_hash,
               i.vintage_quality, i.source_observed_at,
               i.quality_status, i.fetched_at
        FROM (SELECT DISTINCT stock_code, trade_date FROM daily_kline) d
        JOIN LATERAL (
            SELECT i.*
            FROM fina_indicator_standard i
            WHERE i.ts_code = d.stock_code
              AND i.effective_announce_date IS NOT NULL
              AND i.effective_announce_date <= d.trade_date
              AND i.version_available_date IS NOT NULL
              AND i.version_available_date <= d.trade_date
            ORDER BY i.report_date DESC, i.version_available_date DESC,
                     i.effective_announce_date DESC,
                     i.record_version DESC, i.fetched_at DESC
            LIMIT 1
        ) i ON TRUE
    ) f
    ON CONFLICT (decision_date, ts_code) DO UPDATE SET
      report_date=EXCLUDED.report_date, effective_announce_date=EXCLUDED.effective_announce_date,
      version_available_date=EXCLUDED.version_available_date,
      ann_date=EXCLUDED.ann_date, f_ann_date=EXCLUDED.f_ann_date,
      report_type=EXCLUDED.report_type, comp_type=EXCLUDED.comp_type,
      record_version=EXCLUDED.record_version, source_record_key=EXCLUDED.source_record_key,
      roe=EXCLUDED.roe, roa=EXCLUDED.roa, debt_to_assets=EXCLUDED.debt_to_assets,
      revenue_yoy=EXCLUDED.revenue_yoy, netprofit_yoy=EXCLUDED.netprofit_yoy,
      or_yoy=EXCLUDED.or_yoy, tr_yoy=EXCLUDED.tr_yoy, q_sales_yoy=EXCLUDED.q_sales_yoy,
      payload_hash=EXCLUDED.payload_hash,
      vintage_quality=EXCLUDED.vintage_quality,
      source_observed_at=EXCLUDED.source_observed_at,
      quality_status=EXCLUDED.quality_status,
      calculated_at=CURRENT_TIMESTAMP
    """
    return _execute("TRUNCATE TABLE pit.financial_asof;\n" + sql)


def run(task: str = "all") -> dict[str, int]:
    tasks = {
        "lifecycle": rebuild_lifecycle,
        "status": rebuild_status_daily,
        "universe": rebuild_universe,
        "tradeability": rebuild_tradeability,
        "dividend": rebuild_dividend,
        "index_daily": rebuild_index_daily,
        "financial_asof": rebuild_financial_asof,
    }
    if task == "all":
        selected = list(tasks)
    elif task in tasks:
        selected = [task]
    else:
        raise ValueError(f"不支持的 task: {task}")
    result = {}
    for name in selected:
        logger.info("phase2 standard task started=%s", name)
        result[name] = tasks[name]()
        logger.info("phase2 standard task finished=%s affected=%s", name, result[name])
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="阶段二 Standard/PIT 构建")
    parser.add_argument("--task", choices=["all", "lifecycle", "status", "universe", "tradeability", "dividend", "index_daily", "financial_asof"], default="all")
    args = parser.parse_args()
    print(run(args.task))
