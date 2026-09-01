"""Typed Research Snapshot reads and manifest persistence.

The job layer owns orchestration and pure hashing/validation rules.  This
repository owns all Research/Release SQL and reaches PostgreSQL only through
Kernel gateways.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.repository.base import BaseRepository
from data_collect.data_access.research_contract import is_foundation_schema


_IMMUTABLE_INPUT_STATS_CACHE: dict[tuple, dict[str, Any]] = {}


class ResearchSnapshotRepository(BaseRepository):
    # Snapshot SQL is set-based, but one code batch must also remain below the
    # governed 30-second statement deadline on the oldest/highest-cost dates.
    # 100 codes keeps each stock/lineage INSERT ... SELECT below the governed
    # 30-second statement deadline even on the oldest, most expensive dates.
    SNAPSHOT_CODE_BATCH_SIZE = 100
    INPUT_STATS_BATCH_ROWS = 2_000
    # Input-manifest hashing must remain row-wise, but a single named cursor
    # over a wide historical feature table can still spend the whole statement
    # deadline sorting/reading before returning its first batch.  Keyset pages
    # keep each read independently bounded and use the date/code indexes.
    INPUT_STATS_PAGE_ROWS = 500
    INPUT_STATS_KEYSET_COLUMNS = {
        "pit.universe_daily": "ts_code",
        "pit.security_tradeability_daily": "ts_code",
        "feature.price_daily_v2": "ts_code",
        "feature.valuation_daily_v1": "ts_code",
        "std.security_status_daily": "ts_code",
        "feature.shareholder_return_v1": "ts_code",
    }

    @staticmethod
    def stock_snapshot_sql():
        return """
        INSERT INTO research.stock_snapshot_daily
        (snapshot_id, research_date, ts_code, symbol, name, market, list_status, list_date, delist_date,
         is_listed_asof, is_st_asof, is_suspended_asof, eligible_by_market, is_tradeable, can_buy, can_sell, limit_status,
         raw_open, raw_high, raw_low, raw_close, raw_volume, raw_amount, up_limit, down_limit,
         adj_close_base, price_feature_version, price_adjustment_mode,
         return_1d, return_20d, return_60d, ma20, ma60, ma120, volume_ratio_20d,
         available_observation_count, ma20_observation_count, ma60_observation_count, ma120_observation_count,
         pe_ttm, pb, ps_ttm, dv_ratio, total_mv, circ_mv, pe_pct_3y, pe_pct_5y, pb_pct_3y, pb_pct_5y,
         is_pe_meaningful, valuation_observation_count, pe_window_start_date_5y,
         has_full_pe_5y_history, report_date, effective_announce_date,
         financial_version_available_date, financial_record_version,
         financial_source_record_key, financial_payload_hash, financial_vintage_quality,
         financial_source_observed_at,
         roe, roa, debt_to_assets, revenue_yoy,
         tr_yoy, q_sales_yoy, netprofit_yoy, operating_cashflow, net_profit, cash_div_12m,
         dividend_event_count_3y, dividend_year_count_3y,
         annual_report_count_5y, roe_above_15_years_5y, roe_5y_min,
         roe_5y_all_above_15, ocf_report_count_3y, ocf_positive_years_3y,
         ocf_3y_all_positive, quarterly_report_count_4q,
         q_sales_yoy_above_30_quarters_4q, q_sales_yoy_4q_all_above_30,
         instrument_source_record_key, instrument_source_record_version,
         instrument_source_payload_hash,
         quality_status)
         SELECT %s, u.trade_date, u.ts_code, b.payload->>'symbol',
                COALESCE(l.name, b.payload->>'name'),
                COALESCE(l.market, b.payload->>'market'),
                CASE
                  WHEN to_date(NULLIF(b.payload->>'list_date',''), 'YYYYMMDD') > u.trade_date
                  THEN 'N'
                  WHEN u.is_listed_asof THEN 'L'
                  ELSE 'D'
                END,
                CASE
                  WHEN to_date(NULLIF(b.payload->>'list_date',''), 'YYYYMMDD') <= u.trade_date
                  THEN to_date(NULLIF(b.payload->>'list_date',''), 'YYYYMMDD')
                  ELSE NULL
                END,
                CASE
                  WHEN to_date(NULLIF(b.payload->>'delist_date',''), 'YYYYMMDD') <= u.trade_date
                  THEN to_date(NULLIF(b.payload->>'delist_date',''), 'YYYYMMDD')
                  ELSE NULL
                END,
               u.is_listed_asof, u.is_st_asof, u.is_suspended_asof, u.eligible_by_market,
               t.is_tradeable, t.can_buy, t.can_sell, t.limit_status,
               d.open, d.high, d.low, p.raw_close, d.volume, d.amount,
               st.up_limit, st.down_limit,
                p.asof_close, p.feature_set_version, p.adjustment_mode,
                p.return_1d, p.return_20d, p.return_60d,
               p.ma20, p.ma60, p.ma120, p.volume_ratio_20d,
               p.available_observation_count, p.ma20_observation_count,
               p.ma60_observation_count, p.ma120_observation_count,
               v.pe_ttm, v.pb, v.ps_ttm, v.dv_ratio, v.total_mv, v.circ_mv,
               v.pe_pct_3y, v.pe_pct_5y, v.pb_pct_3y, v.pb_pct_5y,
               v.is_pe_meaningful, v.valuation_observation_count,
               v.pe_window_start_date_5y, COALESCE(v.has_full_pe_5y_history, FALSE),
                cf.report_date, cf.effective_announce_date, cf.version_available_date,
                cf.record_version,
                cf.source_record_key, cf.payload_hash, cf.vintage_quality,
                cf.source_observed_at,
                cf.roe, cf.roa, cf.debt_to_assets,
                cf.revenue_yoy, cf.tr_yoy, cf.q_sales_yoy, cf.netprofit_yoy,
               cf.operating_cashflow, cf.net_profit,
               sr.cash_div_12m, sr.dividend_event_count_3y, sr.dividend_year_count_3y,
               COALESCE(fh.annual_report_count_5y, 0),
               COALESCE(fh.roe_above_15_years_5y, 0), fh.roe_5y_min,
               COALESCE(fh.roe_5y_all_above_15, FALSE),
               COALESCE(fh.ocf_report_count_3y, 0),
               COALESCE(fh.ocf_positive_years_3y, 0),
               COALESCE(fh.ocf_3y_all_positive, FALSE),
               COALESCE(fh.quarterly_report_count_4q, 0),
               COALESCE(fh.q_sales_yoy_above_30_quarters_4q, 0),
               COALESCE(fh.q_sales_yoy_4q_all_above_30, FALSE),
               b.business_key, b.record_id, b.payload_hash,
               CASE WHEN 'MISSING_SOURCE' = ANY(ARRAY[
                        COALESCE(u.quality_status,'MISSING_SOURCE'), COALESCE(t.quality_status,'MISSING_SOURCE'),
                        COALESCE(p.quality_status,'MISSING_SOURCE'), COALESCE(v.quality_status,'MISSING_SOURCE'),
                        COALESCE(cf.quality_status,'MISSING_SOURCE'), COALESCE(sr.quality_status,'MISSING_SOURCE')])
                    THEN 'MISSING_SOURCE'
                    WHEN 'WARNING' = ANY(ARRAY[
                        COALESCE(u.quality_status,'WARNING'), COALESCE(t.quality_status,'WARNING'),
                        COALESCE(p.quality_status,'WARNING'), COALESCE(v.quality_status,'WARNING'),
                        COALESCE(cf.quality_status,'WARNING'), COALESCE(sr.quality_status,'WARNING')])
                    THEN 'WARNING' ELSE 'VALID' END
        FROM pit.universe_daily u
        JOIN LATERAL (
          SELECT record_id, business_key, payload, payload_hash
          FROM tushare_stock_basic_raw_version b
          WHERE b.ts_code=u.ts_code AND b.record_id <= %s
          ORDER BY b.fetched_at DESC, b.record_id DESC LIMIT 1
        ) b ON TRUE
        LEFT JOIN pit.security_tradeability_daily t
          ON t.trade_date=u.trade_date AND t.ts_code=u.ts_code
         LEFT JOIN feature.price_daily_v2 p
          ON p.trade_date=u.trade_date AND p.ts_code=u.ts_code
        LEFT JOIN LATERAL (
          SELECT d.open, d.high, d.low, d.close, d.volume, d.amount
          FROM std.daily_kline d
          WHERE d.trade_date=u.trade_date AND d.stock_code=u.ts_code
          LIMIT 1
        ) d ON TRUE
        LEFT JOIN std.security_status_daily st
          ON st.trade_date=u.trade_date AND st.ts_code=u.ts_code
        LEFT JOIN feature.valuation_daily_v1 v
          ON v.trade_date=u.trade_date AND v.ts_code=u.ts_code
        LEFT JOIN feature.shareholder_return_v1 sr
          ON sr.asof_date=u.trade_date AND sr.ts_code=u.ts_code
        LEFT JOIN LATERAL (
           SELECT l.name, l.market FROM std.security_lifecycle l
          WHERE l.ts_code=u.ts_code AND l.effective_from <= u.trade_date
            AND (l.effective_to IS NULL OR l.effective_to >= u.trade_date)
          ORDER BY l.effective_from DESC, l.lifecycle_id DESC LIMIT 1
        ) l ON TRUE
        LEFT JOIN LATERAL (
          SELECT c.report_date, c.effective_announce_date, c.version_available_date,
                 c.record_version, c.source_record_key, c.payload_hash,
                 c.roe, c.roa, c.debt_to_assets, c.revenue_yoy, c.tr_yoy,
                 c.q_sales_yoy, c.netprofit_yoy, c.operating_cashflow, c.net_profit,
                 c.vintage_quality, c.source_observed_at, c.quality_status
          FROM feature.company_financial_v1 c
          WHERE c.ts_code=u.ts_code AND c.version_available_date IS NOT NULL
            AND c.version_available_date <= u.trade_date
          ORDER BY c.report_date DESC, c.version_available_date DESC,
                   c.record_version DESC, c.feature_id DESC LIMIT 1
        ) cf ON TRUE
        LEFT JOIN LATERAL (
          WITH visible AS (
            SELECT DISTINCT ON (c.report_date)
                   c.report_date, c.roe, c.operating_cashflow, c.q_sales_yoy
            FROM feature.company_financial_v1 c
            WHERE c.ts_code=u.ts_code
              AND c.version_available_date IS NOT NULL
              AND c.version_available_date <= u.trade_date
            ORDER BY c.report_date, c.version_available_date DESC,
                     c.record_version DESC, c.feature_id DESC
          ), annual5 AS (
            SELECT * FROM visible
            WHERE EXTRACT(MONTH FROM report_date)=12
              AND EXTRACT(DAY FROM report_date)=31
            ORDER BY report_date DESC LIMIT 5
          ), annual3 AS (
            SELECT * FROM annual5 ORDER BY report_date DESC LIMIT 3
          ), quarter4 AS (
            SELECT * FROM visible ORDER BY report_date DESC LIMIT 4
          )
          SELECT
            (SELECT COUNT(*)::integer FROM annual5) AS annual_report_count_5y,
            (SELECT COUNT(*) FILTER (WHERE roe > 15)::integer FROM annual5) AS roe_above_15_years_5y,
            (SELECT MIN(roe) FROM annual5) AS roe_5y_min,
            COALESCE((SELECT COUNT(*)=5 AND COUNT(roe)=5
                             AND MAX(EXTRACT(YEAR FROM report_date))
                                 - MIN(EXTRACT(YEAR FROM report_date))=4
                             AND BOOL_AND(roe > 15)
                      FROM annual5), FALSE) AS roe_5y_all_above_15,
            (SELECT COUNT(*)::integer FROM annual3) AS ocf_report_count_3y,
            (SELECT COUNT(*) FILTER (WHERE operating_cashflow > 0)::integer FROM annual3) AS ocf_positive_years_3y,
            COALESCE((SELECT COUNT(*)=3 AND COUNT(operating_cashflow)=3
                             AND MAX(EXTRACT(YEAR FROM report_date))
                                 - MIN(EXTRACT(YEAR FROM report_date))=2
                             AND BOOL_AND(operating_cashflow > 0)
                      FROM annual3), FALSE) AS ocf_3y_all_positive,
            (SELECT COUNT(*)::integer FROM quarter4) AS quarterly_report_count_4q,
            (SELECT COUNT(*) FILTER (WHERE q_sales_yoy > 30)::integer FROM quarter4) AS q_sales_yoy_above_30_quarters_4q,
            COALESCE((SELECT COUNT(*)=4 AND COUNT(q_sales_yoy)=4
                             AND MAX(EXTRACT(YEAR FROM report_date)::integer * 4
                                     + EXTRACT(QUARTER FROM report_date)::integer)
                                 - MIN(EXTRACT(YEAR FROM report_date)::integer * 4
                                       + EXTRACT(QUARTER FROM report_date)::integer)=3
                             AND BOOL_AND(q_sales_yoy > 30)
                      FROM quarter4), FALSE) AS q_sales_yoy_4q_all_above_30
        ) fh ON TRUE
        WHERE u.trade_date=%s
        ON CONFLICT (snapshot_id, research_date, ts_code) DO NOTHING
        """

    @staticmethod
    def lineage_sql():
        return """
        INSERT INTO research.snapshot_lineage
          (snapshot_id, research_date, ts_code, field_group, source_table,
           source_record_key, source_record_version, source_payload_hash)
        WITH params AS (SELECT %s::uuid AS snapshot_id), groups AS (
            SELECT s.snapshot_id, s.research_date, s.ts_code, g.field_group
            FROM research.stock_snapshot_daily s
            CROSS JOIN LATERAL unnest(ARRAY[
                'universe','tradeability','price','valuation','financial',
                'shareholder_return','lifecycle','instrument','market_facts',
                'fundamental_history'
            ]::varchar[]) AS g(field_group)
            WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
        ), refs AS (
            -- PIT/Feature v1 记录没有独立的版本列；1 表示对应的 v1 派生记录，
            -- 哈希只覆盖本快照实际读取的字段，避免把计算时间等非事实列纳入血缘。
            SELECT s.snapshot_id, s.research_date, s.ts_code, 'universe'::varchar AS field_group,
                   'pit.universe_daily'::varchar AS source_table,
                   s.research_date::text || ':' || s.ts_code AS source_record_key,
                   1::bigint AS source_record_version,
                   encode(digest(convert_to(jsonb_build_object(
                       'trade_date', u.trade_date, 'ts_code', u.ts_code,
                       'is_listed_asof', u.is_listed_asof, 'is_st_asof', u.is_st_asof,
                       'is_suspended_asof', u.is_suspended_asof,
                       'eligible_by_market', u.eligible_by_market,
                       'has_price_history', u.has_price_history,
                       'quality_status', u.quality_status,
                       'source_asof_date', u.source_asof_date
                   )::text, 'UTF8'), 'sha256'), 'hex')::char(64) AS source_payload_hash
            FROM research.stock_snapshot_daily s
            JOIN pit.universe_daily u ON u.trade_date=s.research_date AND u.ts_code=s.ts_code
            WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
            UNION ALL
            SELECT s.snapshot_id, s.research_date, s.ts_code, 'tradeability',
                   'pit.security_tradeability_daily', s.research_date::text || ':' || s.ts_code,
                   1::bigint,
                   encode(digest(convert_to(jsonb_build_object(
                       'trade_date', t.trade_date, 'ts_code', t.ts_code,
                       'is_tradeable', t.is_tradeable, 'can_buy', t.can_buy,
                       'can_sell', t.can_sell, 'is_suspended', t.is_suspended,
                       'limit_status', t.limit_status, 'quality_status', t.quality_status,
                       'source_asof_date', t.source_asof_date
                   )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
            FROM research.stock_snapshot_daily s
            JOIN pit.security_tradeability_daily t ON t.trade_date=s.research_date AND t.ts_code=s.ts_code
            WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
            UNION ALL
            SELECT s.snapshot_id, s.research_date, s.ts_code, 'price',
                    'feature.price_daily_v2', p.ts_code || ':' || p.trade_date::text,
                   1::bigint,
                   encode(digest(convert_to(jsonb_build_object(
                       'ts_code', p.ts_code, 'trade_date', p.trade_date,
                        'raw_close', p.raw_close, 'asof_close', p.asof_close,
                        'asof_adjustment_factor', p.asof_adjustment_factor,
                       'return_1d', p.return_1d, 'return_20d', p.return_20d,
                       'return_60d', p.return_60d, 'ma20', p.ma20, 'ma60', p.ma60,
                       'ma120', p.ma120, 'volume_ratio_20d', p.volume_ratio_20d,
                       'available_observation_count', p.available_observation_count,
                       'ma20_observation_count', p.ma20_observation_count,
                       'ma60_observation_count', p.ma60_observation_count,
                       'ma120_observation_count', p.ma120_observation_count,
                        'feature_set_version', p.feature_set_version,
                        'adjustment_mode', p.adjustment_mode,
                        'quality_status', p.quality_status, 'source_asof_date', p.source_asof_date
                   )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
            FROM research.stock_snapshot_daily s
             JOIN feature.price_daily_v2 p ON p.trade_date=s.research_date AND p.ts_code=s.ts_code
            WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
            UNION ALL
            SELECT s.snapshot_id, s.research_date, s.ts_code, 'valuation',
                   'feature.valuation_daily_v1', v.ts_code || ':' || v.trade_date::text,
                   1::bigint,
                   encode(digest(convert_to(jsonb_build_object(
                       'ts_code', v.ts_code, 'trade_date', v.trade_date,
                       'pe_ttm', v.pe_ttm, 'pb', v.pb, 'ps_ttm', v.ps_ttm,
                       'dv_ratio', v.dv_ratio, 'total_mv', v.total_mv, 'circ_mv', v.circ_mv,
                       'pe_pct_3y', v.pe_pct_3y, 'pe_pct_5y', v.pe_pct_5y,
                       'pb_pct_3y', v.pb_pct_3y, 'pb_pct_5y', v.pb_pct_5y,
                       'is_pe_meaningful', v.is_pe_meaningful,
                       'valuation_observation_count', v.valuation_observation_count,
                       'pe_window_start_date_5y', v.pe_window_start_date_5y,
                       'pe_observation_count_5y', v.pe_observation_count_5y,
                       'has_full_pe_5y_history', v.has_full_pe_5y_history,
                       'quality_status', v.quality_status, 'source_asof_date', v.source_asof_date
                   )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
            FROM research.stock_snapshot_daily s
            JOIN feature.valuation_daily_v1 v ON v.trade_date=s.research_date AND v.ts_code=s.ts_code
            WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
            UNION ALL
            SELECT s.snapshot_id, s.research_date, s.ts_code, 'financial',
                   'feature.company_financial_v1', COALESCE(s.financial_source_record_key,
                       s.ts_code || ':' || s.research_date::text || ':missing'),
                   s.financial_record_version, s.financial_payload_hash
            FROM research.stock_snapshot_daily s WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
            UNION ALL
            SELECT s.snapshot_id, s.research_date, s.ts_code, 'shareholder_return',
                   'feature.shareholder_return_v1', sr.ts_code || ':' || sr.asof_date::text,
                   1::bigint,
                   encode(digest(convert_to(jsonb_build_object(
                       'ts_code', sr.ts_code, 'asof_date', sr.asof_date,
                       'cash_div_12m', sr.cash_div_12m,
                       'dividend_event_count_3y', sr.dividend_event_count_3y,
                       'dividend_year_count_3y', sr.dividend_year_count_3y,
                       'quality_status', sr.quality_status
                   )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
            FROM research.stock_snapshot_daily s
            JOIN feature.shareholder_return_v1 sr ON sr.asof_date=s.research_date AND sr.ts_code=s.ts_code
            WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
            UNION ALL
            SELECT s.snapshot_id, s.research_date, s.ts_code, 'lifecycle',
                   'std.security_lifecycle', l.source_record_key, l.record_version,
                   l.source_payload_hash
            FROM research.stock_snapshot_daily s
            JOIN LATERAL (
                SELECT l.* FROM std.security_lifecycle l
                WHERE l.ts_code=s.ts_code AND l.effective_from <= s.research_date
                  AND (l.effective_to IS NULL OR l.effective_to >= s.research_date)
                ORDER BY l.effective_from DESC, l.lifecycle_id DESC LIMIT 1
            ) l ON TRUE
            WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
            UNION ALL
            SELECT s.snapshot_id, s.research_date, s.ts_code, 'instrument',
                   'tushare_stock_basic_raw_version', s.instrument_source_record_key,
                   s.instrument_source_record_version,
                   s.instrument_source_payload_hash
            FROM research.stock_snapshot_daily s
            WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
            UNION ALL
            SELECT s.snapshot_id, s.research_date, s.ts_code, 'market_facts',
                   'research.stock_snapshot_daily',
                   s.ts_code || ':' || s.research_date::text || ':market-facts',
                   1::bigint,
                   encode(digest(convert_to(jsonb_build_object(
                       'raw_open', s.raw_open, 'raw_high', s.raw_high,
                       'raw_low', s.raw_low, 'raw_close', s.raw_close,
                       'raw_volume', s.raw_volume, 'raw_amount', s.raw_amount,
                       'up_limit', s.up_limit, 'down_limit', s.down_limit
                   )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
            FROM research.stock_snapshot_daily s
            WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
            UNION ALL
            SELECT s.snapshot_id, s.research_date, s.ts_code, 'fundamental_history',
                   'feature.company_financial_v1:pit-visible-summary',
                   s.ts_code || ':' || s.research_date::text || ':fundamental-history',
                   1::bigint,
                   encode(digest(convert_to(jsonb_build_object(
                       'annual_report_count_5y', s.annual_report_count_5y,
                       'roe_above_15_years_5y', s.roe_above_15_years_5y,
                       'roe_5y_min', s.roe_5y_min,
                       'roe_5y_all_above_15', s.roe_5y_all_above_15,
                       'ocf_report_count_3y', s.ocf_report_count_3y,
                       'ocf_positive_years_3y', s.ocf_positive_years_3y,
                       'ocf_3y_all_positive', s.ocf_3y_all_positive,
                       'quarterly_report_count_4q', s.quarterly_report_count_4q,
                       'q_sales_yoy_above_30_quarters_4q', s.q_sales_yoy_above_30_quarters_4q,
                       'q_sales_yoy_4q_all_above_30', s.q_sales_yoy_4q_all_above_30
                   )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
            FROM research.stock_snapshot_daily s
            WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
        )
        SELECT g.snapshot_id, g.research_date, g.ts_code, g.field_group,
               COALESCE(r.source_table, g.field_group || ':missing'),
               COALESCE(r.source_record_key, g.ts_code || ':' || g.research_date::text || ':missing'),
               r.source_record_version, r.source_payload_hash
        FROM groups g
        LEFT JOIN refs r ON r.snapshot_id=g.snapshot_id AND r.research_date=g.research_date
                         AND r.ts_code=g.ts_code AND r.field_group=g.field_group
        ON CONFLICT DO NOTHING
        """

    @staticmethod
    def stock_snapshot_batch_sql():
        """Return the set-based materialization SQL for several cutoff dates."""
        sql = ResearchSnapshotRepository.stock_snapshot_sql()
        marker = "    INSERT INTO research.stock_snapshot_daily"
        if marker not in sql:
            raise RuntimeError("单日 Snapshot SQL 结构已变化，无法生成批量 SQL")
        sql = sql.replace(
            marker,
            "    WITH params AS (\n"
            "        SELECT snapshot_id, research_cutoff_date, %s::bigint AS stock_basic_max_record_id\n"
            "          FROM research.snapshot_manifest\n"
            "         WHERE snapshot_id = ANY(%s::uuid[])\n"
            "    )\n" + marker,
            1,
        )
        sql = sql.replace("SELECT %s, u.trade_date", "SELECT m.snapshot_id, u.trade_date", 1)
        sql = sql.replace("FROM pit.universe_daily u", "FROM params m JOIN pit.universe_daily u ON u.trade_date=m.research_cutoff_date", 1)
        sql = sql.replace("b.record_id <= %s", "b.record_id <= m.stock_basic_max_record_id", 1)
        sql = sql.replace("WHERE u.trade_date=%s", "WHERE TRUE", 1)
        sql = sql.replace(
            "WHERE TRUE\n        ON CONFLICT",
            "WHERE TRUE AND u.ts_code=ANY(%s::text[])\n        ON CONFLICT",
            1,
        )
        return sql

    @staticmethod
    def foundation_snapshot_batch_sql():
        """Return Foundation-v2 enrichment SQL for a bounded snapshot batch."""
        sql = ResearchSnapshotRepository.foundation_snapshot_sql()
        sql = sql.replace(
            "SELECT %s, s.research_date",
            "SELECT s.snapshot_id, s.research_date",
            1,
        )
        sql = sql.replace(
            "WHERE s.snapshot_id=%s",
            "WHERE s.snapshot_id=ANY(%s::uuid[]) "
            "AND s.research_date=ANY(%s::date[]) "
            "AND s.ts_code=ANY(%s::text[])",
            1,
        )
        if sql.count("%s") != 3:
            raise RuntimeError("Foundation-v2 批量 SQL 参数数量异常")
        return sql

    @staticmethod
    def lineage_batch_sql():
        """Return the lineage SQL with a UUID array instead of one UUID."""
        sql = ResearchSnapshotRepository.lineage_sql()
        marker = "WITH params AS (SELECT %s::uuid AS snapshot_id), groups AS ("
        if marker not in sql:
            raise RuntimeError("单日 lineage SQL 结构已变化，无法生成批量 SQL")
        sql = sql.replace(
            marker,
            "WITH params AS (SELECT unnest(%s::uuid[]) AS snapshot_id), "
            "codes AS (SELECT unnest(%s::text[]) AS ts_code), groups AS (",
            1,
        )
        sql = sql.replace(
            "WHERE s.snapshot_id=(SELECT snapshot_id FROM params)",
            "WHERE s.snapshot_id IN (SELECT snapshot_id FROM params) "
            "AND s.ts_code IN (SELECT ts_code FROM codes)",
        )
        return sql


    @staticmethod
    def foundation_inputs(base_inputs):
        result = []
        for definition in base_inputs:
            key = (definition["schema"], definition["table"])
            if key == ("feature", "valuation_daily_v1"):
                result.append({
                    **definition,
                    "columns": definition["columns"] + (
                        "pe_ttm_vendor", "ps_ttm_vendor", "pb_vendor", "dv_ttm_vendor",
                        "dv_ttm_source", "dv_ttm_percentile_5y", "dv_window_start",
                        "dv_window_end", "dv_observation_days", "dv_window_complete",
                        "data_available_timestamp", "feature_contract_version",
                    ),
                })
            elif key == ("pit", "financial_asof"):
                result.append({
                    "schema": "feature", "table": "company_financial_v2",
                    "columns": (
                        "feature_id", "ts_code", "report_date", "data_available_timestamp",
                        "source_observed_at", "report_type", "comp_type", "end_type",
                        "net_profit_consolidated_ttm", "net_profit_parent_ttm",
                        "operating_cashflow_ttm", "ocf_to_net_profit_consolidated_ttm",
                        "roe_ttm", "gross_margin_ttm", "net_margin_consolidated_ttm",
                        "revenue_growth_yoy", "profit_growth_yoy", "ttm_complete",
                        "cashflow_quality_status", "roe_quality_status",
                        "growth_quality_status", "source_fingerprint", "pit_evidence_level",
                        "feature_contract_version",
                    ),
                    "predicate": "latest data_available_timestamp before cutoff end-of-day",
                    "where": "company_financial_v2.feature_id IN (SELECT DISTINCT ON (ts_code) feature_id FROM feature.company_financial_v2 visible WHERE visible.data_available_timestamp < (%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai' AND visible.report_date <= %s ORDER BY ts_code, report_date DESC, data_available_timestamp DESC, feature_id DESC)",
                    "params": lambda cutoff: (cutoff, cutoff),
                    "order": "ts_code, report_date, data_available_timestamp, feature_id",
                    "date_column": "report_date",
                })
            else:
                result.append(definition)
        return tuple(result)

    @staticmethod
    def foundation_snapshot_sql() -> str:
        """Return the typed Foundation-v2 enrichment statement.

        The statement intentionally preserves explicit missing lineage: a
        missing valuation/financial source is represented as ``:missing`` and
        never silently converted into a current-value fallback.  Execution is
        performed by the Repository through a typed RebuildGateway method.
        """
        return """
        INSERT INTO research.stock_snapshot_daily
            (snapshot_id, research_date, ts_code,
             financial_feature_contract_version, financial_data_available_timestamp,
             net_profit_consolidated_ttm, net_profit_parent_ttm,
             operating_cashflow_ttm, roe_ttm, gross_margin_ttm,
             net_margin_consolidated_ttm, revenue_growth_yoy, profit_growth_yoy,
             ttm_complete, dv_ttm, dv_ttm_percentile_5y, dv_window_start,
             dv_window_end, dv_observation_days, dv_window_complete,
             financial_component_lineage, quality_status)
        SELECT %s, s.research_date, s.ts_code,
               COALESCE(f.feature_contract_version, 'feature-contract-v2.0'),
               f.data_available_timestamp, f.net_profit_consolidated_ttm,
               f.net_profit_parent_ttm, f.operating_cashflow_ttm, f.roe_ttm,
               f.gross_margin_ttm, f.net_margin_consolidated_ttm,
               f.revenue_growth_yoy, f.profit_growth_yoy, f.ttm_complete,
               v.dv_ttm, v.dv_ttm_percentile_5y, v.dv_window_start,
               v.dv_window_end, v.dv_observation_days, v.dv_window_complete,
               CASE WHEN f.feature_id IS NULL
                    THEN to_jsonb(s.ts_code || ':' || s.research_date::text || ':missing')
                    ELSE COALESCE(f.component_lineage, '{}'::jsonb) END,
               CASE WHEN f.feature_id IS NULL OR v.ts_code IS NULL
                    THEN 'MISSING_SOURCE' ELSE s.quality_status END
          FROM research.stock_snapshot_daily s
          LEFT JOIN LATERAL (
                SELECT * FROM feature.company_financial_v2 f
                 WHERE f.ts_code=s.ts_code
                   AND f.data_available_timestamp <
                       (s.research_date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'
                   AND f.report_date <= s.research_date
                 ORDER BY f.report_date DESC, f.data_available_timestamp DESC,
                          f.feature_id DESC LIMIT 1
          ) f ON TRUE
          LEFT JOIN feature.valuation_daily_v1 v
            ON v.ts_code=s.ts_code AND v.trade_date=s.research_date
         WHERE s.snapshot_id=%s
        ON CONFLICT (snapshot_id, research_date, ts_code) DO UPDATE SET
            financial_feature_contract_version=EXCLUDED.financial_feature_contract_version,
            financial_data_available_timestamp=EXCLUDED.financial_data_available_timestamp,
            net_profit_consolidated_ttm=EXCLUDED.net_profit_consolidated_ttm,
            net_profit_parent_ttm=EXCLUDED.net_profit_parent_ttm,
            operating_cashflow_ttm=EXCLUDED.operating_cashflow_ttm,
            roe_ttm=EXCLUDED.roe_ttm, gross_margin_ttm=EXCLUDED.gross_margin_ttm,
            net_margin_consolidated_ttm=EXCLUDED.net_margin_consolidated_ttm,
            revenue_growth_yoy=EXCLUDED.revenue_growth_yoy,
            profit_growth_yoy=EXCLUDED.profit_growth_yoy,
            ttm_complete=EXCLUDED.ttm_complete, dv_ttm=EXCLUDED.dv_ttm,
            dv_ttm_percentile_5y=EXCLUDED.dv_ttm_percentile_5y,
            dv_window_start=EXCLUDED.dv_window_start, dv_window_end=EXCLUDED.dv_window_end,
            dv_observation_days=EXCLUDED.dv_observation_days,
            dv_window_complete=EXCLUDED.dv_window_complete,
            financial_component_lineage=EXCLUDED.financial_component_lineage,
            quality_status=EXCLUDED.quality_status
        """

    def create_manifest(
        self, snapshot_id: str, cutoff: date, baseline_tag: str, schema_version: str,
        metadata: dict[str, Any], quality_hash: str, baseline_quality_hash: str,
        baseline_source_manifest_hash: str, quality_report: dict[str, Any],
        notes: str, context: RunContext,
    ) -> int:
        row = {
            "snapshot_id": snapshot_id, "research_cutoff_date": cutoff,
            "data_baseline_tag": baseline_tag, "schema_version": schema_version,
            "code_version": metadata["code_version"], "git_commit": metadata["commit"],
            "git_dirty": metadata["dirty"], "diff_hash": metadata["diff_hash"],
            "quality_report_hash": quality_hash,
            "baseline_quality_report_hash": baseline_quality_hash,
            "baseline_source_manifest_hash": baseline_source_manifest_hash,
            "quality_report": json.dumps(quality_report, ensure_ascii=False, default=str),
            "status": "DRAFT", "notes": notes,
            "feature_contract_version": "feature-contract-v2.0"
            if is_foundation_schema(schema_version) else None,
            "decision_timestamp": cutoff,
            "data_cutoff_timestamp": cutoff,
            "pit_evidence_level": "SOURCE_DATE_WITH_OBSERVED_VERSIONS"
            if is_foundation_schema(schema_version) else None,
        }
        return self.kernel.write.upsert(
            context=context, scope=WorkScope(date_from=cutoff, date_to=cutoff, partition=str(snapshot_id)),
            target="research.snapshot_manifest", rows=[row], key=("snapshot_id",),
            update_columns=tuple(key for key in row if key != "snapshot_id"), skip_noop=True,
        )

    @staticmethod
    def _snapshot_scope(snapshot_id: str, cutoff: date | None = None) -> WorkScope:
        return WorkScope(
            date_from=cutoff, date_to=cutoff, partition=str(snapshot_id),
            dimensions={"snapshot_id": str(snapshot_id)},
        )

    def _delete_snapshot_rows(self, snapshot_id: str, context: RunContext, target: str, cutoff: date) -> int:
        scope = self._snapshot_scope(snapshot_id, cutoff)
        plan = SqlPlan(
            operation=Operation.REBUILD, target=target, scope=scope,
            expected_mode="SET_BASED", estimated_rows=0, allow_delete=True,
        )
        # ``stock_snapshot_daily`` is range-partitioned by research_date.  A
        # snapshot_id-only delete forces PostgreSQL to inspect every yearly
        # partition before the next rebuild and can exceed the governed
        # statement deadline.  Lineage is not partitioned today, but the same
        # date predicate keeps the delete bounded and matches the WorkScope.
        if target in {"research.stock_snapshot_daily", "research.snapshot_lineage"}:
            sql = f"DELETE FROM {target} WHERE snapshot_id=%s AND research_date=%s"
            params = (snapshot_id, cutoff)
        else:
            sql = f"DELETE FROM {target} WHERE snapshot_id=%s"
            params = (snapshot_id,)
        return self.kernel.rebuild.run_scoped_merge(sql, plan, context, params)

    def _precompute_input_manifest(
        self,
        definitions,
        cutoff: date,
        source_manifest: dict[str, Any] | None,
        context: RunContext,
        row_bytes,
        manifest_hash,
    ) -> tuple[list[dict[str, Any]], str]:
        """Compute source statistics/hash before the snapshot write lock.

        Each input_stats call is independently bounded by ReadGateway.  Keeping
        this phase outside session_exclusive prevents source scans and hashing
        from extending the snapshot materialization lock window.
        """
        stats = [
            self.input_stats(definition, cutoff, source_manifest, context, row_bytes)
            for definition in definitions
        ]
        return stats, manifest_hash(stats)

    def build_snapshot(
        self,
        snapshot_id: str,
        manifest: dict[str, Any],
        baseline: tuple,
        quality_report: dict[str, Any],
        quality_hash: str,
        metadata: dict[str, Any],
        definitions,
        row_bytes,
        manifest_hash,
        context: RunContext,
    ) -> None:
        """Materialize one snapshot through typed rebuild/write operations."""
        cutoff = manifest["cutoff"]
        scope = self._snapshot_scope(snapshot_id, cutoff)
        try:
            stats, input_manifest_hash = self._precompute_input_manifest(
                definitions, cutoff, baseline[5], context, row_bytes, manifest_hash,
            )
            input_rows = [
                {
                    "snapshot_id": snapshot_id, "schema_name": item["schema_name"],
                    "table_name": item["table_name"], "selection_predicate": item["selection_predicate"],
                    "selected_columns": item["selected_columns"], "hash_algorithm": item["hash_algorithm"],
                    "row_count": item["row_count"], "min_date": item["min_date"],
                    "max_date": item["max_date"], "content_hash": item["content_hash"],
                }
                for item in stats
            ]
            with self.kernel.lock_manager.session_exclusive("research.snapshot", str(snapshot_id)):
                changed = self.kernel.write.transition_snapshot_manifest(
                    context, scope, snapshot_id, ("DRAFT", "FAILED"), "BUILDING",
                    {
                        "quality_report_hash": quality_hash,
                        "quality_report": json.dumps(quality_report, ensure_ascii=False, default=str),
                        "code_version": metadata["code_version"], "git_commit": metadata["commit"],
                        "git_dirty": metadata["dirty"], "diff_hash": metadata["diff_hash"],
                        "failure_reason": None,
                    },
                )
                if changed != 1:
                    raise RuntimeError("Snapshot 状态在构建期间发生变化")
                for target in (
                    "research.snapshot_lineage", "research.stock_snapshot_daily", "research.snapshot_input",
                ):
                    self._delete_snapshot_rows(snapshot_id, context, target, cutoff)
                high_water = baseline[5]["append_only_sources"]["tushare_stock_basic_raw_version"]["max_record_id"]
                if high_water is None:
                    raise RuntimeError("基线缺少 stock_basic Raw 版本高水位")
                stock_plan = SqlPlan(
                    operation=Operation.REBUILD, target="research.stock_snapshot_daily", scope=scope,
                    expected_mode="SET_BASED", allow_insert_select=True,
                )
                code_rows = self.kernel.read._repository_query(
                    context, "pit.universe_daily",
                    "SELECT ts_code FROM pit.universe_daily WHERE trade_date=%s ORDER BY ts_code",
                    (cutoff,), WorkScope(date_to=cutoff, partition=f"snapshot-codes:{snapshot_id}"),
                    max_rows=10_000,
                )
                codes = [str(row["ts_code"]) for row in code_rows]
                if not codes:
                    raise RuntimeError(f"no universe rows available for {cutoff}")
                stock_sql = self.stock_snapshot_sql().replace(
                    "WHERE u.trade_date=%s",
                    "WHERE u.trade_date=%s AND u.ts_code=ANY(%s)",
                    1,
                )
                written = 0
                for offset in range(0, len(codes), self.SNAPSHOT_CODE_BATCH_SIZE):
                    code_batch = codes[offset:offset + self.SNAPSHOT_CODE_BATCH_SIZE]
                    written += self.kernel.rebuild.run_insert_select(
                        stock_sql, stock_plan, context,
                        (snapshot_id, high_water, cutoff, code_batch),
                    )
                if written <= 0:
                    raise RuntimeError(f"截止日没有可生成的 universe 数据: {cutoff}")
                if is_foundation_schema(manifest["schema_version"]):
                    foundation_plan = SqlPlan(
                        operation=Operation.REBUILD, target="research.stock_snapshot_daily", scope=scope,
                        expected_mode="SET_BASED",
                    )
                    foundation_sql = self.foundation_snapshot_sql().replace(
                        "WHERE s.snapshot_id=%s",
                        "WHERE s.snapshot_id=%s AND s.research_date=%s AND s.ts_code=ANY(%s)",
                        1,
                    )
                    for offset in range(0, len(codes), self.SNAPSHOT_CODE_BATCH_SIZE):
                        code_batch = codes[offset:offset + self.SNAPSHOT_CODE_BATCH_SIZE]
                        self.kernel.rebuild.run_scoped_merge(
                            foundation_sql, foundation_plan, context,
                            (snapshot_id, snapshot_id, cutoff, code_batch),
                        )
                lineage_plan = SqlPlan(
                    operation=Operation.REBUILD, target="research.snapshot_lineage", scope=scope,
                    expected_mode="SET_BASED", allow_insert_select=True,
                )
                lineage_sql = self.lineage_sql().replace(
                    "WITH params AS (SELECT %s::uuid AS snapshot_id),",
                    "WITH params AS (SELECT %s::uuid AS snapshot_id, %s::date AS research_date, %s::text[] AS codes),",
                    1,
                ).replace(
                    "WHERE s.snapshot_id=(SELECT snapshot_id FROM params)",
                    "WHERE s.snapshot_id=(SELECT snapshot_id FROM params) "
                    "AND s.research_date=(SELECT research_date FROM params) "
                    "AND s.ts_code IN (SELECT unnest(codes) FROM params)",
                )
                for offset in range(0, len(codes), self.SNAPSHOT_CODE_BATCH_SIZE):
                    code_batch = codes[offset:offset + self.SNAPSHOT_CODE_BATCH_SIZE]
                    self.kernel.rebuild.run_insert_select(
                        lineage_sql, lineage_plan, context, (snapshot_id, cutoff, code_batch)
                    )
                self.kernel.write.bulk_insert(context, scope, "research.snapshot_input", input_rows)
                changed = self.kernel.write.transition_snapshot_manifest(
                    context, scope, snapshot_id, ("BUILDING",), "BUILDING",
                    {"input_manifest_hash": input_manifest_hash},
                )
                if changed != 1:
                    raise RuntimeError("Snapshot 输入 Manifest 更新失败")
        except Exception as exc:
            self.kernel.write.transition_snapshot_manifest(
                context, scope, snapshot_id, ("DRAFT", "BUILDING", "FAILED"), "FAILED",
                {"failure_reason": str(exc)[:2000]},
            )
            raise

    def build_snapshots_batch(
        self,
        manifests: list[dict[str, Any]],
        baseline: tuple,
        quality_reports: dict[str, tuple[dict, str]],
        metadata: dict[str, Any],
        definitions,
        row_bytes,
        manifest_hash,
        context: RunContext,
    ) -> None:
        """Materialize a small date batch with shared set-based reads.

        Snapshot lifecycle and input manifests remain independent per date, but
        stock/foundation/lineage materialization shares the same code batches.
        This removes the repeated query-planning and source-join work that made
        a two-date ``build_snapshots_batch`` equivalent to two full single-day
        builds.  Every Gateway call is still its own transaction, so the batch
        never becomes one large transaction; an interrupted batch is rebuilt by
        the normal BUILDING completeness checkpoint.
        """
        if not manifests:
            return
        if len(manifests) > 20:
            raise ValueError("snapshot build batches are limited to 20")
        ids = tuple(str(item["snapshot_id"]) for item in manifests)
        cutoffs = tuple(item["cutoff"] for item in manifests)
        if len(set(ids)) != len(ids) or len(set(cutoffs)) != len(cutoffs):
            raise ValueError("snapshot build batch must contain unique ids and cutoff dates")
        baseline_tags = {item["baseline"] for item in manifests}
        schema_versions = {item["schema_version"] for item in manifests}
        if len(baseline_tags) != 1 or len(schema_versions) != 1:
            raise ValueError("同一批次必须使用同一 baseline_tag/schema_version")
        schema_version = next(iter(schema_versions))

        prepared: list[dict[str, Any]] = []
        for manifest in manifests:
            snapshot_id = str(manifest["snapshot_id"])
            if manifest["status"] not in {"DRAFT", "FAILED"}:
                raise RuntimeError(
                    f"批量构建只允许 DRAFT/FAILED Snapshot: {snapshot_id} ({manifest['status']})"
                )
            if (
                baseline[3] != manifest["baseline_quality_hash"]
                or baseline[4] != manifest["baseline_source_manifest_hash"]
            ):
                raise RuntimeError("数据基线 Manifest 已变化，拒绝批量构建")
            report, quality_hash = quality_reports.get(snapshot_id, (None, None))
            if report is None or quality_hash is None:
                raise RuntimeError(f"批量构建缺少质量证据: {snapshot_id}")
            if report["failures"]:
                raise RuntimeError(f"质量门禁未通过: {manifest['cutoff']} {report['failures']}")
            stats, input_hash = self._precompute_input_manifest(
                definitions, manifest["cutoff"], baseline[5], context, row_bytes, manifest_hash,
            )
            prepared.append({
                "manifest": manifest,
                "quality_hash": quality_hash,
                "report": report,
                "input_hash": input_hash,
                "input_rows": [
                    {
                        "snapshot_id": snapshot_id,
                        "schema_name": item["schema_name"],
                        "table_name": item["table_name"],
                        "selection_predicate": item["selection_predicate"],
                        "selected_columns": item["selected_columns"],
                        "hash_algorithm": item["hash_algorithm"],
                        "row_count": item["row_count"],
                        "min_date": item["min_date"],
                        "max_date": item["max_date"],
                        "content_hash": item["content_hash"],
                    }
                    for item in stats
                ],
            })

        batch_scope = WorkScope(
            date_from=min(cutoffs), date_to=max(cutoffs),
            partition=f"snapshot-batch:{min(cutoffs)}:{max(cutoffs)}",
            dimensions={
                "snapshot_id": f"batch:{hashlib.sha256('|'.join(ids).encode('utf-8')).hexdigest()[:24]}",
                "snapshot_ids": ids,
            },
        )
        started: list[dict[str, Any]] = []
        try:
            with self.kernel.lock_manager.session_exclusive_many("research.snapshot", ids):
                for item in prepared:
                    manifest = item["manifest"]
                    snapshot_id = str(manifest["snapshot_id"])
                    scope = self._snapshot_scope(snapshot_id, manifest["cutoff"])
                    changed = self.kernel.write.transition_snapshot_manifest(
                        context, scope, snapshot_id, ("DRAFT", "FAILED"), "BUILDING",
                        {
                            "quality_report_hash": item["quality_hash"],
                            "quality_report": json.dumps(
                                item["report"], ensure_ascii=False, default=str,
                            ),
                            "code_version": metadata["code_version"],
                            "git_commit": metadata["commit"],
                            "git_dirty": metadata["dirty"],
                            "diff_hash": metadata["diff_hash"],
                            "failure_reason": None,
                        },
                    )
                    if changed != 1:
                        raise RuntimeError(f"Snapshot 状态在构建期间发生变化: {snapshot_id}")
                    for target in (
                        "research.snapshot_lineage",
                        "research.stock_snapshot_daily",
                        "research.snapshot_input",
                    ):
                        self._delete_snapshot_rows(snapshot_id, context, target, manifest["cutoff"])
                    started.append(item)

                high_water = baseline[5]["append_only_sources"][
                    "tushare_stock_basic_raw_version"
                ]["max_record_id"]
                if high_water is None:
                    raise RuntimeError("基线缺少 stock_basic Raw 版本高水位")
                code_rows = self.kernel.read._repository_query(
                    context,
                    "pit.universe_daily",
                    """SELECT DISTINCT ts_code
                         FROM pit.universe_daily
                        WHERE trade_date=ANY(%s::date[])
                        ORDER BY ts_code""",
                    (list(cutoffs),),
                    WorkScope(
                        date_from=min(cutoffs), date_to=max(cutoffs),
                        partition="snapshot-batch-codes",
                    ),
                    max_rows=100_000,
                )
                codes = tuple(sorted({str(row["ts_code"]) for row in code_rows}))
                if not codes:
                    raise RuntimeError("批量快照没有可生成的 universe 数据")

                stock_plan = SqlPlan(
                    operation=Operation.REBUILD,
                    target="research.stock_snapshot_daily",
                    scope=batch_scope,
                    expected_mode="SET_BASED",
                    allow_insert_select=True,
                )
                stock_sql = self.stock_snapshot_batch_sql()
                written = 0
                for offset in range(0, len(codes), self.SNAPSHOT_CODE_BATCH_SIZE):
                    code_batch = codes[offset:offset + self.SNAPSHOT_CODE_BATCH_SIZE]
                    written += self.kernel.rebuild.run_insert_select(
                        stock_sql, stock_plan, context,
                        (high_water, list(ids), list(code_batch)),
                    )
                if written <= 0:
                    raise RuntimeError("批量快照没有写入 universe 数据")

                if is_foundation_schema(schema_version):
                    foundation_plan = SqlPlan(
                        operation=Operation.REBUILD,
                        target="research.stock_snapshot_daily",
                        scope=batch_scope,
                        expected_mode="SET_BASED",
                    )
                    foundation_sql = self.foundation_snapshot_batch_sql()
                    for offset in range(0, len(codes), self.SNAPSHOT_CODE_BATCH_SIZE):
                        code_batch = codes[offset:offset + self.SNAPSHOT_CODE_BATCH_SIZE]
                        self.kernel.rebuild.run_scoped_merge(
                            foundation_sql, foundation_plan, context,
                            (list(ids), list(cutoffs), list(code_batch)),
                        )

                # The lineage statement contains eleven UNION ALL branches
                # with per-source hashing.  Keep it per snapshot so the
                # planner can use the snapshot_id index and each statement
                # remains below the 30-second deadline.  The expensive stock
                # and Foundation-v2 joins above still benefit from sharing the
                # same code batches across dates.
                for item in prepared:
                    manifest = item["manifest"]
                    snapshot_id = str(manifest["snapshot_id"])
                    cutoff = manifest["cutoff"]
                    lineage_plan = SqlPlan(
                        operation=Operation.REBUILD,
                        target="research.snapshot_lineage",
                        scope=self._snapshot_scope(snapshot_id, cutoff),
                        expected_mode="SET_BASED",
                        allow_insert_select=True,
                    )
                    lineage_sql = self.lineage_sql().replace(
                        "WITH params AS (SELECT %s::uuid AS snapshot_id),",
                        "WITH params AS (SELECT %s::uuid AS snapshot_id, "
                        "%s::date AS research_date, %s::text[] AS codes),",
                        1,
                    ).replace(
                        "WHERE s.snapshot_id=(SELECT snapshot_id FROM params)",
                        "WHERE s.snapshot_id=(SELECT snapshot_id FROM params) "
                        "AND s.research_date=(SELECT research_date FROM params) "
                        "AND s.ts_code IN (SELECT unnest(codes) FROM params)",
                    )
                    for offset in range(0, len(codes), self.SNAPSHOT_CODE_BATCH_SIZE):
                        code_batch = codes[offset:offset + self.SNAPSHOT_CODE_BATCH_SIZE]
                        self.kernel.rebuild.run_insert_select(
                            lineage_sql, lineage_plan, context,
                            (snapshot_id, cutoff, list(code_batch)),
                        )

                all_input_rows = [
                    row for item in prepared for row in item["input_rows"]
                ]
                self.kernel.write.bulk_insert(
                    context, batch_scope, "research.snapshot_input", all_input_rows,
                )
                for item in prepared:
                    manifest = item["manifest"]
                    snapshot_id = str(manifest["snapshot_id"])
                    changed = self.kernel.write.transition_snapshot_manifest(
                        context,
                        self._snapshot_scope(snapshot_id, manifest["cutoff"]),
                        snapshot_id,
                        ("BUILDING",), "BUILDING",
                        {"input_manifest_hash": item["input_hash"]},
                    )
                    if changed != 1:
                        raise RuntimeError(f"Snapshot 输入 Manifest 更新失败: {snapshot_id}")
        except Exception as exc:
            reason = f"batch build failed: {str(exc)[:1900]}"
            for item in started:
                manifest = item["manifest"]
                try:
                    self.kernel.write.transition_snapshot_manifest(
                        context,
                        self._snapshot_scope(str(manifest["snapshot_id"]), manifest["cutoff"]),
                        str(manifest["snapshot_id"]),
                        ("DRAFT", "BUILDING", "FAILED"), "FAILED",
                        {"failure_reason": reason},
                    )
                except Exception:
                    # Preserve the original batch error; the next resume will
                    # inspect BUILDING completeness before rebuilding.
                    pass
            raise

    def validate_snapshot(self, manifest: dict[str, Any], baseline: tuple, quality_hash: str,
                          issues: list[dict[str, Any]], context: RunContext) -> None:
        if issues:
            raise RuntimeError(f"Research 快照校验失败: {issues}")
        scope = self._snapshot_scope(str(manifest["snapshot_id"]), manifest["cutoff"])
        changed = self.kernel.write.transition_snapshot_manifest(
            context, scope, str(manifest["snapshot_id"]), ("BUILDING",), "VALIDATED", {},
        )
        if changed != 1:
            raise RuntimeError("Research 快照状态在校验期间发生变化")

    def mark_failed(self, snapshot_id: str, cutoff: date, reason: str, context: RunContext) -> None:
        changed = self.kernel.write.transition_snapshot_manifest(
            context, self._snapshot_scope(snapshot_id, cutoff), snapshot_id,
            ("DRAFT", "BUILDING", "FAILED"), "FAILED", {"failure_reason": reason[:2000]},
        )
        if changed not in {0, 1}:
            raise RuntimeError("Research 快照失败状态更新异常")

    def freeze_snapshot(self, manifest: dict[str, Any], metadata: dict[str, Any],
                        archive: dict[str, Any], context: RunContext) -> None:
        """Freeze without overwriting the code identity captured at build.

        ``metadata`` describes the operational freezer, not the code that
        materialized the snapshot.  The current schema has only one code
        identity slot, so replacing it here would destroy build lineage.
        """
        _ = metadata
        snapshot_id = str(manifest["snapshot_id"])
        scope = self._snapshot_scope(snapshot_id, manifest["cutoff"])
        changed = self.kernel.write.transition_snapshot_manifest(
            context, scope, snapshot_id, ("VALIDATED",), "FROZEN",
            {
                "code_archive_path": archive.get("path"),
                "code_archive_sha256": archive.get("sha256"),
                "code_archive_size_bytes": archive.get("size"),
            },
        )
        if changed != 1:
            raise RuntimeError("Research 快照状态在冻结期间发生变化，拒绝并发冻结")

    def input_stats(
        self,
        definition: dict[str, Any],
        cutoff: date,
        source_manifest: dict[str, Any] | None,
        context: RunContext,
        row_bytes,
    ) -> dict[str, Any]:
        columns = tuple(definition["columns"])
        source = f'{definition["schema"]}.{definition["table"]}'
        where = definition["where"]
        if "params_from_manifest" in definition:
            if not source_manifest:
                raise RuntimeError(f"{source} missing baseline source manifest")
            params = definition["params_from_manifest"](cutoff, source_manifest)
        else:
            params = definition["params"](cutoff)
        date_column = definition["date_column"]
        cache_key = None
        if source == "public.tushare_stock_basic_raw_version":
            cache_key = (source, tuple(str(value) for value in params), columns)
            cached = _IMMUTABLE_INPUT_STATS_CACHE.get(cache_key)
            if cached is not None:
                return dict(cached)
        selected = ", ".join(f'"{column}"' for column in columns)
        digest = hashlib.sha256()
        row_count = 0
        min_date = None
        max_date = None
        keyset_column = self.INPUT_STATS_KEYSET_COLUMNS.get(source)
        if keyset_column:
            batches = self._paged_input_rows(
                context=context,
                source=source,
                sql=(
                    f'SELECT {selected} FROM {source} '
                    f'WHERE {where} AND "{keyset_column}" > %s '
                    f'ORDER BY "{keyset_column}" LIMIT %s'
                ),
                params_prefix=tuple(params),
                page_rows=self.INPUT_STATS_PAGE_ROWS,
                cutoff=cutoff,
                scope_partition=f"snapshot-input:{source}",
            )
        elif source == "feature.company_financial_v2":
            # ``foundation_inputs`` selects one latest PIT-visible row per
            # stock.  Push the keyset predicate into that selection instead
            # of rerunning a full DISTINCT-ON subquery for every page.
            batches = self._paged_input_rows(
                context=context,
                source=source,
                sql=(
                    f'SELECT DISTINCT ON ("ts_code") {selected} FROM {source} '
                    'WHERE "data_available_timestamp" < '
                    '(%s::date + 1)::timestamp AT TIME ZONE '\
                    "'Asia/Shanghai' "
                    'AND "report_date" <= %s '
                    'AND "ts_code" > %s '
                    'ORDER BY "ts_code", "report_date" DESC, '
                    '"data_available_timestamp" DESC, "feature_id" DESC '
                    'LIMIT %s'
                ),
                params_prefix=(cutoff, cutoff),
                page_rows=self.INPUT_STATS_PAGE_ROWS,
                cutoff=cutoff,
                scope_partition=f"snapshot-input:{source}",
            )
        else:
            rows_sql = f'SELECT {selected} FROM {source} WHERE {where} ORDER BY {definition["order"]}'
            batches = self.kernel.read._repository_stream(
                context, source, rows_sql, params,
                WorkScope(date_to=cutoff, partition=f"snapshot-input:{source}"),
                batch_rows=self.INPUT_STATS_BATCH_ROWS,
            )
        for batch in batches:
            for row in batch:
                row_count += 1
                if date_column:
                    value = row[date_column]
                    if value is not None:
                        min_date = value if min_date is None or value < min_date else min_date
                        max_date = value if max_date is None or value > max_date else max_date
                digest.update(row_bytes(tuple(row[column] for column in columns)))
        result = {
            "schema_name": definition["schema"],
            "table_name": definition["table"],
            "selection_predicate": definition["predicate"],
            "selected_columns": list(columns),
            "hash_algorithm": "sha256",
            "row_count": row_count,
            "min_date": min_date,
            "max_date": max_date,
            "content_hash": digest.hexdigest(),
        }
        if cache_key is not None:
            _IMMUTABLE_INPUT_STATS_CACHE[cache_key] = dict(result)
        return result

    def _paged_input_rows(
        self,
        *,
        context: RunContext,
        source: str,
        sql: str,
        params_prefix: tuple[Any, ...],
        page_rows: int,
        cutoff: date,
        scope_partition: str,
    ):
        """Yield bounded keyset pages for wide snapshot input sources.

        The SQL supplied by this Repository method must expose ``ts_code`` as
        its first keyset column and have a final ``LIMIT %s`` placeholder.
        Each page is a separate governed Repository read, so a slow page is
        retried/rolled back without creating one long transaction.
        """
        last_code = ""
        while True:
            params = params_prefix + (last_code, page_rows)
            rows = self.kernel.read._repository_query(
                context,
                source,
                sql,
                params,
                WorkScope(date_to=cutoff, partition=scope_partition),
                max_rows=page_rows,
            )
            if not rows:
                return
            yield rows
            next_code = str(rows[-1]["ts_code"])
            if next_code <= last_code or len(rows) < page_rows:
                return
            last_code = next_code

    def baseline(
        self,
        cutoff: date,
        tag: str,
        context: RunContext,
        verify_source_manifest: bool = True,
    ) -> tuple:
        rows = self.kernel.read._repository_query(
            context,
            "ops.tushare_data_release",
            """SELECT release_tag, baseline_cutoff_date, status,
                       quality_report_hash, source_manifest_hash,
                       source_manifest, schema_version, quality_report
                  FROM ops.tushare_data_release WHERE release_tag=%s""",
            (tag,), WorkScope(date_to=cutoff, partition=f"release:{tag}"), 2,
        )
        if not rows or rows[0]["status"] != "validated":
            raise RuntimeError(f"data baseline must be validated: {tag}")
        row = rows[0]
        if cutoff > row["baseline_cutoff_date"]:
            raise ValueError(f"research cutoff {cutoff} is after baseline cutoff {row['baseline_cutoff_date']}")
        if not row["quality_report_hash"]:
            raise RuntimeError(f"data baseline missing quality hash: {tag}")
        if not row["source_manifest_hash"] or not row["source_manifest"]:
            raise RuntimeError(f"data baseline missing source manifest: {tag}")
        if verify_source_manifest:
            from data_collect.data_access.repository.release_repository import ReleaseRepository
            _, current_hash = ReleaseRepository(self.kernel).source_manifest(row["baseline_cutoff_date"], context)
            if current_hash != row["source_manifest_hash"]:
                raise RuntimeError(f"baseline source manifest changed; rebuild release: {tag}")
        return (
            row["release_tag"], row["baseline_cutoff_date"], row["status"],
            row["quality_report_hash"], row["source_manifest_hash"],
            row["source_manifest"], row["schema_version"], row["quality_report"],
        )

    def existing_snapshot(self, cutoff: date, baseline_tag: str, schema_version: str, context: RunContext):
        rows = self.kernel.read._repository_query(
            context, "research.snapshot_manifest",
            """SELECT snapshot_id, status FROM research.snapshot_manifest
                 WHERE research_cutoff_date=%s AND data_baseline_tag=%s
                   AND schema_version=%s""",
            (cutoff, baseline_tag, schema_version),
            WorkScope(date_from=cutoff, date_to=cutoff, partition="snapshot-manifest"), 2,
        )
        return (str(rows[0]["snapshot_id"]), rows[0]["status"]) if rows else None

    def existing_snapshots(self, cutoffs: list[date], baseline_tag: str, schema_version: str, context: RunContext) -> dict[date, tuple[str, str]]:
        if not cutoffs:
            return {}
        rows = self.kernel.read._repository_query(
            context, "research.snapshot_manifest",
            """SELECT snapshot_id, research_cutoff_date, status
                 FROM research.snapshot_manifest
                WHERE data_baseline_tag=%s AND schema_version=%s
                  AND research_cutoff_date=ANY(%s::date[])""",
            (baseline_tag, schema_version, list(cutoffs)),
            WorkScope(date_from=min(cutoffs), date_to=max(cutoffs), partition="snapshot-manifest-batch"),
            max_rows=max(2, len(cutoffs) + 1),
        )
        return {row["research_cutoff_date"]: (str(row["snapshot_id"]), row["status"]) for row in rows}

    def create_manifests(
        self, candidates: list[dict[str, Any]], context: RunContext,
    ) -> int:
        total = 0
        for row in candidates:
            total += self.create_manifest(
                row["snapshot_id"], row["research_cutoff_date"], row["data_baseline_tag"],
                row["schema_version"], row["metadata"], row["quality_report_hash"],
                row["baseline_quality_report_hash"], row["baseline_source_manifest_hash"],
                row["quality_report"], row["notes"], context,
            )
        return total

    def manifest(self, snapshot_id: str, context: RunContext, for_update: bool = False) -> dict[str, Any]:
        lock = " FOR UPDATE" if for_update else ""
        rows = self.kernel.read._repository_query(
            context, "research.snapshot_manifest",
            """SELECT snapshot_id, research_cutoff_date AS cutoff,
                       data_baseline_tag AS baseline, schema_version, status,
                       code_version, git_commit, git_dirty, diff_hash,
                       code_archive_path, code_archive_sha256,
                       code_archive_size_bytes AS code_archive_size,
                       quality_report_hash AS quality_hash,
                       baseline_quality_report_hash AS baseline_quality_hash,
                       baseline_source_manifest_hash, input_manifest_hash AS input_hash,
                       quality_report
                  FROM research.snapshot_manifest WHERE snapshot_id=%s""" + lock,
            (snapshot_id,), WorkScope(partition=str(snapshot_id)), 2,
        )
        if not rows:
            raise ValueError(f"unknown snapshot_id: {snapshot_id}")
        return rows[0]

    def status_rows(self, context: RunContext) -> list[tuple]:
        rows = self.kernel.read._repository_query(
            context, "research.snapshot_manifest",
            """SELECT snapshot_id,research_cutoff_date,data_baseline_tag,status,
                       schema_version,code_version,git_dirty,created_at,
                       validated_at,frozen_at
                  FROM research.snapshot_manifest ORDER BY created_at""",
            scope=WorkScope(partition="snapshot-status"), max_rows=100_000,
        )
        return [tuple(row.values()) for row in rows]

    def price_vintage(self, cutoff: date, schema_version: str, context: RunContext) -> None:
        if schema_version == "research-v1.2":
            rows = self.kernel.read._repository_query(
                context, "std.daily_kline",
                """SELECT COUNT(*) AS invalid_rows FROM std.daily_kline d
                    LEFT JOIN std.adj_factor_daily a
                      ON a.stock_code=d.stock_code AND a.trade_date=d.trade_date
                     AND a.source='tushare'
                   WHERE d.trade_date BETWEEN (%s - INTERVAL '365 days')::date AND %s
                     AND (a.adj_factor IS NULL OR a.fetched_at::date > %s)""",
                (cutoff, cutoff, cutoff), WorkScope(date_from=cutoff, date_to=cutoff, partition="price-vintage"), 2,
            )
            invalid = int(rows[0]["invalid_rows"] or 0)
            if invalid:
                raise RuntimeError(f"adjusted-factor vintage invalid: cutoff={cutoff}, rows={invalid}")
            return
        if not is_foundation_schema(schema_version) and schema_version != "research-v1.3":
            return
        rows = self.kernel.read._repository_query(
            context, "feature.price_daily_v2",
            """SELECT
                (SELECT COUNT(*) FROM std.daily_kline WHERE trade_date<=%s) AS daily_rows,
                (SELECT COUNT(*) FROM feature.price_daily_v2 WHERE trade_date<=%s) AS price_rows,
                (SELECT COUNT(*) FROM feature.price_adjustment_factor_asof WHERE trade_date<=%s) AS factor_rows,
                (SELECT COUNT(*) FROM feature.price_daily_v2 WHERE trade_date<=%s AND quality_status='MISSING_SOURCE') AS invalid_rows""",
            (cutoff, cutoff, cutoff, cutoff), WorkScope(date_to=cutoff, partition="price-vintage"), 2,
        )
        row = rows[0]
        if row["daily_rows"] != row["price_rows"] or row["daily_rows"] != row["factor_rows"] or row["invalid_rows"]:
            raise RuntimeError(f"point-in-time price feature is incomplete: {row}")

    def price_vintage_dates(self, cutoffs: list[date], snapshot_ids: list[str], schema_version: str, context: RunContext) -> None:
        if not cutoffs or (not is_foundation_schema(schema_version) and schema_version != "research-v1.3"):
            return
        rows = self.kernel.read._repository_query(
            context, "feature.price_daily_v2",
            """WITH dates(trade_date) AS (SELECT unnest(%s::date[])),
                daily AS (SELECT trade_date,COUNT(*) n FROM std.daily_kline WHERE trade_date=ANY(%s::date[]) GROUP BY trade_date),
                prices AS (SELECT trade_date,COUNT(*) n FROM feature.price_daily_v2 WHERE trade_date=ANY(%s::date[]) GROUP BY trade_date),
                factors AS (SELECT trade_date,COUNT(*) n FROM feature.price_adjustment_factor_asof WHERE trade_date=ANY(%s::date[]) GROUP BY trade_date),
                invalid AS (SELECT trade_date,COUNT(*) n FROM feature.price_daily_v2 WHERE trade_date=ANY(%s::date[]) AND quality_status='MISSING_SOURCE' GROUP BY trade_date)
                SELECT d.trade_date,COALESCE(x.n,0) daily_rows,COALESCE(p.n,0) price_rows,
                       COALESCE(f.n,0) factor_rows,COALESCE(i.n,0) invalid_rows
                  FROM dates d LEFT JOIN daily x USING(trade_date) LEFT JOIN prices p USING(trade_date)
                       LEFT JOIN factors f USING(trade_date) LEFT JOIN invalid i USING(trade_date)
                 WHERE COALESCE(x.n,0)<>COALESCE(p.n,0) OR COALESCE(x.n,0)<>COALESCE(f.n,0) OR COALESCE(i.n,0)<>0""",
            (list(cutoffs), list(cutoffs), list(cutoffs), list(cutoffs), list(cutoffs)),
            WorkScope(date_from=min(cutoffs), date_to=max(cutoffs), partition="price-vintage-batch"), 10_000,
        )
        if rows:
            raise RuntimeError(f"point-in-time price batch coverage invalid: {rows[:10]}")

    def check_snapshot(
        self, manifest: dict[str, Any], source_manifest: dict[str, Any],
        definitions, row_bytes, manifest_hash, required_lineage_groups: int, context: RunContext,
    ) -> list[dict[str, Any]]:
        snapshot_id = manifest["snapshot_id"]
        cutoff = manifest["cutoff"]
        scope = WorkScope(date_from=cutoff, date_to=cutoff, partition=str(snapshot_id))
        rows = self.kernel.read._repository_query(
            context, "research.stock_snapshot_daily",
            """SELECT COUNT(*) AS row_count, COUNT(DISTINCT research_date) AS date_count,
                       MIN(research_date) AS min_date, MAX(research_date) AS max_date
                  FROM research.stock_snapshot_daily
                 WHERE snapshot_id=%s AND research_date=%s""",
            (snapshot_id, cutoff), scope, 2,
        )[0]
        issues = []
        if rows["row_count"] == 0 or rows["date_count"] != 1 or rows["min_date"] != cutoff or rows["max_date"] != cutoff:
            issues.append({"snapshot_rows": rows["row_count"], "distinct_dates": rows["date_count"], "min_date": rows["min_date"], "max_date": rows["max_date"]})
        expected = self.kernel.read._repository_query(
            context, "pit.universe_daily", "SELECT COUNT(*) AS value FROM pit.universe_daily WHERE trade_date=%s", (cutoff,), scope, 2,
        )[0]["value"]
        if rows["row_count"] != expected:
            issues.append({"snapshot_universe_row_mismatch": {"snapshot_rows": rows["row_count"], "universe_rows": expected}})
        missing = self.kernel.read._repository_query(
            context, "research.snapshot_lineage",
            """SELECT COUNT(*) AS value FROM (
                SELECT snapshot_id,research_date,ts_code FROM research.snapshot_lineage
                 WHERE snapshot_id=%s AND research_date=%s
                 GROUP BY snapshot_id,research_date,ts_code
                 HAVING COUNT(DISTINCT field_group)<>%s) x""",
            (snapshot_id, cutoff, required_lineage_groups), scope, 2,
        )[0]["value"]
        if missing:
            issues.append({"rows_with_incomplete_lineage": missing})
        incomplete = self.kernel.read._repository_query(
            context, "research.snapshot_lineage",
            """SELECT field_group, COUNT(*) AS value FROM research.snapshot_lineage
                 WHERE snapshot_id=%s AND research_date=%s
                   AND source_record_key NOT LIKE '%%:missing'
                   AND (source_record_version IS NULL OR source_payload_hash IS NULL)
                 GROUP BY field_group ORDER BY field_group""",
            (snapshot_id, cutoff), scope, 100,
        )
        if incomplete:
            issues.append({"lineage_refs_without_version_or_hash": [(row["field_group"], row["value"]) for row in incomplete]})
        stored_inputs = self.kernel.read._repository_query(
            context, "research.snapshot_input",
            """SELECT schema_name, table_name, selection_predicate, selected_columns,
                              hash_algorithm, row_count, min_date, max_date, content_hash
                         FROM research.snapshot_input
                        WHERE snapshot_id=%s
                        ORDER BY schema_name, table_name""",
            (snapshot_id,), scope, max(2, len(definitions) + 1),
        )
        expected_keys = [(item["schema"], item["table"]) for item in definitions]
        stored_by_key = {(row["schema_name"], row["table_name"]): row for row in stored_inputs}
        if len(stored_inputs) != len(definitions) or set(stored_by_key) != set(expected_keys):
            issues.append({
                "snapshot_input_manifest_incomplete": {
                    "expected": expected_keys, "stored": sorted(stored_by_key),
                }
            })
        else:
            ordered_inputs = [stored_by_key[key] for key in expected_keys]
            current_hash = manifest_hash(ordered_inputs)
            if current_hash != manifest["input_hash"]:
                issues.append({
                    "input_manifest_hash_changed": {
                        "stored": manifest["input_hash"], "current": current_hash,
                    }
                })
        if manifest["schema_version"] == "research-v1.3":
            maturity = self.kernel.read._repository_query(
                context, "research.stock_snapshot_daily",
                """SELECT
                    COUNT(*) FILTER (WHERE ma20 IS NOT NULL AND COALESCE(ma20_observation_count,0)<20) AS ma20,
                    COUNT(*) FILTER (WHERE ma60 IS NOT NULL AND COALESCE(ma60_observation_count,0)<60) AS ma60,
                    COUNT(*) FILTER (WHERE ma120 IS NOT NULL AND COALESCE(ma120_observation_count,0)<120) AS ma120,
                    COUNT(*) FILTER (WHERE pe_pct_5y IS NOT NULL AND (NOT COALESCE(has_full_pe_5y_history,FALSE) OR COALESCE(valuation_observation_count,0)<1000 OR pe_window_start_date_5y IS NULL OR research_date-pe_window_start_date_5y<1794)) AS pe,
                    COUNT(*) FILTER (WHERE financial_version_available_date>research_date) AS financial_date,
                    COUNT(*) FILTER (WHERE financial_vintage_quality='WAREHOUSE_OBSERVED_ONLY' AND financial_source_observed_at::date>research_date) AS financial_observed,
                    COUNT(*) FILTER (WHERE instrument_source_record_key IS NULL OR instrument_source_record_version IS NULL OR instrument_source_payload_hash IS NULL) AS instrument,
                    COUNT(*) FILTER (WHERE roe_5y_all_above_15 AND annual_report_count_5y<>5) AS roe,
                    COUNT(*) FILTER (WHERE ocf_3y_all_positive AND ocf_report_count_3y<>3) AS ocf,
                    COUNT(*) FILTER (WHERE q_sales_yoy_4q_all_above_30 AND quarterly_report_count_4q<>4) AS quarterly
                  FROM research.stock_snapshot_daily
                 WHERE snapshot_id=%s AND research_date=%s""",
                (snapshot_id, cutoff), scope, 2,
            )[0]
            if any(maturity.values()):
                issues.append({"research_v1_2_maturity_or_pit_failures": tuple(maturity.values())})
        return issues

    def check_snapshots_batch(
        self, manifests: list[dict[str, Any]], source_manifest: dict[str, Any],
        definitions, manifest_hash, required_lineage_groups: int, context: RunContext,
    ) -> dict[str, list[dict[str, Any]]]:
        """Check a bounded snapshot batch with set-based, date-scoped reads.

        This is deliberately limited to the small inspection batch supplied by
        the caller.  It removes the per-snapshot query loop from series
        finalization while retaining the same row, lineage, and input-manifest
        checks as ``check_snapshot``.
        """
        if not manifests:
            return {}
        if len(manifests) > 100:
            raise ValueError("snapshot inspection batches are limited to 100")
        ids = [str(item["snapshot_id"]) for item in manifests]
        cutoffs = [item["cutoff"] for item in manifests]
        scope = WorkScope(
            date_from=min(cutoffs), date_to=max(cutoffs),
            partition="snapshot-check-batch",
        )
        issues: dict[str, list[dict[str, Any]]] = {}

        def add(snapshot_id: str, issue: dict[str, Any]) -> None:
            issues.setdefault(snapshot_id, []).append(issue)

        stock_rows = self.kernel.read._repository_query(
            context, "research.stock_snapshot_daily",
            """SELECT snapshot_id, research_date, COUNT(*) AS row_count,
                       COUNT(DISTINCT research_date) AS date_count,
                       MIN(research_date) AS min_date, MAX(research_date) AS max_date
                  FROM research.stock_snapshot_daily
                 WHERE snapshot_id=ANY(%s::uuid[])
                   AND research_date=ANY(%s::date[])
                 GROUP BY snapshot_id, research_date""",
            (ids, cutoffs), scope, max_rows=max(2, len(ids) + 1),
        )
        stock_by_id = {str(row["snapshot_id"]): row for row in stock_rows}

        universe_rows = self.kernel.read._repository_query(
            context, "pit.universe_daily",
            """SELECT trade_date, COUNT(*) AS value
                  FROM pit.universe_daily
                 WHERE trade_date=ANY(%s::date[])
                 GROUP BY trade_date""",
            (cutoffs,), scope, max_rows=max(2, len(set(cutoffs)) + 1),
        )
        universe_by_date = {row["trade_date"]: row["value"] for row in universe_rows}
        for manifest in manifests:
            snapshot_id = str(manifest["snapshot_id"])
            cutoff = manifest["cutoff"]
            row = stock_by_id.get(snapshot_id)
            if row is None or row["row_count"] == 0 or row["date_count"] != 1 \
                    or row["min_date"] != cutoff or row["max_date"] != cutoff:
                add(snapshot_id, {
                    "snapshot_rows": row["row_count"] if row else 0,
                    "distinct_dates": row["date_count"] if row else 0,
                    "min_date": row["min_date"] if row else None,
                    "max_date": row["max_date"] if row else None,
                })
            if row is None or row["row_count"] != universe_by_date.get(cutoff, 0):
                add(snapshot_id, {
                    "snapshot_universe_row_mismatch": {
                        "snapshot_rows": row["row_count"] if row else 0,
                        "universe_rows": universe_by_date.get(cutoff, 0),
                    }
                })

        incomplete_lineage = self.kernel.read._repository_query(
            context, "research.snapshot_lineage",
            """SELECT snapshot_id, COUNT(*) AS value FROM (
                    SELECT snapshot_id, research_date, ts_code
                      FROM research.snapshot_lineage
                     WHERE snapshot_id=ANY(%s::uuid[])
                       AND research_date=ANY(%s::date[])
                     GROUP BY snapshot_id, research_date, ts_code
                    HAVING COUNT(DISTINCT field_group)<>%s
                ) x GROUP BY snapshot_id""",
            (ids, cutoffs, required_lineage_groups), scope,
            max_rows=max(2, len(ids) + 1),
        )
        for row in incomplete_lineage:
            add(str(row["snapshot_id"]), {"rows_with_incomplete_lineage": row["value"]})

        missing_refs = self.kernel.read._repository_query(
            context, "research.snapshot_lineage",
            """SELECT snapshot_id, field_group, COUNT(*) AS value
                  FROM research.snapshot_lineage
                 WHERE snapshot_id=ANY(%s::uuid[])
                   AND research_date=ANY(%s::date[])
                   AND source_record_key NOT LIKE '%%:missing'
                   AND (source_record_version IS NULL OR source_payload_hash IS NULL)
                 GROUP BY snapshot_id, field_group
                 ORDER BY snapshot_id, field_group""",
            (ids, cutoffs), scope,
            max_rows=max(2, len(ids) * required_lineage_groups + 1),
        )
        missing_by_id: dict[str, list[tuple[str, Any]]] = {}
        for row in missing_refs:
            missing_by_id.setdefault(str(row["snapshot_id"]), []).append(
                (row["field_group"], row["value"])
            )
        for snapshot_id, values in missing_by_id.items():
            add(snapshot_id, {"lineage_refs_without_version_or_hash": values})

        stored_inputs = self.kernel.read._repository_query(
            context, "research.snapshot_input",
            """SELECT snapshot_id, schema_name, table_name, selection_predicate,
                              selected_columns, hash_algorithm, row_count, min_date,
                              max_date, content_hash
                         FROM research.snapshot_input
                        WHERE snapshot_id=ANY(%s::uuid[])
                        ORDER BY snapshot_id, schema_name, table_name""",
            (ids,), scope, max_rows=max(2, len(ids) * len(definitions) + 1),
        )
        expected_keys = [(item["schema"], item["table"]) for item in definitions]
        inputs_by_id: dict[str, list[dict[str, Any]]] = {}
        for row in stored_inputs:
            inputs_by_id.setdefault(str(row["snapshot_id"]), []).append(row)
        for manifest in manifests:
            snapshot_id = str(manifest["snapshot_id"])
            rows = inputs_by_id.get(snapshot_id, [])
            stored_by_key = {(row["schema_name"], row["table_name"]): row for row in rows}
            if len(rows) != len(definitions) or set(stored_by_key) != set(expected_keys):
                add(snapshot_id, {
                    "snapshot_input_manifest_incomplete": {
                        "expected": expected_keys, "stored": sorted(stored_by_key),
                    }
                })
            else:
                # ``snapshot_id`` is only the batch grouping key.  The
                # persisted input hash has always been defined over the
                # original input-manifest columns, excluding that grouping
                # column; keep the historical hash contract unchanged.
                ordered = [
                    {key: row[key] for key in row if key != "snapshot_id"}
                    for key in expected_keys
                    for row in [stored_by_key[key]]
                ]
                current_hash = manifest_hash(ordered)
                if current_hash != manifest["input_hash"]:
                    add(snapshot_id, {
                        "input_manifest_hash_changed": {
                            "stored": manifest["input_hash"], "current": current_hash,
                        }
                    })

        if manifests[0].get("schema_version") == "research-v1.3":
            maturity = self.kernel.read._repository_query(
                context, "research.stock_snapshot_daily",
                """SELECT snapshot_id,
                    COUNT(*) FILTER (WHERE ma20 IS NOT NULL AND COALESCE(ma20_observation_count,0)<20) AS ma20,
                    COUNT(*) FILTER (WHERE ma60 IS NOT NULL AND COALESCE(ma60_observation_count,0)<60) AS ma60,
                    COUNT(*) FILTER (WHERE ma120 IS NOT NULL AND COALESCE(ma120_observation_count,0)<120) AS ma120,
                    COUNT(*) FILTER (WHERE pe_pct_5y IS NOT NULL AND (NOT COALESCE(has_full_pe_5y_history,FALSE) OR COALESCE(valuation_observation_count,0)<1000 OR pe_window_start_date_5y IS NULL OR research_date-pe_window_start_date_5y<1794)) AS pe,
                    COUNT(*) FILTER (WHERE financial_version_available_date>research_date) AS financial_date,
                    COUNT(*) FILTER (WHERE financial_vintage_quality='WAREHOUSE_OBSERVED_ONLY' AND financial_source_observed_at::date>research_date) AS financial_observed,
                    COUNT(*) FILTER (WHERE instrument_source_record_key IS NULL OR instrument_source_record_version IS NULL OR instrument_source_payload_hash IS NULL) AS instrument,
                    COUNT(*) FILTER (WHERE roe_5y_all_above_15 AND annual_report_count_5y<>5) AS roe,
                    COUNT(*) FILTER (WHERE ocf_3y_all_positive AND ocf_report_count_3y<>3) AS ocf,
                    COUNT(*) FILTER (WHERE q_sales_yoy_4q_all_above_30 AND quarterly_report_count_4q<>4) AS quarterly
                  FROM research.stock_snapshot_daily
                 WHERE snapshot_id=ANY(%s::uuid[]) AND research_date=ANY(%s::date[])
                 GROUP BY snapshot_id""",
                (ids, cutoffs), scope, max_rows=max(2, len(ids) + 1),
            )
            for row in maturity:
                values = tuple(row[key] for key in (
                    "ma20", "ma60", "ma120", "pe", "financial_date",
                    "financial_observed", "instrument", "roe", "ocf", "quarterly",
                ))
                if any(values):
                    add(str(row["snapshot_id"]), {
                        "research_v1_2_maturity_or_pit_failures": values,
                    })
        return issues
