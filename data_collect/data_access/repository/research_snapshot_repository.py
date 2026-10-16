"""Typed Research Snapshot reads and manifest persistence.

The job layer owns orchestration and pure hashing/validation rules.  This
repository owns all Research/Release SQL and reaches PostgreSQL only through
Kernel gateways.
"""
from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from datetime import date
from typing import Any, Callable, Iterable, NamedTuple

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.repository.base import BaseRepository
from data_collect.data_access.research_contract import is_foundation_schema
from data_collect.data_access.snapshot_lifecycle import (
    MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE,
    require_snapshot_batch,
    snapshot_id_batches,
)


_IMMUTABLE_INPUT_STATS_CACHE: dict[tuple, dict[str, Any]] = {}


class ResearchBaseline(NamedTuple):
    """Validated release evidence shared by Research Snapshot operations.

    This remains a tuple for compatibility with existing internal callers,
    while named fields make the repository contract explicit.
    """

    release_tag: str
    baseline_cutoff_date: date
    status: str
    quality_report_hash: str
    source_manifest_hash: str
    source_manifest: dict[str, Any]
    schema_version: str | None
    quality_report: dict[str, Any] | None = None


class ResearchSnapshotRepository(BaseRepository):
    LEGACY_LINEAGE_TARGET = "research.snapshot_lineage"
    BUNDLE_LINEAGE_TARGET = "research.snapshot_lineage_bundle"
    SOURCE_LINEAGE_TARGET = "research.lineage_source_record"
    LINEAGE_STORAGE_V2 = "v2"
    # The Foundation financial contract joins materialized snapshot rows to
    # the large source-record registry. Keep each validation query below the
    # production statement timeout; the public method still returns one
    # aggregate issue map for the complete input.
    FOUNDATION_FINANCIAL_CONTRACT_BATCH_SIZE = 5
    # Snapshot SQL is set-based, but one code batch must also remain below the
    # governed 30-second statement deadline on the oldest/highest-cost dates.
    # 100 codes keeps the stock INSERT ... SELECT below the governed
    # 30-second statement deadline on the production WSL/PostgreSQL storage;
    # the adaptive runner below can split a batch further for an unusually
    # expensive date without widening the transaction deadline.
    SNAPSHOT_CODE_BATCH_SIZE = 100
    # The compact lineage statement writes one row per stock and uses
    # statement-level guards, so it can safely use a larger independent batch.
    # Its source dictionary is still bounded to roughly ten records per code.
    LINEAGE_CODE_BATCH_SIZE = 1_000
    # Legacy migration reads an existing ten-row-per-code relation and does
    # not materialize the wide snapshot row.  One thousand codes keep the
    # source aggregation safely below the 30-second statement deadline on the
    # production WSL/PostgreSQL storage while keeping each transaction small.
    LEGACY_LINEAGE_CODE_BATCH_SIZE = 1_000
    # Rebuild retries must remove any partially materialized snapshot first.
    # A complete snapshot has roughly 50k lineage rows; deleting all of them
    # in one governed statement can exceed the 30-second statement deadline
    # even when the lookup itself uses the snapshot/date index.  Keep each
    # delete as its own short Kernel transaction.
    SNAPSHOT_DELETE_CHUNK_SIZE = 2_000
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
    LINEAGE_FIELD_GROUPS = (
        "universe", "tradeability", "price", "valuation", "financial",
        "shareholder_return", "lifecycle", "instrument", "market_facts",
        "fundamental_history",
    )
    MAX_SNAPSHOT_CODES = 20_000

    @contextmanager
    def lifecycle_lock(self, snapshot_id: str):
        """Serialize one Snapshot's build, validation, and freeze lifecycle."""
        with self.kernel.lock_manager.session_exclusive(
            "research.snapshot", str(snapshot_id), wait=True,
        ):
            yield

    @contextmanager
    def lifecycle_locks(self, snapshot_ids: Iterable[str]):
        """Acquire one bounded set of Snapshot lifecycle locks in stable order."""
        ids = require_snapshot_batch(
            snapshot_ids, "snapshot lifecycle lock", allow_empty=False,
        )
        with self.kernel.lock_manager.session_exclusive_many(
            "research.snapshot", ids, wait=True,
        ):
            yield

    @contextmanager
    def _snapshot_build_locks(
        self, snapshot_ids: Iterable[str], context: RunContext,
    ):
        """Mark started builds failed before releasing their lifecycle locks."""
        started: list[tuple[str, date]] = []
        with self.lifecycle_locks(snapshot_ids):
            try:
                yield started
            except Exception as exc:
                reason = str(exc)[:2000]
                for snapshot_id, cutoff in started:
                    try:
                        self.kernel.write.transition_snapshot_manifest(
                            context,
                            self._snapshot_scope(snapshot_id, cutoff),
                            snapshot_id,
                            ("BUILDING",),
                            "FAILED",
                            {"failure_reason": reason},
                        )
                    except Exception:
                        # Keep the original build error; incomplete BUILDING
                        # data is still rejected by the acceptance checks.
                        pass
                raise

    # A timeout means the database rolled back the whole typed Gateway
    # statement.  Split the exact code set and retry only the smaller
    # idempotent unit; never increase the statement deadline or retry an
    # arbitrary SQL operation.  25 keeps the fallback bounded on the most
    # expensive Foundation-v2 joins while avoiding one-row thrashing.
    MIN_ADAPTIVE_CODE_BATCH_SIZE = 25
    # The Foundation replacement path runs a relatively expensive lineage
    # CTE against the snapshot and its source dictionary.  Keep this path
    # deliberately smaller than the normal builder's batch: a replacement
    # must be restartable and must never leave an orphaned, multi-minute
    # statement holding the database I/O queue hostage on WSL/PostgreSQL.
    REPLACEMENT_LINEAGE_CODE_BATCH_SIZE = 100
    REPLACEMENT_INTEGRITY_BATCH_SIZE = 20
    # All persisted stock fields are copied from the immutable source Snapshot
    # before the Foundation-v2 financial enrichment runs.  Keeping this
    # allowlisted projection explicit avoids SELECT * drift when the table is
    # extended by a later migration.
    REPLACEMENT_STOCK_COLUMNS = (
        "research_date", "ts_code", "symbol", "name", "market", "list_status",
        "list_date", "delist_date", "is_listed_asof", "is_st_asof",
        "is_suspended_asof", "eligible_by_market", "is_tradeable", "can_buy",
        "can_sell", "limit_status", "raw_open", "raw_high", "raw_low",
        "raw_close", "raw_volume", "raw_amount", "up_limit", "down_limit",
        "adj_close_base", "price_feature_version", "price_adjustment_mode",
        "return_1d", "return_20d", "return_60d", "ma20", "ma60", "ma120",
        "volume_ratio_20d", "available_observation_count", "ma20_observation_count",
        "ma60_observation_count", "ma120_observation_count", "pe_ttm", "pb",
        "ps_ttm", "dv_ratio", "total_mv", "circ_mv", "pe_pct_3y", "pe_pct_5y",
        "pb_pct_3y", "pb_pct_5y", "is_pe_meaningful", "valuation_observation_count",
        "pe_window_start_date_5y", "has_full_pe_5y_history", "report_date",
        "effective_announce_date", "financial_version_available_date",
        "financial_record_version", "financial_source_record_key",
        "financial_payload_hash", "financial_vintage_quality",
        "financial_source_observed_at", "roe", "roa", "debt_to_assets",
        "revenue_yoy", "tr_yoy", "q_sales_yoy", "netprofit_yoy",
        "operating_cashflow", "net_profit", "cash_div_12m",
        "dividend_event_count_3y", "dividend_year_count_3y", "annual_report_count_5y",
        "roe_above_15_years_5y", "roe_5y_min", "roe_5y_all_above_15",
        "ocf_report_count_3y", "ocf_positive_years_3y", "ocf_3y_all_positive",
        "quarterly_report_count_4q", "q_sales_yoy_above_30_quarters_4q",
        "q_sales_yoy_4q_all_above_30", "instrument_source_record_key",
        "instrument_source_record_version", "instrument_source_payload_hash",
        "financial_feature_contract_version", "financial_data_available_timestamp",
        "net_profit_consolidated_ttm", "net_profit_parent_ttm",
        "operating_cashflow_ttm", "ocf_to_net_profit_consolidated_ttm",
        "roe_ttm", "gross_margin_ttm", "net_margin_consolidated_ttm",
        "revenue_growth_yoy", "profit_growth_yoy", "cashflow_quality_status",
        "roe_quality_status", "growth_quality_status", "ttm_complete", "dv_ttm",
        "dv_ttm_percentile_5y", "dv_ttm_source", "dv_window_start", "dv_window_end",
        "dv_observation_days", "dv_window_complete", "financial_component_lineage",
        "financial_source_fingerprint", "quality_status",
    )

    @classmethod
    def lineage_target(cls, manifest: dict[str, Any]) -> str:
        """Select the physical lineage target recorded by the manifest."""
        if manifest.get("lineage_storage_version") == cls.LINEAGE_STORAGE_V2:
            return cls.BUNDLE_LINEAGE_TARGET
        return cls.LEGACY_LINEAGE_TARGET

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
                p.adj_close_base, p.feature_set_version, p.adjustment_mode,
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
    def lineage_sql(*, foundation: bool = False):
        sql = """
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
                        'raw_close', p.raw_close, 'adj_close_base', p.adj_close_base,
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
        if foundation:
            # Foundation-v2's formal financial values are enriched from
            # company_financial_v2.  The base statement still retains the
            # V1 compatibility fields and those are declared in the input
            # manifest; the single financial lineage record must therefore
            # point to the V2 record selected by the enrichment statement.
            sql = sql.replace(
                "'feature.company_financial_v1', COALESCE(s.financial_source_record_key,",
                "'feature.company_financial_v2', COALESCE(s.financial_source_record_key,",
                1,
            )
        return sql

    @staticmethod
    def lineage_bundle_sql(*, foundation: bool = False) -> str:
        """Return the compact lineage bundle statement for one snapshot.

        The logical lineage contract is unchanged from ``lineage_sql``: ten
        field groups are resolved for every stock.  The physical write stores
        one row per stock and puts the verbose source metadata in a global,
        content-addressed dictionary.  The resulting statement is deliberately
        accepted only by the named RebuildGateway lineage operation.
        """
        sql = ResearchSnapshotRepository.lineage_sql(foundation=foundation)
        header = (
            "        INSERT INTO research.snapshot_lineage\n"
            "          (snapshot_id, research_date, ts_code, field_group, source_table,\n"
            "           source_record_key, source_record_version, source_payload_hash)\n"
        )
        if header not in sql:
            raise RuntimeError("legacy lineage SQL header changed; cannot build bundle SQL")
        sql = sql.replace(header, "", 1)
        final_select = (
            "        SELECT g.snapshot_id, g.research_date, g.ts_code, g.field_group,\n"
            "               COALESCE(r.source_table, g.field_group || ':missing'),\n"
            "               COALESCE(r.source_record_key, g.ts_code || ':' || g.research_date::text || ':missing'),\n"
            "               r.source_record_version, r.source_payload_hash\n"
            "        FROM groups g\n"
            "        LEFT JOIN refs r ON r.snapshot_id=g.snapshot_id AND r.research_date=g.research_date\n"
            "                         AND r.ts_code=g.ts_code AND r.field_group=g.field_group\n"
            "        ON CONFLICT DO NOTHING\n"
        )
        if final_select not in sql:
            raise RuntimeError("legacy lineage SQL final projection changed; cannot build bundle SQL")
        bundle_select = """        , normalized_refs AS (
            SELECT g.snapshot_id, g.research_date, g.ts_code, g.field_group,
                   COALESCE(r.source_table, g.field_group || ':missing') AS source_table,
                   COALESCE(r.source_record_key, g.ts_code || ':' || g.research_date::text || ':missing') AS source_record_key,
                   r.source_record_version,
                   r.source_payload_hash,
                   digest(convert_to(jsonb_build_object(
                       'source_table', COALESCE(r.source_table, g.field_group || ':missing'),
                       'source_record_key', COALESCE(r.source_record_key, g.ts_code || ':' || g.research_date::text || ':missing'),
                       'source_record_version', r.source_record_version,
                       'source_payload_hash', r.source_payload_hash
                   )::text, 'UTF8'), 'sha256') AS source_identity_hash
            FROM groups g
            LEFT JOIN refs r ON r.snapshot_id=g.snapshot_id AND r.research_date=g.research_date
                             AND r.ts_code=g.ts_code AND r.field_group=g.field_group
        ), source_rows AS (
            SELECT DISTINCT source_table, source_record_key, source_record_version,
                            source_payload_hash, source_identity_hash
            FROM normalized_refs
        ), inserted_source AS (
            INSERT INTO research.lineage_source_record
                (source_identity_hash, source_table, source_record_key,
                 source_record_version, source_payload_hash)
            SELECT source_identity_hash, source_table, source_record_key,
                   source_record_version, source_payload_hash
              FROM source_rows
            ON CONFLICT (source_identity_hash) DO NOTHING
            RETURNING source_ref_id, source_identity_hash
        ), resolved_refs AS (
            SELECT n.snapshot_id, n.research_date, n.ts_code, n.field_group,
                   n.source_identity_hash,
                   COALESCE(i.source_ref_id, d.source_ref_id) AS source_ref_id
              FROM normalized_refs n
              LEFT JOIN inserted_source i ON i.source_identity_hash=n.source_identity_hash
              LEFT JOIN research.lineage_source_record d
                ON d.source_identity_hash=n.source_identity_hash
        ), bundles AS (
            SELECT snapshot_id, research_date, ts_code,
                   MAX(source_ref_id) FILTER (WHERE field_group='universe') AS universe_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='tradeability') AS tradeability_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='price') AS price_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='valuation') AS valuation_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='financial') AS financial_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='shareholder_return') AS shareholder_return_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='lifecycle') AS lifecycle_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='instrument') AS instrument_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='market_facts') AS market_facts_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='fundamental_history') AS fundamental_history_source_ref_id,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='universe'))[1] AS universe_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='tradeability'))[1] AS tradeability_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='price'))[1] AS price_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='valuation'))[1] AS valuation_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='financial'))[1] AS financial_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='shareholder_return'))[1] AS shareholder_return_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='lifecycle'))[1] AS lifecycle_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='instrument'))[1] AS instrument_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='market_facts'))[1] AS market_facts_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='fundamental_history'))[1] AS fundamental_history_identity_hash
              FROM resolved_refs
             GROUP BY snapshot_id, research_date, ts_code
            HAVING COUNT(DISTINCT field_group)=10
        )
        INSERT INTO research.snapshot_lineage_bundle
            (snapshot_id, research_date, ts_code,
             universe_source_ref_id, tradeability_source_ref_id,
             price_source_ref_id, valuation_source_ref_id,
             financial_source_ref_id, shareholder_return_source_ref_id,
             lifecycle_source_ref_id, instrument_source_ref_id,
             market_facts_source_ref_id, fundamental_history_source_ref_id,
             source_bundle_hash)
        SELECT snapshot_id, research_date, ts_code,
               universe_source_ref_id, tradeability_source_ref_id,
               price_source_ref_id, valuation_source_ref_id,
               financial_source_ref_id, shareholder_return_source_ref_id,
               lifecycle_source_ref_id, instrument_source_ref_id,
               market_facts_source_ref_id, fundamental_history_source_ref_id,
               encode(digest(convert_to(jsonb_build_object(
                   'universe', encode(universe_identity_hash, 'hex'),
                   'tradeability', encode(tradeability_identity_hash, 'hex'),
                   'price', encode(price_identity_hash, 'hex'),
                   'valuation', encode(valuation_identity_hash, 'hex'),
                   'financial', encode(financial_identity_hash, 'hex'),
                   'shareholder_return', encode(shareholder_return_identity_hash, 'hex'),
                   'lifecycle', encode(lifecycle_identity_hash, 'hex'),
                   'instrument', encode(instrument_identity_hash, 'hex'),
                   'market_facts', encode(market_facts_identity_hash, 'hex'),
                   'fundamental_history', encode(fundamental_history_identity_hash, 'hex')
               )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
          FROM bundles
        ON CONFLICT (research_date, snapshot_id, ts_code) DO UPDATE SET
            universe_source_ref_id=EXCLUDED.universe_source_ref_id,
            tradeability_source_ref_id=EXCLUDED.tradeability_source_ref_id,
            price_source_ref_id=EXCLUDED.price_source_ref_id,
            valuation_source_ref_id=EXCLUDED.valuation_source_ref_id,
            financial_source_ref_id=EXCLUDED.financial_source_ref_id,
            shareholder_return_source_ref_id=EXCLUDED.shareholder_return_source_ref_id,
            lifecycle_source_ref_id=EXCLUDED.lifecycle_source_ref_id,
            instrument_source_ref_id=EXCLUDED.instrument_source_ref_id,
            market_facts_source_ref_id=EXCLUDED.market_facts_source_ref_id,
            fundamental_history_source_ref_id=EXCLUDED.fundamental_history_source_ref_id,
            source_bundle_hash=EXCLUDED.source_bundle_hash
        """
        return sql.replace(final_select, bundle_select, 1)

    @staticmethod
    def foundation_lineage_clone_sql() -> str:
        """Return the bounded financial-only lineage replacement statement.

        A Foundation repair reuses an immutable source Snapshot whose nine
        non-financial lineage references are already validated.  Recomputing
        all ten lineage groups from PIT/Feature for every replacement date
        would turn a format/materialization repair into a full rebuild.  This
        statement therefore copies the nine existing references, creates only
        the V2 financial source dictionary entries from the repaired stock
        rows, and recomputes the deterministic bundle hash.
        """
        return """
        WITH params AS (
            SELECT %s::uuid AS target_snapshot_id,
                   %s::uuid AS source_snapshot_id,
                   %s::date AS research_date,
                   %s::text[] AS codes
        ), financial AS (
            SELECT s.ts_code,
                   'feature.company_financial_v2'::varchar AS source_table,
                   COALESCE(
                       s.financial_source_record_key,
                       s.ts_code || ':' || s.research_date::text || ':missing'
                   ) AS source_record_key,
                   s.financial_record_version AS source_record_version,
                   s.financial_payload_hash AS source_payload_hash,
                   digest(convert_to(jsonb_build_object(
                       'source_table', 'feature.company_financial_v2',
                       'source_record_key', COALESCE(
                           s.financial_source_record_key,
                           s.ts_code || ':' || s.research_date::text || ':missing'
                       ),
                       'source_record_version', s.financial_record_version,
                       'source_payload_hash', s.financial_payload_hash
                   )::text, 'UTF8'), 'sha256') AS source_identity_hash
              FROM research.stock_snapshot_daily s
              JOIN params p
                ON p.target_snapshot_id=s.snapshot_id
               AND p.research_date=s.research_date
               AND s.ts_code=ANY(p.codes)
        ), source_rows AS (
            SELECT DISTINCT source_table, source_record_key,
                            source_record_version, source_payload_hash,
                            source_identity_hash
              FROM financial
        ), inserted_source AS (
            INSERT INTO research.lineage_source_record
                (source_identity_hash, source_table, source_record_key,
                 source_record_version, source_payload_hash)
            SELECT source_identity_hash, source_table, source_record_key,
                   source_record_version, source_payload_hash
              FROM source_rows
            ON CONFLICT (source_identity_hash) DO NOTHING
            RETURNING source_ref_id, source_identity_hash
        ), resolved_financial AS (
            SELECT f.ts_code, f.source_identity_hash,
                   COALESCE(i.source_ref_id, d.source_ref_id) AS source_ref_id
              FROM financial f
              LEFT JOIN inserted_source i
                ON i.source_identity_hash=f.source_identity_hash
              LEFT JOIN research.lineage_source_record d
                ON d.source_identity_hash=f.source_identity_hash
        ), source_bundle AS (
            SELECT b.ts_code,
                   b.universe_source_ref_id,
                   b.tradeability_source_ref_id,
                   b.price_source_ref_id,
                   b.valuation_source_ref_id,
                   b.shareholder_return_source_ref_id,
                   b.lifecycle_source_ref_id,
                   b.instrument_source_ref_id,
                   b.market_facts_source_ref_id,
                   b.fundamental_history_source_ref_id,
                   u.source_identity_hash AS universe_identity_hash,
                   t.source_identity_hash AS tradeability_identity_hash,
                   pr.source_identity_hash AS price_identity_hash,
                   v.source_identity_hash AS valuation_identity_hash,
                   sr.source_identity_hash AS shareholder_return_identity_hash,
                   l.source_identity_hash AS lifecycle_identity_hash,
                   i.source_identity_hash AS instrument_identity_hash,
                   mf.source_identity_hash AS market_facts_identity_hash,
                   fh.source_identity_hash AS fundamental_history_identity_hash
              FROM research.snapshot_lineage_bundle b
              JOIN params p
                ON p.source_snapshot_id=b.snapshot_id
               AND p.research_date=b.research_date
               AND b.ts_code=ANY(p.codes)
              JOIN research.lineage_source_record u
                ON u.source_ref_id=b.universe_source_ref_id
              JOIN research.lineage_source_record t
                ON t.source_ref_id=b.tradeability_source_ref_id
              JOIN research.lineage_source_record pr
                ON pr.source_ref_id=b.price_source_ref_id
              JOIN research.lineage_source_record v
                ON v.source_ref_id=b.valuation_source_ref_id
              JOIN research.lineage_source_record sr
                ON sr.source_ref_id=b.shareholder_return_source_ref_id
              JOIN research.lineage_source_record l
                ON l.source_ref_id=b.lifecycle_source_ref_id
              JOIN research.lineage_source_record i
                ON i.source_ref_id=b.instrument_source_ref_id
              JOIN research.lineage_source_record mf
                ON mf.source_ref_id=b.market_facts_source_ref_id
              JOIN research.lineage_source_record fh
                ON fh.source_ref_id=b.fundamental_history_source_ref_id
        )
        INSERT INTO research.snapshot_lineage_bundle
            (snapshot_id, research_date, ts_code,
             universe_source_ref_id, tradeability_source_ref_id,
             price_source_ref_id, valuation_source_ref_id,
             financial_source_ref_id, shareholder_return_source_ref_id,
             lifecycle_source_ref_id, instrument_source_ref_id,
             market_facts_source_ref_id, fundamental_history_source_ref_id,
             source_bundle_hash)
        SELECT p.target_snapshot_id, p.research_date, b.ts_code,
               b.universe_source_ref_id, b.tradeability_source_ref_id,
               b.price_source_ref_id, b.valuation_source_ref_id,
               f.source_ref_id, b.shareholder_return_source_ref_id,
               b.lifecycle_source_ref_id, b.instrument_source_ref_id,
               b.market_facts_source_ref_id, b.fundamental_history_source_ref_id,
               encode(digest(convert_to(jsonb_build_object(
                   'universe', encode(b.universe_identity_hash, 'hex'),
                   'tradeability', encode(b.tradeability_identity_hash, 'hex'),
                   'price', encode(b.price_identity_hash, 'hex'),
                   'valuation', encode(b.valuation_identity_hash, 'hex'),
                   'financial', encode(f.source_identity_hash, 'hex'),
                   'shareholder_return', encode(b.shareholder_return_identity_hash, 'hex'),
                   'lifecycle', encode(b.lifecycle_identity_hash, 'hex'),
                   'instrument', encode(b.instrument_identity_hash, 'hex'),
                   'market_facts', encode(b.market_facts_identity_hash, 'hex'),
                   'fundamental_history', encode(b.fundamental_history_identity_hash, 'hex')
               )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
          FROM params p
          JOIN source_bundle b ON TRUE
          JOIN resolved_financial f ON f.ts_code=b.ts_code
        ON CONFLICT (research_date, snapshot_id, ts_code) DO UPDATE SET
            universe_source_ref_id=EXCLUDED.universe_source_ref_id,
            tradeability_source_ref_id=EXCLUDED.tradeability_source_ref_id,
            price_source_ref_id=EXCLUDED.price_source_ref_id,
            valuation_source_ref_id=EXCLUDED.valuation_source_ref_id,
            financial_source_ref_id=EXCLUDED.financial_source_ref_id,
            shareholder_return_source_ref_id=EXCLUDED.shareholder_return_source_ref_id,
            lifecycle_source_ref_id=EXCLUDED.lifecycle_source_ref_id,
            instrument_source_ref_id=EXCLUDED.instrument_source_ref_id,
            market_facts_source_ref_id=EXCLUDED.market_facts_source_ref_id,
            fundamental_history_source_ref_id=EXCLUDED.fundamental_history_source_ref_id,
            source_bundle_hash=EXCLUDED.source_bundle_hash
        """

    @staticmethod
    def legacy_lineage_bundle_sql() -> str:
        """Return the bounded v1-lineage to v2-bundle migration statement.

        This statement intentionally reads only the already materialized
        ``research.snapshot_lineage`` facts.  It does not join or rewrite the
        stock snapshot, PIT or Feature relations.  One invocation is scoped
        by a single snapshot/date and a bounded code array; the final INSERT
        therefore produces at most one bundle row per requested stock.

        The source identity and bundle hashes mirror ``lineage_bundle_sql``.
        Consequently this is a physical-format conversion, not a second
        interpretation of the snapshot's upstream data.
        """
        return f"""
        WITH params AS (
            SELECT %s::uuid AS snapshot_id,
                   %s::date AS research_date
        ), codes AS (
            SELECT unnest(%s::text[]) AS ts_code
        ), legacy_rows AS (
            SELECT l.snapshot_id, l.research_date, l.ts_code, l.field_group,
                   l.source_table, l.source_record_key,
                   l.source_record_version, l.source_payload_hash,
                   digest(convert_to(jsonb_build_object(
                       'source_table', l.source_table,
                       'source_record_key', l.source_record_key,
                       'source_record_version', l.source_record_version,
                       'source_payload_hash', l.source_payload_hash
                   )::text, 'UTF8'), 'sha256') AS source_identity_hash
              FROM research.snapshot_lineage l
              JOIN params p
                ON p.snapshot_id=l.snapshot_id
               AND p.research_date=l.research_date
              JOIN codes c
                ON c.ts_code=l.ts_code
             WHERE l.field_group IN (
                   'universe', 'tradeability', 'price', 'valuation',
                   'financial', 'shareholder_return', 'lifecycle',
                   'instrument', 'market_facts', 'fundamental_history'
               )
        ), complete_keys AS (
            SELECT snapshot_id, research_date, ts_code
              FROM legacy_rows
             GROUP BY snapshot_id, research_date, ts_code
            HAVING COUNT(*)=10
               AND COUNT(DISTINCT field_group)=10
               AND COUNT(*) FILTER (
                   WHERE source_record_key NOT LIKE '%%:missing'
                     AND (source_record_version IS NULL
                          OR source_payload_hash IS NULL)
               )=0
        ), complete_rows AS (
            SELECT l.*
              FROM legacy_rows l
              JOIN complete_keys k
                ON k.snapshot_id=l.snapshot_id
               AND k.research_date=l.research_date
               AND k.ts_code=l.ts_code
        ), source_rows AS (
            SELECT DISTINCT source_table, source_record_key,
                            source_record_version, source_payload_hash,
                            source_identity_hash
              FROM complete_rows
        ), inserted_source AS (
            INSERT INTO research.lineage_source_record
                (source_identity_hash, source_table, source_record_key,
                 source_record_version, source_payload_hash)
            SELECT source_identity_hash, source_table, source_record_key,
                   source_record_version, source_payload_hash
              FROM source_rows
            ON CONFLICT (source_identity_hash) DO NOTHING
            RETURNING source_ref_id, source_identity_hash
        ), resolved_refs AS (
            SELECT c.snapshot_id, c.research_date, c.ts_code, c.field_group,
                   c.source_identity_hash,
                   COALESCE(i.source_ref_id, d.source_ref_id) AS source_ref_id
              FROM complete_rows c
              LEFT JOIN inserted_source i
                ON i.source_identity_hash=c.source_identity_hash
              LEFT JOIN research.lineage_source_record d
                ON d.source_identity_hash=c.source_identity_hash
        ), bundles AS (
            SELECT snapshot_id, research_date, ts_code,
                   MAX(source_ref_id) FILTER (WHERE field_group='universe') AS universe_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='tradeability') AS tradeability_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='price') AS price_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='valuation') AS valuation_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='financial') AS financial_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='shareholder_return') AS shareholder_return_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='lifecycle') AS lifecycle_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='instrument') AS instrument_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='market_facts') AS market_facts_source_ref_id,
                   MAX(source_ref_id) FILTER (WHERE field_group='fundamental_history') AS fundamental_history_source_ref_id,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='universe'))[1] AS universe_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='tradeability'))[1] AS tradeability_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='price'))[1] AS price_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='valuation'))[1] AS valuation_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='financial'))[1] AS financial_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='shareholder_return'))[1] AS shareholder_return_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='lifecycle'))[1] AS lifecycle_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='instrument'))[1] AS instrument_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='market_facts'))[1] AS market_facts_identity_hash,
                   (array_agg(source_identity_hash ORDER BY source_identity_hash)
                       FILTER (WHERE field_group='fundamental_history'))[1] AS fundamental_history_identity_hash
              FROM resolved_refs
             GROUP BY snapshot_id, research_date, ts_code
            HAVING COUNT(*)=10 AND COUNT(DISTINCT field_group)=10
        )
        INSERT INTO research.snapshot_lineage_bundle
            (snapshot_id, research_date, ts_code,
             universe_source_ref_id, tradeability_source_ref_id,
             price_source_ref_id, valuation_source_ref_id,
             financial_source_ref_id, shareholder_return_source_ref_id,
             lifecycle_source_ref_id, instrument_source_ref_id,
             market_facts_source_ref_id, fundamental_history_source_ref_id,
             source_bundle_hash)
        SELECT snapshot_id, research_date, ts_code,
               universe_source_ref_id, tradeability_source_ref_id,
               price_source_ref_id, valuation_source_ref_id,
               financial_source_ref_id, shareholder_return_source_ref_id,
               lifecycle_source_ref_id, instrument_source_ref_id,
               market_facts_source_ref_id, fundamental_history_source_ref_id,
               encode(digest(convert_to(jsonb_build_object(
                   'universe', encode(universe_identity_hash, 'hex'),
                   'tradeability', encode(tradeability_identity_hash, 'hex'),
                   'price', encode(price_identity_hash, 'hex'),
                   'valuation', encode(valuation_identity_hash, 'hex'),
                   'financial', encode(financial_identity_hash, 'hex'),
                   'shareholder_return', encode(shareholder_return_identity_hash, 'hex'),
                   'lifecycle', encode(lifecycle_identity_hash, 'hex'),
                   'instrument', encode(instrument_identity_hash, 'hex'),
                   'market_facts', encode(market_facts_identity_hash, 'hex'),
                   'fundamental_history', encode(fundamental_history_identity_hash, 'hex')
               )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
          FROM bundles
        ON CONFLICT (research_date, snapshot_id, ts_code) DO UPDATE SET
            universe_source_ref_id=EXCLUDED.universe_source_ref_id,
            tradeability_source_ref_id=EXCLUDED.tradeability_source_ref_id,
            price_source_ref_id=EXCLUDED.price_source_ref_id,
            valuation_source_ref_id=EXCLUDED.valuation_source_ref_id,
            financial_source_ref_id=EXCLUDED.financial_source_ref_id,
            shareholder_return_source_ref_id=EXCLUDED.shareholder_return_source_ref_id,
            lifecycle_source_ref_id=EXCLUDED.lifecycle_source_ref_id,
            instrument_source_ref_id=EXCLUDED.instrument_source_ref_id,
            market_facts_source_ref_id=EXCLUDED.market_facts_source_ref_id,
            fundamental_history_source_ref_id=EXCLUDED.fundamental_history_source_ref_id,
            source_bundle_hash=EXCLUDED.source_bundle_hash
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
    def lineage_batch_sql(*, foundation: bool = False):
        """Return the lineage SQL with a UUID array instead of one UUID."""
        sql = ResearchSnapshotRepository.lineage_sql(foundation=foundation)
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
                # The base materialization intentionally keeps the legacy V1
                # compatibility fields (roe/roa/vendor growth and the
                # fundamental-history summary).  They are a real read
                # dependency and must be present in snapshot_input instead of
                # being hidden behind the Foundation-v2 enrichment pass.
                result.append({
                    "schema": "feature", "table": "company_financial_v1",
                    "columns": (
                        "feature_id", "ts_code", "report_date",
                        "effective_announce_date", "version_available_date",
                        "vintage_quality", "source_observed_at",
                        "record_version", "source_record_key", "roe", "roa",
                        "debt_to_assets", "revenue_yoy", "tr_yoy",
                        "q_sales_yoy", "netprofit_yoy",
                        "operating_cashflow", "net_profit", "quality_status",
                        "payload_hash",
                    ),
                    "predicate": "latest version_available_date before cutoff",
                    "where": "version_available_date IS NOT NULL AND version_available_date <= %s",
                    "params": lambda cutoff: (cutoff,),
                    "order": "ts_code, report_date, version_available_date, record_version, feature_id",
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
             report_date, effective_announce_date,
             financial_record_version, financial_source_record_key,
             financial_payload_hash, financial_source_fingerprint,
             financial_vintage_quality,
             financial_source_observed_at,
             financial_feature_contract_version, financial_data_available_timestamp,
             net_profit_consolidated_ttm, net_profit_parent_ttm,
             operating_cashflow_ttm, ocf_to_net_profit_consolidated_ttm,
             roe_ttm, gross_margin_ttm,
             net_margin_consolidated_ttm, revenue_growth_yoy, profit_growth_yoy,
             ttm_complete, cashflow_quality_status, roe_quality_status,
             growth_quality_status, dv_ttm, dv_ttm_percentile_5y, dv_window_start,
             dv_window_end, dv_observation_days, dv_window_complete,
             financial_component_lineage, quality_status)
        SELECT %s, s.research_date, s.ts_code,
               f.report_date, f.effective_announce_date,
               f.feature_id,
               CASE WHEN f.feature_id IS NULL THEN NULL
                    ELSE f.ts_code || ':' || f.report_date::text || ':' || f.feature_id::text END,
               f.source_fingerprint, f.source_fingerprint,
               f.pit_evidence_level, f.source_observed_at,
               COALESCE(f.feature_contract_version, 'feature-contract-v2.0'),
               f.data_available_timestamp, f.net_profit_consolidated_ttm,
               f.net_profit_parent_ttm, f.operating_cashflow_ttm,
               f.ocf_to_net_profit_consolidated_ttm, f.roe_ttm,
               f.gross_margin_ttm, f.net_margin_consolidated_ttm,
               f.revenue_growth_yoy, f.profit_growth_yoy, f.ttm_complete,
               f.cashflow_quality_status, f.roe_quality_status,
               f.growth_quality_status,
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
            report_date=EXCLUDED.report_date,
            effective_announce_date=EXCLUDED.effective_announce_date,
            financial_record_version=EXCLUDED.financial_record_version,
            financial_source_record_key=EXCLUDED.financial_source_record_key,
            financial_payload_hash=EXCLUDED.financial_payload_hash,
            financial_source_fingerprint=EXCLUDED.financial_source_fingerprint,
            financial_vintage_quality=EXCLUDED.financial_vintage_quality,
            financial_source_observed_at=EXCLUDED.financial_source_observed_at,
            financial_feature_contract_version=EXCLUDED.financial_feature_contract_version,
            financial_data_available_timestamp=EXCLUDED.financial_data_available_timestamp,
            net_profit_consolidated_ttm=EXCLUDED.net_profit_consolidated_ttm,
            net_profit_parent_ttm=EXCLUDED.net_profit_parent_ttm,
            operating_cashflow_ttm=EXCLUDED.operating_cashflow_ttm,
            ocf_to_net_profit_consolidated_ttm=
                EXCLUDED.ocf_to_net_profit_consolidated_ttm,
            roe_ttm=EXCLUDED.roe_ttm, gross_margin_ttm=EXCLUDED.gross_margin_ttm,
            net_margin_consolidated_ttm=EXCLUDED.net_margin_consolidated_ttm,
            revenue_growth_yoy=EXCLUDED.revenue_growth_yoy,
            profit_growth_yoy=EXCLUDED.profit_growth_yoy,
            ttm_complete=EXCLUDED.ttm_complete,
            cashflow_quality_status=EXCLUDED.cashflow_quality_status,
            roe_quality_status=EXCLUDED.roe_quality_status,
            growth_quality_status=EXCLUDED.growth_quality_status,
            dv_ttm=EXCLUDED.dv_ttm,
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
            "lineage_storage_version": self.LINEAGE_STORAGE_V2
            if is_foundation_schema(schema_version) else "v1",
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
            expected_mode="SET_BASED", estimated_rows=self.SNAPSHOT_DELETE_CHUNK_SIZE,
            allow_delete=True,
        )
        # ``stock_snapshot_daily`` is range-partitioned by research_date.  A
        # snapshot_id-only delete forces PostgreSQL to inspect every yearly
        # partition before the next rebuild and can exceed the governed
        # statement deadline.  Lineage is not partitioned today, but the same
        # date predicate keeps the delete bounded and matches the WorkScope.
        if target == self.LEGACY_LINEAGE_TARGET:
            # Delete by the physical primary key selected from the bounded
            # snapshot/date slice.  This avoids one long DELETE that must
            # remove tens of thousands of index entries and fire a large
            # transition-table trigger in one statement.
            sql = f"""
                WITH victims AS (
                    SELECT lineage_id
                      FROM {target}
                     WHERE snapshot_id=%s AND research_date=%s
                     ORDER BY lineage_id
                     LIMIT %s
                )
                DELETE FROM {target} AS target_rows
                 USING victims
                 WHERE target_rows.lineage_id=victims.lineage_id
            """
            params = (snapshot_id, cutoff, self.SNAPSHOT_DELETE_CHUNK_SIZE)
        elif target == self.BUNDLE_LINEAGE_TARGET:
            # Bundle lineage has one row per stock and is partitioned by
            # research_date.  Delete only a bounded code slice so retries do
            # not create a long transaction or a transition-table spike.
            sql = f"""
                WITH victims AS (
                    SELECT ts_code
                      FROM {target}
                     WHERE snapshot_id=%s AND research_date=%s
                     ORDER BY ts_code
                     LIMIT %s
                )
                DELETE FROM {target} AS target_rows
                 USING victims
                 WHERE target_rows.snapshot_id=%s
                   AND target_rows.research_date=%s
                   AND target_rows.ts_code=victims.ts_code
            """
            params = (
                snapshot_id, cutoff, self.SNAPSHOT_DELETE_CHUNK_SIZE,
                snapshot_id, cutoff,
            )
        elif target == "research.stock_snapshot_daily":
            # The parent is range-partitioned by research_date.  Select only
            # the current partition's business keys, then delete that small
            # key set through the parent so partition pruning remains active.
            sql = f"""
                WITH victims AS (
                    SELECT ts_code
                      FROM {target}
                     WHERE snapshot_id=%s AND research_date=%s
                     ORDER BY ts_code
                     LIMIT %s
                )
                DELETE FROM {target} AS target_rows
                 USING victims
                 WHERE target_rows.snapshot_id=%s
                   AND target_rows.research_date=%s
                   AND target_rows.ts_code=victims.ts_code
            """
            params = (
                snapshot_id, cutoff, self.SNAPSHOT_DELETE_CHUNK_SIZE,
                snapshot_id, cutoff,
            )
        else:
            # snapshot_input is a small, one-row-per-source summary table and
            # does not need chunking, but it remains fully scoped by ID.
            sql = f"DELETE FROM {target} WHERE snapshot_id=%s"
            params = (snapshot_id,)

        total_deleted = 0
        while True:
            deleted = self.kernel.rebuild.run_scoped_merge(
                sql, plan, context, params,
            )
            total_deleted += deleted
            if target == "research.snapshot_input" or deleted < self.SNAPSHOT_DELETE_CHUNK_SIZE:
                return total_deleted

    def snapshot_codes(
        self, snapshot_id: str, cutoff: date, context: RunContext,
    ) -> tuple[str, ...]:
        """Read the existing snapshot code set for a bounded lineage repair.

        The code set is deliberately taken from the already materialized
        snapshot, not regenerated from PIT/Feature sources.  This keeps a
        lineage-format migration from silently changing the snapshot's
        universe.  A small sentinel row detects an unexpected oversized
        snapshot before any bundle write starts.
        """
        rows = self.kernel.read._repository_query(
            context,
            "research.stock_snapshot_daily",
            """SELECT ts_code
                  FROM research.stock_snapshot_daily
                 WHERE snapshot_id=%s AND research_date=%s
                 ORDER BY ts_code
                 LIMIT %s""",
            (snapshot_id, cutoff, self.MAX_SNAPSHOT_CODES + 1),
            self._snapshot_scope(snapshot_id, cutoff),
            max_rows=self.MAX_SNAPSHOT_CODES + 1,
        )
        if len(rows) > self.MAX_SNAPSHOT_CODES:
            raise RuntimeError(
                "snapshot code set exceeds bounded lineage migration limit: "
                f"snapshot_id={snapshot_id}, limit={self.MAX_SNAPSHOT_CODES}"
            )
        codes = tuple(str(row["ts_code"]) for row in rows)
        if len(codes) != len(set(codes)):
            raise RuntimeError(f"snapshot code set contains duplicates: {snapshot_id}")
        return codes

    @staticmethod
    def _is_statement_timeout(exc: BaseException) -> bool:
        """Return true only for the governed PostgreSQL statement timeout."""
        message = str(exc).lower()
        return "statement timeout" in message

    def _run_adaptive_code_operation(
        self,
        codes: Iterable[str],
        operation: Callable[[tuple[str, ...]], int],
        *,
        minimum_batch_size: int | None = None,
    ) -> int:
        """Run one typed code batch, splitting only after statement timeout.

        Every ``operation`` call is a named Gateway operation.  The Gateway
        transaction rolls back before this method receives a timeout, so a
        smaller retry cannot duplicate rows from the timed-out statement.
        Non-timeout errors and batches at the minimum size remain visible to
        the caller instead of being hidden by a retry loop.
        """
        batch = tuple(str(code) for code in codes)
        if not batch:
            return 0
        minimum = (
            self.MIN_ADAPTIVE_CODE_BATCH_SIZE
            if minimum_batch_size is None else int(minimum_batch_size)
        )
        if minimum < 1:
            raise ValueError("minimum_batch_size must be positive")
        if len(batch) <= minimum:
            return int(operation(batch))
        try:
            return int(operation(batch))
        except Exception as exc:
            if not self._is_statement_timeout(exc):
                raise
            midpoint = len(batch) // 2
            left = self._run_adaptive_code_operation(
                batch[:midpoint], operation, minimum_batch_size=minimum,
            )
            right = self._run_adaptive_code_operation(
                batch[midpoint:], operation, minimum_batch_size=minimum,
            )
            return left + right

    def has_legacy_lineage(
        self, snapshot_id: str, cutoff: date, context: RunContext,
    ) -> bool:
        """Return whether a snapshot has an old lineage row.

        This is an existence probe, not a count.  It lets the migration route
        genuinely legacy snapshots to format conversion while routing
        incomplete snapshots that never had legacy lineage to the normal v2
        builder without scanning the large lineage table twice.
        """
        rows = self.kernel.read._repository_query(
            context,
            self.LEGACY_LINEAGE_TARGET,
            """SELECT 1
                  FROM research.snapshot_lineage
                 WHERE snapshot_id=%s AND research_date=%s
                 LIMIT 1""",
            (snapshot_id, cutoff),
            self._snapshot_scope(snapshot_id, cutoff),
            max_rows=2,
        )
        return bool(rows)

    def snapshot_materialization_counts(
        self, snapshot_id: str, cutoff: date, context: RunContext,
    ) -> dict[str, int]:
        """Read bounded child counts for a snapshot without lineage joins."""
        scope = self._snapshot_scope(snapshot_id, cutoff)
        counts: dict[str, int] = {}
        for key, target, sql, params in (
            (
                "stock_rows", "research.stock_snapshot_daily",
                """SELECT COUNT(*) AS value
                      FROM research.stock_snapshot_daily
                     WHERE snapshot_id=%s AND research_date=%s""",
                (snapshot_id, cutoff),
            ),
            (
                "input_rows", "research.snapshot_input",
                """SELECT COUNT(*) AS value
                      FROM research.snapshot_input
                     WHERE snapshot_id=%s""",
                (snapshot_id,),
            ),
            (
                "bundle_rows", "research.snapshot_lineage_bundle",
                """SELECT COUNT(*) AS value
                      FROM research.snapshot_lineage_bundle
                     WHERE snapshot_id=%s AND research_date=%s""",
                (snapshot_id, cutoff),
            ),
        ):
            rows = self.kernel.read._repository_query(
                context, target, sql, params, scope, max_rows=2,
            )
            counts[key] = int(rows[0]["value"] or 0)
        return counts

    def snapshot_materialization_counts_batch(
        self,
        snapshot_dates: Iterable[tuple[str, date]],
        context: RunContext,
    ) -> dict[str, dict[str, int]]:
        """Read child counts for several snapshots with partition pruning.

        Replacement recovery used to issue three queries for every target and
        three more for its immutable source.  That is correct but needlessly
        expensive on a partitioned PostgreSQL warehouse.  This bounded query
        reads the stock/bundle partitions for a small date range and returns
        the same per-snapshot counts, while the caller remains responsible for
        deciding whether a target is complete.
        """
        pairs = [(str(snapshot_id), cutoff) for snapshot_id, cutoff in snapshot_dates]
        if not pairs:
            return {}
        if len(pairs) != len(set(pairs)):
            raise ValueError("snapshot materialization count batches require unique pairs")
        if len(pairs) > self.REPLACEMENT_INTEGRITY_BATCH_SIZE:
            raise ValueError(
                "snapshot materialization count batches are limited to "
                f"{self.REPLACEMENT_INTEGRITY_BATCH_SIZE} pairs"
            )
        if any(not isinstance(cutoff, date) for _snapshot_id, cutoff in pairs):
            raise TypeError("snapshot materialization cutoff must be date values")
        range_start = min(cutoff for _snapshot_id, cutoff in pairs)
        range_end = max(cutoff for _snapshot_id, cutoff in pairs)
        if (range_start.year, range_start.month) != (range_end.year, range_end.month):
            raise ValueError(
                "snapshot materialization count batches must stay within one month"
            )
        rows = self.kernel.read._repository_query(
            context,
            "research.stock_snapshot_daily",
            f"""
            WITH requested AS MATERIALIZED (
                SELECT *
                  FROM unnest(%s::uuid[], %s::date[])
                       AS p(snapshot_id, research_date)
            ), counted AS (
                SELECT 'stock_rows'::text AS kind, s.snapshot_id,
                       COUNT(*)::bigint AS value
                  FROM requested p
                  JOIN research.stock_snapshot_daily s
                    ON s.snapshot_id=p.snapshot_id
                   AND s.research_date=p.research_date
                 WHERE s.research_date BETWEEN DATE '{range_start.isoformat()}'
                                             AND DATE '{range_end.isoformat()}'
                 GROUP BY s.snapshot_id
                UNION ALL
                SELECT 'bundle_rows'::text AS kind, b.snapshot_id,
                       COUNT(*)::bigint AS value
                  FROM requested p
                  JOIN research.snapshot_lineage_bundle b
                    ON b.snapshot_id=p.snapshot_id
                   AND b.research_date=p.research_date
                 WHERE b.research_date BETWEEN DATE '{range_start.isoformat()}'
                                              AND DATE '{range_end.isoformat()}'
                 GROUP BY b.snapshot_id
                UNION ALL
                SELECT 'input_rows'::text AS kind, i.snapshot_id,
                       COUNT(*)::bigint AS value
                  FROM requested p
                  JOIN research.snapshot_input i
                    ON i.snapshot_id=p.snapshot_id
                 GROUP BY i.snapshot_id
            )
            SELECT snapshot_id, kind, value
              FROM counted
            """,
            (
                [snapshot_id for snapshot_id, _cutoff in pairs],
                [cutoff for _snapshot_id, cutoff in pairs],
            ),
            WorkScope(
                date_from=range_start,
                date_to=range_end,
                partition="snapshot:replacement-integrity",
            ),
            max_rows=max(2, len(pairs) * 3 + 1),
        )
        result = {
            snapshot_id: {
                "stock_rows": 0, "bundle_rows": 0, "input_rows": 0,
            }
            for snapshot_id, _cutoff in pairs
        }
        for row in rows:
            snapshot_id = str(row["snapshot_id"])
            if snapshot_id in result and row["kind"] in result[snapshot_id]:
                result[snapshot_id][row["kind"]] = int(row["value"] or 0)
        return result

    def snapshot_manifests(
        self, snapshot_ids: Iterable[str], context: RunContext,
    ) -> dict[str, dict[str, Any]]:
        """Read manifests through bounded primary-key lookups."""
        result: dict[str, dict[str, Any]] = {}
        for ids in snapshot_id_batches(snapshot_ids):
            rows = self.kernel.read._repository_query(
                context,
                "research.snapshot_manifest",
                """
                SELECT snapshot_id, research_cutoff_date AS cutoff,
                       data_baseline_tag AS baseline, schema_version, status,
                       lineage_storage_version, code_version, git_commit,
                       git_dirty, diff_hash, code_archive_path, code_archive_sha256,
                       code_archive_size_bytes AS code_archive_size,
                       quality_report_hash AS quality_hash,
                       baseline_quality_report_hash AS baseline_quality_hash,
                       baseline_source_manifest_hash, input_manifest_hash AS input_hash,
                       quality_report
                  FROM research.snapshot_manifest
                 WHERE snapshot_id=ANY(%s::uuid[])
                """,
                (list(ids),),
                WorkScope(partition="snapshot:replacement-manifests"),
                max_rows=len(ids) + 1,
            )
            result.update({str(row["snapshot_id"]): row for row in rows})
        return result

    def materialize_legacy_lineage_v2(
        self,
        snapshot_ids: Iterable[str],
        context: RunContext,
    ) -> dict[str, int]:
        """Migrate legacy lineage to v2 without rebuilding snapshot rows.

        Each snapshot has its own lifecycle transition and each code slice is
        a separate short Gateway transaction.  The existing stock snapshot
        and input manifest are never deleted or updated; acceptance remains
        responsible for proving that they are complete before VALIDATED is
        restored.
        """
        ids = require_snapshot_batch(snapshot_ids, "legacy lineage migration")
        if not ids:
            return {}

        results: dict[str, int] = {}
        sql = self.legacy_lineage_bundle_sql()
        for snapshot_id in ids:
            manifest = self.manifest(snapshot_id, context)
            if manifest["status"] not in {"DRAFT", "FAILED"}:
                raise RuntimeError(
                    "legacy lineage migration accepts only DRAFT/FAILED snapshots: "
                    f"{snapshot_id} ({manifest['status']})"
                )
            if not is_foundation_schema(manifest["schema_version"]):
                raise RuntimeError(
                    "legacy lineage v2 migration requires a Foundation snapshot: "
                    f"{snapshot_id} ({manifest['schema_version']})"
                )
            if manifest.get("lineage_storage_version") != self.LINEAGE_STORAGE_V2:
                raise RuntimeError(
                    "legacy lineage v2 migration requires the manifest to be "
                    f"prepared as v2: {snapshot_id}"
                )

            cutoff = manifest["cutoff"]
            snapshot_scope = self._snapshot_scope(snapshot_id, cutoff)
            with self._snapshot_build_locks((snapshot_id,), context) as started:
                try:
                    changed = self.kernel.write.transition_snapshot_manifest(
                        context,
                        snapshot_scope,
                        snapshot_id,
                        ("DRAFT", "FAILED"),
                        "BUILDING",
                        {"failure_reason": None},
                    )
                except Exception:
                    # A commit can succeed even if its response is lost.
                    started.append((snapshot_id, cutoff))
                    raise
                if changed != 1:
                    raise RuntimeError(
                        "snapshot lifecycle changed before legacy lineage migration: "
                        f"{snapshot_id}"
                    )
                started.append((snapshot_id, cutoff))

                # A previous interrupted v2 attempt may have left a partial
                # bundle.  Clear only that physical representation.  The
                # legacy lineage source, stock snapshot and snapshot_input
                # remain untouched for audit and recovery.
                self._delete_snapshot_rows(
                    snapshot_id, context, self.BUNDLE_LINEAGE_TARGET, cutoff,
                )
                codes = self.snapshot_codes(snapshot_id, cutoff, context)
                if not codes:
                    raise RuntimeError(
                        "cannot migrate lineage for an empty stock snapshot: "
                        f"{snapshot_id}/{cutoff}"
                    )

                written = 0
                for offset in range(0, len(codes), self.LEGACY_LINEAGE_CODE_BATCH_SIZE):
                    code_batch = codes[
                        offset:offset + self.LEGACY_LINEAGE_CODE_BATCH_SIZE
                    ]
                    batch_scope = WorkScope(
                        date_from=cutoff,
                        date_to=cutoff,
                        codes=code_batch,
                        partition=f"{snapshot_id}:lineage-v2-legacy",
                        dimensions={"snapshot_id": snapshot_id},
                    )
                    plan = SqlPlan(
                        operation=Operation.REBUILD,
                        target=self.BUNDLE_LINEAGE_TARGET,
                        scope=batch_scope,
                        expected_mode="SET_BASED",
                        estimated_rows=len(code_batch),
                        allow_insert_select=True,
                    )
                    affected = self.kernel.rebuild.run_snapshot_lineage_bundle_from_legacy(
                        sql,
                        plan,
                        context,
                        (snapshot_id, cutoff, list(code_batch)),
                    )
                    if affected != len(code_batch):
                        raise RuntimeError(
                            "legacy lineage bundle row count mismatch: "
                            f"snapshot_id={snapshot_id}, expected={len(code_batch)}, "
                            f"written={affected}"
                        )
                    written += affected
                results[snapshot_id] = written
        return results

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

    @staticmethod
    def foundation_financial_contract_sql() -> str:
        """Return the bounded Foundation-v2 financial identity check.

        Foundation snapshots may retain compatibility fields from V1, but the
        financial field group consumed by Foundation-v2 must resolve to the
        V2 feature record that was actually selected.  This check is kept in
        the Repository because it joins the frozen snapshot and lineage
        tables; callers only receive aggregate issues, never SQL or cursors.
        """
        return """
        WITH requested AS (
            SELECT *
              FROM unnest(%s::uuid[], %s::date[])
                   AS p(snapshot_id, research_date)
        ), base AS MATERIALIZED (
            SELECT b.snapshot_id, b.research_date, b.ts_code,
                   b.financial_source_ref_id,
                   s.financial_record_version, s.financial_source_record_key,
                   s.financial_payload_hash, s.financial_source_fingerprint,
                   s.financial_feature_contract_version,
                   s.financial_data_available_timestamp
              FROM requested p
              JOIN research.snapshot_lineage_bundle b
                ON b.snapshot_id=p.snapshot_id
               AND b.research_date=p.research_date
               AND b.research_date BETWEEN %s AND %s
              JOIN research.stock_snapshot_daily s
                ON s.snapshot_id=b.snapshot_id
               AND s.research_date=b.research_date
               AND s.ts_code=b.ts_code
               AND s.research_date BETWEEN %s AND %s
        ), rows AS (
            SELECT b.snapshot_id, b.research_date, b.ts_code,
                   b.financial_source_ref_id,
                   d.source_ref_id, d.source_table, d.source_record_key,
                   d.source_record_version, d.source_payload_hash,
                   b.financial_record_version, b.financial_source_record_key,
                   b.financial_payload_hash, b.financial_source_fingerprint,
                   b.financial_feature_contract_version,
                   b.financial_data_available_timestamp
              FROM base b
              LEFT JOIN research.lineage_source_record d
                ON d.source_ref_id=b.financial_source_ref_id
        )
        SELECT snapshot_id,
               COUNT(*)::integer AS snapshot_rows,
               COUNT(*) FILTER (
                   WHERE financial_source_ref_id IS NULL OR source_ref_id IS NULL
               )::integer AS missing_lineage_rows,
               COUNT(*) FILTER (
                   WHERE source_record_key IS NOT NULL
                     AND source_record_key NOT LIKE '%%:missing'
                     AND source_table IS DISTINCT FROM 'feature.company_financial_v2'
               )::integer AS non_v2_source_rows,
               COUNT(*) FILTER (
                   WHERE source_table='feature.company_financial_v2'
                     AND source_record_key NOT LIKE '%%:missing'
                     AND (
                         financial_record_version IS NULL
                         OR financial_source_record_key IS DISTINCT FROM source_record_key
                         OR financial_payload_hash IS DISTINCT FROM source_payload_hash
                         OR financial_source_fingerprint IS DISTINCT FROM source_payload_hash
                         OR financial_feature_contract_version IS DISTINCT FROM 'feature-contract-v2.0'
                         OR financial_data_available_timestamp IS NULL
                     )
               )::integer AS invalid_v2_rows,
               COUNT(*) FILTER (
                   WHERE source_table='feature.company_financial_v2'
                     AND source_record_key NOT LIKE '%%:missing'
                     AND financial_data_available_timestamp >=
                         (research_date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'
               )::integer AS future_financial_availability_rows
          FROM rows
         GROUP BY snapshot_id
        """

    def foundation_financial_contract_issues(
        self, manifests: list[dict[str, Any]], context: RunContext,
    ) -> dict[str, dict[str, int]]:
        """Validate V2 financial source identity for a bounded snapshot batch."""
        if not manifests or not all(
            is_foundation_schema(str(item.get("schema_version")))
            for item in manifests
        ):
            return {}
        issues: dict[str, dict[str, int]] = {}
        # ``snapshot_lineage_bundle`` is monthly range-partitioned and its
        # primary index starts with research_date. Bind parameters inside a
        # named cursor can leave PostgreSQL with a generic plan, which scans
        # every historical partition before applying the requested dates.
        # Embedding the validated date range for each small batch makes
        # partition pruning deterministic while snapshot IDs and all business
        # values remain bound parameters. More importantly, it prevents one
        # Dataset handoff from holding the source-registry join until
        # statement_timeout; callers still receive a complete issue map.
        for offset in range(
            0, len(manifests), self.FOUNDATION_FINANCIAL_CONTRACT_BATCH_SIZE
        ):
            batch = manifests[
                offset:offset + self.FOUNDATION_FINANCIAL_CONTRACT_BATCH_SIZE
            ]
            snapshot_ids = [str(item["snapshot_id"]) for item in batch]
            cutoffs = [item["cutoff"] for item in batch]
            range_start = min(cutoffs)
            range_end = max(cutoffs)
            if not isinstance(range_start, date) or not isinstance(range_end, date):
                raise TypeError(
                    "Foundation financial contract cutoff must be date values"
                )
            contract_sql = self.foundation_financial_contract_sql().replace(
                "b.research_date BETWEEN %s AND %s",
                f"b.research_date BETWEEN DATE '{range_start.isoformat()}' "
                f"AND DATE '{range_end.isoformat()}'",
                1,
            )
            contract_sql = contract_sql.replace(
                "s.research_date BETWEEN %s AND %s",
                f"s.research_date BETWEEN DATE '{range_start.isoformat()}' "
                f"AND DATE '{range_end.isoformat()}'",
                1,
            )
            rows = self.kernel.read._repository_query(
                context,
                self.BUNDLE_LINEAGE_TARGET,
                contract_sql,
                (snapshot_ids, cutoffs),
                WorkScope(
                    date_from=range_start, date_to=range_end,
                    partition=(
                        "snapshot-foundation-financial-contract-"
                        f"{offset // self.FOUNDATION_FINANCIAL_CONTRACT_BATCH_SIZE}"
                    ),
                ),
                max_rows=max(2, len(snapshot_ids) + 1),
            )
            returned_snapshot_ids: set[str] = set()
            for row in rows:
                snapshot_id = str(row["snapshot_id"])
                returned_snapshot_ids.add(snapshot_id)
                values = {
                    key: int(row.get(key) or 0)
                    for key in (
                        "snapshot_rows", "missing_lineage_rows",
                        "non_v2_source_rows", "invalid_v2_rows",
                        "future_financial_availability_rows",
                    )
                }
                if any(values[key] for key in values if key != "snapshot_rows"):
                    issues[snapshot_id] = values
            for snapshot_id in snapshot_ids:
                if snapshot_id not in returned_snapshot_ids:
                    issues[snapshot_id] = {
                        "snapshot_rows": 0,
                        "missing_snapshot_rows": 1,
                        "missing_lineage_rows": 0,
                        "non_v2_source_rows": 0,
                        "invalid_v2_rows": 0,
                        "future_financial_availability_rows": 0,
                    }
        return issues

    def build_financial_replacement_snapshot(
        self,
        snapshot_id: str,
        source_snapshot_id: str,
        context: RunContext,
    ) -> dict[str, int]:
        """Build a V2.1 replacement by reusing an immutable frozen Snapshot.

        The source Snapshot is known to have correct non-financial fields; the
        repair only changes the financial V2 projection and its compact
        lineage. Input manifests are copied verbatim because the source
        Snapshot already records the exact baseline-bounded inputs. This is a
        set-based replacement operation, not a generic full Snapshot rebuild.
        """
        target = self.manifest(snapshot_id, context)
        source = self.manifest(source_snapshot_id, context)
        if target["status"] not in {"DRAFT", "FAILED"}:
            raise RuntimeError(
                f"replacement target must be DRAFT/FAILED: {snapshot_id} ({target['status']})"
            )
        if source["status"] != "FROZEN":
            raise RuntimeError(
                f"replacement source must be FROZEN: {source_snapshot_id} ({source['status']})"
            )
        if not is_foundation_schema(target["schema_version"]):
            raise RuntimeError(
                f"replacement target must be Foundation schema: {target['schema_version']}"
            )
        if target["cutoff"] != source["cutoff"] or target["baseline"] != source["baseline"]:
            raise RuntimeError("replacement target/source cutoff or baseline mismatch")
        cutoff = target["cutoff"]
        scope = self._snapshot_scope(snapshot_id, cutoff)
        if self.lineage_target(target) != self.BUNDLE_LINEAGE_TARGET:
            raise RuntimeError("Foundation replacement requires compact lineage v2")
        try:
            with self._snapshot_build_locks((snapshot_id,), context) as started:
                try:
                    changed = self.kernel.write.transition_snapshot_manifest(
                        context, scope, snapshot_id, ("DRAFT", "FAILED"), "BUILDING",
                        {"failure_reason": None},
                    )
                except Exception:
                    started.append((str(snapshot_id), cutoff))
                    raise
                if changed != 1:
                    raise RuntimeError("replacement Snapshot lifecycle changed before build")
                started.append((str(snapshot_id), cutoff))
                for child in (
                    self.BUNDLE_LINEAGE_TARGET,
                    "research.stock_snapshot_daily",
                    "research.snapshot_input",
                ):
                    self._delete_snapshot_rows(snapshot_id, context, child, cutoff)

                codes = self.snapshot_codes(source_snapshot_id, cutoff, context)
                if not codes:
                    raise RuntimeError("replacement source has no stock rows")
                columns = ", ".join(self.REPLACEMENT_STOCK_COLUMNS)
                source_columns = ", ".join(
                    f"s.{column}" for column in self.REPLACEMENT_STOCK_COLUMNS
                )
                clone_stock_sql = f"""
                    INSERT INTO research.stock_snapshot_daily
                        (snapshot_id, {columns})
                    SELECT %s, {source_columns}
                      FROM research.stock_snapshot_daily s
                     WHERE s.snapshot_id=%s
                       AND s.research_date=%s
                       AND s.ts_code=ANY(%s)
                """
                clone_plan = SqlPlan(
                    operation=Operation.REBUILD,
                    target="research.stock_snapshot_daily",
                    scope=scope,
                    expected_mode="SET_BASED",
                    allow_insert_select=True,
                    estimated_rows=self.SNAPSHOT_CODE_BATCH_SIZE,
                )
                cloned = 0
                for offset in range(0, len(codes), self.SNAPSHOT_CODE_BATCH_SIZE):
                    code_batch = codes[offset:offset + self.SNAPSHOT_CODE_BATCH_SIZE]
                    cloned += self.kernel.rebuild.run_insert_select(
                        clone_stock_sql, clone_plan, context,
                        (snapshot_id, source_snapshot_id, cutoff, list(code_batch)),
                    )
                if cloned != len(codes):
                    raise RuntimeError(
                        "replacement stock row count mismatch: "
                        f"expected={len(codes)}, cloned={cloned}"
                    )

                clone_input_sql = """
                    INSERT INTO research.snapshot_input
                        (snapshot_id, schema_name, table_name,
                         selection_predicate, selected_columns, hash_algorithm,
                         row_count, min_date, max_date, content_hash)
                    SELECT %s, schema_name, table_name,
                           selection_predicate, selected_columns, hash_algorithm,
                           row_count, min_date, max_date, content_hash
                      FROM research.snapshot_input
                     WHERE snapshot_id=%s
                """
                input_plan = SqlPlan(
                    operation=Operation.REBUILD,
                    target="research.snapshot_input",
                    scope=scope,
                    expected_mode="SET_BASED",
                    allow_insert_select=True,
                )
                copied_inputs = self.kernel.rebuild.run_insert_select(
                    clone_input_sql, input_plan, context,
                    (snapshot_id, source_snapshot_id),
                )
                if copied_inputs <= 0 or not source["input_hash"]:
                    raise RuntimeError("replacement input manifest is empty or unhashed")

                foundation_sql = self.foundation_snapshot_sql().replace(
                    "WHERE s.snapshot_id=%s",
                    "WHERE s.snapshot_id=%s AND s.research_date=%s "
                    "AND s.ts_code=ANY(%s)",
                    1,
                )
                foundation_plan = SqlPlan(
                    operation=Operation.REBUILD,
                    target="research.stock_snapshot_daily",
                    scope=scope,
                    expected_mode="SET_BASED",
                    estimated_rows=self.SNAPSHOT_CODE_BATCH_SIZE,
                )
                enriched = 0
                for offset in range(0, len(codes), self.SNAPSHOT_CODE_BATCH_SIZE):
                    code_batch = codes[offset:offset + self.SNAPSHOT_CODE_BATCH_SIZE]
                    enriched += self._run_adaptive_code_operation(
                        code_batch,
                        lambda batch: self.kernel.rebuild.run_scoped_merge(
                            foundation_sql, foundation_plan, context,
                            (snapshot_id, snapshot_id, cutoff, list(batch)),
                        ),
                    )

                lineage_sql = self.foundation_lineage_clone_sql()
                bundled = 0
                for offset in range(
                    0, len(codes), self.REPLACEMENT_LINEAGE_CODE_BATCH_SIZE,
                ):
                    code_batch = codes[
                        offset:offset + self.REPLACEMENT_LINEAGE_CODE_BATCH_SIZE
                    ]
                    lineage_scope = WorkScope(
                        date_from=cutoff, date_to=cutoff,
                        codes=tuple(code_batch), partition=str(snapshot_id),
                        dimensions={"snapshot_id": str(snapshot_id),
                                    "source_snapshot_id": str(source_snapshot_id)},
                    )
                    lineage_plan = SqlPlan(
                        operation=Operation.REBUILD,
                        target=self.BUNDLE_LINEAGE_TARGET,
                        scope=lineage_scope,
                        expected_mode="SET_BASED",
                        allow_insert_select=True,
                        estimated_rows=len(code_batch),
                    )
                    bundled += self._run_adaptive_code_operation(
                        code_batch,
                        lambda batch: self.kernel.rebuild.run_snapshot_lineage_bundle(
                            lineage_sql, lineage_plan, context,
                            (snapshot_id, source_snapshot_id, cutoff, list(batch)),
                        ),
                    )
                changed = self.kernel.write.transition_snapshot_manifest(
                    context, scope, snapshot_id, ("BUILDING",), "BUILDING",
                    {"input_manifest_hash": source["input_hash"]},
                )
                if changed != 1:
                    raise RuntimeError("replacement input hash update failed")
                return {
                    "stock_rows": cloned,
                    "input_rows": copied_inputs,
                    "financial_rows": enriched,
                    "lineage_rows": bundled,
                }
        except Exception:
            raise

    def build_snapshot(
        self,
        snapshot_id: str,
        manifest: dict[str, Any],
        baseline: ResearchBaseline,
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
        lineage_target = self.lineage_target(manifest)
        try:
            stats, input_manifest_hash = self._precompute_input_manifest(
                definitions, cutoff, baseline.source_manifest, context, row_bytes, manifest_hash,
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
            with self._snapshot_build_locks((snapshot_id,), context) as started:
                try:
                    changed = self.kernel.write.transition_snapshot_manifest(
                        context, scope, snapshot_id, ("DRAFT", "FAILED"), "BUILDING",
                        {
                            "quality_report_hash": quality_hash,
                            "quality_report": json.dumps(
                                quality_report, ensure_ascii=False, default=str,
                            ),
                            "code_version": metadata["code_version"],
                            "git_commit": metadata["commit"],
                            "git_dirty": metadata["dirty"], "diff_hash": metadata["diff_hash"],
                            "failure_reason": None,
                        },
                    )
                except Exception:
                    started.append((str(snapshot_id), cutoff))
                    raise
                if changed != 1:
                    raise RuntimeError("Snapshot 状态在构建期间发生变化")
                started.append((str(snapshot_id), cutoff))
                for target in (
                    lineage_target, "research.stock_snapshot_daily", "research.snapshot_input",
                ):
                    self._delete_snapshot_rows(snapshot_id, context, target, cutoff)
                high_water = baseline.source_manifest["append_only_sources"]["tushare_stock_basic_raw_version"]["max_record_id"]
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
                    written += self._run_adaptive_code_operation(
                        code_batch,
                        lambda batch: self.kernel.rebuild.run_insert_select(
                            stock_sql, stock_plan, context,
                            (snapshot_id, high_water, cutoff, list(batch)),
                        ),
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
                        self._run_adaptive_code_operation(
                            code_batch,
                            lambda batch: self.kernel.rebuild.run_scoped_merge(
                                foundation_sql, foundation_plan, context,
                                (snapshot_id, snapshot_id, cutoff, list(batch)),
                            ),
                        )
                lineage_plan = SqlPlan(
                    operation=Operation.REBUILD, target=lineage_target, scope=scope,
                    expected_mode="SET_BASED", allow_insert_select=True,
                    estimated_rows=self.LINEAGE_CODE_BATCH_SIZE,
                )
                lineage_builder = (
                    self.lineage_bundle_sql
                    if lineage_target == self.BUNDLE_LINEAGE_TARGET
                    else self.lineage_sql
                )
                lineage_sql = lineage_builder(
                    foundation=is_foundation_schema(manifest["schema_version"])
                ).replace(
                    "WITH params AS (SELECT %s::uuid AS snapshot_id),",
                    "WITH params AS (SELECT %s::uuid AS snapshot_id, %s::date AS research_date, %s::text[] AS codes),",
                    1,
                ).replace(
                    "WHERE s.snapshot_id=(SELECT snapshot_id FROM params)",
                    "WHERE s.snapshot_id=(SELECT snapshot_id FROM params) "
                    "AND s.research_date=(SELECT research_date FROM params) "
                    "AND s.ts_code IN (SELECT unnest(codes) FROM params)",
                )
                for offset in range(0, len(codes), self.LINEAGE_CODE_BATCH_SIZE):
                    code_batch = codes[offset:offset + self.LINEAGE_CODE_BATCH_SIZE]
                    if lineage_target == self.BUNDLE_LINEAGE_TARGET:
                        self._run_adaptive_code_operation(
                            code_batch,
                            lambda batch: self.kernel.rebuild.run_snapshot_lineage_bundle(
                                lineage_sql, lineage_plan, context,
                                (snapshot_id, cutoff, list(batch)),
                            ),
                        )
                    else:
                        self._run_adaptive_code_operation(
                            code_batch,
                            lambda batch: self.kernel.rebuild.run_insert_select(
                                lineage_sql, lineage_plan, context,
                                (snapshot_id, cutoff, list(batch)),
                            ),
                        )
                self.kernel.write.bulk_insert(context, scope, "research.snapshot_input", input_rows)
                changed = self.kernel.write.transition_snapshot_manifest(
                    context, scope, snapshot_id, ("BUILDING",), "BUILDING",
                    {"input_manifest_hash": input_manifest_hash},
                )
                if changed != 1:
                    raise RuntimeError("Snapshot 输入 Manifest 更新失败")
        except Exception:
            raise

    def build_snapshots_batch(
        self,
        manifests: list[dict[str, Any]],
        baseline: ResearchBaseline,
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
        if len(manifests) > MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE:
            raise ValueError(
                "snapshot build batches are limited to "
                f"{MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE}"
            )
        ids = tuple(str(item["snapshot_id"]) for item in manifests)
        cutoffs = tuple(item["cutoff"] for item in manifests)
        if len(set(ids)) != len(ids) or len(set(cutoffs)) != len(cutoffs):
            raise ValueError("snapshot build batch must contain unique ids and cutoff dates")
        baseline_tags = {item["baseline"] for item in manifests}
        schema_versions = {item["schema_version"] for item in manifests}
        if len(baseline_tags) != 1 or len(schema_versions) != 1:
            raise ValueError("同一批次必须使用同一 baseline_tag/schema_version")
        schema_version = next(iter(schema_versions))
        lineage_targets = {self.lineage_target(item) for item in manifests}
        if len(lineage_targets) != 1:
            raise ValueError("snapshot build batch must use one lineage storage version")
        lineage_target = next(iter(lineage_targets))

        prepared: list[dict[str, Any]] = []
        for manifest in manifests:
            snapshot_id = str(manifest["snapshot_id"])
            if manifest["status"] not in {"DRAFT", "FAILED"}:
                raise RuntimeError(
                    f"批量构建只允许 DRAFT/FAILED Snapshot: {snapshot_id} ({manifest['status']})"
                )
            if (
                baseline.quality_report_hash != manifest["baseline_quality_hash"]
                or baseline.source_manifest_hash != manifest["baseline_source_manifest_hash"]
            ):
                raise RuntimeError("数据基线 Manifest 已变化，拒绝批量构建")
            report, quality_hash = quality_reports.get(snapshot_id, (None, None))
            if report is None or quality_hash is None:
                raise RuntimeError(f"批量构建缺少质量证据: {snapshot_id}")
            if report["failures"]:
                raise RuntimeError(f"质量门禁未通过: {manifest['cutoff']} {report['failures']}")
            stats, input_hash = self._precompute_input_manifest(
                definitions, manifest["cutoff"], baseline.source_manifest, context, row_bytes, manifest_hash,
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
        started: list[tuple[str, date]] = []
        try:
            with self._snapshot_build_locks(ids, context) as started:
                for item in prepared:
                    manifest = item["manifest"]
                    snapshot_id = str(manifest["snapshot_id"])
                    scope = self._snapshot_scope(snapshot_id, manifest["cutoff"])
                    try:
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
                    except Exception:
                        started.append((snapshot_id, manifest["cutoff"]))
                        raise
                    if changed != 1:
                        raise RuntimeError(f"Snapshot 状态在构建期间发生变化: {snapshot_id}")
                    started.append((snapshot_id, manifest["cutoff"]))
                    for target in (
                        lineage_target,
                        "research.stock_snapshot_daily",
                        "research.snapshot_input",
                    ):
                        self._delete_snapshot_rows(snapshot_id, context, target, manifest["cutoff"])

                high_water = baseline.source_manifest["append_only_sources"][
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
                    written += self._run_adaptive_code_operation(
                        code_batch,
                        lambda batch: self.kernel.rebuild.run_insert_select(
                            stock_sql, stock_plan, context,
                            (high_water, list(ids), list(batch)),
                        ),
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
                        self._run_adaptive_code_operation(
                            code_batch,
                            lambda batch: self.kernel.rebuild.run_scoped_merge(
                                foundation_sql, foundation_plan, context,
                                (list(ids), list(cutoffs), list(batch)),
                            ),
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
                        target=lineage_target,
                        scope=self._snapshot_scope(snapshot_id, cutoff),
                        expected_mode="SET_BASED",
                        allow_insert_select=True,
                        estimated_rows=self.LINEAGE_CODE_BATCH_SIZE,
                    )
                    lineage_builder = (
                        self.lineage_bundle_sql
                        if lineage_target == self.BUNDLE_LINEAGE_TARGET
                        else self.lineage_sql
                    )
                    lineage_sql = lineage_builder(
                        foundation=is_foundation_schema(schema_version)
                    ).replace(
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
                    for offset in range(0, len(codes), self.LINEAGE_CODE_BATCH_SIZE):
                        code_batch = codes[offset:offset + self.LINEAGE_CODE_BATCH_SIZE]
                        if lineage_target == self.BUNDLE_LINEAGE_TARGET:
                            self._run_adaptive_code_operation(
                                code_batch,
                                lambda batch: self.kernel.rebuild.run_snapshot_lineage_bundle(
                                    lineage_sql, lineage_plan, context,
                                    (snapshot_id, cutoff, list(batch)),
                                ),
                            )
                        else:
                            self._run_adaptive_code_operation(
                                code_batch,
                                lambda batch: self.kernel.rebuild.run_insert_select(
                                    lineage_sql, lineage_plan, context,
                                    (snapshot_id, cutoff, list(batch)),
                                ),
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
        except Exception:
            raise

    def validate_snapshot(self, manifest: dict[str, Any], baseline: ResearchBaseline, quality_hash: str,
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
        if source == "feature.company_financial_v1":
            # The base materialization selects one latest PIT-visible V1 row
            # per stock.  Hash that exact selected set rather than every
            # historical V1 row, otherwise each snapshot would perform a
            # much larger and semantically different input scan.
            batches = self._paged_input_rows(
                context=context,
                source=source,
                sql=(
                    f'SELECT DISTINCT ON ("ts_code") {selected} FROM {source} '
                    'WHERE "version_available_date" IS NOT NULL '
                    'AND "version_available_date" <= %s '
                    'AND "ts_code" > %s '
                    'ORDER BY "ts_code", "report_date" DESC, '
                    '"version_available_date" DESC, "record_version" DESC, '
                    '"feature_id" DESC LIMIT %s'
                ),
                params_prefix=(cutoff,),
                page_rows=self.INPUT_STATS_PAGE_ROWS,
                cutoff=cutoff,
                scope_partition=f"snapshot-input:{source}",
            )
        elif keyset_column:
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
    ) -> ResearchBaseline:
        rows = self.kernel.read._repository_query(
            context,
            "ops.tushare_data_release",
            """SELECT release_tag, baseline_domain, baseline_cutoff_date, status,
                       quality_report_hash, source_manifest_hash,
                       source_manifest, schema_version, quality_report
                  FROM ops.tushare_data_release WHERE release_tag=%s""",
            (tag,), WorkScope(date_to=cutoff, partition=f"release:{tag}"), 2,
        )
        if not rows or rows[0]["status"] != "validated" or rows[0]["baseline_domain"] != "RESEARCH":
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
        return ResearchBaseline(
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
        result = {}
        dates = tuple(dict.fromkeys(cutoffs))
        for offset in range(0, len(dates), MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE):
            batch = dates[offset:offset + MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE]
            rows = self.kernel.read._repository_query(
                context, "research.snapshot_manifest",
                """SELECT snapshot_id, research_cutoff_date, status
                     FROM research.snapshot_manifest
                    WHERE data_baseline_tag=%s AND schema_version=%s
                      AND research_cutoff_date=ANY(%s::date[])""",
                (baseline_tag, schema_version, list(batch)),
                WorkScope(
                    date_from=min(batch), date_to=max(batch),
                    partition="snapshot-manifest-batch",
                ),
                max_rows=len(batch) + 1,
            )
            result.update({
                row["research_cutoff_date"]: (str(row["snapshot_id"]), row["status"])
                for row in rows
            })
        return result

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
                       lineage_storage_version,
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
        if len(cutoffs) > MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE:
            raise ValueError(
                "snapshot price vintage batches are limited to "
                f"{MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE}"
            )
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

    @staticmethod
    def _input_definition_for_stored_rows(stored_rows, definitions):
        """Return the explicit input contract represented by stored rows.

        Foundation-v2 was first materialized before the V1 compatibility
        source was registered as a manifest dependency.  Those snapshots have
        a valid ten-input manifest; current builds intentionally have eleven
        inputs.  This is a narrowly defined historical compatibility rule,
        not permission to accept an arbitrary or incomplete manifest.
        """
        current = tuple(definitions)
        current_keys = tuple((item["schema"], item["table"]) for item in current)
        stored_keys = tuple(
            (row["schema_name"], row["table_name"])
            for row in stored_rows
        )
        variants = [(current_keys, current)]
        compatibility_key = ("feature", "company_financial_v1")
        if compatibility_key in current_keys:
            legacy = tuple(
                item for item in current
                if (item["schema"], item["table"]) != compatibility_key
            )
            variants.append(
                (tuple((item["schema"], item["table"]) for item in legacy), legacy)
            )
        stored_key_set = set(stored_keys)
        for expected_keys, candidate in variants:
            if len(stored_rows) == len(candidate) and stored_key_set == set(expected_keys):
                return candidate
        return None

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
        foundation_financial = self.foundation_financial_contract_issues(
            [manifest], context,
        ).get(str(snapshot_id))
        if foundation_financial:
            issues.append({"foundation_financial_contract": foundation_financial})
        expected = self.kernel.read._repository_query(
            context, "pit.universe_daily", "SELECT COUNT(*) AS value FROM pit.universe_daily WHERE trade_date=%s", (cutoff,), scope, 2,
        )[0]["value"]
        if rows["row_count"] != expected:
            issues.append({"snapshot_universe_row_mismatch": {"snapshot_rows": rows["row_count"], "universe_rows": expected}})
        lineage_target = self.lineage_target(manifest)
        if lineage_target == self.BUNDLE_LINEAGE_TARGET:
            bundle_rows = self.kernel.read._repository_query(
                context, lineage_target,
                """SELECT COUNT(*) AS value
                     FROM research.snapshot_lineage_bundle
                    WHERE snapshot_id=%s AND research_date=%s""",
                (snapshot_id, cutoff), scope, 2,
            )[0]["value"]
            if bundle_rows != rows["row_count"]:
                issues.append({
                    "snapshot_lineage_row_mismatch": {
                        "snapshot_rows": rows["row_count"],
                        "lineage_bundle_rows": bundle_rows,
                    }
                })
            incomplete = self.kernel.read._repository_query(
                context, lineage_target,
                """WITH refs AS (
                         SELECT b.snapshot_id, b.research_date, b.ts_code,
                                v.field_group, v.source_ref_id
                           FROM research.snapshot_lineage_bundle b
                           CROSS JOIN LATERAL (VALUES
                             ('universe', b.universe_source_ref_id),
                             ('tradeability', b.tradeability_source_ref_id),
                             ('price', b.price_source_ref_id),
                             ('valuation', b.valuation_source_ref_id),
                             ('financial', b.financial_source_ref_id),
                             ('shareholder_return', b.shareholder_return_source_ref_id),
                             ('lifecycle', b.lifecycle_source_ref_id),
                             ('instrument', b.instrument_source_ref_id),
                             ('market_facts', b.market_facts_source_ref_id),
                             ('fundamental_history', b.fundamental_history_source_ref_id)
                           ) v(field_group, source_ref_id)
                          WHERE b.snapshot_id=%s AND b.research_date=%s
                       )
                       SELECT r.field_group, COUNT(*) AS value
                         FROM refs r
                         JOIN research.lineage_source_record d
                           ON d.source_ref_id=r.source_ref_id
                        WHERE d.source_record_key NOT LIKE '%%:missing'
                          AND (d.source_record_version IS NULL OR d.source_payload_hash IS NULL)
                        GROUP BY r.field_group ORDER BY r.field_group""",
                (snapshot_id, cutoff), scope, 100,
            )
        else:
            missing = self.kernel.read._repository_query(
                context, lineage_target,
                """SELECT COUNT(*) AS value FROM (
                    SELECT snapshot_id,research_date,ts_code
                      FROM research.snapshot_lineage
                     WHERE snapshot_id=%s AND research_date=%s
                     GROUP BY snapshot_id,research_date,ts_code
                    HAVING COUNT(DISTINCT field_group)<>%s) x""",
                (snapshot_id, cutoff, required_lineage_groups), scope, 2,
            )[0]["value"]
            if missing:
                issues.append({"rows_with_incomplete_lineage": missing})
            incomplete = self.kernel.read._repository_query(
                context, lineage_target,
                """SELECT field_group, COUNT(*) AS value FROM research.snapshot_lineage
                     WHERE snapshot_id=%s AND research_date=%s
                       AND source_record_key NOT LIKE '%%:missing'
                       AND (source_record_version IS NULL OR source_payload_hash IS NULL)
                     GROUP BY field_group ORDER BY field_group""",
                (snapshot_id, cutoff), scope, 100,
            )
        if incomplete:
            issues.append({"lineage_refs_without_version_or_hash": [
                (row["field_group"], row["value"]) for row in incomplete
            ]})
        stored_inputs = self.kernel.read._repository_query(
            context, "research.snapshot_input",
            """SELECT schema_name, table_name, selection_predicate, selected_columns,
                              hash_algorithm, row_count, min_date, max_date, content_hash
                         FROM research.snapshot_input
                        WHERE snapshot_id=%s
                        ORDER BY schema_name, table_name""",
            (snapshot_id,), scope, max(2, len(definitions) + 1),
        )
        stored_by_key = {(row["schema_name"], row["table_name"]): row for row in stored_inputs}
        expected_definitions = self._input_definition_for_stored_rows(
            stored_inputs, definitions,
        )
        if expected_definitions is None:
            expected_keys = [(item["schema"], item["table"]) for item in definitions]
            issues.append({
                "snapshot_input_manifest_incomplete": {
                    "expected": expected_keys, "stored": sorted(stored_by_key),
                }
            })
        else:
            expected_keys = [
                (item["schema"], item["table"]) for item in expected_definitions
            ]
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

    def check_legacy_lineage_v2(
        self, manifest: dict[str, Any], context: RunContext,
    ) -> list[dict[str, Any]]:
        """Validate a format-converted snapshot without current input drift.

        A legacy snapshot may have a valid historical input manifest that is
        smaller than today's input definition (for example, before the V1
        compatibility source was registered).  The migration must therefore
        validate the representation conversion itself: the existing stock
        code set and ten legacy groups must map to one complete bundle row per
        stock, with every dictionary reference resolvable.  It must not ask
        the current snapshot builder to reinterpret or rewrite that manifest.
        """
        snapshot_id = str(manifest["snapshot_id"])
        cutoff = manifest["cutoff"]
        scope = WorkScope(
            date_from=cutoff, date_to=cutoff,
            partition=f"legacy-lineage-check:{snapshot_id}",
        )
        issues: list[dict[str, Any]] = []

        stock_rows = int(self.kernel.read._repository_query(
            context,
            "research.stock_snapshot_daily",
            """SELECT COUNT(*) AS value
                  FROM research.stock_snapshot_daily
                 WHERE snapshot_id=%s AND research_date=%s""",
            (snapshot_id, cutoff), scope, 2,
        )[0]["value"] or 0)
        legacy_summary = self.kernel.read._repository_query(
            context,
            "research.snapshot_lineage",
            """SELECT COUNT(*) AS row_count,
                       COUNT(DISTINCT ts_code) AS code_count,
                       COUNT(*) FILTER (WHERE source_record_key NOT LIKE '%%:missing'
                                         AND (source_record_version IS NULL
                                              OR source_payload_hash IS NULL)) AS invalid_metadata,
                       COUNT(*) FILTER (WHERE source_record_key IS NULL) AS null_keys
                  FROM research.snapshot_lineage
                 WHERE snapshot_id=%s AND research_date=%s""",
            (snapshot_id, cutoff), scope, 2,
        )[0]
        legacy_incomplete = int(self.kernel.read._repository_query(
            context,
            "research.snapshot_lineage",
            """SELECT COUNT(*) AS value
                  FROM (
                    SELECT ts_code
                      FROM research.snapshot_lineage
                     WHERE snapshot_id=%s AND research_date=%s
                     GROUP BY ts_code
                    HAVING COUNT(*)<>10
                        OR COUNT(DISTINCT field_group)<>10
                  ) incomplete""",
            (snapshot_id, cutoff), scope, 2,
        )[0]["value"] or 0)
        bundle_summary = self.kernel.read._repository_query(
            context,
            "research.snapshot_lineage_bundle",
            """SELECT COUNT(*) AS row_count,
                       COUNT(*) FILTER (WHERE source_bundle_hash IS NULL) AS null_hashes
                  FROM research.snapshot_lineage_bundle
                 WHERE snapshot_id=%s AND research_date=%s""",
            (snapshot_id, cutoff), scope, 2,
        )[0]
        broken_refs = int(self.kernel.read._repository_query(
            context,
            "research.snapshot_lineage_bundle",
            """WITH refs AS (
                    SELECT b.snapshot_id, b.research_date, v.field_group,
                           v.source_ref_id
                      FROM research.snapshot_lineage_bundle b
                      CROSS JOIN LATERAL (VALUES
                        ('universe', b.universe_source_ref_id),
                        ('tradeability', b.tradeability_source_ref_id),
                        ('price', b.price_source_ref_id),
                        ('valuation', b.valuation_source_ref_id),
                        ('financial', b.financial_source_ref_id),
                        ('shareholder_return', b.shareholder_return_source_ref_id),
                        ('lifecycle', b.lifecycle_source_ref_id),
                        ('instrument', b.instrument_source_ref_id),
                        ('market_facts', b.market_facts_source_ref_id),
                        ('fundamental_history', b.fundamental_history_source_ref_id)
                      ) v(field_group, source_ref_id)
                     WHERE b.snapshot_id=%s AND b.research_date=%s
                )
                SELECT COUNT(*) AS value
                  FROM refs r
                  LEFT JOIN research.lineage_source_record d
                    ON d.source_ref_id=r.source_ref_id
                 WHERE r.source_ref_id IS NULL
                    OR d.source_ref_id IS NULL
                    OR (d.source_record_key NOT LIKE '%%:missing'
                        AND (d.source_record_version IS NULL
                             OR d.source_payload_hash IS NULL))""",
            (snapshot_id, cutoff), scope, 2,
        )[0]["value"] or 0)

        legacy_rows = int(legacy_summary["row_count"] or 0)
        legacy_codes = int(legacy_summary["code_count"] or 0)
        invalid_metadata = int(legacy_summary["invalid_metadata"] or 0)
        null_keys = int(legacy_summary["null_keys"] or 0)
        bundle_rows = int(bundle_summary["row_count"] or 0)
        null_hashes = int(bundle_summary["null_hashes"] or 0)
        if stock_rows <= 0:
            issues.append({"snapshot_rows": stock_rows})
        if (
            legacy_rows != stock_rows * 10
            or legacy_codes != stock_rows
            or invalid_metadata
            or null_keys
            or legacy_incomplete
        ):
            issues.append({
                "legacy_lineage_incomplete": {
                    "stock_rows": stock_rows,
                    "legacy_rows": legacy_rows,
                    "legacy_codes": legacy_codes,
                    "invalid_metadata": invalid_metadata,
                    "null_keys": null_keys,
                    "incomplete_codes": legacy_incomplete,
                }
            })
        if bundle_rows != stock_rows:
            issues.append({
                "snapshot_lineage_row_mismatch": {
                    "snapshot_rows": stock_rows,
                    "lineage_bundle_rows": bundle_rows,
                }
            })
        if null_hashes or broken_refs:
            issues.append({
                "lineage_bundle_refs_invalid": {
                    "null_bundle_hashes": null_hashes,
                    "broken_refs": broken_refs,
                }
            })
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
        if len(manifests) > MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE:
            raise ValueError(
                "snapshot inspection batches are limited to "
                f"{MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE}"
            )
        require_snapshot_batch(
            (item["snapshot_id"] for item in manifests), "snapshot inspection batch",
            allow_empty=False,
        )
        ids = [str(item["snapshot_id"]) for item in manifests]
        cutoffs = [item["cutoff"] for item in manifests]
        lineage_targets = {self.lineage_target(item) for item in manifests}
        if len(lineage_targets) != 1:
            raise ValueError("snapshot inspection batch must use one lineage storage version")
        lineage_target = next(iter(lineage_targets))
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

        foundation_financial = self.foundation_financial_contract_issues(
            manifests, context,
        )
        for snapshot_id, values in foundation_financial.items():
            add(snapshot_id, {"foundation_financial_contract": values})

        if lineage_target == self.BUNDLE_LINEAGE_TARGET:
            bundle_rows = self.kernel.read._repository_query(
                context, lineage_target,
                """SELECT snapshot_id, research_date, COUNT(*) AS value
                       FROM research.snapshot_lineage_bundle
                      WHERE snapshot_id=ANY(%s::uuid[])
                        AND research_date=ANY(%s::date[])
                      GROUP BY snapshot_id, research_date""",
                (ids, cutoffs), scope,
                max_rows=max(2, len(ids) + 1),
            )
            bundle_by_id = {str(row["snapshot_id"]): row["value"] for row in bundle_rows}
            for manifest in manifests:
                snapshot_id = str(manifest["snapshot_id"])
                expected_rows = stock_by_id.get(snapshot_id, {}).get("row_count", 0)
                actual_rows = bundle_by_id.get(snapshot_id, 0)
                if actual_rows != expected_rows:
                    add(snapshot_id, {
                        "snapshot_lineage_row_mismatch": {
                            "snapshot_rows": expected_rows,
                            "lineage_bundle_rows": actual_rows,
                        }
                    })
            missing_refs = self.kernel.read._repository_query(
                context, lineage_target,
                """WITH refs AS (
                         SELECT b.snapshot_id, b.research_date, v.field_group,
                                v.source_ref_id
                           FROM research.snapshot_lineage_bundle b
                           CROSS JOIN LATERAL (VALUES
                             ('universe', b.universe_source_ref_id),
                             ('tradeability', b.tradeability_source_ref_id),
                             ('price', b.price_source_ref_id),
                             ('valuation', b.valuation_source_ref_id),
                             ('financial', b.financial_source_ref_id),
                             ('shareholder_return', b.shareholder_return_source_ref_id),
                             ('lifecycle', b.lifecycle_source_ref_id),
                             ('instrument', b.instrument_source_ref_id),
                             ('market_facts', b.market_facts_source_ref_id),
                             ('fundamental_history', b.fundamental_history_source_ref_id)
                           ) v(field_group, source_ref_id)
                          WHERE b.snapshot_id=ANY(%s::uuid[])
                            AND b.research_date=ANY(%s::date[])
                       )
                       SELECT r.snapshot_id, r.field_group, COUNT(*) AS value
                         FROM refs r
                         JOIN research.lineage_source_record d
                           ON d.source_ref_id=r.source_ref_id
                        WHERE d.source_record_key NOT LIKE '%%:missing'
                          AND (d.source_record_version IS NULL OR d.source_payload_hash IS NULL)
                        GROUP BY r.snapshot_id, r.field_group
                        ORDER BY r.snapshot_id, r.field_group""",
                (ids, cutoffs), scope,
                max_rows=max(2, len(ids) * required_lineage_groups + 1),
            )
        else:
            incomplete_lineage = self.kernel.read._repository_query(
                context, lineage_target,
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
                context, lineage_target,
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
        inputs_by_id: dict[str, list[dict[str, Any]]] = {}
        for row in stored_inputs:
            inputs_by_id.setdefault(str(row["snapshot_id"]), []).append(row)
        for manifest in manifests:
            snapshot_id = str(manifest["snapshot_id"])
            rows = inputs_by_id.get(snapshot_id, [])
            stored_by_key = {(row["schema_name"], row["table_name"]): row for row in rows}
            expected_definitions = self._input_definition_for_stored_rows(
                rows, definitions,
            )
            if expected_definitions is None:
                expected_keys = [
                    (item["schema"], item["table"]) for item in definitions
                ]
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
                expected_keys = [
                    (item["schema"], item["table"])
                    for item in expected_definitions
                ]
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
