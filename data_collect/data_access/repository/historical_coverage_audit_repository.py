"""Bounded, read-only coverage summaries for historical v2.2 readiness."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class HistoricalCoverageAuditRepository(BaseRepository):
    """Summarize one explicit history range; never materialize research data."""

    @staticmethod
    def _scope(start: date, end: date, partition: str) -> WorkScope:
        if start > end:
            raise ValueError("coverage start must not exceed end")
        return WorkScope(date_from=start, date_to=end, partition=partition)

    def explain_source_scan(
        self, start: date, end: date, context: RunContext,
    ) -> dict[str, Any]:
        """Estimate the bounded source scans without executing them."""
        scope = self._scope(start, end, "historical-coverage:explain")
        queries = {
            "raw_financial_versions": (
                "tushare_financial_raw_version",
                (
                    "SELECT COUNT(*) FROM tushare_financial_raw_version "
                    "WHERE end_date BETWEEN %s AND %s"
                ),
            ),
            "financial_standard": (
                "std.financial_income_standard",
                (
                    "SELECT COUNT(*) FROM std.financial_income_standard "
                    "WHERE report_date BETWEEN %s AND %s"
                ),
            ),
            "financial_pit": (
                "pit.financial_asof",
                (
                    "SELECT COUNT(*) FROM pit.financial_asof "
                    "WHERE decision_date BETWEEN %s AND %s"
                ),
            ),
            "company_financial_v2": (
                "feature.company_financial_v2",
                (
                    "SELECT COUNT(*) FROM feature.company_financial_v2 "
                    "WHERE report_date BETWEEN %s AND %s"
                ),
            ),
            "debt_to_assets_v1": (
                "feature.debt_to_assets_v1",
                (
                    "SELECT COUNT(*) FROM feature.debt_to_assets_v1 "
                    "WHERE report_date BETWEEN %s AND %s"
                ),
            ),
        }
        return {
            name: self.kernel.read._repository_explain(
                context, source, sql, (start, end), scope,
            )
            for name, (source, sql) in queries.items()
        }

    def calendar_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context,
            "std.trading_calendar",
            """SELECT to_char(trade_date, 'YYYY-MM') AS month,
                      COUNT(DISTINCT trade_date) AS open_trade_days,
                      MIN(trade_date) AS first_trade_date,
                      MAX(trade_date) AS last_trade_date,
                      COUNT(DISTINCT source) AS source_count
                 FROM std.trading_calendar
                WHERE exchange='SSE' AND is_open IS TRUE
                  AND trade_date BETWEEN %s AND %s
                GROUP BY 1 ORDER BY 1""",
            (start, end),
            self._scope(start, end, "historical-coverage:calendar"),
            100,
        )

    def calendar_dates(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        """Return bounded open dates for maturity checks (at most the audited window)."""
        return self.kernel.read._repository_query(
            context,
            "std.trading_calendar",
            """SELECT trade_date
                 FROM std.trading_calendar
                WHERE exchange='SSE' AND is_open IS TRUE
                  AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date""",
            (start, end),
            self._scope(start, end, "historical-coverage:calendar-dates"),
            2_000,
        )

    def universe_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context,
            "pit.universe_daily",
            """SELECT to_char(trade_date, 'YYYY-MM') AS month,
                      COUNT(*) AS security_date_rows,
                      COUNT(DISTINCT trade_date) AS signal_dates,
                      COUNT(DISTINCT ts_code) AS distinct_securities,
                      COUNT(*) FILTER (WHERE is_listed_asof IS TRUE) AS listed_rows,
                      COUNT(*) FILTER (WHERE eligible_by_market IS TRUE) AS market_eligible_rows,
                      COUNT(*) FILTER (WHERE has_price_history IS TRUE) AS has_price_history_rows,
                      COUNT(*) FILTER (WHERE has_price_history IS FALSE OR has_price_history IS NULL) AS no_price_history_rows,
                      COUNT(*) FILTER (WHERE is_suspended_asof IS TRUE) AS suspended_rows,
                      COUNT(*) FILTER (WHERE quality_status IN ('VALID','PASS')) AS quality_valid_rows,
                      COUNT(*) FILTER (WHERE quality_status IS NULL OR quality_status NOT IN ('VALID','PASS')) AS quality_rejected_rows
                 FROM pit.universe_daily
                WHERE trade_date BETWEEN %s AND %s
                GROUP BY 1 ORDER BY 1""",
            (start, end),
            self._scope(start, end, "historical-coverage:universe"),
            100,
        )

    def market_bars_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context,
            "std.daily_kline",
            """SELECT to_char(trade_date, 'YYYY-MM') AS month,
                      COUNT(*) AS bar_rows,
                      COUNT(DISTINCT trade_date) AS price_dates,
                      COUNT(DISTINCT stock_code) AS securities,
                      COUNT(*) FILTER (WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
                                      OR open<=0 OR high<=0 OR low<=0 OR close<=0) AS invalid_ohlc_rows
                 FROM std.daily_kline
                WHERE trade_date BETWEEN %s AND %s
                GROUP BY 1 ORDER BY 1""",
            (start, end),
            self._scope(start, end, "historical-coverage:daily-kline"),
            100,
        )

    def security_bar_tradeability_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        """Compare PIT listed observations with bars and PIT suspension evidence."""
        return self.kernel.read._repository_query(
            context,
            "pit.universe_daily",
            """SELECT to_char(u.trade_date,'YYYY-MM') AS month,
                      COUNT(*) AS listed_universe_rows,
                      COUNT(d.stock_code) AS matched_bar_rows,
                      COUNT(*) FILTER (WHERE d.stock_code IS NULL) AS missing_bar_rows,
                      COUNT(*) FILTER (WHERE d.stock_code IS NULL AND u.is_suspended_asof IS TRUE) AS missing_bar_universe_suspended,
                      COUNT(*) FILTER (WHERE d.stock_code IS NULL AND t.is_suspended IS TRUE) AS missing_bar_tradeability_suspended,
                      COUNT(*) FILTER (WHERE d.stock_code IS NULL AND t.is_tradeable IS FALSE) AS missing_bar_not_tradeable,
                      COUNT(*) FILTER (WHERE d.stock_code IS NULL AND t.is_tradeable IS TRUE) AS missing_bar_marked_tradeable,
                      COUNT(*) FILTER (WHERE d.stock_code IS NULL AND t.quality_status NOT IN ('VALID','PASS')) AS missing_bar_tradeability_quality_unverified,
                      COUNT(*) FILTER (WHERE d.stock_code IS NOT NULL
                                         AND (d.open IS NULL OR d.high IS NULL OR d.low IS NULL OR d.close IS NULL
                                              OR d.open<=0 OR d.high<=0 OR d.low<=0 OR d.close<=0)) AS invalid_bar_rows,
                      COUNT(*) FILTER (WHERE d.stock_code IS NOT NULL
                                         AND (d.open IS NULL OR d.high IS NULL OR d.low IS NULL OR d.close IS NULL
                                              OR d.open<=0 OR d.high<=0 OR d.low<=0 OR d.close<=0)
                                         AND t.is_suspended IS TRUE) AS invalid_bar_suspended,
                      COUNT(*) FILTER (WHERE d.stock_code IS NOT NULL
                                         AND (d.open IS NULL OR d.high IS NULL OR d.low IS NULL OR d.close IS NULL
                                              OR d.open<=0 OR d.high<=0 OR d.low<=0 OR d.close<=0)
                                         AND t.is_tradeable IS TRUE) AS invalid_bar_marked_tradeable
                 FROM pit.universe_daily u
                 LEFT JOIN std.daily_kline d
                   ON d.trade_date=u.trade_date AND d.stock_code=u.ts_code
                 LEFT JOIN pit.security_tradeability_daily t
                   ON t.trade_date=u.trade_date AND t.ts_code=u.ts_code
                WHERE u.trade_date BETWEEN %s AND %s AND u.is_listed_asof IS TRUE
                GROUP BY 1 ORDER BY 1""",
            (start, end),
            self._scope(start, end, "historical-coverage:bar-tradeability"),
            100,
        )

    def raw_financial_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        sql = """SELECT source, month, COUNT(*) AS version_rows,
                        COUNT(DISTINCT ts_code) AS securities,
                        COUNT(DISTINCT report_date) AS report_dates,
                        COUNT(*) FILTER (WHERE payload_hash IS NULL) AS missing_hash_rows
                   FROM (
                         SELECT dataset AS source, end_date AS report_date,
                                to_char(end_date,'YYYY-MM') AS month,
                                ts_code, payload_hash
                           FROM tushare_financial_raw_version
                          WHERE end_date BETWEEN %s AND %s
                         UNION ALL
                         SELECT 'fina_indicator' AS source, end_date AS report_date,
                                to_char(end_date,'YYYY-MM') AS month,
                                ts_code, payload_hash
                           FROM fina_indicator_raw_version
                          WHERE end_date BETWEEN %s AND %s
                   ) raw_rows
                  GROUP BY source, month ORDER BY source, month"""
        return self.kernel.read._repository_query(
            context, "tushare_financial_raw_version", sql,
            (start, end, start, end),
            self._scope(start, end, "historical-coverage:raw-financial"),
            500,
        )

    def standard_financial_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        sql = """SELECT source, month, COUNT(*) AS version_rows,
                        COUNT(DISTINCT ts_code) AS securities,
                        COUNT(*) FILTER (WHERE quality_status IN ('VALID','PASS')) AS quality_valid_rows,
                        COUNT(*) FILTER (WHERE quality_status IS NULL OR quality_status NOT IN ('VALID','PASS')) AS quality_rejected_rows,
                        COUNT(*) FILTER (WHERE data_available_timestamp IS NOT NULL) AS timestamp_rows,
                        COUNT(*) FILTER (WHERE version_available_date IS NOT NULL) AS version_date_rows,
                        COUNT(*) FILTER (WHERE vintage_quality='SOURCE_ANNOUNCEMENT_ASSUMED') AS assumed_pit_rows,
                        COUNT(*) FILTER (WHERE vintage_quality='WAREHOUSE_OBSERVED_ONLY') AS warehouse_pit_rows,
                        COUNT(*) FILTER (WHERE COALESCE(report_type,'1')='1'
                                          AND data_available_timestamp IS NOT NULL) AS primary_report_time_eligible_rows,
                        COUNT(*) FILTER (WHERE input_core_complete) AS input_core_complete_rows,
                        COUNT(*) FILTER (WHERE da_inputs_complete) AS debt_to_assets_source_input_rows,
                        COUNT(*) FILTER (WHERE da_inputs_complete
                                          AND COALESCE(report_type,'1')='1'
                                          AND data_available_timestamp IS NOT NULL
                                          AND quality_status IN ('VALID','PASS')) AS debt_to_assets_quality_pit_input_rows,
                        COUNT(*) FILTER (WHERE source_record_key IS NULL OR record_version IS NULL
                                          OR payload_hash IS NULL) AS missing_lineage_rows
                   FROM (
                         SELECT 'income' AS source, report_date,
                                to_char(report_date,'YYYY-MM') AS month,
                                stock_code AS ts_code, quality_status, data_available_timestamp,
                                version_available_date, vintage_quality, source_record_key,
                                record_version, payload_hash, report_type,
                                (revenue IS NOT NULL AND net_profit_parent IS NOT NULL) AS input_core_complete,
                                FALSE AS da_inputs_complete
                           FROM std.financial_income_standard
                          WHERE report_date BETWEEN %s AND %s
                         UNION ALL
                         SELECT 'balance_sheet', report_date,
                                to_char(report_date,'YYYY-MM'), stock_code, quality_status,
                                data_available_timestamp, version_available_date, vintage_quality,
                                source_record_key, record_version, payload_hash, report_type,
                                (total_assets IS NOT NULL AND total_liab IS NOT NULL
                                 AND parent_equity_end IS NOT NULL) AS input_core_complete,
                                (total_assets IS NOT NULL AND total_liab IS NOT NULL) AS da_inputs_complete
                           FROM std.financial_balance_sheet_standard
                          WHERE report_date BETWEEN %s AND %s
                         UNION ALL
                         SELECT 'cash_flow', report_date,
                                to_char(report_date,'YYYY-MM'), stock_code, quality_status,
                                data_available_timestamp, version_available_date, vintage_quality,
                                source_record_key, record_version, payload_hash, report_type,
                                operating_cashflow_ytd IS NOT NULL AS input_core_complete,
                                FALSE AS da_inputs_complete
                           FROM std.financial_cash_flow_standard
                          WHERE report_date BETWEEN %s AND %s
                         UNION ALL
                         SELECT 'fina_indicator', report_date,
                                to_char(report_date,'YYYY-MM'), ts_code, quality_status,
                                data_available_timestamp, version_available_date, vintage_quality,
                                source_record_key, record_version, payload_hash, report_type,
                                roe_reported IS NOT NULL AS input_core_complete,
                                FALSE AS da_inputs_complete
                           FROM std.fina_indicator_standard
                          WHERE report_date BETWEEN %s AND %s
                   ) standard_rows
                  GROUP BY source, month ORDER BY source, month"""
        return self.kernel.read._repository_query(
            context, "std.financial_income_standard", sql,
            (start, end, start, end, start, end, start, end),
            self._scope(start, end, "historical-coverage:standard-financial"),
            500,
        )

    def missing_timestamp_raw_lineage_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        """Classify missing financial availability timestamps against exact Raw versions.

        This read-only audit distinguishes a matching source announcement date
        (an explicit PIT assumption), fetch-only warehouse evidence, lineage
        conflicts, and missing source matches. It never projects timestamps or
        writes Standard/PIT rows.
        """
        scope = self._scope(start, end, "historical-coverage:missing-pit-raw-lineage")
        sql = """WITH missing AS (
                    SELECT 'balance_sheet'::text AS source,
                           'balancesheet'::text AS raw_dataset,
                           stock_code AS ts_code, report_date, report_type,
                           comp_type, end_type, announce_date, ann_date, f_ann_date,
                           source_record_key, record_version, payload_hash
                      FROM std.financial_balance_sheet_standard
                     WHERE report_date BETWEEN %s AND %s
                       AND data_available_timestamp IS NULL
                    UNION ALL
                    SELECT 'cash_flow', 'cashflow', stock_code, report_date,
                           report_type, comp_type, end_type, announce_date, ann_date,
                           f_ann_date, source_record_key, record_version, payload_hash
                      FROM std.financial_cash_flow_standard
                     WHERE report_date BETWEEN %s AND %s
                       AND data_available_timestamp IS NULL
                    UNION ALL
                    SELECT 'income', 'income', stock_code, report_date,
                           report_type, comp_type, end_type, announce_date, ann_date,
                           f_ann_date, source_record_key, record_version, payload_hash
                      FROM std.financial_income_standard
                     WHERE report_date BETWEEN %s AND %s
                       AND data_available_timestamp IS NULL
                    UNION ALL
                    SELECT 'fina_indicator', 'fina_indicator', ts_code, report_date,
                           report_type, comp_type, end_type, effective_announce_date,
                           ann_date, f_ann_date, source_record_key, record_version,
                           payload_hash
                      FROM std.fina_indicator_standard
                     WHERE report_date BETWEEN %s AND %s
                       AND data_available_timestamp IS NULL
                ), matched AS (
                    SELECT m.*,
                           r.record_id AS raw_record_id,
                           r.business_key AS raw_business_key,
                           r.ts_code AS raw_ts_code,
                           r.end_date AS raw_report_date,
                           r.report_type AS raw_report_type,
                           r.comp_type AS raw_comp_type,
                           r.end_type AS raw_end_type,
                           r.payload_hash AS raw_payload_hash,
                           r.ann_date AS raw_ann_date,
                           r.f_ann_date AS raw_f_ann_date,
                           r.fetched_at AS raw_fetched_at,
                           key_versions.raw_version_count,
                           key_versions.raw_payload_count
                      FROM missing m
                      LEFT JOIN tushare_financial_raw_version r
                        ON r.dataset=m.raw_dataset
                       AND r.record_id=m.record_version
                      LEFT JOIN LATERAL (
                           SELECT COUNT(*) AS raw_version_count,
                                  COUNT(DISTINCT candidate.payload_hash) AS raw_payload_count
                             FROM tushare_financial_raw_version candidate
                            WHERE candidate.dataset=m.raw_dataset
                              AND candidate.business_key=concat_ws(
                                  ':', split_part(m.source_record_key,':',1),
                                  replace(split_part(m.source_record_key,':',2),'-',''),
                                  split_part(m.source_record_key,':',3),
                                  split_part(m.source_record_key,':',4),
                                  split_part(m.source_record_key,':',5))
                      ) key_versions ON TRUE
                )
                SELECT source, to_char(report_date,'YYYY-MM') AS report_month,
                       COUNT(*) AS missing_timestamp_rows,
                       COUNT(DISTINCT ts_code) AS securities,
                       COUNT(*) FILTER (WHERE source_record_key IS NULL
                                          OR record_version IS NULL
                                          OR payload_hash IS NULL) AS incomplete_standard_lineage_rows,
                       COUNT(*) FILTER (WHERE raw_record_id IS NOT NULL
                                          AND raw_ts_code=ts_code
                                          AND raw_report_date=report_date
                                          AND raw_report_type IS NOT DISTINCT FROM report_type
                                          AND raw_comp_type IS NOT DISTINCT FROM comp_type
                                          AND raw_end_type IS NOT DISTINCT FROM end_type
                                          AND raw_payload_hash=payload_hash) AS exact_raw_identity_matches,
                       COUNT(*) FILTER (WHERE raw_record_id IS NOT NULL
                                          AND raw_ts_code=ts_code
                                          AND raw_report_date=report_date
                                          AND raw_report_type IS NOT DISTINCT FROM report_type
                                          AND raw_comp_type IS NOT DISTINCT FROM comp_type
                                          AND raw_end_type IS NOT DISTINCT FROM end_type
                                          AND raw_payload_hash=payload_hash
                                          AND replace(raw_business_key,'-','')<>
                                              replace(source_record_key,'-','')) AS normalized_key_format_mismatch,
                       COUNT(*) FILTER (WHERE raw_record_id IS NOT NULL
                                          AND raw_ts_code=ts_code
                                          AND raw_report_date=report_date
                                          AND raw_report_type IS NOT DISTINCT FROM report_type
                                          AND raw_comp_type IS NOT DISTINCT FROM comp_type
                                          AND raw_end_type IS NOT DISTINCT FROM end_type
                                          AND raw_payload_hash=payload_hash
                                          AND COALESCE(raw_f_ann_date,raw_ann_date) IS NOT NULL) AS exact_raw_with_announcement_date,
                       COUNT(*) FILTER (WHERE raw_record_id IS NOT NULL
                                          AND raw_ts_code=ts_code
                                          AND raw_report_date=report_date
                                          AND raw_payload_hash=payload_hash
                                          AND COALESCE(raw_f_ann_date,raw_ann_date) IS NOT NULL
                                          AND announce_date IS DISTINCT FROM
                                              COALESCE(raw_f_ann_date,raw_ann_date)) AS standard_raw_announcement_conflicts,
                       COUNT(*) FILTER (WHERE raw_record_id IS NOT NULL
                                          AND raw_ts_code=ts_code
                                          AND raw_report_date=report_date
                                          AND raw_payload_hash=payload_hash
                                          AND COALESCE(raw_f_ann_date,raw_ann_date) IS NULL
                                          AND raw_fetched_at IS NOT NULL) AS exact_raw_fetch_only_evidence,
                       COUNT(*) FILTER (WHERE raw_record_id IS NOT NULL
                                          AND (raw_ts_code IS DISTINCT FROM ts_code
                                           OR raw_report_date IS DISTINCT FROM report_date
                                           OR raw_report_type IS DISTINCT FROM report_type
                                           OR raw_comp_type IS DISTINCT FROM comp_type
                                           OR raw_end_type IS DISTINCT FROM end_type
                                           OR raw_payload_hash IS DISTINCT FROM payload_hash)) AS record_version_identity_conflicts,
                       COUNT(*) FILTER (WHERE raw_record_id IS NOT NULL
                                          AND raw_ts_code IS DISTINCT FROM ts_code) AS raw_security_conflicts,
                       COUNT(*) FILTER (WHERE raw_record_id IS NOT NULL
                                          AND raw_report_date IS DISTINCT FROM report_date) AS raw_report_date_conflicts,
                       COUNT(*) FILTER (WHERE raw_record_id IS NOT NULL
                                          AND raw_report_type IS DISTINCT FROM report_type) AS raw_report_type_conflicts,
                       COUNT(*) FILTER (WHERE raw_record_id IS NOT NULL
                                          AND raw_comp_type IS DISTINCT FROM comp_type) AS raw_comp_type_conflicts,
                       COUNT(*) FILTER (WHERE raw_record_id IS NOT NULL
                                          AND raw_end_type IS DISTINCT FROM end_type) AS raw_end_type_conflicts,
                       COUNT(*) FILTER (WHERE raw_record_id IS NOT NULL
                                          AND raw_payload_hash IS DISTINCT FROM payload_hash) AS raw_payload_hash_conflicts,
                       COUNT(*) FILTER (WHERE raw_record_id IS NULL
                                          AND raw_version_count > 0
                                          AND raw_payload_count > 0) AS source_key_other_version_matches,
                       COUNT(*) FILTER (WHERE raw_record_id IS NULL
                                          AND raw_version_count > 1
                                          AND raw_payload_count > 1) AS unresolved_multiversion_keys,
                       COUNT(*) FILTER (WHERE raw_record_id IS NULL
                                          AND COALESCE(raw_version_count,0)=0) AS no_raw_source_key_match
                  FROM matched
                 GROUP BY source, report_month
                 ORDER BY source, report_month"""
        return self.kernel.read._repository_query(
            context,
            "std.financial_balance_sheet_standard",
            sql,
            (start, end, start, end, start, end, start, end),
            WorkScope(
                date_from=start,
                date_to=end,
                partition=scope.partition,
                dimensions={"date_column": "report_date", "classification": "raw-lineage-v1"},
            ),
            500,
        )

    def missing_timestamp_raw_lineage_conflicts(
        self, start: date, end: date, context: RunContext, limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return a bounded sample of missing-timestamp rows whose Raw version differs."""
        if not 0 < limit <= 500:
            raise ValueError("lineage conflict sample limit must be between 1 and 500")
        scope = self._scope(start, end, "historical-coverage:missing-pit-raw-conflicts")
        sql = """WITH missing AS (
                    SELECT 'balance_sheet'::text AS source, 'balancesheet'::text AS raw_dataset,
                           stock_code AS ts_code, report_date, report_type, comp_type,
                           end_type, announce_date, source_record_key, record_version, payload_hash
                      FROM std.financial_balance_sheet_standard
                     WHERE report_date BETWEEN %s AND %s AND data_available_timestamp IS NULL
                    UNION ALL
                    SELECT 'cash_flow', 'cashflow', stock_code, report_date, report_type,
                           comp_type, end_type, announce_date, source_record_key,
                           record_version, payload_hash
                      FROM std.financial_cash_flow_standard
                     WHERE report_date BETWEEN %s AND %s AND data_available_timestamp IS NULL
                    UNION ALL
                    SELECT 'income', 'income', stock_code, report_date, report_type,
                           comp_type, end_type, announce_date, source_record_key,
                           record_version, payload_hash
                      FROM std.financial_income_standard
                     WHERE report_date BETWEEN %s AND %s AND data_available_timestamp IS NULL
                )
                SELECT m.source, m.ts_code, m.report_date, m.report_type, m.comp_type,
                       m.end_type AS standard_end_type, m.announce_date,
                       m.source_record_key, m.record_version, m.payload_hash AS standard_payload_hash,
                       r.end_type AS raw_end_type, r.ann_date AS raw_ann_date,
                       r.f_ann_date AS raw_f_ann_date, r.fetched_at AS raw_fetched_at,
                       r.payload_hash AS raw_payload_hash,
                       (r.ts_code IS DISTINCT FROM m.ts_code) AS security_conflict,
                       (r.end_date IS DISTINCT FROM m.report_date) AS report_date_conflict,
                       (r.report_type IS DISTINCT FROM m.report_type) AS report_type_conflict,
                       (r.comp_type IS DISTINCT FROM m.comp_type) AS comp_type_conflict,
                       (r.end_type IS DISTINCT FROM m.end_type) AS end_type_conflict,
                       (r.payload_hash IS DISTINCT FROM m.payload_hash) AS payload_hash_conflict
                  FROM missing m
                  JOIN tushare_financial_raw_version r
                    ON r.dataset=m.raw_dataset AND r.record_id=m.record_version
                 WHERE r.ts_code IS DISTINCT FROM m.ts_code
                    OR r.end_date IS DISTINCT FROM m.report_date
                    OR r.report_type IS DISTINCT FROM m.report_type
                    OR r.comp_type IS DISTINCT FROM m.comp_type
                    OR r.end_type IS DISTINCT FROM m.end_type
                    OR r.payload_hash IS DISTINCT FROM m.payload_hash
                 ORDER BY m.source, m.report_date, m.ts_code
                 LIMIT %s"""
        return self.kernel.read._repository_query(
            context,
            "std.financial_balance_sheet_standard",
            sql,
            (start, end, start, end, start, end, limit),
            WorkScope(
                date_from=start,
                date_to=end,
                partition=scope.partition,
                dimensions={"date_column": "report_date", "classification": "raw-lineage-conflicts-v1"},
            ),
            limit,
        )

    def missing_timestamp_projection_batch(
        self,
        source: str,
        start: date,
        end: date,
        context: RunContext,
        *,
        after_standard_id: int = 0,
        limit: int = 2_000,
    ) -> list[dict[str, Any]]:
        """Page missing-timestamp Standard rows with their exact Raw identity.

        This method only prepares a versioned projection manifest. It does
        not assign availability timestamps or write either Standard or PIT.
        The cursor is the Standard surrogate key, so a caller can checkpoint
        each bounded page without using unstable OFFSET pagination.
        """
        if source not in {"balance_sheet", "cash_flow", "income"}:
            raise ValueError("unsupported missing-timestamp source")
        if after_standard_id < 0:
            raise ValueError("after_standard_id must be nonnegative")
        if not 0 < limit <= 5_000:
            raise ValueError("projection manifest page limit must be between 1 and 5000")
        table, id_column, code_column, raw_dataset = {
            "balance_sheet": (
                "std.financial_balance_sheet_standard", "financial_id",
                "stock_code", "balancesheet",
            ),
            "cash_flow": (
                "std.financial_cash_flow_standard", "financial_id",
                "stock_code", "cashflow",
            ),
            "income": (
                "std.financial_income_standard", "financial_id",
                "stock_code", "income",
            ),
        }[source]
        sql = f"""SELECT s.{id_column} AS standard_id, %s::text AS source,
                         s.{code_column} AS ts_code, s.report_date,
                         s.report_type, s.comp_type, s.end_type AS standard_end_type,
                         s.announce_date AS standard_announce_date,
                         s.source_record_key, s.record_version,
                         s.payload_hash AS standard_payload_hash,
                         r.record_id AS raw_record_id, r.business_key AS raw_business_key,
                         r.ts_code AS raw_ts_code, r.end_date AS raw_report_date,
                         r.report_type AS raw_report_type, r.comp_type AS raw_comp_type,
                         r.end_type AS raw_end_type, r.payload_hash AS raw_payload_hash,
                         COALESCE(r.f_ann_date, r.ann_date) AS raw_announce_date,
                         r.fetched_at AS raw_fetched_at,
                         (r.record_id IS NOT NULL
                          AND r.ts_code=s.{code_column}
                          AND r.end_date=s.report_date
                          AND r.report_type IS NOT DISTINCT FROM s.report_type
                          AND r.comp_type IS NOT DISTINCT FROM s.comp_type
                          AND r.end_type IS NOT DISTINCT FROM s.end_type
                          AND r.payload_hash=s.payload_hash) AS raw_identity_matches,
                         (s.end_type='NaN' AND r.end_type IS NULL) AS null_end_type_normalization
                    FROM {table} s
                    LEFT JOIN tushare_financial_raw_version r
                      ON r.dataset=%s AND r.record_id=s.record_version
                   WHERE s.report_date BETWEEN %s AND %s
                     AND s.data_available_timestamp IS NULL
                     AND s.{id_column}>%s
                   ORDER BY s.{id_column}
                   LIMIT %s"""
        return self.kernel.read._repository_query(
            context,
            table,
            sql,
            (source, raw_dataset, start, end, after_standard_id, limit),
            WorkScope(
                date_from=start,
                date_to=end,
                partition=f"historical-coverage:missing-pit-projection:{source}",
                dimensions={
                    "date_column": "report_date",
                    "classification": "financial-pit-projection-candidate-v1",
                },
            ),
            limit,
        )

    def financial_report_period_codes(
        self,
        source: str,
        report_date: date,
        cutoff_at: datetime,
        context: RunContext,
    ) -> list[str]:
        """Return securities with a supported report visible before an exact cutoff."""
        if source not in {"income", "balance_sheet"}:
            raise ValueError("source must be income or balance_sheet")
        if cutoff_at.tzinfo is None or cutoff_at.utcoffset() is None:
            raise ValueError("cutoff_at must be timezone-aware")
        table, code_column = {
            "income": ("std.financial_income_standard", "stock_code"),
            "balance_sheet": ("std.financial_balance_sheet_standard", "stock_code"),
        }[source]
        scope = self._scope(report_date, cutoff_at.date(), f"historical-coverage:report-codes:{source}")
        rows = self.kernel.read._repository_query(
            context,
            table,
            f"""SELECT DISTINCT {code_column} AS ts_code
                   FROM {table}
                  WHERE report_date=%s AND COALESCE(report_type,'1')='1'
                    AND data_available_timestamp IS NOT NULL
                    AND data_available_timestamp < %s::timestamptz
                  ORDER BY {code_column}""",
            (report_date, cutoff_at),
            WorkScope(
                date_from=report_date,
                date_to=cutoff_at.date(),
                partition=scope.partition,
                dimensions={
                    "date_column": "report_date",
                    "cutoff_timestamp": cutoff_at.isoformat(),
                },
            ),
            self.kernel.policy_registry.require(table).max_read_rows,
        )
        return [str(row["ts_code"]) for row in rows]

    def financial_feature_rows(
        self,
        source: str,
        report_date: date,
        cutoff_at: datetime,
        context: RunContext,
    ) -> list[dict[str, Any]]:
        """Read bounded version rows for an exact report period and PIT cutoff."""
        if cutoff_at.tzinfo is None or cutoff_at.utcoffset() is None:
            raise ValueError("cutoff_at must be timezone-aware")
        columns = {
            "company_financial_v2": (
                "feature.company_financial_v2",
                (
                    "feature_id, ts_code, report_date, data_available_timestamp, "
                    "source_observed_at, report_type, comp_type, end_type, roe_ttm, "
                    "debt_to_assets, revenue_growth_yoy, profit_growth_yoy, "
                    "roe_quality_reason, roe_quality_status, growth_quality_status, "
                    "component_lineage, source_fingerprint, pit_evidence_level, "
                    "quality_status, feature_contract_version"
                ),
            ),
            "debt_to_assets_v1": (
                "feature.debt_to_assets_v1",
                (
                    "feature_id, ts_code, report_date, data_available_timestamp, "
                    "source_observed_at, report_type, comp_type, end_type, total_assets, "
                    "total_liab, debt_to_assets, quality_status, quality_reason, "
                    "source_quality_status, vintage_quality, source_record_key, "
                    "record_version, payload_hash, source_fingerprint, feature_contract_version"
                ),
            ),
        }
        if source not in columns:
            raise ValueError("unsupported financial feature source")
        table, selected_columns = columns[source]
        scope = self._scope(report_date, cutoff_at.date(), f"historical-coverage:feature-rows:{source}")
        return self.kernel.read._repository_query(
            context,
            table,
            f"""SELECT {selected_columns}
                   FROM {table}
                  WHERE report_date=%s
                    AND data_available_timestamp < %s::timestamptz
                  ORDER BY ts_code, data_available_timestamp, feature_id""",
            (report_date, cutoff_at),
            WorkScope(
                date_from=report_date,
                date_to=cutoff_at.date(),
                partition=scope.partition,
                dimensions={
                    "date_column": "report_date",
                    "cutoff_timestamp": cutoff_at.isoformat(),
                },
            ),
            self.kernel.policy_registry.require(table).max_read_rows,
        )

    def financial_v2_rebuild_candidates(
        self, report_date: date, context: RunContext, limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Select a deterministic, bounded sample missing a v2 Feature row."""
        if not 0 < limit <= 100:
            raise ValueError("financial Feature candidate limit must be between 1 and 100")
        scope = self._scope(report_date, report_date, "historical-coverage:financial-v2-rebuild-candidates")
        sql = """SELECT i.stock_code, i.report_date, i.report_type,
                       i.comp_type, i.quality_status, i.data_available_timestamp,
                       i.source_record_key, i.record_version, i.payload_hash
                  FROM std.financial_income_standard i
                 WHERE i.report_date=%s
                   AND COALESCE(i.report_type,'1')='1'
                   AND i.data_available_timestamp IS NOT NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM feature.company_financial_v2 f
                        WHERE f.ts_code=i.stock_code AND f.report_date=i.report_date
                   )
                 ORDER BY i.stock_code, i.comp_type, i.source_record_key, i.record_version
                 LIMIT %s"""
        return self.kernel.read._repository_query(
            context,
            "std.financial_income_standard",
            sql,
            (report_date, limit),
            WorkScope(
                date_from=report_date,
                date_to=report_date,
                partition=scope.partition,
                dimensions={"date_column": "report_date", "feature_contract": "feature-contract-v2.0"},
            ),
            limit,
        )

    def financial_feature_cutoff_comparison(
        self, report_date: date, cutoff_date: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        """Compare source/Feature PIT coverage at one explicit historical cutoff."""
        scope = self._scope(report_date, report_date, "historical-coverage:financial-feature-cutoff")
        cutoff = "(%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'"
        sql = f"""SELECT source, row_count, securities, before_cutoff_rows,
                         before_cutoff_securities, after_cutoff_rows,
                         after_cutoff_securities, missing_timestamp_rows
                    FROM (
                      SELECT 'standard_income'::text AS source,
                             COUNT(*) AS row_count,
                             COUNT(DISTINCT stock_code) AS securities,
                             COUNT(*) FILTER (WHERE data_available_timestamp < {cutoff}) AS before_cutoff_rows,
                             COUNT(DISTINCT stock_code) FILTER (WHERE data_available_timestamp < {cutoff}) AS before_cutoff_securities,
                             COUNT(*) FILTER (WHERE data_available_timestamp >= {cutoff}) AS after_cutoff_rows,
                             COUNT(DISTINCT stock_code) FILTER (WHERE data_available_timestamp >= {cutoff}) AS after_cutoff_securities,
                             COUNT(*) FILTER (WHERE data_available_timestamp IS NULL) AS missing_timestamp_rows
                        FROM std.financial_income_standard
                       WHERE report_date=%s AND COALESCE(report_type,'1')='1'
                      UNION ALL
                      SELECT 'company_financial_v2', COUNT(*), COUNT(DISTINCT ts_code),
                             COUNT(*) FILTER (WHERE data_available_timestamp < {cutoff}),
                             COUNT(DISTINCT ts_code) FILTER (WHERE data_available_timestamp < {cutoff}),
                             COUNT(*) FILTER (WHERE data_available_timestamp >= {cutoff}),
                             COUNT(DISTINCT ts_code) FILTER (WHERE data_available_timestamp >= {cutoff}),
                             COUNT(*) FILTER (WHERE data_available_timestamp IS NULL)
                        FROM feature.company_financial_v2
                       WHERE report_date=%s
                    ) coverage
                   ORDER BY source"""
        return self.kernel.read._repository_query(
            context,
            "std.financial_income_standard",
            sql,
            (cutoff_date, cutoff_date, cutoff_date, cutoff_date, report_date,
             cutoff_date, cutoff_date, cutoff_date, cutoff_date, report_date),
            WorkScope(
                date_from=report_date,
                date_to=report_date,
                partition=scope.partition,
                dimensions={"date_column": "report_date", "pit_cutoff_date": cutoff_date.isoformat()},
            ),
            2,
        )

    def pit_financial_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context,
            "pit.financial_asof",
            """SELECT to_char(decision_date,'YYYY-MM') AS month,
                      COUNT(*) AS pit_rows,
                      COUNT(DISTINCT ts_code) AS securities,
                      COUNT(DISTINCT decision_date) AS decision_dates,
                      COUNT(*) FILTER (WHERE debt_to_assets IS NOT NULL) AS debt_to_assets_values,
                      COUNT(*) FILTER (WHERE roe IS NOT NULL) AS roe_values,
                      COUNT(*) FILTER (WHERE revenue_yoy IS NOT NULL) AS revenue_growth_values,
                      COUNT(*) FILTER (WHERE netprofit_yoy IS NOT NULL) AS profit_growth_values,
                      COUNT(*) FILTER (WHERE quality_status IN ('VALID','PASS')) AS quality_valid_rows,
                      COUNT(*) FILTER (WHERE quality_status IS NULL OR quality_status NOT IN ('VALID','PASS')) AS quality_rejected_rows,
                      COUNT(*) FILTER (WHERE version_available_date IS NULL
                                         OR version_available_date>decision_date) AS pit_date_violations,
                      COUNT(*) FILTER (WHERE vintage_quality='SOURCE_ANNOUNCEMENT_ASSUMED') AS assumed_pit_rows,
                      COUNT(*) FILTER (WHERE vintage_quality='WAREHOUSE_OBSERVED_ONLY') AS warehouse_pit_rows,
                      COUNT(*) FILTER (WHERE source_record_key IS NULL OR record_version IS NULL) AS missing_lineage_rows
                 FROM pit.financial_asof
                WHERE decision_date BETWEEN %s AND %s
                GROUP BY 1 ORDER BY 1""",
            (start, end),
            self._scope(start, end, "historical-coverage:financial-pit"),
            100,
        )

    def company_financial_v2_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context,
            "feature.company_financial_v2",
            """SELECT to_char(report_date,'YYYY-MM') AS month,
                      COUNT(*) AS feature_rows,
                      COUNT(DISTINCT ts_code) AS securities,
                      COUNT(*) FILTER (WHERE roe_ttm IS NOT NULL) AS roe_value_rows,
                      COUNT(*) FILTER (WHERE revenue_growth_yoy IS NOT NULL) AS revenue_growth_value_rows,
                      COUNT(*) FILTER (WHERE profit_growth_yoy IS NOT NULL) AS profit_growth_value_rows,
                      COUNT(*) FILTER (WHERE roe_ttm IS NOT NULL AND roe_quality_status IN ('VALID','PASS')) AS roe_quality_pass_rows,
                      COUNT(*) FILTER (WHERE revenue_growth_yoy IS NOT NULL AND growth_quality_status IN ('VALID','INSUFFICIENT_HISTORY')) AS revenue_growth_quality_rows,
                      COUNT(*) FILTER (WHERE profit_growth_yoy IS NOT NULL AND growth_quality_status IN ('VALID','INSUFFICIENT_HISTORY')) AS profit_growth_quality_rows,
                      COUNT(*) FILTER (WHERE quality_status IN ('VALID','PASS')) AS row_quality_valid_rows,
                      COUNT(*) FILTER (WHERE pit_evidence_level='SOURCE_ANNOUNCEMENT_ASSUMED') AS assumed_pit_rows,
                      COUNT(*) FILTER (WHERE pit_evidence_level='WAREHOUSE_OBSERVED_ONLY') AS warehouse_pit_rows,
                      COUNT(*) FILTER (WHERE data_available_timestamp IS NULL OR source_fingerprint IS NULL
                                         OR component_lineage IS NULL OR component_lineage='{}'::jsonb) AS missing_evidence_rows,
                      COUNT(*) FILTER (WHERE component_lineage->'__quality_evidence__'->>'version'='financial-source-quality-v1') AS source_quality_evidence_rows,
                      COUNT(*) FILTER (WHERE EXISTS (
                                          SELECT 1 FROM jsonb_each(component_lineage) c
                                           WHERE c.key<>'__quality_evidence__'
                                             AND COALESCE(c.value->'used_by','[]'::jsonb) ? 'roe_ttm')) AS roe_lineage_rows,
                      COUNT(*) FILTER (WHERE EXISTS (
                                          SELECT 1 FROM jsonb_each(component_lineage) c
                                           WHERE c.key<>'__quality_evidence__'
                                             AND COALESCE(c.value->'used_by','[]'::jsonb) ? 'revenue_growth_yoy')) AS revenue_growth_lineage_rows,
                      COUNT(*) FILTER (WHERE EXISTS (
                                          SELECT 1 FROM jsonb_each(component_lineage) c
                                           WHERE c.key<>'__quality_evidence__'
                                             AND COALESCE(c.value->'used_by','[]'::jsonb) ? 'profit_growth_yoy')) AS profit_growth_lineage_rows,
                      COUNT(*) FILTER (WHERE feature_contract_version IS DISTINCT FROM 'feature-contract-v2.0') AS contract_mismatch_rows
                 FROM feature.company_financial_v2
                WHERE report_date BETWEEN %s AND %s
                GROUP BY 1 ORDER BY 1""",
            (start, end),
            self._scope(start, end, "historical-coverage:company-financial-v2"),
            100,
        )

    def debt_to_assets_v1_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context,
            "feature.debt_to_assets_v1",
            """SELECT to_char(report_date,'YYYY-MM') AS month,
                      COUNT(*) AS feature_rows,
                      COUNT(DISTINCT ts_code) AS securities,
                      COUNT(*) FILTER (WHERE debt_to_assets IS NOT NULL) AS value_rows,
                      COUNT(*) FILTER (WHERE quality_status IN ('VALID','PASS')) AS feature_quality_pass_rows,
                      COUNT(*) FILTER (WHERE source_quality_status IN ('VALID','PASS')) AS source_quality_pass_rows,
                      COUNT(*) FILTER (WHERE data_available_timestamp IS NOT NULL) AS availability_timestamp_rows,
                      COUNT(*) FILTER (WHERE vintage_quality='SOURCE_ANNOUNCEMENT_ASSUMED') AS assumed_pit_rows,
                      COUNT(*) FILTER (WHERE vintage_quality='WAREHOUSE_OBSERVED_ONLY') AS warehouse_pit_rows,
                      COUNT(*) FILTER (WHERE source_record_key IS NULL OR record_version IS NULL
                                         OR payload_hash IS NULL OR length(payload_hash)<>64) AS missing_source_identity_rows,
                      COUNT(*) FILTER (WHERE feature_contract_version IS DISTINCT FROM 'debt-to-assets-v1') AS contract_mismatch_rows
                 FROM feature.debt_to_assets_v1
                WHERE report_date BETWEEN %s AND %s
                GROUP BY 1 ORDER BY 1""",
            (start, end),
            self._scope(start, end, "historical-coverage:debt-to-assets-v1"),
            100,
        )

    def snapshots_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context,
            "research.snapshot_manifest",
            """SELECT to_char(m.research_cutoff_date,'YYYY-MM') AS month,
                      m.schema_version, m.status,
                      COUNT(DISTINCT m.snapshot_id) AS snapshot_count,
                      COUNT(DISTINCT m.research_cutoff_date) AS signal_dates
                 FROM research.snapshot_manifest m
                WHERE m.research_cutoff_date BETWEEN %s AND %s
                GROUP BY 1,m.schema_version,m.status ORDER BY 1,m.schema_version,m.status""",
            (start, end),
            self._scope(start, end, "historical-coverage:snapshots"),
            500,
        )

    def release_inventory(
        self, start: date, cutoff: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context,
            "ops.tushare_data_release",
            """SELECT release_tag, baseline_domain, baseline_cutoff_date,
                      schema_version, status, quality_report_hash,
                      source_manifest_hash, temporal_evidence_level,
                      visibility_policy_version
                 FROM ops.tushare_data_release
                WHERE baseline_cutoff_date BETWEEN %s AND %s
                ORDER BY baseline_cutoff_date, release_tag""",
            (start, cutoff),
            self._scope(start, cutoff, "historical-coverage:release-inventory"),
            500,
        )

    def dividend_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context,
            "std.dividend_event",
            """SELECT to_char(ex_date,'YYYY-MM') AS month,
                      COUNT(*) AS event_rows,
                      COUNT(DISTINCT ts_code) AS securities,
                      COUNT(*) FILTER (WHERE implementation_status='实施') AS implemented_rows,
                      COUNT(*) FILTER (WHERE quality_status IN ('VALID','PASS')) AS quality_valid_rows,
                      COUNT(*) FILTER (WHERE source_record_key IS NULL OR record_version IS NULL
                                         OR payload_hash IS NULL) AS missing_lineage_rows
                 FROM std.dividend_event
                WHERE ex_date BETWEEN %s AND %s
                GROUP BY 1 ORDER BY 1""",
            (start, end),
            self._scope(start, end, "historical-coverage:dividends"),
            100,
        )

    @staticmethod
    def _financial_factor_gate(factor: str, value_field: str, quality_sql: str) -> str:
        lineage = "COALESCE(s.component_lineage, '{}'::jsonb)"
        used = (
            "EXISTS (SELECT 1 FROM jsonb_each(" + lineage + ") AS component(key,value) "
            f"WHERE component.key<>'__quality_evidence__' "
            f"AND COALESCE(component.value->'used_by','[]'::jsonb) ? '{factor}')"
        )
        bad_source = (
            "EXISTS (SELECT 1 FROM jsonb_each(" + lineage + ") AS component(key,value) "
            f"WHERE component.key<>'__quality_evidence__' "
            f"AND COALESCE(component.value->'used_by','[]'::jsonb) ? '{factor}' "
            "AND UPPER(COALESCE(component.value->>'source_quality_status','')) "
            "NOT IN ('VALID','PASS'))"
        )
        return (
            f"(s.financial_feature_id IS NOT NULL AND s.{value_field} IS NOT NULL "
            f"AND {quality_sql} AND {used} AND NOT {bad_source} "
            "AND s.feature_contract_version='feature-contract-v2.0' "
            "AND s.pit_evidence_level='SOURCE_ANNOUNCEMENT_ASSUMED' "
            "AND s.component_lineage->'__quality_evidence__'->>'version'="
            "'financial-source-quality-v1')"
        )

    @classmethod
    def _factor_asof_sql(cls) -> str:
        roe_valid = cls._financial_factor_gate(
            "roe_ttm", "roe_ttm", "s.roe_quality_status IN ('VALID','PASS')",
        )
        revenue_valid = cls._financial_factor_gate(
            "revenue_growth_yoy", "revenue_growth_yoy",
            "s.growth_quality_status IN ('VALID','INSUFFICIENT_HISTORY')",
        )
        profit_valid = cls._financial_factor_gate(
            "profit_growth_yoy", "profit_growth_yoy",
            "s.growth_quality_status IN ('VALID','INSUFFICIENT_HISTORY')",
        )
        da_valid = """(s.da_feature_id IS NOT NULL AND s.debt_to_assets IS NOT NULL
                   AND s.da_feature_contract_version='debt-to-assets-v1'
                   AND s.da_quality_status IN ('VALID','PASS')
                   AND s.da_source_quality_status IN ('VALID','PASS')
                   AND NULLIF(BTRIM(s.da_source_record_key),'') IS NOT NULL
                   AND s.da_record_version IS NOT NULL
                   AND s.da_payload_hash ~ '^[0-9a-fA-F]{64}$'
                   AND (s.da_vintage_quality='SOURCE_ANNOUNCEMENT_ASSUMED'
                        OR (s.da_vintage_quality='WAREHOUSE_OBSERVED_ONLY'
                            AND s.da_source_observed_at <
                                (s.trade_date + TIME '15:00') AT TIME ZONE 'Asia/Shanghai')))"""
        return f"""WITH universe AS (
                    SELECT trade_date, ts_code
                      FROM pit.universe_daily
                     WHERE trade_date BETWEEN %s AND %s
                       AND is_listed_asof IS TRUE
                ), selected AS (
                    SELECT u.trade_date, u.ts_code,
                           f.feature_id AS financial_feature_id,
                           f.report_date AS financial_report_date,
                           f.roe_ttm, f.roe_quality_status,
                           f.revenue_growth_yoy, f.profit_growth_yoy,
                           f.growth_quality_status, f.feature_contract_version,
                           f.pit_evidence_level, f.component_lineage,
                           d.feature_id AS da_feature_id, d.report_date AS da_report_date,
                           d.debt_to_assets, d.quality_status AS da_quality_status,
                           d.source_quality_status AS da_source_quality_status,
                           d.source_record_key AS da_source_record_key,
                           d.record_version AS da_record_version,
                           d.payload_hash AS da_payload_hash,
                           d.feature_contract_version AS da_feature_contract_version,
                           d.vintage_quality AS da_vintage_quality,
                           d.source_observed_at AS da_source_observed_at
                      FROM universe u
                      LEFT JOIN LATERAL (
                          SELECT f.* FROM feature.company_financial_v2 f
                           WHERE f.ts_code=u.ts_code AND f.report_date<=u.trade_date
                             AND f.data_available_timestamp <
                                 (u.trade_date + TIME '15:00') AT TIME ZONE 'Asia/Shanghai'
                           ORDER BY f.report_date DESC, f.data_available_timestamp DESC,
                                    f.feature_id DESC LIMIT 1
                      ) f ON TRUE
                      LEFT JOIN LATERAL (
                          SELECT d.* FROM feature.debt_to_assets_v1 d
                           WHERE d.ts_code=u.ts_code AND d.report_date<=u.trade_date
                             AND d.data_available_timestamp <
                                 (u.trade_date + TIME '15:00') AT TIME ZONE 'Asia/Shanghai'
                           ORDER BY d.report_date DESC, d.data_available_timestamp DESC,
                                    d.record_version DESC, d.feature_id DESC LIMIT 1
                      ) d ON TRUE
                )
                SELECT to_char(trade_date,'YYYY-MM') AS month,
                       COUNT(*) AS listed_signal_observations,
                       COUNT(*) FILTER (WHERE financial_feature_id IS NOT NULL) AS financial_feature_rows,
                       COUNT(*) FILTER (WHERE s.roe_ttm IS NOT NULL) AS roe_value_rows,
                       COUNT(*) FILTER (WHERE {roe_valid}) AS roe_valid_factor_rows,
                       COUNT(*) FILTER (WHERE s.revenue_growth_yoy IS NOT NULL) AS revenue_growth_value_rows,
                       COUNT(*) FILTER (WHERE {revenue_valid}) AS revenue_growth_valid_factor_rows,
                       COUNT(*) FILTER (WHERE s.profit_growth_yoy IS NOT NULL) AS profit_growth_value_rows,
                       COUNT(*) FILTER (WHERE {profit_valid}) AS profit_growth_valid_factor_rows,
                       COUNT(*) FILTER (WHERE s.da_feature_id IS NOT NULL) AS da_feature_rows,
                       COUNT(*) FILTER (WHERE s.debt_to_assets IS NOT NULL) AS da_value_rows,
                       COUNT(*) FILTER (WHERE {da_valid}) AS da_valid_factor_rows,
                       COUNT(*) FILTER (WHERE s.financial_feature_id IS NOT NULL
                                         AND s.pit_evidence_level='SOURCE_ANNOUNCEMENT_ASSUMED') AS assumed_financial_pit_rows,
                       COUNT(*) FILTER (WHERE s.financial_feature_id IS NOT NULL
                                         AND s.pit_evidence_level='WAREHOUSE_OBSERVED_ONLY') AS warehouse_financial_pit_rows,
                       COUNT(*) FILTER (WHERE s.financial_feature_id IS NOT NULL
                                         AND (s.component_lineage IS NULL OR s.component_lineage='{{}}'::jsonb)) AS missing_financial_lineage_rows,
                       COUNT(*) FILTER (WHERE s.da_feature_id IS NOT NULL
                                         AND s.da_vintage_quality='SOURCE_ANNOUNCEMENT_ASSUMED') AS assumed_da_pit_rows,
                       COUNT(*) FILTER (WHERE s.da_feature_id IS NOT NULL
                                         AND s.da_vintage_quality='WAREHOUSE_OBSERVED_ONLY') AS warehouse_da_pit_rows
                  FROM selected s
                 GROUP BY 1 ORDER BY 1"""

    @classmethod
    def _financial_factor_asof_sql(cls) -> str:
        roe_valid = cls._financial_factor_gate(
            "roe_ttm", "roe_ttm", "s.roe_quality_status IN ('VALID','PASS')",
        )
        revenue_valid = cls._financial_factor_gate(
            "revenue_growth_yoy", "revenue_growth_yoy",
            "s.growth_quality_status IN ('VALID','INSUFFICIENT_HISTORY')",
        )
        profit_valid = cls._financial_factor_gate(
            "profit_growth_yoy", "profit_growth_yoy",
            "s.growth_quality_status IN ('VALID','INSUFFICIENT_HISTORY')",
        )
        return f"""WITH universe AS (
                    SELECT trade_date, ts_code
                      FROM pit.universe_daily
                     WHERE trade_date BETWEEN %s AND %s
                       AND is_listed_asof IS TRUE
                ), selected AS (
                    SELECT u.trade_date, u.ts_code,
                           f.feature_id AS financial_feature_id,
                           f.roe_ttm, f.roe_quality_status,
                           f.revenue_growth_yoy, f.profit_growth_yoy,
                           f.growth_quality_status, f.feature_contract_version,
                           f.pit_evidence_level, f.component_lineage
                      FROM universe u
                      LEFT JOIN LATERAL (
                          SELECT f.* FROM feature.company_financial_v2 f
                           WHERE f.ts_code=u.ts_code AND f.report_date<=u.trade_date
                             AND f.data_available_timestamp <
                                 (u.trade_date + TIME '15:00') AT TIME ZONE 'Asia/Shanghai'
                           ORDER BY f.report_date DESC, f.data_available_timestamp DESC,
                                    f.feature_id DESC LIMIT 1
                      ) f ON TRUE
                )
                SELECT to_char(trade_date,'YYYY-MM') AS month,
                       COUNT(*) AS listed_signal_observations,
                       COUNT(*) FILTER (WHERE financial_feature_id IS NOT NULL) AS feature_rows,
                       COUNT(*) FILTER (WHERE roe_ttm IS NOT NULL) AS roe_value_rows,
                       COUNT(*) FILTER (WHERE {roe_valid}) AS roe_valid_factor_rows,
                       COUNT(*) FILTER (WHERE revenue_growth_yoy IS NOT NULL) AS revenue_growth_value_rows,
                       COUNT(*) FILTER (WHERE {revenue_valid}) AS revenue_growth_valid_factor_rows,
                       COUNT(*) FILTER (WHERE profit_growth_yoy IS NOT NULL) AS profit_growth_value_rows,
                       COUNT(*) FILTER (WHERE {profit_valid}) AS profit_growth_valid_factor_rows,
                       COUNT(*) FILTER (WHERE financial_feature_id IS NOT NULL
                                         AND pit_evidence_level='SOURCE_ANNOUNCEMENT_ASSUMED') AS assumed_financial_pit_rows,
                       COUNT(*) FILTER (WHERE financial_feature_id IS NOT NULL
                                         AND pit_evidence_level='WAREHOUSE_OBSERVED_ONLY') AS warehouse_financial_pit_rows,
                       COUNT(*) FILTER (WHERE financial_feature_id IS NOT NULL
                                         AND (component_lineage IS NULL OR component_lineage='{{}}'::jsonb)) AS missing_financial_lineage_rows,
                       COUNT(*) FILTER (WHERE financial_feature_id IS NOT NULL
                                         AND feature_contract_version IS DISTINCT FROM 'feature-contract-v2.0') AS contract_mismatch_rows
                  FROM selected s
                 GROUP BY 1 ORDER BY 1"""

    def financial_factor_asof_monthly(
        self, start: date, end: date, context: RunContext,
    ) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context,
            "pit.universe_daily",
            self._financial_factor_asof_sql(),
            (start, end),
            self._scope(start, end, "historical-coverage:financial-factor-asof"),
            100,
        )

    def explain_financial_factor_asof(
        self, start: date, end: date, context: RunContext,
    ) -> dict[str, Any]:
        return self.kernel.read._repository_explain(
            context,
            "pit.universe_daily",
            self._financial_factor_asof_sql(),
            (start, end),
            self._scope(start, end, "historical-coverage:financial-factor-asof-explain"),
        )
