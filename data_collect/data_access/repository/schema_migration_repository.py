"""Auditable, reversible control-plane operations for physical cutover."""
from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class SchemaMigrationRepository(BaseRepository):
    TARGET = "ops.foundation_table_migration"
    LEGACY_FINANCIAL_INDICATOR_TARGET = "std.stock_financial_indicator"
    FINANCIAL_STANDARD_SPECS = {
        "financial_income_standard": {
            "target": "std.financial_income_standard",
            "key": "financial_id",
        },
        "financial_balance_sheet_standard": {
            "target": "std.financial_balance_sheet_standard",
            "key": "financial_id",
        },
        "financial_cash_flow_standard": {
            "target": "std.financial_cash_flow_standard",
            "key": "financial_id",
        },
        "fina_indicator_standard": {
            "target": "std.fina_indicator_standard",
            "key": "indicator_id",
        },
    }

    def _scope(self, migration_id: str) -> WorkScope:
        return WorkScope(partition=f"schema-migration:{migration_id}")

    def register(self, context: RunContext, *, migration_id: str,
                 source_table: str, target_table: str, notes: str = "") -> int:
        UUID(str(migration_id))
        if not source_table or not target_table or source_table == target_table:
            raise ValueError("source_table and target_table must be distinct")
        return self.kernel.write.upsert(
            context, self._scope(migration_id), self.TARGET, [{
                "migration_id": migration_id, "source_table": source_table,
                "target_table": target_table, "status": "REGISTERED", "notes": notes,
            }], key=("migration_id",), update_columns=("notes",), skip_noop=True,
        )

    def record_validation(self, context: RunContext, *, migration_id: str,
                          source_row_count: int, target_row_count: int,
                          source_content_hash: str, target_content_hash: str) -> int:
        if source_row_count < 0 or target_row_count < 0:
            raise ValueError("migration row counts cannot be negative")
        if source_content_hash != target_content_hash:
            raise ValueError("source and target content hashes differ")
        current = self.get(context, migration_id)
        if not current:
            raise ValueError(f"unknown migration_id: {migration_id}")
        if current["status"] not in {
            "REGISTERED", "DUAL_READ", "DUAL_WRITE", "VALIDATED",
        }:
            raise ValueError(
                f"cannot record validation from status={current['status']}"
            )
        return self.kernel.write.upsert(
            context, self._scope(migration_id), self.TARGET, [{
                "migration_id": migration_id, "status": "VALIDATED",
                "source_table": current["source_table"],
                "target_table": current["target_table"],
                "source_row_count": source_row_count, "target_row_count": target_row_count,
                "source_content_hash": source_content_hash,
                "target_content_hash": target_content_hash,
                "validated_at": datetime.now(timezone.utc),
            }], key=("migration_id",), update_columns=(
                "status", "source_row_count", "target_row_count",
                "source_content_hash", "target_content_hash", "validated_at",
            ), skip_noop=False,
        )

    def prepare_trading_calendar(self, context: RunContext, migration_id: str) -> int:
        """Copy the small pilot through RebuildGateway, leaving public intact."""
        scope = self._scope(migration_id)
        plan = SqlPlan(
            operation=Operation.REBUILD,
            target="std.trading_calendar",
            scope=scope,
            expected_mode="SET_BASED",
            estimated_rows=10_000,
            allow_insert_select=True,
        )
        return self.kernel.rebuild.run_insert_select(
            """INSERT INTO std.trading_calendar
                    (exchange, trade_date, is_open, pretrade_date, source, fetched_at)
                SELECT exchange, trade_date, is_open, pretrade_date, source, fetched_at
                  FROM public.trading_calendar
                ON CONFLICT (exchange, trade_date, source) DO UPDATE SET
                    is_open=EXCLUDED.is_open,
                    pretrade_date=EXCLUDED.pretrade_date,
                    fetched_at=EXCLUDED.fetched_at""",
            plan, context,
        )

    def validate_trading_calendar(
        self, context: RunContext, migration_id: str, *, persist: bool = True,
    ) -> dict[str, Any]:
        """Compare count and deterministic row hash without touching source data."""
        scope = self._scope(migration_id)
        sql = """SELECT COUNT(*) AS row_count,
                    encode(digest(COALESCE(string_agg(
                        exchange || '|' || trade_date::text || '|' || is_open::text || '|' ||
                        COALESCE(pretrade_date::text, '') || '|' || source || '|' || fetched_at::text,
                        E'\\n' ORDER BY exchange, trade_date, source), ''), 'sha256'), 'hex') AS content_hash
                 FROM {table}"""
        source = self.kernel.read._repository_query(
            context, "public.trading_calendar", sql.format(table="public.trading_calendar"), (), scope, 2,
        )[0]
        target = self.kernel.read._repository_query(
            context, "std.trading_calendar", sql.format(table="std.trading_calendar"), (), scope, 2,
        )[0]
        result = {
            "source_row_count": int(source["row_count"] or 0),
            "target_row_count": int(target["row_count"] or 0),
            "source_content_hash": source["content_hash"],
            "target_content_hash": target["content_hash"],
        }
        if persist:
            self.record_validation(context, migration_id=migration_id, **result)
        return result

    def prepare_stock_basic(self, context: RunContext, migration_id: str) -> int:
        scope = self._scope(migration_id)
        plan = SqlPlan(
            operation=Operation.REBUILD, target="std.stock_basic_info", scope=scope,
            expected_mode="SET_BASED", estimated_rows=10_000, allow_insert_select=True,
        )
        return self.kernel.rebuild.run_insert_select(
            """INSERT INTO std.stock_basic_info
                    (stock_code, symbol, name, area, industry, market, list_date,
                     delist_date, list_status, source, fetched_at)
                SELECT stock_code, symbol, name, area, industry, market, list_date,
                       delist_date, list_status, source, fetched_at
                  FROM public.stock_basic_info
                ON CONFLICT (stock_code) DO UPDATE SET
                    symbol=EXCLUDED.symbol, name=EXCLUDED.name, area=EXCLUDED.area,
                    industry=EXCLUDED.industry, market=EXCLUDED.market,
                    list_date=EXCLUDED.list_date, delist_date=EXCLUDED.delist_date,
                    list_status=EXCLUDED.list_status, source=EXCLUDED.source,
                    fetched_at=EXCLUDED.fetched_at""",
            plan, context,
        )

    def validate_stock_basic(
        self, context: RunContext, migration_id: str, *, persist: bool = True,
    ) -> dict[str, Any]:
        scope = self._scope(migration_id)
        sql = """SELECT COUNT(*) AS row_count,
                    encode(digest(COALESCE(string_agg(
                        stock_code || '|' || COALESCE(symbol, '') || '|' || COALESCE(name, '') || '|' ||
                        COALESCE(area, '') || '|' || COALESCE(industry, '') || '|' || COALESCE(market, '') || '|' ||
                        COALESCE(list_date::text, '') || '|' || COALESCE(delist_date::text, '') || '|' ||
                        COALESCE(list_status, '') || '|' || source || '|' || fetched_at::text,
                        E'\\n' ORDER BY stock_code), ''), 'sha256'), 'hex') AS content_hash
                 FROM {table}"""
        source = self.kernel.read._repository_query(
            context, "public.stock_basic_info", sql.format(table="public.stock_basic_info"), (), scope, 2,
        )[0]
        target = self.kernel.read._repository_query(
            context, "std.stock_basic_info", sql.format(table="std.stock_basic_info"), (), scope, 2,
        )[0]
        result = {
            "source_row_count": int(source["row_count"] or 0),
            "target_row_count": int(target["row_count"] or 0),
            "source_content_hash": source["content_hash"],
            "target_content_hash": target["content_hash"],
        }
        if persist:
            self.record_validation(context, migration_id=migration_id, **result)
        return result

    def prepare_daily_kline_window(
        self, context: RunContext, migration_id: str, start_date: date, end_date_exclusive: date,
    ) -> int:
        """Copy one bounded date window into the partitioned Standard target."""
        if start_date >= end_date_exclusive:
            raise ValueError("daily_kline migration window must be non-empty")
        scope = WorkScope(
            date_from=start_date, date_to=end_date_exclusive - timedelta(days=1),
            partition=f"schema-migration:{migration_id}:daily-kline",
        )
        plan = SqlPlan(
            operation=Operation.REBUILD, target="std.daily_kline", scope=scope,
            # The copy is intentionally one seven-day bounded transaction.
            # Keep the declared estimate at the framework transaction budget;
            # the date predicate, statement timeout, and resumable windows are
            # the actual controls for the set-based operation.
            expected_mode="SET_BASED", estimated_rows=50_000, allow_insert_select=True,
        )
        return self.kernel.rebuild.run_insert_select(
            """INSERT INTO std.daily_kline
                    (stock_code, trade_date, open, high, low, close, volume, amount)
                SELECT stock_code, trade_date, open, high, low, close, volume, amount
                  FROM public.daily_kline
                 WHERE trade_date >= %s AND trade_date < %s
                ON CONFLICT (stock_code, trade_date) DO UPDATE SET
                    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                    close=EXCLUDED.close, volume=EXCLUDED.volume, amount=EXCLUDED.amount""",
            plan, context, params=(start_date, end_date_exclusive),
        )

    def validate_daily_kline_window(
        self, context: RunContext, migration_id: str, start_date: date, end_date_exclusive: date,
    ) -> dict[str, Any]:
        """Return deterministic count/hash evidence for one copied window."""
        if start_date >= end_date_exclusive:
            raise ValueError("daily_kline validation window must be non-empty")
        scope = WorkScope(
            date_from=start_date, date_to=end_date_exclusive - timedelta(days=1),
            partition=f"schema-migration:{migration_id}:daily-kline",
        )
        sql = """SELECT COUNT(*) AS row_count,
                    encode(digest(COALESCE(string_agg(
                        stock_code || '|' || trade_date::text || '|' ||
                        COALESCE(open::text, '') || '|' || COALESCE(high::text, '') || '|' ||
                        COALESCE(low::text, '') || '|' || COALESCE(close::text, '') || '|' ||
                        COALESCE(volume::text, '') || '|' || COALESCE(amount::text, ''),
                        E'\\n' ORDER BY stock_code, trade_date), ''), 'sha256'), 'hex') AS content_hash
                 FROM {table}
                WHERE trade_date >= %s AND trade_date < %s"""
        params = (start_date, end_date_exclusive)
        source = self.kernel.read._repository_query(
            context, "public.daily_kline", sql.format(table="public.daily_kline"), params, scope, 2,
        )[0]
        target = self.kernel.read._repository_query(
            context, "std.daily_kline", sql.format(table="std.daily_kline"), params, scope, 2,
        )[0]
        return {
            "start_date": start_date,
            "end_date_exclusive": end_date_exclusive,
            "source_row_count": int(source["row_count"] or 0),
            "target_row_count": int(target["row_count"] or 0),
            "source_content_hash": source["content_hash"],
            "target_content_hash": target["content_hash"],
        }

    def prepare_adj_factor_daily_window(
        self, context: RunContext, migration_id: str,
        start_date: date, end_date_exclusive: date,
    ) -> int:
        if start_date >= end_date_exclusive:
            raise ValueError("adj_factor_daily migration window must be non-empty")
        scope = WorkScope(
            date_from=start_date, date_to=end_date_exclusive - timedelta(days=1),
            partition=f"schema-migration:{migration_id}:adj-factor-daily",
        )
        plan = SqlPlan(
            operation=Operation.REBUILD, target="std.adj_factor_daily", scope=scope,
            expected_mode="SET_BASED", estimated_rows=50_000, allow_insert_select=True,
        )
        return self.kernel.rebuild.run_insert_select(
            """INSERT INTO std.adj_factor_daily
                    (stock_code, trade_date, adj_factor, source, fetched_at)
                SELECT stock_code, trade_date, adj_factor, source, fetched_at
                  FROM public.adj_factor_daily
                 WHERE trade_date >= %s AND trade_date < %s
                ON CONFLICT (stock_code, trade_date, source) DO UPDATE SET
                    adj_factor=EXCLUDED.adj_factor, fetched_at=EXCLUDED.fetched_at""",
            plan, context, params=(start_date, end_date_exclusive),
        )

    def validate_adj_factor_daily_window(
        self, context: RunContext, migration_id: str,
        start_date: date, end_date_exclusive: date,
    ) -> dict[str, Any]:
        if start_date >= end_date_exclusive:
            raise ValueError("adj_factor_daily validation window must be non-empty")
        scope = WorkScope(
            date_from=start_date, date_to=end_date_exclusive - timedelta(days=1),
            partition=f"schema-migration:{migration_id}:adj-factor-daily",
        )
        sql = """SELECT COUNT(*) AS row_count,
                    encode(digest(COALESCE(string_agg(
                        stock_code || '|' || trade_date::text || '|' ||
                        COALESCE(adj_factor::text, '') || '|' || source || '|' || fetched_at::text,
                        E'\\n' ORDER BY stock_code, trade_date, source), ''), 'sha256'), 'hex') AS content_hash
                 FROM {table}
                WHERE trade_date >= %s AND trade_date < %s"""
        params = (start_date, end_date_exclusive)
        source = self.kernel.read._repository_query(
            context, "public.adj_factor_daily", sql.format(table="public.adj_factor_daily"), params, scope, 2,
        )[0]
        target = self.kernel.read._repository_query(
            context, "std.adj_factor_daily", sql.format(table="std.adj_factor_daily"), params, scope, 2,
        )[0]
        return {
            "start_date": start_date, "end_date_exclusive": end_date_exclusive,
            "source_row_count": int(source["row_count"] or 0),
            "target_row_count": int(target["row_count"] or 0),
            "source_content_hash": source["content_hash"],
            "target_content_hash": target["content_hash"],
        }

    def prepare_valuation_daily_window(
        self, context: RunContext, migration_id: str,
        start_date: date, end_date_exclusive: date,
    ) -> int:
        if start_date >= end_date_exclusive:
            raise ValueError("valuation_daily migration window must be non-empty")
        scope = WorkScope(
            date_from=start_date, date_to=end_date_exclusive - timedelta(days=1),
            partition=f"schema-migration:{migration_id}:valuation-daily",
        )
        plan = SqlPlan(
            operation=Operation.REBUILD, target="std.valuation_daily", scope=scope,
            expected_mode="SET_BASED", estimated_rows=50_000, allow_insert_select=True,
        )
        return self.kernel.rebuild.run_insert_select(
            """INSERT INTO std.valuation_daily
                    (stock_code, trade_date, turnover_rate, pe, pe_ttm, pb, ps, ps_ttm,
                     dv_ratio, total_mv, circ_mv, source, fetched_at, pe_ttm_vendor,
                     ps_ttm_vendor, pb_vendor, dv_ttm, dv_ttm_vendor, dv_ttm_source,
                     source_observed_at, data_available_timestamp)
                SELECT stock_code, trade_date, turnover_rate, pe, pe_ttm, pb, ps, ps_ttm,
                       dv_ratio, total_mv, circ_mv, source, fetched_at, pe_ttm_vendor,
                       ps_ttm_vendor, pb_vendor, dv_ttm, dv_ttm_vendor, dv_ttm_source,
                       source_observed_at, data_available_timestamp
                  FROM public.valuation_daily
                 WHERE trade_date >= %s AND trade_date < %s
                ON CONFLICT (stock_code, trade_date, source) DO UPDATE SET
                    turnover_rate=EXCLUDED.turnover_rate, pe=EXCLUDED.pe,
                    pe_ttm=EXCLUDED.pe_ttm, pb=EXCLUDED.pb, ps=EXCLUDED.ps,
                    ps_ttm=EXCLUDED.ps_ttm, dv_ratio=EXCLUDED.dv_ratio,
                    total_mv=EXCLUDED.total_mv, circ_mv=EXCLUDED.circ_mv,
                    fetched_at=EXCLUDED.fetched_at, pe_ttm_vendor=EXCLUDED.pe_ttm_vendor,
                    ps_ttm_vendor=EXCLUDED.ps_ttm_vendor, pb_vendor=EXCLUDED.pb_vendor,
                    dv_ttm=EXCLUDED.dv_ttm, dv_ttm_vendor=EXCLUDED.dv_ttm_vendor,
                    dv_ttm_source=EXCLUDED.dv_ttm_source,
                    source_observed_at=EXCLUDED.source_observed_at,
                    data_available_timestamp=EXCLUDED.data_available_timestamp""",
            plan, context, params=(start_date, end_date_exclusive),
        )

    def validate_valuation_daily_window(
        self, context: RunContext, migration_id: str,
        start_date: date, end_date_exclusive: date,
    ) -> dict[str, Any]:
        if start_date >= end_date_exclusive:
            raise ValueError("valuation_daily validation window must be non-empty")
        scope = WorkScope(
            date_from=start_date, date_to=end_date_exclusive - timedelta(days=1),
            partition=f"schema-migration:{migration_id}:valuation-daily",
        )
        sql = """SELECT COUNT(*) AS row_count,
                    encode(digest(COALESCE(string_agg(md5(row_to_json(t)::text), ''
                        ORDER BY t.stock_code, t.trade_date, t.source), ''), 'sha256'), 'hex') AS content_hash
                 FROM {table} t
                WHERE t.trade_date >= %s AND t.trade_date < %s"""
        params = (start_date, end_date_exclusive)
        source = self.kernel.read._repository_query(
            context, "public.valuation_daily", sql.format(table="public.valuation_daily"), params, scope, 2,
        )[0]
        target = self.kernel.read._repository_query(
            context, "std.valuation_daily", sql.format(table="std.valuation_daily"), params, scope, 2,
        )[0]
        return {
            "start_date": start_date, "end_date_exclusive": end_date_exclusive,
            "source_row_count": int(source["row_count"] or 0),
            "target_row_count": int(target["row_count"] or 0),
            "source_content_hash": source["content_hash"],
            "target_content_hash": target["content_hash"],
        }

    def financial_standard_upper_id(
        self, context: RunContext, source_table: str,
    ) -> int:
        """Return a bounded migration upper watermark for one financial table."""
        spec = self.FINANCIAL_STANDARD_SPECS.get(source_table)
        if spec is None:
            raise ValueError(f"unsupported financial Standard source: {source_table}")
        scope = self._scope(f"financial-standard-bounds:{source_table}")
        key = spec["key"]
        rows = self.kernel.read._repository_query(
            context,
            f"public.{source_table}",
            f"SELECT COALESCE(MAX({key}), 0) AS upper_id "
            f"FROM public.{source_table}",
            (), scope, 2,
        )
        return int(rows[0]["upper_id"] or 0)

    def prepare_financial_standard_window(
        self, context: RunContext, migration_id: str,
        source_table: str, start_id: int, end_id: int,
    ) -> int:
        """Copy one primary-key window into a same-shape Standard target.

        The source/target table names are selected only from the fixed spec
        above.  This is a named migration operation, not a generic SQL runner.
        ``ON CONFLICT DO NOTHING`` makes a retried committed window idempotent;
        the following content-hash validation detects any unexpected drift.
        """
        spec = self.FINANCIAL_STANDARD_SPECS.get(source_table)
        if spec is None:
            raise ValueError(f"unsupported financial Standard source: {source_table}")
        if start_id < 0 or end_id <= start_id:
            raise ValueError("financial Standard migration window is invalid")
        target = spec["target"]
        key = spec["key"]
        scope = WorkScope(partition=f"schema-migration:{migration_id}:{source_table}")
        plan = SqlPlan(
            operation=Operation.REBUILD, target=target, scope=scope,
            expected_mode="SET_BASED", estimated_rows=end_id - start_id,
            allow_insert_select=True,
        )
        return self.kernel.rebuild.run_insert_select(
            f"""INSERT INTO {target}
                    SELECT * FROM public.{source_table}
                     WHERE {key} > %s AND {key} <= %s
                    ON CONFLICT ({key}) DO NOTHING""",
            plan, context, params=(start_id, end_id),
        )

    def validate_financial_standard_window(
        self, context: RunContext, migration_id: str,
        source_table: str, start_id: int, end_id: int,
    ) -> dict[str, Any]:
        """Compare one bounded window without building a table-sized aggregate."""
        spec = self.FINANCIAL_STANDARD_SPECS.get(source_table)
        if spec is None:
            raise ValueError(f"unsupported financial Standard source: {source_table}")
        if start_id < 0 or end_id <= start_id:
            raise ValueError("financial Standard validation window is invalid")
        key = spec["key"]
        target = spec["target"]
        scope = WorkScope(partition=f"schema-migration:{migration_id}:{source_table}")

        def rows_and_hash(source: str, policy_source: str) -> tuple[int, str]:
            rows = self.kernel.read._repository_query(
                context,
                policy_source,
                f"SELECT row_to_json(t)::text AS row_json "
                f"FROM {source} t WHERE t.{key} > %s AND t.{key} <= %s "
                f"ORDER BY t.{key}",
                (start_id, end_id), scope, end_id - start_id,
            )
            digest = hashlib.sha256()
            for row in rows:
                value = str(row["row_json"]).encode("utf-8")
                digest.update(len(value).to_bytes(8, "big"))
                digest.update(value)
            return len(rows), digest.hexdigest()

        source_count, source_hash = rows_and_hash(
            f"public.{source_table}", f"public.{source_table}",
        )
        target_count, target_hash = rows_and_hash(target, target)
        return {
            "start_id": start_id,
            "end_id": end_id,
            "source_row_count": source_count,
            "target_row_count": target_count,
            "source_content_hash": source_hash,
            "target_content_hash": target_hash,
        }

    def stock_financial_indicator_bounds(
        self, context: RunContext, migration_id: str,
    ) -> tuple[date, date] | None:
        scope = self._scope(f"stock-financial-indicator-bounds:{migration_id}")
        rows = self.kernel.read._repository_query(
            context, "public.stock_financial_indicator",
            "SELECT MIN(report_date) AS start_date, MAX(report_date) AS end_date "
            "FROM public.stock_financial_indicator",
            (), scope, 2,
        )
        if not rows or rows[0]["start_date"] is None:
            return None
        return rows[0]["start_date"], rows[0]["end_date"]

    def prepare_stock_financial_indicator_window(
        self, context: RunContext, migration_id: str,
        start_date: date, end_date_exclusive: date,
    ) -> int:
        if start_date >= end_date_exclusive:
            raise ValueError("legacy financial indicator window is invalid")
        scope = WorkScope(
            date_from=start_date, date_to=end_date_exclusive - timedelta(days=1),
            partition=f"schema-migration:{migration_id}:stock-financial-indicator",
        )
        plan = SqlPlan(
            operation=Operation.REBUILD, target=self.LEGACY_FINANCIAL_INDICATOR_TARGET,
            scope=scope, expected_mode="SET_BASED", estimated_rows=10_000,
            allow_insert_select=True,
        )
        return self.kernel.rebuild.run_insert_select(
            f"""INSERT INTO {self.LEGACY_FINANCIAL_INDICATOR_TARGET}
                    SELECT * FROM public.stock_financial_indicator
                     WHERE report_date >= %s AND report_date < %s
                    ON CONFLICT DO NOTHING""",
            plan, context, params=(start_date, end_date_exclusive),
        )

    def validate_stock_financial_indicator_window(
        self, context: RunContext, migration_id: str,
        start_date: date, end_date_exclusive: date,
    ) -> dict[str, Any]:
        if start_date >= end_date_exclusive:
            raise ValueError("legacy financial indicator validation window is invalid")
        scope = WorkScope(
            date_from=start_date, date_to=end_date_exclusive - timedelta(days=1),
            partition=f"schema-migration:{migration_id}:stock-financial-indicator",
        )
        sql = """SELECT row_to_json(t)::text AS row_json
                    FROM {table} t
                   WHERE t.report_date >= %s AND t.report_date < %s
                   ORDER BY t.stock_code, t.report_date, t.announce_date NULLS FIRST, t.source"""

        def rows_and_hash(table: str, policy_source: str) -> tuple[int, str]:
            rows = self.kernel.read._repository_query(
                context, policy_source, sql.format(table=table),
                (start_date, end_date_exclusive), scope, 10_000,
            )
            digest = hashlib.sha256()
            for row in rows:
                value = str(row["row_json"]).encode("utf-8")
                digest.update(len(value).to_bytes(8, "big")); digest.update(value)
            return len(rows), digest.hexdigest()

        source_count, source_hash = rows_and_hash(
            "public.stock_financial_indicator", "public.stock_financial_indicator",
        )
        target_count, target_hash = rows_and_hash(
            self.LEGACY_FINANCIAL_INDICATOR_TARGET, self.LEGACY_FINANCIAL_INDICATOR_TARGET,
        )
        return {
            "start_date": start_date, "end_date_exclusive": end_date_exclusive,
            "source_row_count": source_count, "target_row_count": target_count,
            "source_content_hash": source_hash, "target_content_hash": target_hash,
        }

    def snapshot_date_range(self, context: RunContext) -> tuple[date, date] | None:
        scope = WorkScope(partition="schema-migration:research-snapshot:bounds")
        rows = self.kernel.read._repository_query(
            context, "research.snapshot_manifest",
            "SELECT MIN(research_cutoff_date) AS start_date, MAX(research_cutoff_date) AS end_date "
            "FROM research.snapshot_manifest",
            (), scope, 2,
        )
        if not rows or rows[0]["start_date"] is None:
            return None
        return rows[0]["start_date"], rows[0]["end_date"]

    def prepare_research_snapshot_window(
        self, context: RunContext, migration_id: str,
        start_date: date, end_date_exclusive: date,
    ) -> int:
        """Copy one empty date window into the partitioned target.

        The caller validates the window before invoking this method.  Keeping
        the normal path as a plain INSERT avoids an expensive partition-wide
        ``ON CONFLICT`` probe as the migration target grows.  A retry with
        pre-existing rows must use ``repair_research_snapshot_window``.
        """
        if start_date >= end_date_exclusive:
            raise ValueError("research snapshot migration window must be non-empty")
        scope = WorkScope(
            date_from=start_date, date_to=end_date_exclusive - timedelta(days=1),
            partition=f"schema-migration:{migration_id}:snapshot",
        )
        plan = SqlPlan(
            operation=Operation.REBUILD,
            target="research.stock_snapshot_daily_partitioned",
            scope=scope, expected_mode="SET_BASED", estimated_rows=10_000,
            allow_insert_select=True,
        )
        return self.kernel.rebuild.run_insert_select(
            """INSERT INTO research.stock_snapshot_daily_partitioned
                SELECT s.* FROM research.stock_snapshot_daily s
                 JOIN research.snapshot_manifest m ON m.snapshot_id=s.snapshot_id
                 WHERE m.research_cutoff_date >= %s AND m.research_cutoff_date < %s
                   AND s.research_date >= %s AND s.research_date < %s
                """ ,
            plan, context,
            params=(start_date, end_date_exclusive,
                    start_date, end_date_exclusive),
        )

    def repair_research_snapshot_window(
        self, context: RunContext, migration_id: str,
        start_date: date, end_date_exclusive: date,
    ) -> int:
        """Repair only a window whose source/target evidence differs."""
        if start_date >= end_date_exclusive:
            raise ValueError("research snapshot repair window must be non-empty")
        scope = WorkScope(
            date_from=start_date, date_to=end_date_exclusive - timedelta(days=1),
            partition=f"schema-migration:{migration_id}:snapshot:repair",
        )
        plan = SqlPlan(
            operation=Operation.REBUILD,
            target="research.stock_snapshot_daily_partitioned",
            scope=scope, expected_mode="SET_BASED", estimated_rows=10_000,
            allow_insert_select=True, allow_delete=True,
        )
        return self.kernel.rebuild.run_scoped_merge(
            """DELETE FROM research.stock_snapshot_daily_partitioned t
                  USING research.snapshot_manifest m
                 WHERE m.snapshot_id=t.snapshot_id
                   AND m.research_cutoff_date >= %s AND m.research_cutoff_date < %s
                   AND t.research_date >= %s AND t.research_date < %s;
                INSERT INTO research.stock_snapshot_daily_partitioned
                SELECT s.* FROM research.stock_snapshot_daily s
                 JOIN research.snapshot_manifest m ON m.snapshot_id=s.snapshot_id
                 WHERE m.research_cutoff_date >= %s AND m.research_cutoff_date < %s
                   AND s.research_date >= %s AND s.research_date < %s
                """ ,
            plan, context,
            params=(start_date, end_date_exclusive,
                    start_date, end_date_exclusive,
                    start_date, end_date_exclusive),
        )

    def validate_research_snapshot_window(
        self, context: RunContext, migration_id: str,
        start_date: date, end_date_exclusive: date,
    ) -> dict[str, Any]:
        if start_date >= end_date_exclusive:
            raise ValueError("research snapshot validation window must be non-empty")
        scope = WorkScope(
            date_from=start_date, date_to=end_date_exclusive - timedelta(days=1),
            partition=f"schema-migration:{migration_id}:snapshot",
        )
        sql = """SELECT COUNT(*) AS row_count,
                    md5(COALESCE(string_agg(md5(row_to_json(t)::text), ''
                        ORDER BY t.snapshot_id, t.research_date, t.ts_code), '')) AS content_hash
                 FROM {table} t
                 JOIN research.snapshot_manifest m ON m.snapshot_id=t.snapshot_id
                WHERE m.research_cutoff_date >= %s AND m.research_cutoff_date < %s
                  AND t.research_date >= %s AND t.research_date < %s"""
        params = (start_date, end_date_exclusive, start_date, end_date_exclusive)
        source = self.kernel.read._repository_query(
            context, "research.stock_snapshot_daily",
            sql.format(table="research.stock_snapshot_daily"), params, scope, 2,
        )[0]
        target = self.kernel.read._repository_query(
            context, "research.stock_snapshot_daily_partitioned",
            sql.format(table="research.stock_snapshot_daily_partitioned"), params, scope, 2,
        )[0]
        return {
            "start_date": start_date,
            "end_date_exclusive": end_date_exclusive,
            "source_row_count": int(source["row_count"] or 0),
            "target_row_count": int(target["row_count"] or 0),
            "source_content_hash": source["content_hash"],
            "target_content_hash": target["content_hash"],
        }

    def cutover_research_snapshot_partitioned(
        self, context: RunContext, migration_id: str, approved_by: str,
    ) -> str:
        if not approved_by.strip():
            raise ValueError("approved_by is required for partitioned snapshot cutover")
        current = self.get(context, migration_id)
        if not current or current["status"] != "VALIDATED":
            raise ValueError("snapshot partition cutover requires VALIDATED migration")
        backup = self.kernel.rebuild.swap_research_snapshot_partitioned(
            context, self._scope(migration_id), migration_id, approved_by,
        )
        return backup

    def cutover_daily_kline_partitioned(
        self, context: RunContext, migration_id: str, approved_by: str,
    ) -> str:
        if not approved_by.strip():
            raise ValueError("approved_by is required for daily_kline cutover")
        current = self.get(context, migration_id)
        if not current or current["status"] != "VALIDATED":
            raise ValueError("daily_kline cutover requires VALIDATED migration")
        return self.kernel.rebuild.swap_daily_kline_partitioned(
            context, self._scope(migration_id), migration_id, approved_by,
        )

    def cutover_adj_factor_daily_partitioned(
        self, context: RunContext, migration_id: str, approved_by: str,
    ) -> str:
        if not approved_by.strip():
            raise ValueError("approved_by is required for adj_factor_daily cutover")
        current = self.get(context, migration_id)
        if not current or current["status"] != "VALIDATED":
            raise ValueError("adj_factor_daily cutover requires VALIDATED migration")
        return self.kernel.rebuild.swap_adj_factor_daily_partitioned(
            context, self._scope(migration_id), migration_id, approved_by,
        )

    def cutover_valuation_daily_partitioned(
        self, context: RunContext, migration_id: str, approved_by: str,
    ) -> str:
        if not approved_by.strip():
            raise ValueError("approved_by is required for valuation_daily cutover")
        current = self.get(context, migration_id)
        if not current or current["status"] != "VALIDATED":
            raise ValueError("valuation_daily cutover requires VALIDATED migration")
        return self.kernel.rebuild.swap_valuation_daily_partitioned(
            context, self._scope(migration_id), migration_id, approved_by,
        )

    def cutover_financial_standard(
        self, context: RunContext, migration_id: str,
        source_table: str, approved_by: str,
    ) -> str:
        if source_table not in self.FINANCIAL_STANDARD_SPECS and source_table != "stock_financial_indicator":
            raise ValueError(f"unsupported financial Standard source: {source_table}")
        if not approved_by.strip():
            raise ValueError("approved_by is required for financial Standard cutover")
        current = self.get(context, migration_id)
        if not current or current["status"] != "VALIDATED":
            raise ValueError("financial Standard cutover requires VALIDATED migration")
        backup = self.kernel.rebuild.retire_legacy_financial_standard(
            context, self._scope(migration_id), migration_id, source_table, approved_by,
        )
        # The catalog rename and the control-plane status update are kept as
        # two named, short operations.  The former is executed by the
        # SECURITY DEFINER function; this update records the auditable result
        # through the normal Repository -> Kernel write path.
        self.cutover(context, migration_id, approved_by)
        return backup

    def cutover(self, context: RunContext, migration_id: str, approved_by: str) -> int:
        if not approved_by.strip():
            raise ValueError("approved_by is required for schema cutover")
        current = self.get(context, migration_id)
        if not current:
            raise ValueError(f"unknown migration_id: {migration_id}")
        if current["status"] != "VALIDATED":
            raise ValueError("cutover requires current status VALIDATED")
        return self.kernel.write.upsert(
            context, self._scope(migration_id), self.TARGET, [{
                "migration_id": migration_id, "source_table": current["source_table"],
                "target_table": current["target_table"], "status": "CUTOVER",
                "cutover_at": datetime.now(timezone.utc), "approved_by": approved_by,
            }], key=("migration_id",), update_columns=("status", "cutover_at", "approved_by"),
            skip_noop=False,
        )

    def rollback(self, context: RunContext, migration_id: str, reason: str) -> int:
        if not reason.strip():
            raise ValueError("rollback reason is required")
        current = self.get(context, migration_id)
        if not current:
            raise ValueError(f"unknown migration_id: {migration_id}")
        if current["status"] != "CUTOVER":
            raise ValueError("rollback requires current status CUTOVER")
        return self.kernel.write.upsert(
            context, self._scope(migration_id), self.TARGET, [{
                "migration_id": migration_id, "source_table": current["source_table"],
                "target_table": current["target_table"], "status": "ROLLED_BACK",
                "rollback_at": datetime.now(timezone.utc), "error_message": reason,
            }], key=("migration_id",), update_columns=("status", "rollback_at", "error_message"),
            skip_noop=False,
        )

    def get(self, context: RunContext, migration_id: str) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, self.TARGET,
            """SELECT migration_id,source_table,target_table,status,
                             source_row_count,target_row_count,source_content_hash,
                             target_content_hash,validated_at,cutover_at,rollback_at,
                             approved_by,notes,error_message
                        FROM ops.foundation_table_migration
                       WHERE migration_id=%s""",
            (migration_id,), self._scope(migration_id), 2,
        )
        return rows[0] if rows else None
