"""FinancialRepository：财务领域仓储。

财务 v2 的复杂读取和发布都集中在这里；业务层只接收普通 Python 行，
不持有 Cursor/Connection，也不拼接 SQL。
"""
from __future__ import annotations

from datetime import date
import json
from datetime import datetime
import hashlib
from collections.abc import Mapping
from typing import Any, Iterable

from data_collect.data_access.context import RunContext, RebuildMode, WorkScope
from data_collect.data_access.repository.base import BaseRepository

CONTRACT_VERSION = "feature-contract-v2.0"


class FinancialRepository(BaseRepository):
    # Financial feature calculation already works in five-stock partitions.
    # Keep the repository read bounded even when a caller supplies a larger
    # code set, instead of bypassing DatasetPolicy with a 500k-row request.
    _EVENT_CODE_BATCH_SIZE = 5
    _LEGACY_RAW_TARGETS = frozenset({
        "tushare_fina_indicator_raw",
        "tushare_income_raw",
        "tushare_balancesheet_raw",
        "tushare_cashflow_raw",
    })
    _LEGACY_XT_TARGETS = {
        "fin_balance": ("stock_code", "report_date", "announce_date"),
        "fin_income": ("stock_code", "report_date", "announce_date"),
        "fin_cashflow": ("stock_code", "report_date", "announce_date"),
        "fin_indicator": ("stock_code", "report_date", "announce_date"),
        "fin_capital": ("stock_code", "report_date"),
        "fin_holdernum": ("stock_code", "end_date"),
        "fin_top10_holder": ("stock_code", "end_date", "rank"),
        "fin_top10_flow_holder": ("stock_code", "end_date", "rank"),
    }

    def save_legacy_xt_table(self, target: str, rows: Iterable[dict[str, Any]], context: RunContext) -> tuple[int, int]:
        key = self._LEGACY_XT_TARGETS.get(target)
        if key is None:
            raise ValueError(f"unsupported legacy XT financial target: {target}")
        materialized = list(rows)
        if not materialized:
            return 0, 0
        if any(column not in materialized[0] for column in key):
            raise ValueError(f"financial rows missing primary key for {target}")
        update_columns = tuple(column for column in materialized[0] if column not in key)
        count = self.kernel.write.upsert(
            context, WorkScope(partition=f"legacy-xt:{target}"), target, materialized,
            key=key, update_columns=update_columns, skip_noop=True,
        )
        return len(materialized), count

    def append_legacy_raw_reports(
        self, target: str, rows: Iterable[dict[str, Any]], codes: Iterable[str], context: RunContext
    ) -> int:
        materialized = list(rows)
        if target not in self._LEGACY_RAW_TARGETS:
            raise ValueError(f"unsupported legacy financial raw target: {target}")
        if not materialized:
            return 0
        columns = ("ts_code", "ann_date", "end_date", "payload")
        if any(not all(column in row for column in columns) for row in materialized):
            raise ValueError("legacy financial raw rows are incomplete")
        scope = WorkScope(codes=tuple(codes), partition=target)
        return self.kernel.raw.append_payload_rows(
            context=context,
            target=target,
            rows=materialized,
            business_key=("ts_code", "ann_date", "end_date"),
            scope=scope,
        )

    def save_financial_versions(
        self, dataset: str, records: Iterable[dict[str, Any]], batch_id: str, context: RunContext,
    ) -> int:
        if dataset not in {"income", "balancesheet", "cashflow"}:
            raise ValueError(f"unsupported financial version dataset: {dataset}")
        materialized = list(records)
        if not materialized:
            return 0
        rows = []
        for record in materialized:
            clean = {key: value for key, value in record.items() if not key.startswith("_")}
            encoded = json.dumps(clean, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)
            rows.append({
                "dataset": dataset,
                "business_key": ":".join(str(clean.get(key) or "") for key in ("ts_code", "end_date", "report_type", "comp_type", "end_type")),
                "ts_code": clean.get("ts_code"), "ann_date": self._api_date(clean.get("ann_date")),
                "f_ann_date": self._api_date(clean.get("f_ann_date")), "end_date": self._api_date(clean.get("end_date")),
                "report_type": clean.get("report_type"), "comp_type": clean.get("comp_type"),
                "end_type": clean.get("end_type"), "update_flag": clean.get("update_flag"),
                "payload": encoded, "source": "tushare", "batch_id": batch_id,
                "payload_hash": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
                "fetched_at": record.get("_fetched_at", datetime.now()),
            })
        return self.kernel.raw.append(
            context=context, dataset="tushare_financial_raw_version", rows=rows,
            business_key=("dataset", "business_key"),
        )

    def backfill_financial_versions(
        self, dataset: str, source: str, batch_size: int, batch_id: str, context: RunContext,
    ) -> int:
        if batch_size <= 0 or batch_size > 5000:
            raise ValueError("batch_size must be in 1..5000")
        if source not in {"tushare_income_raw", "tushare_balancesheet_raw", "tushare_cashflow_raw"}:
            raise ValueError(f"unsupported financial Raw source: {source}")
        last_key = None
        inserted = 0
        scope = WorkScope(partition=f"financial-backfill:{dataset}")
        while True:
            if last_key is None:
                sql = f"""SELECT ts_code, ann_date, end_date, payload, fetched_at FROM {source}
                           ORDER BY ts_code, ann_date, end_date LIMIT %s"""
                params = (batch_size,)
            else:
                sql = f"""SELECT ts_code, ann_date, end_date, payload, fetched_at FROM {source}
                           WHERE (ts_code, ann_date, end_date) > (%s, %s, %s)
                           ORDER BY ts_code, ann_date, end_date LIMIT %s"""
                params = (*last_key, batch_size)
            rows = self.kernel.read._repository_query(context, source, sql, params=params, scope=scope, max_rows=batch_size)
            if not rows:
                break
            records = []
            for row in rows:
                record = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
                record["_fetched_at"] = row["fetched_at"]
                records.append(record)
            inserted += self.save_financial_versions(dataset, records, batch_id, context)
            last_key = (rows[-1]["ts_code"], rows[-1]["ann_date"], rows[-1]["end_date"])
        return inserted

    def upsert_legacy_indicator(
        self, rows: Iterable[dict[str, Any]], codes: Iterable[str], context: RunContext
    ) -> int:
        materialized = list(rows)
        if not materialized:
            return 0
        key = ("stock_code", "report_date", "announce_date", "source")
        update_columns = tuple(column for column in materialized[0] if column not in key)
        return self.kernel.write.upsert(
            context=context,
            scope=WorkScope(codes=tuple(codes), partition="legacy-financial-indicator"),
            target="std.stock_financial_indicator",
            rows=materialized,
            key=key,
            update_columns=update_columns,
        )

    @staticmethod
    def _api_date(value: Any):
        if value in (None, ""):
            return None
        parsed = str(value).replace("-", "")
        if len(parsed) == 8 and parsed.isdigit():
            return date(int(parsed[:4]), int(parsed[4:6]), int(parsed[6:8]))
        return value

    def append_api_rows(
        self, api_name: str, records: Iterable[dict[str, Any]], codes: Iterable[str],
        batch_id: str, context: RunContext,
    ) -> int:
        """Persist one API page to immutable Raw and its typed version layer."""
        if api_name not in {"income", "balancesheet", "cashflow", "fina_indicator"}:
            raise ValueError(f"unsupported financial API: {api_name}")
        source_rows = list(records)
        if not source_rows:
            return 0
        raw_target = {
            "income": "tushare_income_raw",
            "balancesheet": "tushare_balancesheet_raw",
            "cashflow": "tushare_cashflow_raw",
            "fina_indicator": "tushare_fina_indicator_raw",
        }[api_name]
        now = datetime.now()
        raw_rows = []
        version_rows = []
        for record in source_rows:
            clean = {key: value for key, value in record.items() if not key.startswith("_")}
            encoded = json.dumps(clean, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)
            digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
            ts_code = clean.get("ts_code")
            ann_date = self._api_date(clean.get("ann_date"))
            f_ann_date = self._api_date(clean.get("f_ann_date"))
            end_date = self._api_date(clean.get("end_date"))
            raw_rows.append({
                "ts_code": ts_code, "ann_date": ann_date, "end_date": end_date,
                "payload": encoded,
            })
            business_key = ":".join(str(clean.get(key) or "") for key in (
                "ts_code", "end_date", "report_type", "comp_type", "end_type",
            ))
            base = {
                "business_key": business_key, "ts_code": ts_code,
                "ann_date": ann_date, "f_ann_date": f_ann_date, "end_date": end_date,
                "report_type": clean.get("report_type"), "comp_type": clean.get("comp_type"),
                "end_type": clean.get("end_type"), "update_flag": clean.get("update_flag"),
                "payload": encoded, "source": "tushare", "batch_id": batch_id,
                "payload_hash": digest, "fetched_at": record.get("_fetched_at", now),
            }
            if api_name == "fina_indicator":
                version_rows.append(base)
            else:
                version_rows.append({"dataset": api_name, **base})
        scope = WorkScope(codes=tuple(codes), partition=f"financial-api:{api_name}")
        self.kernel.raw.append_payload_rows(
            context=context, target=raw_target, rows=raw_rows,
            business_key=("ts_code", "ann_date", "end_date"), scope=scope,
        )
        version_target = "fina_indicator_raw_version" if api_name == "fina_indicator" else "tushare_financial_raw_version"
        version_key = ("business_key", "payload_hash") if api_name == "fina_indicator" else ("dataset", "business_key", "payload_hash")
        return self.kernel.raw.append(
            context=context, dataset=version_target, rows=version_rows,
            business_key=tuple(column for column in version_key if column != "payload_hash"),
        )
    _EVENTS_SQL = """
        SELECT 'income' AS dataset, stock_code AS ts_code, report_date,
               data_available_timestamp, source_observed_at, report_type,
               comp_type, end_type, source_record_key, record_version,
               payload_hash, vintage_quality,
               jsonb_build_object(
                   'revenue', revenue, 'oper_cost', oper_cost,
                   'total_cogs', total_cogs,
                   'net_profit_consolidated', net_profit_consolidated,
                   'net_profit_parent', net_profit_parent) AS values
          FROM std.financial_income_standard
         WHERE stock_code=ANY(%s) AND report_date BETWEEN %s AND %s
           AND COALESCE(report_type,'1')='1'
           AND data_available_timestamp IS NOT NULL
           AND data_available_timestamp <
               (%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'
        UNION ALL
        SELECT 'cashflow', stock_code, report_date, data_available_timestamp,
               source_observed_at, report_type, comp_type, end_type,
               source_record_key, record_version, payload_hash, vintage_quality,
               jsonb_build_object('operating_cashflow_ytd', operating_cashflow_ytd)
          FROM std.financial_cash_flow_standard
         WHERE stock_code=ANY(%s) AND report_date BETWEEN %s AND %s
           AND COALESCE(report_type,'1')='1'
           AND data_available_timestamp IS NOT NULL
           AND data_available_timestamp <
               (%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'
        UNION ALL
        SELECT 'balance', stock_code, report_date, data_available_timestamp,
               source_observed_at, report_type, comp_type, end_type,
               source_record_key, record_version, payload_hash, vintage_quality,
               jsonb_build_object(
                   'parent_equity_end', parent_equity_end,
                   'total_equity_end', total_equity_end,
                   'total_assets', total_assets, 'total_liab', total_liab)
          FROM std.financial_balance_sheet_standard
         WHERE stock_code=ANY(%s) AND report_date BETWEEN %s AND %s
           AND COALESCE(report_type,'1')='1'
           AND data_available_timestamp IS NOT NULL
           AND data_available_timestamp <
               (%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'
        UNION ALL
        SELECT 'indicator', ts_code, report_date, data_available_timestamp,
               source_observed_at, report_type,
               COALESCE(linked_comp_type, comp_type), end_type,
               source_record_key, record_version, payload_hash, vintage_quality,
               jsonb_build_object(
                   'roe_reported', roe_reported,
                   'roe_annualized', roe_annualized,
                   'roe_quarterly', roe_quarterly,
                   'debt_to_assets', debt_to_assets)
          FROM std.fina_indicator_standard
         WHERE ts_code=ANY(%s) AND report_date BETWEEN %s AND %s
           AND COALESCE(report_type,'1')='1'
           AND data_available_timestamp IS NOT NULL
           AND data_available_timestamp <
               (%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'
         ORDER BY ts_code, data_available_timestamp, report_date, dataset,
                  record_version
    """

    def load_events(
        self, codes: Iterable[str], warmup_start: date, output_end: date, context: RunContext
    ) -> list[dict[str, Any]]:
        codes = tuple(codes)
        if not codes or warmup_start > output_end:
            return []
        read_limit = min(
            100_000,
            self.kernel.policy_registry.require("std.financial_income_standard").max_read_rows,
        )
        events: list[dict[str, Any]] = []
        for offset in range(0, len(codes), self._EVENT_CODE_BATCH_SIZE):
            code_batch = codes[offset : offset + self._EVENT_CODE_BATCH_SIZE]
            params: list[Any] = []
            for _ in range(4):
                params.extend((list(code_batch), warmup_start, output_end, output_end))
            scope = WorkScope(
                date_from=warmup_start, date_to=output_end, codes=code_batch,
                dimensions={"date_column": "report_date", "code_column": "ts_code"},
            )
            events.extend(self.kernel.read._repository_query(
                context=context,
                source="std.financial_income_standard",
                sql=self._EVENTS_SQL,
                params=params,
                scope=scope,
                max_rows=read_limit,
            ))
        return events

    def stock_codes(
        self, start: date, end: date, context: RunContext, requested: Iterable[str] | None = None
    ) -> list[str]:
        scope = WorkScope(date_from=start, date_to=end, codes=tuple(requested or ()))
        requested_codes = tuple(requested or ())
        code_filter = " AND stock_code=ANY(%s)" if requested_codes else ""
        params: list[Any] = [start, end]
        if requested_codes:
            params.append(list(requested_codes))
        rows = self.kernel.read._repository_query(
            context=context,
            source="std.financial_income_standard",
            sql=(
                "SELECT DISTINCT stock_code FROM std.financial_income_standard "
                "WHERE report_date BETWEEN %s AND %s" + code_filter +
                " ORDER BY stock_code"
            ),
            params=params,
            scope=scope,
            max_rows=self.kernel.policy_registry.require(
                "std.financial_income_standard"
            ).max_read_rows,
        )
        return [str(row["stock_code"]) for row in rows]

    def publish_feature_partition(
        self,
        target: str,
        rows: Iterable[dict[str, Any]],
        codes: Iterable[str],
        output_start: date,
        output_end: date,
        context: RunContext,
    ) -> int:
        codes = tuple(codes)
        scope = WorkScope(
            date_from=output_start, date_to=output_end, codes=codes,
            dimensions={"date_column": "report_date", "code_column": "ts_code"},
        )
        normalized_rows = self._normalize_feature_rows(rows)
        return self.kernel.rebuild.publish(
            context=context,
            scope=scope,
            target=target,
            producer=lambda _scope: normalized_rows,
            mode=RebuildMode.SCOPED_MERGE,
        )

    @staticmethod
    def _normalize_feature_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Encode the feature JSONB field at the Repository/database boundary.

        The production connection may use the COPY fallback path, whose driver
        adapter accepts scalar values but not a raw Python dict.  Keep JSONB
        knowledge in this typed Repository rather than teaching business code
        about driver adapters or weakening the generic Gateway contract.
        """
        normalized = []
        for row in rows:
            item = dict(row)
            lineage = item.get("component_lineage")
            if isinstance(lineage, (Mapping, list, tuple)):
                item["component_lineage"] = json.dumps(
                    lineage, ensure_ascii=False, sort_keys=True,
                    default=str, allow_nan=False,
                )
            normalized.append(item)
        return normalized

    _VALUATION_COLUMNS = (
        "stock_code,trade_date,pe_ttm,pb,ps_ttm,dv_ratio,total_mv,circ_mv,"
        "pe_ttm_vendor,ps_ttm_vendor,pb_vendor,dv_ttm,dv_ttm_vendor,"
        "dv_ttm_source,source_observed_at,data_available_timestamp"
    )

    def valuation_codes(
        self,
        warmup_start: date,
        output_end: date,
        context: RunContext,
        requested: Iterable[str] | None = None,
        *,
        selection_start: date | None = None,
    ) -> list[str]:
        """Return codes having source rows in the requested output window.

        ``warmup_start`` is the read horizon needed by the feature
        calculation.  It must not decide which securities are published:
        doing so would include delisted securities whose last observation is
        years before the requested output window and would create an empty
        scoped merge that the quality gate correctly rejects.
        """
        selection_start = selection_start or warmup_start
        params: list[Any] = [selection_start, output_end]
        code_clause = ""
        if requested:
            code_clause = " AND stock_code=ANY(%s)"
            params.append(list(requested))
        sql = (
            "SELECT DISTINCT stock_code FROM std.valuation_daily "
            "WHERE trade_date BETWEEN %s AND %s" + code_clause +
            " ORDER BY stock_code"
        )
        scope = WorkScope(
            date_from=selection_start,
            date_to=output_end,
            codes=tuple(requested or ()),
        )
        # Do not use public read.query here: it applies the 100k-row detail
        # limit before deduplication, which can silently omit securities when
        # the source contains millions of daily observations.  The Repository
        # query returns the bounded distinct security set directly.
        rows = self.kernel.read._repository_query(
            context=context,
            source="std.valuation_daily",
            sql=sql,
            params=params,
            scope=scope,
            max_rows=self.kernel.policy_registry.require(
                "std.valuation_daily"
            ).max_read_rows,
        )
        return sorted({str(row["stock_code"]) for row in rows})

    def load_valuation_input(
        self,
        code: str,
        warmup_start: date,
        output_end: date,
        context: RunContext,
    ) -> list[dict[str, Any]]:
        rows = self.load_valuation_inputs((code,), warmup_start, output_end, context)
        return rows

    def load_valuation_inputs(
        self,
        codes: Iterable[str],
        warmup_start: date,
        output_end: date,
        context: RunContext,
    ) -> list[dict[str, Any]]:
        codes = tuple(codes)
        if not codes:
            return []
        sql = f"""
            SELECT {self._VALUATION_COLUMNS}
              FROM std.valuation_daily
             WHERE stock_code=ANY(%s) AND trade_date BETWEEN %s AND %s
             ORDER BY stock_code, trade_date
        """
        scope = WorkScope(
            date_from=warmup_start,
            date_to=output_end,
            codes=codes,
        )
        return self.kernel.read._repository_query(
            context=context,
            source="std.valuation_daily",
            sql=sql,
            params=(list(codes), warmup_start, output_end),
            scope=scope,
            max_rows=100_000,
        )

    def publish_valuation_partition(
        self,
        target: str,
        code: str,
        rows: Iterable[dict[str, Any]],
        output_start: date,
        output_end: date,
        context: RunContext,
    ) -> int:
        scope = WorkScope(
            date_from=output_start,
            date_to=output_end,
            codes=(code,),
            dimensions={"date_column": "trade_date", "code_column": "ts_code"},
        )
        return self.kernel.rebuild.publish(
            context=context,
            scope=scope,
            target=target,
            producer=lambda _scope: rows,
            mode=RebuildMode.SCOPED_MERGE,
        )

    def publish_valuation_partitions(
        self,
        target: str,
        partitions: Iterable[tuple[str, Iterable[dict[str, Any]]]],
        output_start: date,
        output_end: date,
        context: RunContext,
        *,
        max_rows: int = 20_000,
    ) -> int:
        """Publish one bounded stock batch in a single scoped merge.

        The old per-stock method remains available for explicit callers, but
        daily valuation rebuilds should use this batch method.  It reduces
        thousands of tiny transactions to one transaction per bounded stock
        batch without allowing an unbounded materialization.
        """
        materialized: list[dict[str, Any]] = []
        codes: list[str] = []
        for code, rows in partitions:
            codes.append(code)
            materialized.extend(dict(row) for row in rows)
        if not materialized:
            return 0
        if len(materialized) > max_rows:
            raise ValueError(
                f"valuation publish batch rows {len(materialized)} exceeds max_rows={max_rows}"
            )
        observed_dates = [
            row.get("trade_date") for row in materialized
            if isinstance(row.get("trade_date"), date)
        ]
        if not observed_dates:
            raise ValueError("valuation publish batch has no valid trade_date")
        # A stock may have stopped trading before the global rebuild end.
        # Freshness for this one-stock publication must use its actual last
        # source date; requiring the global end date would reject valid
        # delisted/suspended history.  The production builder defaults to one
        # stock per publication, so no other stock can hide a stale partition.
        effective_end = max(observed_dates)
        scope = WorkScope(
            date_from=output_start,
            date_to=effective_end,
            codes=tuple(dict.fromkeys(codes)),
            dimensions={"date_column": "trade_date", "code_column": "ts_code"},
        )
        return self.kernel.rebuild.publish(
            context=context,
            scope=scope,
            target=target,
            producer=lambda _scope: materialized,
            mode=RebuildMode.SCOPED_MERGE,
        )

    def create_feature_target(self, source: str, target: str, context: RunContext) -> str:
        return self.kernel.rebuild._create_like(context, source, target, unlogged=True)

    def foundation_lock(self, mode: str):
        # Foundation Standard/PIT/Feature writers share the same lock as
        # warehouse_write_job and financial schema migrations.  ``mode`` is
        # retained for the call-site audit trail, but must not create a
        # separate lock domain that permits concurrent financial writes.
        return self.kernel.lock_manager.session_exclusive("warehouse-write", "foundation-rebuild")

    def assert_foundation_schema(self, required: set[tuple[str, str, str]], context: RunContext) -> None:
        values = list(required)
        rows = self.kernel.read._repository_query(
            context=context,
            source="information_schema.columns",
            sql="""
                SELECT table_schema, table_name, column_name
                  FROM information_schema.columns
                 WHERE (table_schema, table_name, column_name) IN (
                    SELECT * FROM unnest(%s::text[], %s::text[], %s::text[])
                 )
            """,
            params=(
                [value[0] for value in values],
                [value[1] for value in values],
                [value[2] for value in values],
            ),
            scope=WorkScope(),
            max_rows=1000,
        )
        found = {(row["table_schema"], row["table_name"], row["column_name"]) for row in rows}
        missing = required - found
        if missing:
            raise RuntimeError(
                "foundation schema missing: " + ", ".join(".".join(item) for item in sorted(missing))
            )

    def latest_completed_trade_date(self, context: RunContext) -> date:
        rows = self.kernel.read._repository_query(
            context=context,
            source="std.trading_calendar",
            sql="""
                SELECT trade_date
                  FROM std.trading_calendar
                 WHERE exchange='SSE' AND is_open
                   AND trade_date <= CASE
                       WHEN (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai')::time < TIME '17:00'
                       THEN (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai')::date - 1
                       ELSE (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai')::date
                   END
                 ORDER BY trade_date DESC
                 LIMIT 1
            """,
            scope=WorkScope(dimensions={"exchange": "SSE"}),
            max_rows=1,
        )
        if not rows:
            raise RuntimeError("trading calendar has no completed date")
        value = rows[0]["trade_date"]
        return value if isinstance(value, date) else date.fromisoformat(str(value))

    def active_stock_codes(self, context: RunContext) -> list[str]:
        rows = self.kernel.read.query(
            context=context,
            source="std.stock_basic_info",
            columns=["stock_code"],
            where={"list_status__in": ["L", "D", "P", "G"]},
        )
        return sorted(str(row[0]) for row in rows)

    def select_smoke_codes(self, context: RunContext, fixed: Iterable[str], limit: int = 10) -> list[str]:
        result: list[str] = []
        fixed_rows = self.kernel.read.query(
            context=context,
            source="std.stock_basic_info",
            columns=["stock_code"],
            where={"stock_code__in": list(fixed)},
        )
        result.extend(code for code in fixed if code in {row[0] for row in fixed_rows})
        discovery = (
            ("tushare_financial_raw_version", """
                SELECT ts_code FROM tushare_financial_raw_version
                 WHERE dataset='income' AND comp_type IS NOT NULL AND comp_type<>'1'
                 ORDER BY end_date DESC LIMIT 2"""),
            ("tushare_financial_raw_version", """
                SELECT ts_code FROM tushare_financial_raw_version
                 WHERE dataset='income'
                   AND NULLIF(payload->>'n_income','')::double precision<=0
                 ORDER BY end_date DESC LIMIT 2"""),
            ("tushare_financial_raw_version", """
                SELECT ts_code FROM tushare_financial_raw_version
                 WHERE dataset='income'
                 GROUP BY ts_code,business_key HAVING COUNT(DISTINCT payload_hash)>1
                 ORDER BY ts_code LIMIT 2"""),
            ("std.stock_basic_info", """
                SELECT stock_code FROM std.stock_basic_info
                 WHERE name ILIKE '%%ST%%' OR list_date>=CURRENT_DATE-INTERVAL '2 years'
                 ORDER BY list_date DESC NULLS LAST LIMIT 2"""),
        )
        for source, sql in discovery:
            try:
                rows = self.kernel.read._repository_query(
                    context=context, source=source, sql=sql,
                    scope=WorkScope(partition="smoke"), max_rows=2,
                )
            except Exception:
                continue
            for row in rows:
                code = str(next(iter(row.values())))
                if code not in result:
                    result.append(code)
            if len(result) >= limit:
                break
        return result[:limit]

    def _run_record(self, run_id: str, context: RunContext) -> dict[str, Any]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="ops.foundation_rebuild_run",
            sql="SELECT * FROM ops.foundation_rebuild_run WHERE run_id=%s",
            params=(run_id,), scope=WorkScope(), max_rows=1,
        )
        if not rows:
            raise ValueError(f"unknown foundation run: {run_id}")
        return rows[0]

    def foundation_run_metadata(self, run_id: str, context: RunContext) -> dict[str, Any]:
        """Return the small immutable identity portion needed for safe resume."""
        row = self._run_record(run_id, context)
        return {
            "git_commit": row.get("git_commit"),
            "parameters": row.get("parameters"),
            "status": row.get("status"),
        }

    @staticmethod
    def _adapt_run_record(row: dict[str, Any]) -> dict[str, Any]:
        """Keep JSONB parameters adaptable when a row is round-tripped."""
        parameters = row.get("parameters")
        if isinstance(parameters, (dict, list)):
            row["parameters"] = json.dumps(
                parameters, ensure_ascii=False, default=str,
            )
        return row

    def mark_run(self, run_id: str, status: str, context: RunContext, error: str | None = None) -> int:
        # The upsert must still carry NOT NULL columns because PostgreSQL
        # checks the INSERT side before ON CONFLICT. Normalize JSONB before
        # reusing the read row; psycopg2 otherwise cannot adapt the dict.
        row = self._adapt_run_record(self._run_record(run_id, context))
        row["status"] = status
        row["error_message"] = error
        return self.kernel.write.upsert(
            context=context,
            scope=WorkScope(dimensions={"run_id": run_id}),
            target="ops.foundation_rebuild_run", rows=[row], key=["run_id"],
            update_columns=["status", "error_message"],
        )

    def resume_run(
        self, run_id: str, mode: str, output_end: date,
        metadata: dict[str, Any], context: RunContext,
        stage_identities: Mapping[str, tuple[str | None, str]] | None = None,
    ) -> None:
        row = self._run_record(run_id, context)
        if row.get("run_mode") != mode or row.get("output_end") != output_end:
            raise RuntimeError("resume parameters do not match original run")
        if row.get("contract_version") != CONTRACT_VERSION:
            raise RuntimeError("resume contract version mismatch")
        if row.get("status") not in {"FAILED", "RUNNING"}:
            raise RuntimeError(f"run is not resumable: {run_id} ({row.get('status')})")
        changed = row.get("git_commit") != metadata.get("commit") or row.get("diff_hash") != metadata.get("diff_hash")
        if changed:
            if stage_identities is None:
                self.kernel.write.invalidate_foundation_checkpoints(
                    context=context,
                    scope=WorkScope(partition=f"foundation-run:{run_id}", dimensions={"run_id": run_id}),
                    run_id=run_id,
                    error="invalidated: code identity changed",
                )
            else:
                changed_stages = sorted(
                    stage for stage, (previous, current) in stage_identities.items()
                    if not previous or not current or previous != current
                )
                if changed_stages:
                    self.kernel.write.invalidate_foundation_checkpoints(
                        context=context,
                        scope=WorkScope(partition=f"foundation-run:{run_id}", dimensions={"run_id": run_id}),
                        run_id=run_id,
                        error="invalidated: stage computation identity changed",
                        stages=changed_stages,
                    )
        parameters = row.get("parameters") or {}
        if isinstance(parameters, str):
            try:
                parameters = json.loads(parameters)
            except json.JSONDecodeError:
                parameters = {}
        if not isinstance(parameters, dict):
            parameters = {}
        if stage_identities:
            parameters["_stage_code_identities"] = {
                stage: current for stage, (_, current) in stage_identities.items()
            }
        resume_row = {
            "run_id": run_id,
            "contract_version": row["contract_version"],
            "run_mode": row["run_mode"],
            "status": "RUNNING",
            "warmup_start": row.get("warmup_start"),
            "output_start": row.get("output_start"),
            "output_end": row.get("output_end"),
            "parameters": json.dumps(parameters, ensure_ascii=False),
            "error_message": None,
            "code_version": metadata.get("code_version", context.code_version),
            "git_commit": metadata.get("commit"),
            "git_dirty": metadata.get("dirty", True),
            "diff_hash": metadata.get("diff_hash"),
        }
        resume_row = self._adapt_run_record(resume_row)
        self.kernel.write.upsert(
            context=context, scope=WorkScope(dimensions={"run_id": run_id}),
            target="ops.foundation_rebuild_run", rows=[resume_row], key=["run_id"],
            update_columns=[
                "status", "error_message", "parameters", "code_version",
                "git_commit", "git_dirty", "diff_hash",
            ],
        )

    def create_run(
        self,
        run_id: str,
        mode: str,
        warmup_start: date,
        output_start: date,
        output_end: date,
        metadata: dict[str, Any],
        parameters: dict[str, Any] | None,
        context: RunContext,
    ) -> int:
        row = {
            "run_id": run_id,
            "contract_version": context.pipeline_version,
            "run_mode": mode,
            "status": "RUNNING",
            "warmup_start": warmup_start,
            "output_start": output_start,
            "output_end": output_end,
            "parameters": json.dumps(parameters or {}, ensure_ascii=False),
            "code_version": metadata.get("code_version", context.code_version),
            "git_commit": metadata.get("commit", "unknown"),
            "git_dirty": bool(metadata.get("dirty", False)),
            "diff_hash": metadata.get("diff_hash", "unknown"),
        }
        return self.kernel.write.upsert(
            context=context,
            scope=WorkScope(dimensions={"run_id": run_id}),
            target="ops.foundation_rebuild_run",
            rows=[row],
            key=["run_id"],
            update_columns=[
                "contract_version", "run_mode", "status", "warmup_start",
                "output_start", "output_end", "parameters", "code_version",
                "git_commit", "git_dirty", "diff_hash",
            ],
        )

    def checkpoint_status(
        self, run_id: str, stage: str, partition_key: str, context: RunContext
    ) -> str | None:
        rows = self.kernel.read.query(
            context=context,
            source="ops.foundation_rebuild_checkpoint",
            columns=["status"],
            where={
                "run_id__eq": run_id,
                "stage__eq": stage,
                "partition_key__eq": partition_key,
            },
        )
        return str(rows[0][0]) if rows else None

    def completed_checkpoint_keys(
        self, run_id: str, stage: str, context: RunContext
    ) -> set[str]:
        """Load completed partitions for one stage in one bounded read.

        Resuming a production rebuild must not issue one database query per
        stock partition.  The result is only a reuse hint; a partition is
        still reusable only when the enclosing run has passed its code/
        contract identity checks.
        """
        policy = self.kernel.policy_registry.require(
            "ops.foundation_rebuild_checkpoint"
        )
        rows = self.kernel.read._repository_query(
            context=context,
            source="ops.foundation_rebuild_checkpoint",
            sql=(
                "SELECT partition_key FROM ops.foundation_rebuild_checkpoint "
                "WHERE run_id=%s AND stage=%s AND status='COMPLETED'"
            ),
            params=(run_id, stage),
            scope=WorkScope(
                partition=f"foundation-run:{run_id}",
                dimensions={"stage": stage},
            ),
            max_rows=policy.max_read_rows,
        )
        return {str(row["partition_key"]) for row in rows}

    def save_checkpoint(
        self,
        run_id: str,
        stage: str,
        partition_key: str,
        status: str,
        context: RunContext,
        attempted: int = 0,
        written: int = 0,
        result_hash: str | None = None,
        error: str | None = None,
    ) -> int:
        row = {
            "run_id": run_id,
            "stage": stage,
            "partition_key": partition_key,
            "status": status,
            "rows_attempted": attempted,
            "rows_written": written,
            "result_hash": result_hash,
            "error_message": error,
        }
        return self.kernel.write.upsert(
            context=context,
            scope=WorkScope(dimensions={"run_id": run_id, "stage": stage}),
            target="ops.foundation_rebuild_checkpoint",
            rows=[row],
            key=["run_id", "stage", "partition_key"],
            update_columns=[
                "status", "rows_attempted", "rows_written", "result_hash",
                "error_message",
            ],
        )
