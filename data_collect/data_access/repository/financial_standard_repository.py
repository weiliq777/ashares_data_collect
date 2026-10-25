"""Bounded projection of immutable financial Raw versions into Standard."""
from __future__ import annotations

import json
from typing import ClassVar, Iterator

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class FinancialStandardRepository(BaseRepository):
    TARGETS = frozenset({
        "std.financial_income_standard",
        "std.financial_balance_sheet_standard",
        "std.financial_cash_flow_standard",
    })

    RAW_DATASETS = frozenset({"income", "balancesheet", "cashflow"})

    STANDARD_TABLES: ClassVar[dict[str, tuple[str, str]]] = {
        "income": ("std.financial_income_standard", "financial_id"),
        "balance_sheet": ("std.financial_balance_sheet_standard", "financial_id"),
        "cash_flow": ("std.financial_cash_flow_standard", "financial_id"),
    }

    def raw_versions_by_ids(
        self,
        dataset: str,
        record_ids: list[int],
        context: RunContext,
        report_start,
        report_end,
        page_limit: int = 200,
    ) -> list[dict]:
        """Read a bounded set of exact immutable Raw versions for reconciliation.

        This is intentionally keyed by persisted record IDs. It does not choose
        a replacement version by business key and cannot be used as a broad
        financial history scan.
        """
        if dataset not in self.RAW_DATASETS:
            raise ValueError(f"unsupported financial Raw dataset: {dataset}")
        if not 0 < page_limit <= 500:
            raise ValueError("raw version lookup limit must be between 1 and 500")
        ids = tuple(dict.fromkeys(int(record_id) for record_id in record_ids))
        if not ids:
            return []
        if len(ids) > page_limit or any(record_id <= 0 for record_id in ids):
            raise ValueError("raw version lookup requires positive IDs within the bounded limit")
        if report_start is None or report_end is None or report_start > report_end:
            raise ValueError("raw version lookup requires a valid report date range")

        scope = WorkScope(
            date_from=report_start,
            date_to=report_end,
            partition=f"financial-raw-record-ids:{dataset}",
            dimensions={"record_id_count": len(ids)},
        )
        query = """SELECT r.record_id, r.business_key AS logical_business_key,
                          r.payload, r.payload_hash, r.fetched_at,
                          EXISTS (
                              SELECT 1 FROM tushare_financial_raw_version other
                               WHERE other.dataset=r.dataset
                                 AND other.business_key=r.business_key
                                 AND COALESCE(other.f_ann_date, other.ann_date)
                                     IS NOT DISTINCT FROM COALESCE(r.f_ann_date, r.ann_date)
                                 AND other.payload_hash <> r.payload_hash
                          ) AS ambiguous_same_day_revision
                     FROM tushare_financial_raw_version r
                    WHERE r.dataset=%s AND r.record_id=ANY(%s)
                    ORDER BY r.record_id"""
        rows = self.kernel.read._repository_query(
            context,
            "tushare_financial_raw_version",
            query,
            (dataset, list(ids)),
            scope,
            page_limit,
        )
        result = []
        for row in rows:
            payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
            payload.update({
                "_record_id": row["record_id"],
                "_logical_business_key": row["logical_business_key"],
                "_payload_hash": row["payload_hash"],
                "_fetched_at": row["fetched_at"],
                "_ambiguous_same_day_revision": row["ambiguous_same_day_revision"],
            })
            result.append(payload)
        return result

    def standard_rows_by_ids(
        self,
        source_dataset: str,
        standard_ids: list[int],
        context: RunContext,
        report_start,
        report_end,
        page_limit: int = 200,
    ) -> list[dict]:
        """Read exact Standard rows for a bounded source identity manifest."""
        table_identity = self.STANDARD_TABLES.get(source_dataset)
        if table_identity is None:
            raise ValueError(f"unsupported financial Standard dataset: {source_dataset}")
        if not 0 < page_limit <= 500:
            raise ValueError("Standard row lookup limit must be between 1 and 500")
        ids = tuple(dict.fromkeys(int(value) for value in standard_ids))
        if not ids:
            return []
        if len(ids) > page_limit or any(value <= 0 for value in ids):
            raise ValueError("Standard lookup requires positive IDs within the bounded limit")
        if report_start is None or report_end is None or report_start > report_end:
            raise ValueError("Standard lookup requires a valid report date range")
        table, id_column = table_identity
        scope = WorkScope(
            date_from=report_start,
            date_to=report_end,
            partition=f"financial-standard-record-ids:{source_dataset}",
            dimensions={"record_id_count": len(ids)},
        )
        rows = self.kernel.read._repository_query(
            context,
            table,
            f"""SELECT s.{id_column} AS requested_standard_id,
                       to_jsonb(s) AS standard_row
                  FROM {table} s
                 WHERE s.{id_column}=ANY(%s)
                   AND s.report_date BETWEEN %s AND %s
                 ORDER BY s.{id_column}""",
            (list(ids), report_start, report_end),
            scope,
            page_limit,
        )
        result = []
        for row in rows:
            values = row["standard_row"]
            if not isinstance(values, dict):
                raise TypeError("Standard row JSON projection did not return an object")
            values["_standard_id"] = int(row["requested_standard_id"])
            result.append(values)
        return result

    def standard_rows_by_source_identities(
        self,
        source_dataset: str,
        source_record_keys: list[str],
        record_versions: list[int],
        context: RunContext,
        report_start,
        report_end,
        page_limit: int = 500,
    ) -> list[dict]:
        """Read a bounded set of rows by the immutable Standard source identity."""
        table_identity = self.STANDARD_TABLES.get(source_dataset)
        if table_identity is None:
            raise ValueError(f"unsupported financial Standard dataset: {source_dataset}")
        if len(source_record_keys) != len(record_versions):
            raise ValueError("source keys and record versions must have equal lengths")
        if not 0 < page_limit <= 500 or len(source_record_keys) > page_limit:
            raise ValueError("Standard source identity lookup limit must be in 1..500")
        if not source_record_keys:
            return []
        if any(not key for key in source_record_keys) or any(int(value) <= 0 for value in record_versions):
            raise ValueError("Standard source identities must be non-empty and versioned")
        if report_start is None or report_end is None or report_start > report_end:
            raise ValueError("Standard lookup requires a valid report date range")
        table, id_column = table_identity
        scope = WorkScope(
            date_from=report_start,
            date_to=report_end,
            partition=f"financial-standard-source-identities:{source_dataset}",
            dimensions={"identity_count": len(source_record_keys)},
        )
        rows = self.kernel.read._repository_query(
            context,
            table,
            f"""WITH requested AS (
                       SELECT * FROM unnest(%s::text[], %s::bigint[])
                           AS r(source_record_key, record_version)
                   )
                   SELECT s.{id_column} AS requested_standard_id,
                          to_jsonb(s) AS standard_row
                     FROM {table} s
                     JOIN requested r USING (source_record_key, record_version)
                    WHERE s.report_date BETWEEN %s AND %s
                    ORDER BY s.{id_column}""",
            (source_record_keys, [int(value) for value in record_versions], report_start, report_end),
            scope,
            page_limit,
        )
        result = []
        for row in rows:
            values = row["standard_row"]
            if not isinstance(values, dict):
                raise TypeError("Standard row JSON projection did not return an object")
            values["_standard_id"] = int(row["requested_standard_id"])
            result.append(values)
        return result

    def iter_version_rows(
        self,
        dataset: str,
        context: RunContext,
        chunk_size: int,
        stock_codes: list[str] | None = None,
        report_start=None,
        report_end=None,
    ) -> Iterator[list[dict]]:
        if dataset not in {"income", "balancesheet", "cashflow"}:
            raise ValueError(f"unsupported financial dataset: {dataset}")
        if chunk_size <= 0 or chunk_size > 5000:
            raise ValueError("chunk_size must be in 1..5000")
        conditions = ["r.dataset=%s"]
        params: list = [dataset]
        if stock_codes:
            conditions.append("r.ts_code=ANY(%s)")
            params.append(stock_codes)
        if report_start is not None:
            conditions.append("r.end_date >= %s")
            params.append(report_start)
        if report_end is not None:
            conditions.append("r.end_date <= %s")
            params.append(report_end)
        where = " AND ".join(conditions)
        scope = WorkScope(
            date_from=report_start,
            date_to=report_end,
            codes=tuple(stock_codes) if stock_codes else None,
            partition=f"financial-standard:{dataset}",
        )
        upper = self.kernel.read._repository_query(
            context,
            "tushare_financial_raw_version",
            f"SELECT COALESCE(MAX(r.record_id),0) AS upper_record_id FROM tushare_financial_raw_version r WHERE {where}",
            params=params,
            scope=scope,
            max_rows=2,
        )[0]["upper_record_id"]
        upper_record_id = int(upper or 0)
        last_record_id = 0
        while last_record_id < upper_record_id:
            page_where = f"{where} AND r.record_id > %s AND r.record_id <= %s"
            page_params = [*params, last_record_id, upper_record_id]
            query = f"""
                WITH page AS (
                    SELECT r.*,
                           concat_ws(':', r.ts_code, r.end_date::text,
                                     COALESCE(r.report_type,''), COALESCE(r.comp_type,''),
                                     COALESCE(r.end_type,'')) AS logical_business_key,
                           COALESCE(r.f_ann_date, r.ann_date) AS effective_date
                      FROM tushare_financial_raw_version r
                     WHERE {page_where}
                       AND NOT EXISTS (
                           SELECT 1 FROM tushare_financial_raw_version older
                            WHERE older.dataset=r.dataset
                              AND older.business_key=r.business_key
                              AND older.payload_hash=r.payload_hash
                              AND older.record_id < r.record_id
                       )
                     ORDER BY r.record_id
                     LIMIT %s
                ), annotated AS (
                    SELECT p.*,
                           EXISTS (
                               SELECT 1 FROM tushare_financial_raw_version other
                                WHERE other.dataset=p.dataset
                                  AND other.business_key=p.business_key
                                  AND COALESCE(other.f_ann_date, other.ann_date)
                                      IS NOT DISTINCT FROM p.effective_date
                                  AND other.payload_hash <> p.payload_hash
                           ) AS ambiguous_same_day_revision
                      FROM page p
                )
                SELECT record_id, logical_business_key, payload, payload_hash,
                       fetched_at, ambiguous_same_day_revision
                  FROM annotated
                 ORDER BY record_id
            """
            rows = self.kernel.read._repository_query(
                context,
                "tushare_financial_raw_version",
                query,
                params=[*page_params, chunk_size],
                scope=scope,
                max_rows=chunk_size,
            )
            if not rows:
                probe = self.kernel.read._repository_query(
                    context,
                    "tushare_financial_raw_version",
                    f"SELECT COALESCE(MAX(r.record_id),%s) AS next_record_id FROM tushare_financial_raw_version r WHERE {page_where}",
                    params=[last_record_id, *page_params],
                    scope=scope,
                    max_rows=2,
                )[0]["next_record_id"]
                next_record_id = int(probe or last_record_id)
                if next_record_id <= last_record_id:
                    break
                last_record_id = next_record_id
                continue
            result = []
            for row in rows:
                payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
                payload.update({
                    "_record_id": row["record_id"],
                    "_logical_business_key": row["logical_business_key"],
                    "_payload_hash": row["payload_hash"],
                    "_fetched_at": row["fetched_at"],
                    "_ambiguous_same_day_revision": row["ambiguous_same_day_revision"],
                })
                result.append(payload)
            last_record_id = max(int(row["record_id"]) for row in rows)
            yield result

    def upsert_standard_rows(
        self,
        target: str,
        rows: list[dict],
        context: RunContext,
        page_size: int = 2000,
        scope: WorkScope | None = None,
    ) -> int:
        if target not in self.TARGETS:
            raise ValueError(f"unsupported financial Standard target: {target}")
        if page_size <= 0 or page_size > 5000:
            raise ValueError("page_size must be in 1..5000")
        if not rows:
            return 0
        columns = tuple(rows[0])
        if any(tuple(row) != columns for row in rows):
            raise ValueError("standard rows have inconsistent columns")
        key = ("source", "source_record_key", "record_version")
        update_columns = tuple(column for column in columns if column not in key)
        return self.kernel.write.upsert(
            context=context,
            scope=scope or WorkScope(partition=f"financial-standard:{target}"),
            target=target,
            rows=rows,
            key=key,
            update_columns=update_columns,
            skip_noop=True,
        )
