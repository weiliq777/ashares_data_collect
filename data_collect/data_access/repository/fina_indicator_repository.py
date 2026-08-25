"""Repository for fina_indicator immutable versions and Standard projection."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class FinaIndicatorRepository(BaseRepository):
    def save_versions(self, records: list[dict], batch_id: str, context: RunContext) -> int:
        if not records:
            return 0
        rows = []
        for record in records:
            fetched_at = record.get("_fetched_at")
            payload_record = {key: value for key, value in record.items() if key != "_fetched_at"}
            encoded = json.dumps(payload_record, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)
            rows.append({
                "business_key": ":".join(str(record.get(key) or "") for key in ("ts_code", "end_date", "report_type", "comp_type", "end_type")),
                "ts_code": record.get("ts_code"), "ann_date": self._date(record.get("ann_date")),
                "f_ann_date": self._date(record.get("f_ann_date")), "end_date": self._date(record.get("end_date")),
                "report_type": record.get("report_type"), "comp_type": record.get("comp_type"),
                "end_type": record.get("end_type"), "update_flag": record.get("update_flag"),
                "payload": encoded, "source": "tushare", "batch_id": batch_id,
                "payload_hash": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
                "fetched_at": fetched_at or datetime.now(),
            })
        return self.kernel.raw.append(
            context=context, dataset="fina_indicator_raw_version", rows=rows,
            business_key=("business_key",),
        )

    def backfill_from_raw(self, batch_size: int, batch_id: str, context: RunContext) -> int:
        if batch_size <= 0 or batch_size > 5000:
            raise ValueError("batch_size must be in 1..5000")
        inserted = 0
        last_key = None
        scope = WorkScope(partition="fina-indicator-backfill")
        while True:
            if last_key is None:
                sql = """SELECT ts_code, ann_date, end_date, payload, fetched_at
                            FROM tushare_fina_indicator_raw
                           ORDER BY ts_code, ann_date, end_date LIMIT %s"""
                params = (batch_size,)
            else:
                sql = """SELECT ts_code, ann_date, end_date, payload, fetched_at
                            FROM tushare_fina_indicator_raw
                           WHERE (ts_code, ann_date, end_date) > (%s, %s, %s)
                           ORDER BY ts_code, ann_date, end_date LIMIT %s"""
                params = (*last_key, batch_size)
            rows = self.kernel.read._repository_query(
                context, "tushare_fina_indicator_raw", sql, params=params, scope=scope, max_rows=batch_size,
            )
            if not rows:
                break
            records = []
            for row in rows:
                record = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
                record["_fetched_at"] = row["fetched_at"]
                records.append(record)
            inserted += self.save_versions(records, batch_id, context)
            last_key = (rows[-1]["ts_code"], rows[-1]["ann_date"], rows[-1]["end_date"])
        return inserted

    def rebuild_standard(
        self, batch_size: int, context: RunContext,
        stock_codes: list[str] | None = None, report_start=None, report_end=None,
    ) -> int:
        if batch_size <= 0 or batch_size > 5000:
            raise ValueError("batch_size must be in 1..5000")
        numeric = {field: self._number_sql(field) for field in (
            "roe", "roe_yearly", "q_roe", "roa", "debt_to_assets", "grossprofit_margin", "netprofit_margin",
            "ocf_to_or", "eps", "bps", "or_yoy", "tr_yoy", "q_sales_yoy", "netprofit_yoy",
        )}
        filters, filter_params = [], []
        if stock_codes:
            filters.append("r.ts_code=ANY(%s)"); filter_params.append(stock_codes)
        if report_start is not None:
            filters.append("r.end_date >= %s"); filter_params.append(report_start)
        if report_end is not None:
            filters.append("r.end_date <= %s"); filter_params.append(report_end)
        filter_sql = "".join(f" AND {condition}" for condition in filters)
        update_columns = (
            "report_date", "ann_date", "f_ann_date", "effective_announce_date", "version_available_date",
            "data_available_timestamp", "vintage_quality", "source_observed_at", "report_type", "comp_type",
            "end_type", "update_flag", "roe", "roa", "debt_to_assets", "grossprofit_margin",
            "netprofit_margin", "ocf_to_or", "eps", "bps", "or_yoy", "tr_yoy", "q_sales_yoy", "netprofit_yoy",
            "quality_status", "fetched_at", "roe_reported", "roe_annualized", "roe_quarterly",
            "linked_comp_type", "comp_type_quality_status",
        )
        changed = " OR ".join(f"fina_indicator_standard.{column} IS DISTINCT FROM EXCLUDED.{column}" for column in update_columns)
        statement = f"""
            INSERT INTO std.fina_indicator_standard
            (ts_code, report_date, ann_date, f_ann_date, effective_announce_date, version_available_date,
             data_available_timestamp, vintage_quality, source_observed_at, report_type, comp_type, end_type,
             update_flag, roe, roa, debt_to_assets, grossprofit_margin, netprofit_margin, ocf_to_or, eps, bps,
             or_yoy, tr_yoy, q_sales_yoy, netprofit_yoy, source, source_record_key, record_version, payload_hash,
             quality_status, fetched_at, roe_reported, roe_annualized, roe_quarterly, linked_comp_type,
             comp_type_quality_status)
            WITH page AS (
                SELECT r.*, COALESCE(f_ann_date, ann_date) AS effective_date,
                       EXISTS (SELECT 1 FROM fina_indicator_raw_version other
                                WHERE other.business_key=r.business_key
                                  AND COALESCE(other.f_ann_date, other.ann_date) IS NOT DISTINCT FROM COALESCE(r.f_ann_date, r.ann_date)
                                  AND other.payload_hash<>r.payload_hash) AS ambiguous_same_day_revision
                  FROM fina_indicator_raw_version r
                 WHERE r.record_id > %s {filter_sql}
                 ORDER BY r.record_id LIMIT %s
            ), enriched AS (
                SELECT p.*, linked.comp_type AS statement_comp_type FROM page p
                LEFT JOIN LATERAL (
                    SELECT i.comp_type FROM std.financial_income_standard i
                     WHERE i.stock_code=p.ts_code AND i.report_date=p.end_date
                       AND COALESCE(i.report_type, '1')='1' AND i.comp_type IS NOT NULL
                       AND i.data_available_timestamp <= (COALESCE(p.f_ann_date, p.ann_date, p.fetched_at::date)+1)::timestamp AT TIME ZONE 'Asia/Shanghai'
                     ORDER BY i.data_available_timestamp DESC NULLS LAST, i.record_version DESC LIMIT 1
                ) linked ON TRUE
            )
            SELECT ts_code, end_date, ann_date, f_ann_date, COALESCE(f_ann_date, ann_date),
                   COALESCE(f_ann_date, ann_date, fetched_at::date)+1,
                   (COALESCE(f_ann_date, ann_date, fetched_at::date)+1)::timestamp AT TIME ZONE 'Asia/Shanghai',
                   CASE WHEN COALESCE(f_ann_date, ann_date) IS NULL THEN 'WAREHOUSE_OBSERVED_ONLY' ELSE 'SOURCE_ANNOUNCEMENT_ASSUMED' END,
                   fetched_at, report_type, COALESCE(comp_type, statement_comp_type), end_type, update_flag,
                   {numeric['roe']}, {numeric['roa']}, {numeric['debt_to_assets']}, {numeric['grossprofit_margin']},
                   {numeric['netprofit_margin']}, {numeric['ocf_to_or']}, {numeric['eps']}, {numeric['bps']},
                   {numeric['or_yoy']}, {numeric['tr_yoy']}, {numeric['q_sales_yoy']}, {numeric['netprofit_yoy']},
                   source, business_key, record_id, payload_hash,
                   CASE
                       WHEN end_date IS NULL OR COALESCE(f_ann_date, ann_date) IS NULL THEN 'MISSING_SOURCE'
                       WHEN ambiguous_same_day_revision THEN 'WARNING'
                       ELSE 'VALID'
                   END,
                   fetched_at, {numeric['roe']}, {numeric['roe_yearly']}, {numeric['q_roe']},
                   COALESCE(comp_type, statement_comp_type), CASE WHEN COALESCE(comp_type, statement_comp_type) IS NULL THEN 'UNKNOWN_COMP_TYPE' ELSE 'VALID' END
              FROM enriched
            ON CONFLICT (source, source_record_key, record_version, payload_hash) DO UPDATE SET
              {', '.join(f'{column}=EXCLUDED.{column}' for column in update_columns)}
            WHERE {changed}
        """
        scope = WorkScope(date_from=report_start, date_to=report_end, codes=tuple(stock_codes or ()), partition="fina-indicator-standard")
        total = 0
        last_record_id = 0
        while True:
            rows = self.kernel.read._repository_query(
                context, "fina_indicator_raw_version",
                f"SELECT r.record_id FROM fina_indicator_raw_version r WHERE r.record_id>%s {filter_sql} ORDER BY r.record_id LIMIT %s",
                params=[last_record_id, *filter_params, batch_size], scope=scope, max_rows=batch_size,
            )
            if not rows:
                break
            plan = SqlPlan(operation=Operation.REBUILD, target="std.fina_indicator_standard", scope=scope, expected_mode="SET_BASED", estimated_rows=len(rows), allow_insert_select=True)
            total += self.kernel.rebuild.run_insert_select(statement, plan, context, params=[last_record_id, *filter_params, batch_size])
            last_record_id = int(rows[-1]["record_id"])
        return total

    @staticmethod
    def _date(value):
        if value in (None, ""):
            return None
        text = str(value).replace("-", "")
        if len(text) == 8 and text.isdigit():
            from datetime import date
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        return value

    @staticmethod
    def _number_sql(field: str) -> str:
        return f"NULLIF(NULLIF(payload->>'{field}', ''), 'NaN')::double precision"
