"""Repository for VIP financial version ingestion and change-queue semantics."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class FinancialVipRepository(BaseRepository):
    @staticmethod
    def _assert_queue_lookup_complete(
        version_rows: list[dict[str, Any]], selected: list[dict[str, Any]],
    ) -> None:
        """Reject a partial Raw-version lookup before the transaction commits.

        The queue is the notification boundary for Raw -> Standard/Feature
        derivation.  A partial lookup would otherwise commit Raw rows and mark
        the source page complete while silently losing the corresponding
        change notifications.
        """
        requested_pairs = {
            (str(row["business_key"]), str(row["payload_hash"]))
            for row in version_rows
        }
        selected_pairs = {
            (str(row["business_key"]), str(row["payload_hash"]))
            for row in selected
        }
        if selected_pairs == requested_pairs:
            return
        missing = sorted(requested_pairs - selected_pairs)[:5]
        extra = sorted(selected_pairs - requested_pairs)[:5]
        raise RuntimeError(
            "financial queue lookup incomplete; "
            f"requested={len(requested_pairs)} found={len(selected_pairs)} "
            f"missing={missing} extra={extra}"
        )

    def checkpoint_status(self, dataset: str, key: str, context: RunContext):
        rows = self.kernel.read.query(
            context=context, source="ops.tushare_history_checkpoint", columns=["status", "rows_written"],
            where={"dataset__eq": dataset, "checkpoint_key__eq": key},
            scope=WorkScope(partition=f"vip-checkpoint:{dataset}:{key}"), limit=1,
        )
        return rows[0] if rows else None

    def save_checkpoint(self, dataset: str, key: str, status: str, rows: int, context: RunContext, error=None):
        return self.kernel.write.upsert(
            context=context, scope=WorkScope(partition=f"vip-checkpoint:{dataset}:{key}"),
            target="ops.tushare_history_checkpoint",
            rows=[{"dataset": dataset, "checkpoint_key": key, "status": status,
                   "rows_written": int(rows), "error_message": error}],
            key=("dataset", "checkpoint_key"),
            update_columns=("status", "rows_written", "error_message"),
        )

    def record_anomaly(self, dataset: str, api_name: str, period: str | None, batch_id: str,
                       reason: str, handling: str, details: dict, context: RunContext):
        row = {
            "dataset": dataset, "api_name": api_name,
            "report_period": self._date(period), "batch_id": batch_id,
            "reason_code": reason, "handling": handling,
            "details": json.dumps(details, ensure_ascii=False, default=str),
        }
        return self.kernel.write.bulk_insert(
            context=context, scope=WorkScope(partition=f"vip-anomaly:{dataset}:{period or 'none'}"),
            target="ops.financial_ingest_anomaly", rows=[row],
        )

    def write_versions_and_queue(self, dataset: str, records: list[dict], batch_id: str, context: RunContext) -> int:
        if not records:
            return 0
        if dataset == "fina_indicator":
            target = "fina_indicator_raw_version"
            version_rows = [self._indicator_row(record, batch_id) for record in records]
            key = ("business_key", "payload_hash")
        else:
            target = "tushare_financial_raw_version"
            version_rows = [self._financial_row(record, dataset, batch_id) for record in records]
            key = ("dataset", "business_key", "payload_hash")
        scope = WorkScope(partition=f"vip-version:{dataset}")
        business_key_columns = tuple(column for column in key if column != "payload_hash")

        def persist_version_and_queue() -> int:
            inserted = self.kernel.raw.append(
                context=context, dataset=target, rows=version_rows,
                business_key=business_key_columns,
            )

            # Select by the page's exact (business_key, payload_hash) pairs,
            # not by batch_id.  Raw append is idempotent: after a crash between
            # Raw commit and queue commit, a retry finds the existing version
            # and can recreate its missing queue row.
            business_keys = [row["business_key"] for row in version_rows]
            payload_hashes = [row["payload_hash"] for row in version_rows]
            dataset_filter = " AND r.dataset=%s" if dataset != "fina_indicator" else ""
            params = (business_keys, payload_hashes, dataset) if dataset_filter else (business_keys, payload_hashes)
            selected = self.kernel.read._repository_query(
                context=context, source=target,
                sql=f"""WITH requested AS (
                            SELECT *
                              FROM unnest(%s::text[], %s::text[])
                                   AS v(business_key, payload_hash)
                        ), ranked AS (
                            SELECT r.record_id, r.business_key, r.ts_code,
                                   r.end_date, r.batch_id, r.payload_hash,
                                   COALESCE(r.f_ann_date, r.ann_date, r.end_date) AS available_date,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY r.business_key, r.payload_hash
                                       ORDER BY r.record_id DESC
                                   ) AS rn
                              FROM {target} r
                              JOIN requested v
                                ON v.business_key=r.business_key
                               AND v.payload_hash=r.payload_hash
                             WHERE TRUE{dataset_filter}
                        )
                        SELECT record_id, business_key, ts_code, end_date,
                               batch_id, payload_hash, available_date
                          FROM ranked
                         WHERE rn=1""",
                params=params, scope=scope, max_rows=max(len(records), 1),
            )
            self._assert_queue_lookup_complete(version_rows, selected)
            queue_rows = [{
                "dataset": dataset, "business_key": row["business_key"],
                "payload_hash": row["payload_hash"], "ts_code": row["ts_code"],
                "report_date": row["end_date"], "source_record_id": row["record_id"],
                "batch_id": row["batch_id"], "available_date": row["available_date"],
            } for row in selected]
            if queue_rows:
                self.kernel.write.bulk_insert_ignore(
                    context=context, scope=WorkScope(partition=f"vip-queue:{dataset}"),
                    target="ops.financial_change_queue", rows=queue_rows,
                )
            return inserted

        # Version and queue are one bounded workflow.  For a 2,000-row page,
        # the two writes stay well below the transaction row/byte budgets while
        # a transient acknowledgement loss remains safely replayable.
        return self.atomic(
            context, scope, persist_version_and_queue,
            estimated_rows=len(records) * 2, idempotent=True,
        )

    def pending_queue(
        self, context: RunContext, limit: int = 10000,
        claim_owner_id: str | None = None, takeover: bool = False,
        through_date: date | None = None,
    ) -> tuple[list[int], list[str]]:
        return self.kernel.write.claim_financial_queue(
            context=context, scope=WorkScope(partition="vip-queue-claim"), limit=limit,
            claim_owner_id=claim_owner_id, takeover=takeover,
            through_date=through_date,
        )

    def renew_queue_lease(
        self, queue_ids: list[int], context: RunContext, lease_seconds: int = 4 * 60 * 60,
        claim_owner_id: str | None = None,
    ) -> int:
        return self.kernel.write.renew_financial_queue_lease(
            context=context,
            scope=WorkScope(partition="vip-queue-lease-renewal"),
            queue_ids=queue_ids,
            lease_seconds=lease_seconds,
            claim_owner_id=claim_owner_id,
        )

    def pending_legacy_codes(self, context: RunContext, *, limit: int = 100) -> list[str]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
            raise ValueError("legacy financial queue limit must be an integer in 1..1000")
        rows = self.kernel.read._repository_query(
            context=context, source="ops.tushare_history_checkpoint", sql="""
                SELECT checkpoint_key FROM ops.tushare_history_checkpoint
                 WHERE dataset='financial_derived_v2' AND status IN ('pending','failed')
                 ORDER BY checkpoint_key
                 LIMIT %s
            """, params=(limit,),
            scope=WorkScope(partition="vip-legacy-pending"), max_rows=limit,
        )
        return sorted({str(row["checkpoint_key"]) for row in rows if row.get("checkpoint_key")})

    def claimed_queue_datasets(
        self, queue_ids: list[int], context: RunContext, claim_owner_id: str,
    ) -> set[str]:
        """Return the typed datasets for one bounded queue claim."""
        ids = tuple(dict.fromkeys(int(value) for value in queue_ids))
        if not ids:
            return set()
        if not isinstance(claim_owner_id, str) or not claim_owner_id.strip():
            raise ValueError("claim_owner_id must be a non-empty string")
        rows = self.kernel.read._repository_query(
            context=context,
            source="ops.financial_change_queue",
            sql="""SELECT DISTINCT dataset
                     FROM ops.financial_change_queue
                    WHERE queue_id=ANY(%s)
                      AND status='RUNNING'
                      AND claim_owner_id=%s""",
            params=(list(ids), claim_owner_id),
            scope=WorkScope(partition="vip-queue-claimed-datasets"),
            max_rows=10,
        )
        return {str(row["dataset"]) for row in rows if row.get("dataset")}

    def finish_queue(
        self, queue_ids: list[int], status: str, error: str | None,
        context: RunContext, claim_owner_id: str | None = None,
        processed_through: date | None = None,
    ):
        if not queue_ids:
            return 0
        ids = tuple(dict.fromkeys(int(value) for value in queue_ids))
        changed = self._queue_update(
            list(ids), status, error, context, claim_owner_id, processed_through
        )
        if changed != len(ids):
            raise RuntimeError(
                "financial queue completion lost claim ownership; "
                f"expected={len(ids)} changed={changed} run_id={context.run_id}"
            )
        return changed

    def queue_status_counts(self, context: RunContext, through_date: date) -> dict[str, int]:
        if not isinstance(through_date, date):
            raise ValueError("queue status counts require through_date")
        rows = self.kernel.read._repository_query(
            context=context,
            source="ops.financial_change_queue",
            sql="""SELECT status, COUNT(*) AS rows
                     FROM ops.financial_change_queue
                    WHERE available_date IS NULL OR available_date <= %s
                    GROUP BY status
                     ORDER BY status""",
            params=(through_date,),
            scope=WorkScope(partition="vip-queue-status"),
            max_rows=10,
        )
        return {str(row["status"]): int(row["rows"] or 0) for row in rows}

    def requeue_quarantined(
        self, queue_ids: list[int], approved_by: str, reason: str,
        context: RunContext, reset_attempts: bool = True,
    ) -> int:
        """Re-enter reviewed quarantine rows into the normal consumer flow."""
        return self.kernel.write.requeue_quarantined_financial_queue(
            context=context,
            scope=WorkScope(partition="vip-queue-quarantine-requeue"),
            queue_ids=queue_ids,
            approved_by=approved_by,
            reason=reason,
            reset_attempts=reset_attempts,
        )

    def resolve_quarantined(
        self, queue_ids: list[int], approved_by: str, reason: str,
        context: RunContext,
    ) -> int:
        """Close reviewed quarantine rows when no automatic derivation is wanted."""
        return self.kernel.write.resolve_quarantined_financial_queue(
            context=context,
            scope=WorkScope(partition="vip-queue-quarantine-resolve"),
            queue_ids=queue_ids,
            approved_by=approved_by,
            reason=reason,
        )

    def finish_legacy(self, codes: list[str], status: str, error: str | None, context: RunContext):
        if not codes:
            return 0
        return self.kernel.write.update_history_checkpoints(
            context=context, scope=WorkScope(partition="vip-legacy-finish"),
            dataset="financial_derived_v2", keys=codes, status=status, error=error,
        )

    def repair_orphan_versions(self, batch_size: int, context: RunContext) -> int:
        if batch_size <= 0 or batch_size > 5000:
            raise ValueError("batch_size must be in 1..5000")
        stores = (
            ("income", "tushare_financial_raw_version", "dataset='income'"),
            ("balancesheet", "tushare_financial_raw_version", "dataset='balancesheet'"),
            ("cashflow", "tushare_financial_raw_version", "dataset='cashflow'"),
            ("fina_indicator", "fina_indicator_raw_version", "TRUE"),
        )
        total = 0
        for dataset, table, filter_sql in stores:
            watermark_rows = self.kernel.read._repository_query(
                context=context, source="ops.financial_version_cutover_watermark",
                sql="SELECT record_id, batch_id FROM ops.financial_version_cutover_watermark WHERE dataset=%s",
                params=(dataset,), scope=WorkScope(partition=f"vip-watermark:{dataset}"), max_rows=1,
            )
            watermark = int(watermark_rows[0]["record_id"]) if watermark_rows else 0
            watermark_batch = watermark_rows[0]["batch_id"] if watermark_rows else "legacy-cutover-046"
            last_id = watermark
            while True:
                rows = self.kernel.read._repository_query(
                    context=context, source=table, sql=f"""
                        SELECT r.record_id, r.business_key, r.ts_code, r.end_date,
                               r.batch_id, r.payload_hash,
                               COALESCE(r.f_ann_date, r.ann_date, r.end_date) AS available_date
                          FROM {table} r
                         WHERE r.record_id > %s AND r.batch_id <> %s AND {filter_sql}
                           AND NOT EXISTS (
                               SELECT 1 FROM ops.financial_change_queue q
                                WHERE q.dataset=%s AND q.business_key=r.business_key AND q.payload_hash=r.payload_hash
                           )
                         ORDER BY r.record_id LIMIT %s
                    """, params=(last_id, watermark_batch, dataset, batch_size),
                    scope=WorkScope(partition=f"vip-orphans:{dataset}"), max_rows=batch_size,
                )
                if not rows:
                    break
                queue_rows = [{
                    "dataset": dataset, "business_key": row["business_key"],
                    "payload_hash": row["payload_hash"], "ts_code": row["ts_code"],
                    "report_date": row["end_date"], "source_record_id": row["record_id"],
                    "batch_id": row["batch_id"], "available_date": row["available_date"],
                } for row in rows]
                total += self.kernel.write.upsert(
                    context=context, scope=WorkScope(partition=f"vip-orphans:{dataset}"),
                    target="ops.financial_change_queue", rows=queue_rows,
                    key=("dataset", "business_key", "payload_hash"), update_columns=(),
                )
                last_id = int(rows[-1]["record_id"])
        return total

    def _queue_update(
        self, queue_ids: list[int], status: str, error: str | None,
        context: RunContext, claim_owner_id: str | None = None,
        processed_through: date | None = None,
    ):
        return self.kernel.write.update_financial_queue(
            context=context, scope=WorkScope(partition="vip-queue-finish"),
            queue_ids=queue_ids, status=status, error=error,
            claim_owner_id=claim_owner_id, processed_through=processed_through,
        )

    @staticmethod
    def _date(value):
        if not value:
            return None
        parsed = str(value).replace("-", "")
        return date(int(parsed[:4]), int(parsed[4:6]), int(parsed[6:8])) if len(parsed) == 8 else value

    @classmethod
    def _payload_values(cls, record: dict, dataset: str, batch_id: str):
        payload = json.dumps(record, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        business_key = ":".join(str(record.get(key) or "") for key in ("ts_code", "end_date", "report_type", "comp_type", "end_type"))
        return (dataset, business_key, record.get("ts_code"), cls._date(record.get("ann_date")), cls._date(record.get("f_ann_date")), cls._date(record.get("end_date")), record.get("report_type"), record.get("comp_type"), record.get("end_type"), record.get("update_flag"), payload, "tushare", batch_id, digest, datetime.now())

    @classmethod
    def _indicator_values(cls, record: dict, batch_id: str):
        payload = json.dumps(record, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        business_key = ":".join(str(record.get(key) or "") for key in ("ts_code", "end_date", "report_type", "comp_type", "end_type"))
        return (business_key, record.get("ts_code"), cls._date(record.get("ann_date")), cls._date(record.get("f_ann_date")), cls._date(record.get("end_date")), record.get("report_type"), record.get("comp_type"), record.get("end_type"), record.get("update_flag"), payload, "tushare", batch_id, digest, datetime.now())

    @classmethod
    def _financial_row(cls, record: dict, dataset: str, batch_id: str) -> dict:
        values = cls._payload_values(record, dataset, batch_id)
        columns = ("dataset", "business_key", "ts_code", "ann_date", "f_ann_date", "end_date", "report_type", "comp_type", "end_type", "update_flag", "payload", "source", "batch_id", "payload_hash", "fetched_at")
        return dict(zip(columns, values, strict=True))

    @classmethod
    def _indicator_row(cls, record: dict, batch_id: str) -> dict:
        values = cls._indicator_values(record, batch_id)
        columns = ("business_key", "ts_code", "ann_date", "f_ann_date", "end_date", "report_type", "comp_type", "end_type", "update_flag", "payload", "source", "batch_id", "payload_hash", "fetched_at")
        return dict(zip(columns, values, strict=True))
