"""WriteGateway：Standard/PIT/Feature 增量写（§4.2 / §4.3）。

upsert 内部自动 ChunkPlanner：调用者不需要写 for batch 循环——循环属于基础设施。
bulk_insert 大量纯插入（staging / 新版本）。
"""
from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any, Mapping, Sequence
from uuid import NAMESPACE_URL, uuid4, uuid5

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.release_change_capture import RELEASE_CHANGE_LOCK_KEY
from data_collect.data_access.query_guard import validate_qualified_name, validate_table_name
from data_collect.data_access.bulk_loader import (
    copy_or_execute_values,
    create_projection_staging,
)


class WriteGateway:
    def __init__(self, kernel) -> None:
        self._kernel = kernel

    def upsert(
        self,
        context: RunContext,
        scope: WorkScope,
        target: str,
        rows: Sequence[Mapping[str, Any]],
        key: Sequence[str],
        update_columns: Sequence[str],
        skip_noop: bool = False,
        restart_checkpoint: bool = False,
    ) -> int:
        if not isinstance(context, RunContext):
            raise TypeError("write.upsert 必须携带 RunContext")
        if not isinstance(scope, WorkScope):
            raise TypeError("write.upsert 必须携带 WorkScope")
        if not rows:
            return 0
        key = tuple(validate_table_name(column) for column in key)
        update_columns = tuple(
            validate_table_name(column) for column in update_columns
        )
        if not key:
            raise ValueError("key 不能为空")

        t = validate_qualified_name(target)
        columns = self._columns(rows)
        for col in key:
            if col not in columns:
                raise ValueError(f"key 不在数据列中: {col}")
        if any(column not in columns for column in update_columns):
            raise ValueError("update_columns 必须是写入数据列的子集")
        policy = self._kernel.policy_registry.require(t)
        if policy.primary_key and key != policy.primary_key:
            raise ValueError(
                f"upsert key 必须匹配 DatasetPolicy.primary_key={policy.primary_key}"
            )

        if policy.immutable_after_freeze and update_columns:
            raise ValueError(
                "immutable contract manifests may only be created with "
                "ON CONFLICT DO NOTHING; use a typed transition for changes"
            )

        plan = SqlPlan(
            operation=Operation.UPSERT,
            target=t,
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=len(rows),
        )
        self._kernel.query_guard.check(plan)

        chunks = self._kernel.chunk_planner.plan(list(rows))
        scope_key = self._kernel._checkpoint_batch_scope_key("upsert", t, scope, rows)
        # A typed rebuild slice may have been physically cleared after a
        # previous attempt. Its old per-batch checkpoint then describes data
        # that no longer exists and must not suppress the rewrite. This flag
        # is explicit and opt-in; ordinary incremental upserts retain normal
        # resumability.
        if restart_checkpoint:
            self._kernel._checkpoint_clear(context, scope_key)
        resume_after, done = self._kernel._checkpoint_cursor(context, scope_key)
        if done:
            return 0
        written = 0
        for idx, chunk in enumerate(chunks, start=1):
            if idx <= resume_after:
                continue
            self._kernel._reserve_atomic_chunk(chunk)
            n = self._kernel._run_chunk(
                lambda: self._upsert_chunk(t, columns, chunk, key, update_columns, skip_noop),
                change_event=self._kernel._build_release_change_event(
                    context, "UPSERT", t, scope, rows=chunk,
                ),
            )
            written += n
            self._kernel._checkpoint_progress(context.run_id, context.job_name, scope_key, idx)
        self._kernel._checkpoint_done(context.run_id, context.job_name, scope_key)
        return written

    def bulk_insert(
        self,
        context: RunContext,
        scope: WorkScope,
        target: str,
        rows: Sequence[Mapping[str, Any]],
        restart_checkpoint: bool = False,
    ) -> int:
        if not isinstance(context, RunContext):
            raise TypeError("write.bulk_insert 必须携带 RunContext")
        if not isinstance(scope, WorkScope):
            raise TypeError("write.bulk_insert 必须携带 WorkScope")
        if not rows:
            return 0

        t = validate_qualified_name(target)
        columns = self._columns(rows)
        plan = SqlPlan(
            operation=Operation.BULK_INSERT,
            target=t,
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=len(rows),
        )
        self._kernel.query_guard.check(plan)

        chunks = self._kernel.chunk_planner.plan(list(rows))
        scope_key = self._kernel._checkpoint_batch_scope_key(
            "bulk_insert", t, scope, rows
        )
        if restart_checkpoint:
            # A caller may have deliberately removed the physical slice
            # described by this checkpoint.  Clearing only this exact input
            # batch makes replay explicit without weakening normal immutable
            # bulk-insert resume behavior elsewhere.
            self._kernel._checkpoint_clear(context, scope_key)
        resume_after, done = self._kernel._checkpoint_cursor(context, scope_key)
        if done:
            return 0
        written = 0
        for idx, chunk in enumerate(chunks, start=1):
            if idx <= resume_after:
                continue
            self._kernel._reserve_atomic_chunk(chunk)
            # Plain INSERT has no intrinsic replay protection.  A connection
            # loss during COMMIT has an ambiguous outcome, so automatic retry
            # could duplicate immutable ledger/audit rows.
            n = self._kernel._run_chunk(
                lambda: self._insert_chunk(t, columns, chunk),
                idempotent=False,
                change_event=self._kernel._build_release_change_event(
                    context, "BULK_INSERT", t, scope, rows=chunk,
                ),
            )
            written += n
            self._kernel._checkpoint_progress(context.run_id, context.job_name, scope_key, idx)
        self._kernel._checkpoint_done(context.run_id, context.job_name, scope_key)
        return written

    def bulk_insert_ignore(
        self,
        context: RunContext,
        scope: WorkScope,
        target: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> tuple[int, int]:
        """Bounded idempotent INSERT ... ON CONFLICT DO NOTHING."""
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError("write.bulk_insert_ignore requires RunContext and WorkScope")
        if not rows:
            return 0, 0
        t = validate_qualified_name(target)
        columns = self._columns(rows)
        plan = SqlPlan(operation=Operation.BULK_INSERT, target=t, scope=scope, expected_mode="ROW_BASED", estimated_rows=len(rows))
        self._kernel.query_guard.check(plan)
        scope_key = self._kernel._checkpoint_batch_scope_key(
            "bulk_insert_ignore", t, scope, rows
        )
        resume_after, done = self._kernel._checkpoint_cursor(context, scope_key)
        if done:
            return len(rows), 0
        written = 0
        for idx, chunk in enumerate(self._kernel.chunk_planner.plan(list(rows)), start=1):
            if idx <= resume_after:
                continue
            self._kernel._reserve_atomic_chunk(chunk)
            written += self._kernel._run_chunk(
                lambda: self._insert_ignore_chunk(t, columns, chunk),
                change_event=self._kernel._build_release_change_event(
                    context, "BULK_INSERT_IGNORE", t, scope, rows=chunk,
                ),
            )
            self._kernel._checkpoint_progress(context.run_id, context.job_name, scope_key, idx)
        self._kernel._checkpoint_done(context.run_id, context.job_name, scope_key)
        return len(rows), written

    def claim_financial_queue(
        self, context: RunContext, scope: WorkScope, limit: int = 10000,
        lease_seconds: int = 4 * 60 * 60, max_attempts: int = 5,
        claim_owner_id: str | None = None, takeover: bool = False,
        through_date: date | None = None,
    ) -> tuple[list[int], list[str]]:
        """Claim a bounded financial change queue batch atomically.

        A claim is a lease owned by ``context.run_id``.  Expired claims are
        reclaimed before picking new work; items that have exhausted their
        retry budget are quarantined instead of being retried forever.
        """
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError("claim_financial_queue requires RunContext and WorkScope")
        if not isinstance(through_date, date):
            raise ValueError(
                "financial queue claim requires through_date; "
                "an unscoped claim is not allowed"
            )
        if limit <= 0 or limit > 10000:
            raise ValueError("financial queue claim limit must be in 1..10000")
        if not isinstance(lease_seconds, int) or isinstance(lease_seconds, bool) or not 60 <= lease_seconds <= 7 * 24 * 60 * 60:
            raise ValueError("financial queue lease_seconds must be an integer in 60..604800")
        if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or not 1 <= max_attempts <= 100:
            raise ValueError("financial queue max_attempts must be an integer in 1..100")
        if claim_owner_id is None:
            claim_owner_id = context.run_id
        if not isinstance(claim_owner_id, str) or not claim_owner_id.strip() or len(claim_owner_id) > 160:
            raise ValueError("financial queue claim_owner_id must be a non-empty string of at most 160 characters")
        if not isinstance(takeover, bool):
            raise TypeError("financial queue takeover must be bool")
        if limit > self._kernel.transaction_manager.config.max_rows_per_tx:
            raise ValueError("financial queue claim exceeds transaction row budget")
        plan = SqlPlan(operation=Operation.UPSERT, target="ops.financial_change_queue", scope=scope, expected_mode="ROW_BASED", estimated_rows=limit)
        self._kernel.query_guard.check(plan)

        def claim() -> tuple[list[int], list[str]]:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                # Bound recovery as well as claiming.  This keeps a large
                # historical outage from turning the recovery step into a
                # single unbounded write transaction.
                cur.execute(
                    """WITH reclaimable AS (
                           SELECT q.queue_id
                             FROM ops.financial_change_queue q
                            WHERE (
                                      q.status='RUNNING'
                                  AND (
                                         q.lease_expires_at <= CURRENT_TIMESTAMP
                                      OR (
                                             q.lease_expires_at IS NULL
                                         AND q.updated_at <= CURRENT_TIMESTAMP - INTERVAL '1 hour'
                                      )
                                      OR (
                                             %s
                                         AND q.claim_run_id=%s
                                         AND q.claim_owner_id IS DISTINCT FROM %s
                                      )
                                  )
                                  AND (q.available_date IS NULL OR q.available_date <= %s)
                            )
                               OR (
                                      q.status IN ('PENDING','FAILED')
                                  AND q.attempts >= %s
                                  AND (q.available_date IS NULL OR q.available_date <= %s)
                            )
                            ORDER BY q.queue_id
                            LIMIT %s
                            FOR UPDATE SKIP LOCKED
                       )
                       UPDATE ops.financial_change_queue q
                          SET status=CASE
                                         WHEN q.attempts >= %s THEN 'QUARANTINED'
                                         ELSE 'FAILED'
                                     END,
                              last_error=CASE
                                             WHEN q.attempts >= %s
                                             THEN 'financial queue retry budget exhausted'
                                             ELSE 'financial queue lease expired and was reclaimed'
                              END,
                              claim_run_id=NULL,
                              claim_owner_id=NULL,
                              lease_expires_at=NULL,
                              updated_at=CURRENT_TIMESTAMP
                         FROM reclaimable r
                        WHERE q.queue_id=r.queue_id""",
                    (
                        takeover, context.run_id, claim_owner_id,
                        through_date, max_attempts, through_date,
                        limit, max_attempts, max_attempts,
                    ),
                )
                cur.execute(
                    """WITH picked AS (
                           SELECT queue_id FROM ops.financial_change_queue
                            WHERE status IN ('PENDING','FAILED')
                              AND attempts < %s
                              AND available_date IS NOT NULL
                              AND available_date <= %s
                            ORDER BY queue_id LIMIT %s FOR UPDATE SKIP LOCKED
                       )
                       UPDATE ops.financial_change_queue q
                          SET status='RUNNING',
                              attempts=q.attempts+1,
                              claim_run_id=%s,
                              claim_owner_id=%s,
                              lease_expires_at=CURRENT_TIMESTAMP + (%s * INTERVAL '1 second'),
                              updated_at=CURRENT_TIMESTAMP
                         FROM picked
                        WHERE q.queue_id=picked.queue_id
                       RETURNING q.queue_id, q.ts_code""",
                    (max_attempts, through_date, limit, context.run_id, claim_owner_id, lease_seconds),
                )
                rows = cur.fetchall()
            return [int(row[0]) for row in rows], [str(row[1]) for row in rows]

        # Claiming changes queue state and increments attempts.  If the server
        # committed but the client lost the acknowledgement, replay could
        # claim a different batch, so this boundary is explicitly non-retryable.
        return self._kernel._run_chunk(claim, idempotent=False)

    def update_financial_queue(
        self, context: RunContext, scope: WorkScope, queue_ids: Sequence[int],
        status: str, error: str | None, claim_owner_id: str | None = None,
        processed_through: date | None = None,
    ) -> int:
        """Complete/fail a typed financial queue batch."""
        ids = tuple(int(value) for value in queue_ids)
        if not ids:
            return 0
        if status not in {"DONE", "FAILED", "PENDING"}:
            raise ValueError(f"unsupported financial queue status: {status}")
        if status == "DONE" and not isinstance(processed_through, date):
            raise ValueError("DONE financial queue updates require processed_through")
        if status != "DONE" and processed_through is not None:
            raise ValueError("processed_through is only valid for DONE financial queue updates")
        owner = context.run_id if claim_owner_id is None else claim_owner_id
        if not isinstance(owner, str) or not owner.strip() or len(owner) > 160:
            raise ValueError("financial queue claim_owner_id must be a non-empty string of at most 160 characters")
        plan = SqlPlan(operation=Operation.UPSERT, target="ops.financial_change_queue", scope=scope, expected_mode="ROW_BASED", estimated_rows=len(ids))
        self._kernel.query_guard.check(plan)

        def update(chunk) -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE ops.financial_change_queue
                          SET status=%s, last_error=%s, updated_at=CURRENT_TIMESTAMP,
                              completed_at=CASE WHEN %s='DONE' THEN CURRENT_TIMESTAMP ELSE completed_at END,
                              processed_through=CASE WHEN %s='DONE' THEN %s ELSE processed_through END,
                              claim_run_id=NULL,
                              claim_owner_id=NULL,
                              lease_expires_at=NULL
                        WHERE queue_id = ANY(%s)
                          AND status='RUNNING'
                          AND claim_owner_id=%s""",
                    (status, error, status, status, processed_through, list(chunk), owner),
                )
                return int(cur.rowcount or 0)

        total = 0
        for index, chunk in enumerate(
            self._kernel.chunk_planner.iter_chunks(ids, bytes_per_row=8), 1
        ):
            self._kernel._reserve_atomic_chunk(chunk)
            total += self._kernel._run_chunk(lambda chunk=chunk: update(chunk))
        return total

    def requeue_quarantined_financial_queue(
        self, context: RunContext, scope: WorkScope, queue_ids: Sequence[int],
        approved_by: str, reason: str, reset_attempts: bool = True,
    ) -> int:
        """Move explicitly approved quarantine rows back to PENDING.

        This is an administrative recovery boundary, not an automatic retry.
        The bounded, owner-independent update is auditable and refuses partial
        transitions so an operator cannot believe that all requested rows were
        recovered when one of them had already changed state.
        """
        ids = tuple(dict.fromkeys(int(value) for value in queue_ids))
        if not ids:
            return 0
        if len(ids) > 1000:
            raise ValueError("at most 1000 quarantined queue IDs may be transitioned per operation")
        self._validate_queue_resolution(approved_by, reason, reset_attempts)
        plan = SqlPlan(operation=Operation.UPSERT, target="ops.financial_change_queue", scope=scope, expected_mode="ROW_BASED", estimated_rows=len(ids))
        self._kernel.query_guard.check(plan)

        def requeue(chunk) -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE ops.financial_change_queue
                              SET status='PENDING',
                                  attempts=CASE WHEN %s THEN 0 ELSE attempts END,
                                  last_error=%s,
                                  claim_run_id=NULL,
                                  claim_owner_id=NULL,
                                  lease_expires_at=NULL,
                                  completed_at=NULL,
                                  processed_through=NULL,
                                  quarantine_resolved_by=%s,
                                  quarantine_resolved_at=CURRENT_TIMESTAMP,
                                  quarantine_resolution_reason=%s,
                                  updated_at=CURRENT_TIMESTAMP
                            WHERE queue_id=ANY(%s)
                              AND status='QUARANTINED'""",
                    (reset_attempts, f"requeued: {reason}", approved_by, reason, list(chunk)),
                )
                return int(cur.rowcount or 0)

        return self._transition_quarantined_queue(
            ids, scope, plan, requeue, context, expected_status="PENDING"
        )

    def resolve_quarantined_financial_queue(
        self, context: RunContext, scope: WorkScope, queue_ids: Sequence[int],
        approved_by: str, reason: str,
    ) -> int:
        """Close explicitly reviewed quarantine rows without deriving them."""
        ids = tuple(dict.fromkeys(int(value) for value in queue_ids))
        if not ids:
            return 0
        if len(ids) > 1000:
            raise ValueError("at most 1000 quarantined queue IDs may be transitioned per operation")
        self._validate_queue_resolution(approved_by, reason, True)
        plan = SqlPlan(operation=Operation.UPSERT, target="ops.financial_change_queue", scope=scope, expected_mode="ROW_BASED", estimated_rows=len(ids))
        self._kernel.query_guard.check(plan)

        def resolve(chunk) -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE ops.financial_change_queue
                              SET status='RESOLVED',
                                  last_error=%s,
                                  claim_run_id=NULL,
                                  claim_owner_id=NULL,
                                  lease_expires_at=NULL,
                                  quarantine_resolved_by=%s,
                                  quarantine_resolved_at=CURRENT_TIMESTAMP,
                                  quarantine_resolution_reason=%s,
                                  updated_at=CURRENT_TIMESTAMP
                            WHERE queue_id=ANY(%s)
                              AND status='QUARANTINED'""",
                    (f"resolved: {reason}", approved_by, reason, list(chunk)),
                )
                return int(cur.rowcount or 0)

        return self._transition_quarantined_queue(
            ids, scope, plan, resolve, context, expected_status="RESOLVED"
        )

    @staticmethod
    def _validate_queue_resolution(approved_by: str, reason: str, reset_attempts: bool) -> None:
        if not isinstance(approved_by, str) or not approved_by.strip() or len(approved_by) > 160:
            raise ValueError("approved_by must be a non-empty string of at most 160 characters")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 2000:
            raise ValueError("reason must be a non-empty string of at most 2000 characters")
        if not isinstance(reset_attempts, bool):
            raise TypeError("reset_attempts must be bool")

    def _transition_quarantined_queue(
        self, ids: tuple[int, ...], scope: WorkScope, plan: SqlPlan,
        operation, context: RunContext, expected_status: str,
    ) -> int:
        if len(ids) > self._kernel.transaction_manager.config.max_rows_per_tx:
            raise ValueError("quarantine transition exceeds transaction row budget")
        self._kernel._reserve_atomic_chunk(ids)

        def transition_all_or_rollback() -> int:
            changed = operation(ids)
            if changed != len(ids):
                # Raise before _run_chunk commits so a stale/mixed ID batch
                # cannot partially change queue state and then report failure.
                raise RuntimeError(
                    "financial quarantine transition was partial; "
                    f"expected={len(ids)} changed={changed} target_status={expected_status} "
                    f"run_id={context.run_id}"
                )
            return changed

        return self._kernel._run_chunk(transition_all_or_rollback, idempotent=False)

    def renew_financial_queue_lease(
        self, context: RunContext, scope: WorkScope, queue_ids: Sequence[int],
        lease_seconds: int = 4 * 60 * 60, claim_owner_id: str | None = None,
    ) -> int:
        """Extend only leases still owned by this RunContext."""
        ids = tuple(dict.fromkeys(int(value) for value in queue_ids))
        if not ids:
            return 0
        if not isinstance(lease_seconds, int) or isinstance(lease_seconds, bool) or not 60 <= lease_seconds <= 7 * 24 * 60 * 60:
            raise ValueError("financial queue lease_seconds must be an integer in 60..604800")
        owner = context.run_id if claim_owner_id is None else claim_owner_id
        if not isinstance(owner, str) or not owner.strip() or len(owner) > 160:
            raise ValueError("financial queue claim_owner_id must be a non-empty string of at most 160 characters")
        plan = SqlPlan(operation=Operation.UPSERT, target="ops.financial_change_queue", scope=scope, expected_mode="ROW_BASED", estimated_rows=len(ids))
        self._kernel.query_guard.check(plan)

        def renew() -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE ops.financial_change_queue
                          SET lease_expires_at=CURRENT_TIMESTAMP + (%s * INTERVAL '1 second'),
                              updated_at=CURRENT_TIMESTAMP
                        WHERE queue_id=ANY(%s)
                          AND status='RUNNING'
                          AND claim_owner_id=%s""",
                    (lease_seconds, list(ids), owner),
                )
                changed = int(cur.rowcount or 0)
            if changed != len(ids):
                raise RuntimeError(
                    "financial queue lease ownership lost; "
                    f"expected={len(ids)} renewed={changed} run_id={context.run_id}"
                )
            return changed

        return self._kernel._run_chunk(renew, idempotent=True)

    def update_history_checkpoints(
        self, context: RunContext, scope: WorkScope, dataset: str,
        keys: Sequence[str], status: str, error: str | None,
    ) -> int:
        """Update a bounded checkpoint batch without exposing SQL to Repository code."""
        values = tuple(str(value) for value in keys)
        if not values:
            return 0
        plan = SqlPlan(operation=Operation.UPSERT, target="ops.tushare_history_checkpoint", scope=scope, expected_mode="ROW_BASED", estimated_rows=len(values))
        self._kernel.query_guard.check(plan)

        def update(chunk) -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE ops.tushare_history_checkpoint
                          SET status=%s, error_message=%s, updated_at=CURRENT_TIMESTAMP
                        WHERE dataset=%s AND checkpoint_key=ANY(%s)""",
                    (status, error, dataset, list(chunk)),
                )
                return int(cur.rowcount or 0)

        total = 0
        scope_key = self._kernel._checkpoint_batch_scope_key(
            "history-checkpoint", dataset, scope, values
        )
        resume_after, done = self._kernel._checkpoint_cursor(context, scope_key)
        if done:
            return 0
        for index, chunk in enumerate(
            self._kernel.chunk_planner.iter_chunks(values), 1
        ):
            if index <= resume_after:
                continue
            self._kernel._reserve_atomic_chunk(chunk)
            total += self._kernel._run_chunk(lambda chunk=chunk: update(chunk))
            self._kernel._checkpoint_progress(
                context.run_id, context.job_name, scope_key, index
            )
        self._kernel._checkpoint_done(context.run_id, context.job_name, scope_key)
        return total

    def invalidate_foundation_checkpoints(
        self, context: RunContext, scope: WorkScope, run_id: str, error: str,
        stages: Sequence[str] | None = None,
    ) -> int:
        """Reset one foundation run's checkpoints in a bounded set-based write.

        Resume invalidation is itself a framework operation.  Updating each
        checkpoint through a separate upsert creates thousands of tiny
        transactions and can take longer than the rebuild.  The run lock
        makes this scoped update exclusive; the checkpoint table's row budget
        keeps it below the normal transaction ceiling.
        """
        if not run_id or not error:
            raise ValueError("run_id and error are required")
        plan = SqlPlan(
            operation=Operation.UPSERT,
            target="ops.foundation_rebuild_checkpoint",
            scope=scope,
            expected_mode="SET_BASED",
            estimated_rows=self._kernel.transaction_manager.config.max_rows_per_tx,
        )
        self._kernel.query_guard.check(plan)

        if stages is not None:
            stages = tuple(dict.fromkeys(str(stage) for stage in stages if str(stage)))
            if not stages:
                return 0

        def invalidate() -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                sql = """UPDATE ops.foundation_rebuild_checkpoint
                            SET status='PENDING', error_message=%s,
                                updated_at=CURRENT_TIMESTAMP
                          WHERE run_id=%s AND status<>'PENDING'"""
                params: tuple[object, ...] = (error, run_id)
                if stages is not None:
                    sql += " AND stage=ANY(%s)"
                    params += (list(stages),)
                cur.execute(sql, params)
                return int(cur.rowcount or 0)

        return self._kernel._run_chunk(invalidate)

    def save_foundation_checkpoint(
        self,
        context: RunContext,
        scope: WorkScope,
        row: Mapping[str, Any],
    ) -> int:
        """Apply the current state of one foundation-stage partition.

        This mutable orchestration row is deliberately not routed through the
        generic batch ``upsert`` checkpoint. A completed data-access cursor
        describes an earlier write attempt, not the row's current database
        state: code-identity invalidation may later move the row back to
        PENDING, after which COMPLETED must be applied again.
        """
        required = (
            "run_id", "stage", "partition_key", "status",
            "rows_attempted", "rows_written", "result_hash", "error_message",
        )
        if any(column not in row for column in required):
            raise ValueError("foundation checkpoint row is incomplete")
        if row["status"] not in {"PENDING", "RUNNING", "COMPLETED", "FAILED"}:
            raise ValueError("invalid foundation checkpoint status")
        plan = SqlPlan(
            operation=Operation.UPSERT,
            target="ops.foundation_rebuild_checkpoint",
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=1,
        )
        self._kernel.query_guard.check(plan)

        def save() -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO ops.foundation_rebuild_checkpoint
                         (run_id, stage, partition_key, status, rows_attempted,
                          rows_written, result_hash, error_message)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (run_id, stage, partition_key) DO UPDATE SET
                         status=EXCLUDED.status,
                         rows_attempted=EXCLUDED.rows_attempted,
                         rows_written=EXCLUDED.rows_written,
                         result_hash=EXCLUDED.result_hash,
                         error_message=EXCLUDED.error_message,
                         updated_at=CURRENT_TIMESTAMP""",
                    tuple(row[column] for column in required),
                )
                return int(cur.rowcount or 0)

        return self._kernel._run_chunk(save)

    def commit_release_validation(
        self, context: RunContext, scope: WorkScope, result: Mapping[str, Any],
    ) -> int:
        """Promote one release, superseding only its domain's previous release."""
        baseline_domain = str(result.get("baseline_domain") or "").strip()
        if baseline_domain not in {"RESEARCH", "MARKET_ACTIVITY"}:
            raise ValueError(f"unsupported release baseline domain: {baseline_domain!r}")
        plan = SqlPlan(operation=Operation.UPSERT, target="ops.tushare_data_release", scope=scope, expected_mode="ROW_BASED", estimated_rows=2)
        self._kernel.query_guard.check(plan)

        watermark = result.get("release_change_watermark")
        if self._kernel.release_change_capture_enabled and watermark is None:
            raise RuntimeError(
                "release change capture is enabled; full release validation must "
                "include its audited event watermark"
            )
        if watermark is not None:
            self._kernel.query_guard.check(SqlPlan(
                operation=Operation.READ, target="ops.release_change_event",
                scope=scope, estimated_rows=1,
            ))

        def update() -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                if watermark is not None:
                    cur.execute(
                        "SELECT pg_advisory_xact_lock(%s)",
                        (RELEASE_CHANGE_LOCK_KEY,),
                    )
                    cur.execute(
                        "SELECT COALESCE(MAX(event_id), 0) FROM ops.release_change_event"
                    )
                    current = int((cur.fetchone() or (0,))[0])
                    if current != int(watermark):
                        raise RuntimeError(
                            "source changes occurred during full release validation; "
                            "rerun the quality audit before publishing"
                        )
                if watermark is not None:
                    cur.execute(
                        "SELECT release_id FROM ops.tushare_data_release "
                        "WHERE release_tag=%s AND status='draft' FOR UPDATE",
                        (result["tag"],),
                    )
                    if cur.fetchone() is None:
                        raise RuntimeError(f"draft release not found: {result['tag']}")
                cur.execute(
                    "UPDATE ops.tushare_data_release SET status='superseded' "
                    "WHERE status='validated' AND baseline_domain=%s AND release_tag<>%s",
                    (baseline_domain, result["tag"]),
                )
                if watermark is None:
                    cur.execute(
                        """UPDATE ops.tushare_data_release
                              SET status='validated', quality_report_hash=%s,
                                  quality_report=%s, source_manifest=%s,
                                  source_manifest_hash=%s,
                                  data_cutoff_timestamp=COALESCE(data_cutoff_timestamp, CURRENT_TIMESTAMP),
                                  visibility_policy_version=COALESCE(visibility_policy_version, 'business-pit-v2'),
                                  temporal_evidence_level=COALESCE(temporal_evidence_level, 'LEGACY'),
                                  validated_at=CURRENT_TIMESTAMP
                            WHERE release_tag=%s AND status='draft'""",
                        (result["quality_hash"], result["gate_json"], result["manifest_json"], result["source_manifest_hash"], result["tag"]),
                    )
                else:
                    cur.execute(
                        """UPDATE ops.tushare_data_release
                              SET status='validated', quality_report_hash=%s,
                                  quality_report=%s, source_manifest=%s,
                                  source_manifest_hash=%s,
                                  release_change_watermark=%s,
                                  data_cutoff_timestamp=COALESCE(data_cutoff_timestamp, CURRENT_TIMESTAMP),
                                  visibility_policy_version=COALESCE(visibility_policy_version, 'business-pit-v2'),
                                  temporal_evidence_level=COALESCE(temporal_evidence_level, 'LEGACY'),
                                  validated_at=CURRENT_TIMESTAMP
                            WHERE release_tag=%s AND status='draft'""",
                        (result["quality_hash"], result["gate_json"], result["manifest_json"], result["source_manifest_hash"], int(watermark), result["tag"]),
                    )
                changed = int(cur.rowcount or 0)
                if watermark is not None and changed != 1:
                    raise RuntimeError(f"failed to publish draft release: {result['tag']}")
                return changed

        return self._kernel._run_chunk(update)

    def begin_release_validation_boundary(
        self, context: RunContext, scope: WorkScope,
    ) -> int | None:
        """Capture a source-event boundary before a full release quality audit.

        The exclusive transaction lock waits for all captured fact writers and
        prevents new ones from entering until this short watermark read commits.
        """
        if not self._kernel.release_change_capture_enabled:
            return None
        plan = SqlPlan(
            operation=Operation.READ, target="ops.release_change_event",
            scope=scope, estimated_rows=1,
        )
        self._kernel.query_guard.check(plan)

        def read_boundary() -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (RELEASE_CHANGE_LOCK_KEY,),
                )
                cur.execute(
                    "SELECT COALESCE(MAX(event_id), 0) FROM ops.release_change_event"
                )
                return int((cur.fetchone() or (0,))[0])

        return self._kernel._run_chunk(read_boundary, idempotent=False)

    def capture_release_delta_candidate(
        self,
        context: RunContext,
        scope: WorkScope,
        baseline_release_id: int,
        baseline_domain: str,
        *,
        max_events: int = 1_000,
    ) -> dict[str, Any]:
        """Seal one bounded, unvalidated event candidate against a full release.

        This only records the exact event set. It does not validate or promote a
        data release; unknown, oversized, incomplete, or failed inputs fail closed.
        """
        domain = str(baseline_domain).strip().upper()
        if domain not in {"RESEARCH", "MARKET_ACTIVITY"}:
            raise ValueError(f"unsupported release baseline domain: {domain!r}")
        if not 1 <= int(max_events) <= 1_000:
            raise ValueError("max_events must be between 1 and 1000")
        if not self._kernel.release_change_capture_enabled:
            raise RuntimeError("release change capture is disabled")
        package_plan = SqlPlan(
            operation=Operation.PUBLISH,
            target="ops.release_delta_package", scope=scope,
            expected_mode="ROW_BASED", estimated_rows=max_events + 1,
        )
        event_plan = SqlPlan(
            operation=Operation.PUBLISH,
            target="ops.release_delta_event", scope=scope,
            expected_mode="ROW_BASED", estimated_rows=max_events,
        )
        parent_plan = SqlPlan(
            operation=Operation.READ,
            target="ops.tushare_data_release", scope=scope, estimated_rows=1,
        )
        event_read_plan = SqlPlan(
            operation=Operation.READ,
            target="ops.release_change_event", scope=scope,
            estimated_rows=max_events + 1,
        )
        self._kernel.query_guard.check(package_plan)
        self._kernel.query_guard.check(event_plan)
        self._kernel.query_guard.check(parent_plan)
        self._kernel.query_guard.check(event_read_plan)

        def seal() -> dict[str, Any]:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (RELEASE_CHANGE_LOCK_KEY,),
                )
                cur.execute(
                    """SELECT release_id, release_change_watermark
                         FROM ops.tushare_data_release
                        WHERE release_id=%s AND baseline_domain=%s AND status='validated'
                        FOR SHARE""",
                    (int(baseline_release_id), domain),
                )
                parent = cur.fetchone()
                if parent is None or parent[1] is None:
                    raise RuntimeError(
                        "delta capture requires the current validated full baseline "
                        "to have a release-change watermark; run a captured full audit first"
                    )
                watermark = int(parent[1])
                cur.execute("SELECT COALESCE(MAX(event_id), 0) FROM ops.release_change_event")
                high_watermark = int((cur.fetchone() or (0,))[0])

                cur.execute(
                    """SELECT package_id, event_through_id, event_manifest_hash, event_count
                         FROM ops.release_delta_package
                        WHERE baseline_release_id=%s
                        ORDER BY created_at DESC LIMIT 1""",
                    (int(baseline_release_id),),
                )
                previous = cur.fetchone()
                if previous is not None:
                    if high_watermark > int(previous[1]):
                        raise RuntimeError(
                            "a captured delta candidate is still awaiting target-specific "
                            "quality validation; refusing to extend or consume it twice"
                        )
                    return {
                        "status": "CAPTURED",
                        "package_id": str(previous[0]),
                        "event_through_id": int(previous[1]),
                        "event_manifest_hash": str(previous[2]),
                        "event_count": int(previous[3]),
                        "idempotent_replay": True,
                    }
                if high_watermark <= watermark:
                    return {
                        "status": "NO_CHANGES", "baseline_release_id": int(baseline_release_id),
                        "event_through_id": watermark, "event_count": 0,
                    }

                cur.execute(
                    """SELECT e.event_id, e.event_key, e.run_id, e.target, e.operation,
                              e.scope_hash, s.delta_eligible, s.full_audit_reason, r.status
                         FROM ops.release_change_event e
                         JOIN ops.release_change_scope s USING (scope_hash)
                         LEFT JOIN ops.run_manifest r USING (run_id)
                        WHERE e.event_id>%s AND e.event_id<=%s
                        ORDER BY e.event_id LIMIT %s""",
                    (watermark, high_watermark, int(max_events) + 1),
                )
                events = cur.fetchall()
                if len(events) > int(max_events):
                    raise RuntimeError(
                        f"release delta event window exceeds {max_events}; full audit required"
                    )
                if not events:
                    # Gaps in BIGSERIAL are normal after rollback; no committed
                    # events in this range means there is nothing to package.
                    return {
                        "status": "NO_CHANGES", "baseline_release_id": int(baseline_release_id),
                        "event_through_id": high_watermark, "event_count": 0,
                    }
                invalid = [
                    row for row in events
                    if not bool(row[6]) or row[8] != "SUCCEEDED"
                ]
                if invalid:
                    details = [
                        {
                            "event_id": int(row[0]), "target": str(row[3]),
                            "reason": str(row[7] or "producer Run Manifest is not SUCCEEDED"),
                            "run_status": row[8],
                        }
                        for row in invalid[:10]
                    ]
                    raise RuntimeError(
                        "release delta requires a full audit because captured events "
                        f"are unbounded or their producer runs are incomplete: {details}"
                    )

                event_digest = hashlib.sha256()
                target_counts: dict[str, int] = {}
                scope_hashes: set[str] = set()
                for row in events:
                    event_id, event_key, _, target, _, scope_hash = row[:6]
                    event_digest.update(f"{int(event_id)}:{str(event_key)}\n".encode("ascii"))
                    target_counts[str(target)] = target_counts.get(str(target), 0) + 1
                    scope_hashes.add(str(scope_hash))
                event_hash = event_digest.hexdigest()
                package_id = str(uuid5(
                    NAMESPACE_URL,
                    f"release-delta:{domain}:{baseline_release_id}:{watermark}:{high_watermark}:{event_hash}",
                ))
                scope_manifest = {
                    "format": "release-delta-scope-v1",
                    "scope_hashes": sorted(scope_hashes),
                    "target_event_counts": dict(sorted(target_counts.items())),
                }
                cur.execute(
                    """INSERT INTO ops.release_delta_package
                           (package_id, baseline_release_id, baseline_domain,
                            event_from_id, event_through_id, event_count,
                            event_manifest_hash, scope_manifest, captured_run_id)
                         VALUES (%s::uuid,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)""",
                    (
                        package_id, int(baseline_release_id), domain,
                        int(events[0][0]), high_watermark, len(events), event_hash,
                        json.dumps(scope_manifest, sort_keys=True), context.run_id,
                    ),
                )
                cur.executemany(
                    """INSERT INTO ops.release_delta_event
                           (package_id, baseline_domain, event_id)
                         VALUES (%s::uuid,%s,%s)""",
                    [(package_id, domain, int(row[0])) for row in events],
                )
                return {
                    "status": "CAPTURED", "package_id": package_id,
                    "baseline_release_id": int(baseline_release_id),
                    "event_from_id": int(events[0][0]),
                    "event_through_id": high_watermark,
                    "event_count": len(events), "event_manifest_hash": event_hash,
                    "scope_manifest": scope_manifest,
                    "requires_quality_validation": True,
                }

        return self._kernel._run_chunk(seal, idempotent=False)

    def revoke_release(
        self, context: RunContext, scope: WorkScope, tag: str,
        reason: str, evidence_hash: str, actor: str,
    ) -> int:
        """Move a validated/superseded release to REVOKED with evidence."""
        if len(evidence_hash) != 64:
            raise ValueError("release evidence hash must be 64 characters")
        plan = SqlPlan(
            operation=Operation.UPSERT, target="ops.tushare_data_release",
            scope=scope, expected_mode="ROW_BASED", estimated_rows=1,
        )
        self._kernel.query_guard.check(plan)

        def update() -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT set_config('app.release_actor', %s, true)", (actor,))
                cur.execute(
                    """UPDATE ops.tushare_data_release
                          SET status='revoked', revoked_at=CURRENT_TIMESTAMP,
                              revoked_by=%s, revocation_reason=%s,
                              revocation_evidence_hash=%s
                        WHERE release_tag=%s AND status IN ('validated','superseded')""",
                    (actor, reason, evidence_hash, tag),
                )
                return int(cur.rowcount or 0)

        return self._kernel._run_chunk(update)

    def fail_building_snapshots(
        self, context: RunContext, scope: WorkScope, snapshot_ids: Sequence[str], reason: str,
    ) -> int:
        """Mark only BUILDING snapshots as FAILED; FROZEN rows remain immutable."""
        ids = tuple(str(value) for value in snapshot_ids)
        if not ids:
            return 0
        plan = SqlPlan(operation=Operation.UPSERT, target="research.snapshot_manifest", scope=scope, expected_mode="ROW_BASED", estimated_rows=len(ids))
        self._kernel.query_guard.check(plan)

        def update(chunk) -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE research.snapshot_manifest
                          SET status='FAILED', failed_at=CURRENT_TIMESTAMP, failure_reason=%s
                        WHERE snapshot_id=ANY(%s::uuid[]) AND status='BUILDING'""",
                    (reason[:2000], list(chunk)),
                )
                return int(cur.rowcount or 0)

        total = 0
        for chunk in self._kernel.chunk_planner.iter_chunks(ids):
            self._kernel._reserve_atomic_chunk(chunk)
            total += self._kernel._run_chunk(lambda chunk=chunk: update(chunk))
        return total

    def replace_day(
        self,
        context: RunContext,
        scope: WorkScope,
        target: str,
        date_column: str,
        day: Any,
        rows: Sequence[Mapping[str, Any]],
    ) -> tuple[int, int]:
        """Atomically replace one bounded day partition for snapshot feeds."""
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError("write.replace_day requires RunContext and WorkScope")
        if not date_column.isidentifier():
            raise ValueError("invalid replace_day date column")
        planned = self._kernel.chunk_planner.plan(list(rows)) if rows else []
        if len(planned) > 1:
            raise ValueError(
                "replace_day exceeds one transaction row/byte budget; use rebuild"
            )
        t = validate_qualified_name(target)
        columns = self._columns(rows) if rows else []
        plan = SqlPlan(operation=Operation.REBUILD, target=t, scope=scope, expected_mode="ROW_BASED", estimated_rows=len(rows), allow_delete=True)
        self._kernel.query_guard.check(plan)
        def replace() -> tuple[int, int]:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {t} WHERE {date_column}=%s", (day,))
                affected = 0
                if rows:
                    col_list = ", ".join(columns)
                    placeholders = ", ".join(["%s"] * len(columns))
                    cur.executemany(
                        f"INSERT INTO {t} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                        [[row[column] for column in columns] for row in rows],
                    )
                    affected = int(cur.rowcount or 0)
                return len(rows), affected
        self._kernel._reserve_atomic_chunk(rows)
        event = self._kernel._build_release_change_event(
            context, "REPLACE_DAY", t, scope, rows=rows,
        )
        if event is not None:
            event["record_empty"] = True
        return self._kernel._run_chunk(replace, change_event=event)

    def transition_snapshot_manifest(
        self,
        context: RunContext,
        scope: WorkScope,
        snapshot_id: str,
        from_statuses: Sequence[str],
        to_status: str,
        fields: Mapping[str, Any] | None = None,
    ) -> int:
        """Guarded state transition for the Research Snapshot lifecycle."""
        return self.transition_snapshot_manifests(
            context, scope, [{"snapshot_id": snapshot_id, **(fields or {})}],
            from_statuses, to_status,
        )

    def set_snapshot_lineage_storage_version(
        self,
        context: RunContext,
        scope: WorkScope,
        snapshot_ids: Sequence[str],
        from_version: str = "v1",
        to_version: str = "v2",
    ) -> int:
        """Switch a bounded set of pre-freeze manifests to lineage v2.

        This is a named metadata migration, not a general UPDATE surface. It
        cannot touch FROZEN manifests and only changes the explicit lineage
        storage selector. The caller must validate the selected series before
        invoking it and may safely replay the operation.
        """
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError(
                "set_snapshot_lineage_storage_version requires RunContext and WorkScope"
            )
        ids = tuple(str(snapshot_id) for snapshot_id in snapshot_ids)
        if not ids or len(ids) > self._kernel.transaction_manager.config.max_rows_per_tx:
            raise ValueError("snapshot lineage version migration batch is out of bounds")
        if from_version not in {"v1", "v2"} or to_version not in {"v1", "v2"}:
            raise ValueError("unsupported snapshot lineage storage version")
        if from_version == to_version:
            raise ValueError("snapshot lineage storage version must change")
        plan = SqlPlan(
            operation=Operation.UPSERT,
            target="research.snapshot_manifest",
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=len(ids),
        )
        self._kernel.query_guard.check(plan)
        sql = """
            UPDATE research.snapshot_manifest
               SET lineage_storage_version=%s
             WHERE snapshot_id=ANY(%s::uuid[])
               AND lineage_storage_version=%s
               AND status <> 'FROZEN'
        """
        return self._kernel._run_chunk(
            lambda: self._update_snapshot_manifest(
                sql, (to_version, list(ids), from_version)
            )
        )

    def prepare_snapshot_lineage_v2_rebuild(
        self,
        context: RunContext,
        scope: WorkScope,
        snapshot_ids: Sequence[str],
        reason: str,
    ) -> int:
        """Atomically switch and reopen a bounded pre-freeze snapshot set.

        The first v2 migration step must not leave a half-switched series if
        the process exits between metadata and lifecycle updates.  This named
        operation changes only v1, non-FROZEN manifests and moves them to
        FAILED with an auditable PRE_FREEZE_REPAIR reason.  It is idempotent:
        replay after the commit sees no v1 rows and leaves already rebuilt
        VALIDATED snapshots untouched.
        """
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError(
                "prepare_snapshot_lineage_v2_rebuild requires RunContext and WorkScope"
            )
        ids = tuple(str(snapshot_id) for snapshot_id in snapshot_ids)
        if not ids or len(ids) > self._kernel.transaction_manager.config.max_rows_per_tx:
            raise ValueError("snapshot lineage v2 rebuild batch is out of bounds")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("snapshot lineage v2 rebuild reason must not be blank")
        plan = SqlPlan(
            operation=Operation.UPSERT,
            target="research.snapshot_manifest",
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=len(ids),
        )
        self._kernel.query_guard.check(plan)
        sql = """
            UPDATE research.snapshot_manifest
               SET lineage_storage_version='v2',
                   status='FAILED',
                   failure_reason=%s
             WHERE snapshot_id=ANY(%s::uuid[])
               AND lineage_storage_version='v1'
               AND status <> 'FROZEN'
        """
        return self._kernel._run_chunk(
            lambda: self._update_snapshot_manifest(
                sql, (f"PRE_FREEZE_REPAIR: {reason[:1980]}", list(ids))
            )
        )

    def transition_observation_snapshot(
        self,
        context: RunContext,
        scope: WorkScope,
        snapshot_id: str,
        from_statuses: Sequence[str],
        to_status: str,
        fields: Mapping[str, Any] | None = None,
    ) -> int:
        """Guarded lifecycle transition for ``observation.snapshot``.

        This is intentionally a named transition rather than a generic update
        entry point. The database trigger remains the final FROZEN guard; this
        method only controls the allowed state machine and mutable metadata.
        """
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError("observation snapshot transition requires RunContext and WorkScope")
        transitions = {
            "DRAFT": {"BUILDING", "FAILED"},
            "FAILED": {"BUILDING", "FAILED"},
            "BUILDING": {"BUILDING", "VALIDATED", "FAILED"},
            "VALIDATED": {"FROZEN", "FAILED"},
        }
        if not from_statuses or any(
            to_status not in transitions.get(str(status), set()) for status in from_statuses
        ):
            raise ValueError(
                f"invalid observation snapshot state transition: {tuple(from_statuses)} -> {to_status}"
            )
        fields = dict(fields or {})
        allowed = {
            "input_manifest_hash", "quality_report_hash", "quality_report",
            "code_identity", "row_count", "stock_count", "sector_count", "date_count",
            "failure_reason", "built_at", "validated_at", "frozen_at", "failed_at",
        }
        if set(fields) - allowed:
            raise ValueError(
                f"unsupported observation snapshot fields: {sorted(set(fields) - allowed)}"
            )
        if to_status == "FROZEN" and any(
            not fields.get(name) for name in ("input_manifest_hash", "quality_report_hash", "frozen_at")
        ):
            raise ValueError("FROZEN observation snapshot requires input/quality hashes and frozen_at")
        columns = tuple(sorted(fields))
        values = [snapshot_id, *(fields.get(column) for column in columns)]
        placeholders = ", ".join(["%s"] * len(values))
        type_casts = {
            "quality_report": "jsonb",
            "code_identity": "jsonb",
            "date_count": "integer",
            "row_count": "bigint",
            "stock_count": "bigint",
            "sector_count": "bigint",
            "built_at": "timestamptz",
            "validated_at": "timestamptz",
            "frozen_at": "timestamptz",
            "failed_at": "timestamptz",
        }
        assignments = ", ".join(
            f"{column}=v.{column}::{type_casts[column]}"
            if column in type_casts else f"{column}=v.{column}"
            for column in columns
        )
        value_columns = ", ".join(("snapshot_id", *columns))
        sql = (
            "UPDATE observation.snapshot AS m SET status=%s"
            + (f", {assignments}" if assignments else "")
            + f" FROM (VALUES ({placeholders})) AS v({value_columns})"
            + " WHERE m.snapshot_id=v.snapshot_id::uuid"
            + " AND m.status=ANY(%s::text[])"
        )
        plan = SqlPlan(
            operation=Operation.UPSERT,
            target="observation.snapshot",
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=1,
        )
        self._kernel.query_guard.check(plan)
        return self._kernel._run_chunk(
            lambda: self._update_snapshot_manifest(
                sql, [to_status, *values, list(from_statuses)]
            )
        )

    def transition_market_activity_snapshot(
        self,
        context: RunContext,
        scope: WorkScope,
        activity_snapshot_id: str,
        from_statuses: Sequence[str],
        to_status: str,
        fields: Mapping[str, Any] | None = None,
    ) -> int:
        """Transition one market-activity snapshot through its named lifecycle.

        This is deliberately separate from the Observation snapshot transition:
        market activity has its own identity and child tables, while sharing the
        same fail-closed lifecycle semantics.  The SQL remains private to the
        Gateway and the database trigger is the final frozen-state authority.
        """
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError(
                "market activity snapshot transition requires RunContext and WorkScope"
            )
        transitions = {
            "DRAFT": {"BUILDING", "FAILED"},
            "FAILED": {"BUILDING", "FAILED"},
            "BUILDING": {"BUILDING", "VALIDATED", "FAILED"},
            "VALIDATED": {"FROZEN", "FAILED"},
        }
        if not from_statuses or any(
            to_status not in transitions.get(str(status), set()) for status in from_statuses
        ):
            raise ValueError(
                "invalid market activity snapshot transition: "
                f"{tuple(from_statuses)} -> {to_status}"
            )
        fields = dict(fields or {})
        allowed = {
            "input_manifest_hash", "quality_report_hash", "quality_report",
            "code_identity", "row_count", "stock_count", "date_count",
            "total_input_row_count", "target_row_count", "invalid_target_row_count",
            "invalid_history_row_count", "insufficient_history_stock_count",
            "failure_reason", "built_at", "validated_at", "frozen_at", "failed_at",
        }
        unsupported = set(fields) - allowed
        if unsupported:
            raise ValueError(
                "unsupported market activity snapshot fields: "
                f"{sorted(unsupported)}"
            )
        if to_status in {"VALIDATED", "FROZEN"} and any(
            not fields.get(name)
            for name in ("input_manifest_hash", "quality_report_hash", "validated_at")
        ):
            raise ValueError(
                f"{to_status} market activity snapshot requires input/quality hashes and validated_at"
            )
        if to_status == "FROZEN" and not fields.get("frozen_at"):
            raise ValueError("FROZEN market activity snapshot requires frozen_at")

        columns = tuple(sorted(fields))
        values = [activity_snapshot_id, *(fields.get(column) for column in columns)]
        placeholders = ", ".join(["%s"] * len(values))
        type_casts = {
            "quality_report": "jsonb",
            "code_identity": "jsonb",
            "date_count": "integer",
            "row_count": "bigint",
            "stock_count": "bigint",
            "total_input_row_count": "bigint",
            "target_row_count": "bigint",
            "invalid_target_row_count": "bigint",
            "invalid_history_row_count": "bigint",
            "insufficient_history_stock_count": "bigint",
            "built_at": "timestamptz",
            "validated_at": "timestamptz",
            "frozen_at": "timestamptz",
            "failed_at": "timestamptz",
        }
        assignments = ", ".join(
            f"{column}=v.{column}::{type_casts[column]}"
            if column in type_casts else f"{column}=v.{column}"
            for column in columns
        )
        value_columns = ", ".join(("activity_snapshot_id", *columns))
        sql = (
            "UPDATE observation.market_activity_snapshot AS m SET status=%s"
            + (f", {assignments}" if assignments else "")
            + f" FROM (VALUES ({placeholders})) AS v({value_columns})"
            + " WHERE m.activity_snapshot_id=v.activity_snapshot_id::uuid"
            + " AND m.status=ANY(%s::text[])"
        )
        plan = SqlPlan(
            operation=Operation.UPSERT,
            target="observation.market_activity_snapshot",
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=1,
        )
        self._kernel.query_guard.check(plan)
        return self._kernel._run_chunk(
            lambda: self._update_snapshot_manifest(
                sql, [to_status, *values, list(from_statuses)]
            )
        )

    def transition_snapshot_manifests(
        self,
        context: RunContext,
        scope: WorkScope,
        rows: Sequence[Mapping[str, Any]],
        from_statuses: Sequence[str],
        to_status: str,
    ) -> int:
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError("snapshot manifest transition requires RunContext and WorkScope")
        if not rows or not from_statuses or not str(to_status).strip():
            raise ValueError("snapshot manifest transition arguments are incomplete")
        transitions = {
            "DRAFT": {"BUILDING", "FAILED"}, "FAILED": {"BUILDING", "FAILED"},
            "BUILDING": {"BUILDING", "VALIDATED", "FAILED"}, "VALIDATED": {"FROZEN", "FAILED"},
        }
        if any(to_status not in transitions.get(str(status), set()) for status in from_statuses):
            raise ValueError(f"invalid snapshot state transition: {tuple(from_statuses)} -> {to_status}")
        allowed = {
            "status", "quality_report_hash", "quality_report", "code_version", "git_commit",
            "git_dirty", "diff_hash", "failure_reason", "failed_at", "input_manifest_hash",
            "code_archive_path", "code_archive_sha256", "code_archive_size_bytes",
            "validated_at", "frozen_at",
        }
        mutable = set().union(*(set(row) - {"snapshot_id"} for row in rows))
        if not mutable <= allowed:
            raise ValueError(f"unsupported snapshot manifest fields: {sorted(mutable - allowed)}")
        if any("snapshot_id" not in row for row in rows):
            raise ValueError("snapshot manifest transition requires snapshot_id")
        columns = tuple(sorted(mutable - {"status"}))
        if any(set(row) - {"snapshot_id", "status", *columns} for row in rows):
            raise ValueError("snapshot manifest transition rows must have a stable field set")
        plan = SqlPlan(
            operation=Operation.UPSERT, target="research.snapshot_manifest", scope=scope,
            expected_mode="ROW_BASED", estimated_rows=len(rows),
        )
        self._kernel.query_guard.check(plan)
        values = []
        for row in rows:
            values.append([row["snapshot_id"], *(row.get(column) for column in columns)])
        placeholders = ", ".join(["%s"] * (len(columns) + 1))
        assignments = ", ".join(
            f"{column}=v.{column}::jsonb" if column == "quality_report"
            else f"{column}=v.{column}::bigint" if column == "code_archive_size_bytes"
            else f"{column}=v.{column}"
            for column in columns
        )
        value_columns = ", ".join(("snapshot_id", *columns))
        sql = (
            "UPDATE research.snapshot_manifest AS m SET status=%s" +
            (f", {assignments}" if assignments else "") +
            f" FROM (VALUES {', '.join(f'({placeholders})' for _ in values)}) "
            f"AS v({value_columns}) WHERE m.snapshot_id=v.snapshot_id::uuid "
            "AND m.status = ANY(%s::text[]) AND m.status <> 'FROZEN'"
        )
        params = [to_status] + [item for row in values for item in row] + [list(from_statuses)]
        return self._kernel._run_chunk(lambda: self._update_snapshot_manifest(sql, params))

    def transition_dataset_manifest(
        self,
        context: RunContext,
        scope: WorkScope,
        dataset_id: str,
        from_statuses: Sequence[str],
        to_status: str,
        fields: Mapping[str, Any] | None = None,
    ) -> int:
        """Guarded Dataset lifecycle transition; FROZEN rows are immutable."""
        transitions = {
            "DRAFT": {"BUILDING", "FAILED"}, "FAILED": {"BUILDING", "FAILED"},
            "BUILDING": {"VALIDATED", "FAILED"}, "VALIDATED": {"FROZEN", "FAILED"},
        }
        if not from_statuses or any(to_status not in transitions.get(status, set()) for status in from_statuses):
            raise ValueError(f"invalid Dataset state transition: {tuple(from_statuses)} -> {to_status}")
        fields = dict(fields or {})
        allowed = {
            "failure_reason", "failed_at", "quality_report", "quality_report_hash",
            "calendar_hash", "rules_hash", "execution_hash", "corporate_action_hash",
            "benchmark_hash", "benchmark_source_manifest", "benchmark_source_manifest_hash",
            "input_manifest_hash", "code_version", "git_commit",
            "git_dirty", "diff_hash", "code_archive_path", "code_archive_sha256",
            "code_archive_size_bytes", "validated_at", "frozen_at",
        }
        if set(fields) - allowed:
            raise ValueError(f"unsupported Dataset manifest fields: {sorted(set(fields) - allowed)}")
        columns = tuple(sorted(fields))
        values = [dataset_id, *(fields.get(column) for column in columns)]
        placeholders = ", ".join(["%s"] * len(values))

        # A VALUES relation has no knowledge of the target table's column
        # types.  In particular, a NULL parameter is otherwise inferred as
        # text and PostgreSQL rejects assignments such as
        # ``failed_at = v.failed_at``.  Keep the cast at this Gateway
        # boundary so callers can pass ordinary Python values, including
        # None, without weakening the manifest state transition contract.
        type_casts = {
            "quality_report": "jsonb",
            "benchmark_source_manifest": "jsonb",
            "code_archive_size_bytes": "bigint",
            "git_dirty": "boolean",
            "failed_at": "timestamp",
            "validated_at": "timestamp",
            "frozen_at": "timestamp",
        }

        def typed_value(column: str) -> str:
            cast = type_casts.get(column)
            return f"v.{column}::{cast}" if cast else f"v.{column}"

        assignments = ", ".join(
            f"{column}={typed_value(column)}"
            for column in columns
        )
        value_columns = ", ".join(("dataset_id", *columns))
        sql = (
            "UPDATE backtest.dataset_manifest AS m SET status=%s"
            + (f", {assignments}" if assignments else "")
            + f" FROM (VALUES ({placeholders})) AS v({value_columns})"
            + " WHERE m.dataset_id=v.dataset_id::uuid AND m.status = ANY(%s::text[]) AND m.status <> 'FROZEN'"
        )
        plan = SqlPlan(operation=Operation.UPSERT, target="backtest.dataset_manifest", scope=scope, expected_mode="ROW_BASED", estimated_rows=1)
        self._kernel.query_guard.check(plan)
        return self._kernel._run_chunk(lambda: self._update_snapshot_manifest(sql, [to_status, *values, list(from_statuses)]))

    def transition_contract_snapshot(
        self,
        context: RunContext,
        scope: WorkScope,
        snapshot_id: str,
        from_statuses: Sequence[str],
        to_status: str,
        fields: Mapping[str, Any],
    ) -> int:
        return self._transition_contract_product(
            context=context,
            scope=scope,
            target="contract.snapshot_manifest",
            id_column="snapshot_id",
            product_id=snapshot_id,
            from_statuses=from_statuses,
            to_status=to_status,
            fields=fields,
            allowed_fields={
                "input_manifest_hash", "quality_report_hash", "manifest_hash",
                "failure_reason", "validated_at", "frozen_at",
            },
        )

    def transition_change_events(
        self,
        context: RunContext,
        scope: WorkScope,
        event_ids: Sequence[str],
        status: str,
    ) -> int:
        if status not in {"RESOLVED", "APPLIED", "FAILED"}:
            raise ValueError("unsupported change event terminal status")
        ids = tuple(str(event_id) for event_id in event_ids)
        if not ids:
            return 0
        plan = SqlPlan(
            operation=Operation.UPSERT,
            target="ops.change_event",
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=len(ids),
        )
        self._kernel.query_guard.check(plan)

        def update(chunk) -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE ops.change_event SET status=%s "
                    "WHERE event_id=ANY(%s::uuid[]) AND status='PENDING'",
                    (status, list(chunk)),
                )
                return int(cur.rowcount or 0)

        total = 0
        for chunk in self._kernel.chunk_planner.iter_chunks(ids):
            self._kernel._reserve_atomic_chunk(chunk)
            total += self._kernel._run_chunk(lambda chunk=chunk: update(chunk))
        return total

    def transition_contract_dataset(
        self,
        context: RunContext,
        scope: WorkScope,
        dataset_id: str,
        from_statuses: Sequence[str],
        to_status: str,
        fields: Mapping[str, Any],
    ) -> int:
        return self._transition_contract_product(
            context=context,
            scope=scope,
            target="contract.dataset_release",
            id_column="dataset_id",
            product_id=dataset_id,
            from_statuses=from_statuses,
            to_status=to_status,
            fields=fields,
            allowed_fields={
                "manifest_hash", "quality_report_hash", "failure_reason",
                "validated_at", "frozen_at",
            },
        )

    def _transition_contract_product(
        self,
        *,
        context: RunContext,
        scope: WorkScope,
        target: str,
        id_column: str,
        product_id: str,
        from_statuses: Sequence[str],
        to_status: str,
        fields: Mapping[str, Any],
        allowed_fields: set[str],
    ) -> int:
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError("contract transition requires RunContext and WorkScope")
        transitions = {
            "DRAFT": {"BUILDING", "FAILED"},
            "FAILED": {"BUILDING", "FAILED"},
            "BUILDING": {"VALIDATED", "FAILED"},
            "VALIDATED": {"FROZEN", "FAILED"},
        }
        if not from_statuses or any(
            to_status not in transitions.get(str(status), set())
            for status in from_statuses
        ):
            raise ValueError(
                f"invalid contract transition: {tuple(from_statuses)} -> {to_status}"
            )
        if set(fields) - allowed_fields:
            raise ValueError(
                f"unsupported contract fields: {sorted(set(fields) - allowed_fields)}"
            )
        if to_status == "FROZEN":
            required = {"manifest_hash", "quality_report_hash", "frozen_at"}
            if target == "contract.snapshot_manifest":
                required.add("input_manifest_hash")
            if any(not fields.get(name) for name in required):
                raise ValueError(f"FROZEN requires fields: {sorted(required)}")
        assignments = ", ".join(f"{column}=%s" for column in fields)
        sql = (
            f"UPDATE {target} SET status=%s"
            + (f", {assignments}" if assignments else "")
            + f" WHERE {id_column}=%s AND status=ANY(%s::text[]) "
              "AND status <> 'FROZEN'"
        )
        plan = SqlPlan(
            operation=Operation.UPSERT,
            target=target,
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=1,
        )
        self._kernel.query_guard.check(plan)
        params = [to_status, *fields.values(), product_id, list(from_statuses)]
        return self._kernel._run_chunk(
            lambda: self._update_snapshot_manifest(sql, params)
        )

    def update_dataset_manifest(
        self,
        context: RunContext,
        scope: WorkScope,
        dataset_id: str,
        allowed_statuses: Sequence[str],
        fields: Mapping[str, Any],
    ) -> int:
        """Update mutable build metadata while explicitly excluding FROZEN rows."""
        if not fields or not allowed_statuses:
            raise ValueError("Dataset manifest update requires fields and statuses")
        allowed = {
            "failure_reason", "failed_at", "quality_report", "quality_report_hash", "calendar_hash",
            "rules_hash", "execution_hash", "corporate_action_hash", "benchmark_hash", "input_manifest_hash",
            "benchmark_source_manifest", "benchmark_source_manifest_hash", "code_version", "git_commit", "git_dirty", "diff_hash",
        }
        if set(fields) - allowed:
            raise ValueError(f"unsupported Dataset metadata fields: {sorted(set(fields) - allowed)}")
        assignments = ", ".join(f"{column}=%s" for column in fields)
        sql = f"UPDATE backtest.dataset_manifest SET {assignments} WHERE dataset_id=%s AND status = ANY(%s::text[]) AND status <> 'FROZEN'"
        plan = SqlPlan(operation=Operation.UPSERT, target="backtest.dataset_manifest", scope=scope, expected_mode="ROW_BASED", estimated_rows=1)
        self._kernel.query_guard.check(plan)
        return self._kernel._run_chunk(lambda: self._update_snapshot_manifest(sql, [*fields.values(), dataset_id, list(allowed_statuses)]))

    def resume_failed_dataset_manifest(
        self,
        context: RunContext,
        scope: WorkScope,
        dataset_id: str,
    ) -> int:
        """Reopen an existing failed Dataset using UPDATE-only semantics.

        A partial upsert is unsafe here: the Dataset manifest has immutable
        baseline identity columns and the baseline trigger runs for the
        attempted INSERT before PostgreSQL reaches its conflict branch.
        """
        sql = (
            "UPDATE backtest.dataset_manifest "
            "SET status='BUILDING', failure_reason=NULL, failed_at=NULL "
            "WHERE dataset_id=%s AND status='FAILED'"
        )
        plan = SqlPlan(
            operation=Operation.UPSERT,
            target="backtest.dataset_manifest",
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=1,
        )
        self._kernel.query_guard.check(plan)
        return self._kernel._run_chunk(
            lambda: self._update_snapshot_manifest(sql, (dataset_id,))
        )

    def update_dataset_member_stats(
        self,
        context: RunContext,
        scope: WorkScope,
        dataset_id: str,
        research_date: Any,
        fields: Mapping[str, Any],
    ) -> int:
        """Update an existing Dataset member's derived build statistics.

        Dataset members are created with required identity columns before the
        build starts.  This is deliberately UPDATE-only: a partial row must
        never be synthesized by an Upsert that omits ``research_snapshot_id``
        or ``member_role``.
        """
        if not fields:
            raise ValueError("Dataset member statistics require fields")
        allowed = {
            "observation_row_count", "execution_row_count", "execution_hash",
            "corporate_action_hash", "quality_status",
        }
        if set(fields) - allowed:
            raise ValueError(
                f"unsupported Dataset member statistic fields: {sorted(set(fields) - allowed)}"
            )
        assignments = ", ".join(
            f"{validate_table_name(column)}=%s" for column in fields
        )
        sql = (
            f"UPDATE backtest.dataset_member SET {assignments} "
            "WHERE dataset_id=%s AND research_date=%s "
            "AND EXISTS (SELECT 1 FROM backtest.dataset_manifest dm "
            "WHERE dm.dataset_id=backtest.dataset_member.dataset_id "
            "AND dm.status <> 'FROZEN')"
        )
        plan = SqlPlan(
            operation=Operation.UPSERT,
            target="backtest.dataset_member",
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=1,
        )
        self._kernel.query_guard.check(plan)
        params = [*fields.values(), dataset_id, research_date]
        return self._kernel._run_chunk(
            lambda: self._update_snapshot_manifest(sql, params)
        )

    def update_dataset_member_stats_batch(
        self,
        context: RunContext,
        scope: WorkScope,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        """Update many existing Dataset members without an insert fallback."""
        if not rows:
            return 0
        allowed = {
            "dataset_id", "research_date", "observation_row_count",
            "execution_row_count", "execution_hash", "corporate_action_hash",
            "quality_status",
        }
        columns = tuple(rows[0].keys())
        if set(columns) - allowed or not {"dataset_id", "research_date"} <= set(columns):
            raise ValueError("Dataset member batch update has unsupported or missing identity fields")
        if any(tuple(row.keys()) != columns for row in rows):
            raise ValueError("Dataset member batch update columns must be identical")
        update_columns = tuple(column for column in columns if column not in {"dataset_id", "research_date"})
        if not update_columns:
            raise ValueError("Dataset member batch update requires statistic fields")
        if set(update_columns) - {
            "observation_row_count", "execution_row_count", "execution_hash",
            "corporate_action_hash", "quality_status",
        }:
            raise ValueError("Dataset member batch update contains non-statistic fields")
        value_columns = ", ".join(columns)
        typed = {
            "dataset_id": "uuid",
            "research_date": "date",
            "observation_row_count": "bigint",
            "execution_row_count": "bigint",
        }
        assignments = ", ".join(
            f"{column}=v.{column}::{typed[column]}"
            if column in typed else f"{column}=v.{column}"
            for column in update_columns
        )
        plan = SqlPlan(
            operation=Operation.UPSERT,
            target="backtest.dataset_member",
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=len(rows),
        )
        self._kernel.query_guard.check(plan)

        total = 0
        materialized = list(rows)
        for chunk in self._kernel.chunk_planner.iter_chunks(materialized):
            self._kernel._reserve_atomic_chunk(chunk)
            params = [value for row in chunk for value in (row[column] for column in columns)]
            chunk_value_sql = ", ".join(
                "(" + ", ".join("%s" for _ in columns) + ")"
                for _ in chunk
            )
            chunk_sql = (
                f"UPDATE backtest.dataset_member AS m SET {assignments} "
                f"FROM (VALUES {chunk_value_sql}) AS v({value_columns}) "
                "WHERE m.dataset_id=v.dataset_id::uuid "
                "AND m.research_date=v.research_date::date "
                "AND EXISTS (SELECT 1 FROM backtest.dataset_manifest dm "
                "WHERE dm.dataset_id=m.dataset_id AND dm.status <> 'FROZEN')"
            )
            total += self._kernel._run_chunk(
                lambda sql=chunk_sql, params=params: self._update_snapshot_manifest(sql, params)
            )
        return total

    def update_backtest_run_status(
        self,
        context: RunContext,
        scope: WorkScope,
        run_id: str,
        status: str,
        failure_reason: str | None = None,
    ) -> int:
        """Transition an existing formal run without partial-row upsert.

        ``run_manifest`` has several NOT NULL identity and audit columns. A
        partial ``INSERT ... ON CONFLICT DO UPDATE`` still has to pass the
        INSERT checks before PostgreSQL can reach the conflict branch, so
        status transitions must use this explicitly named UPDATE operation.
        The database state trigger remains the authority for the transition.
        """
        if status not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            raise ValueError(f"unsupported Backtest Run terminal status: {status}")
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("Backtest Run id cannot be empty")
        plan = SqlPlan(
            operation=Operation.UPSERT,
            target="backtest.run_manifest",
            scope=scope,
            expected_mode="ROW_BASED",
            estimated_rows=1,
        )
        self._kernel.query_guard.check(plan)
        sql = (
            "UPDATE backtest.run_manifest "
            "SET status=%s, failure_reason=%s "
            "WHERE run_id=%s AND status='RUNNING'"
        )
        return self._kernel._run_chunk(
            lambda: self._update_snapshot_manifest(
                sql, (status, failure_reason, run_id)
            )
        )

    def _update_snapshot_manifest(self, sql: str, params: Sequence[Any]) -> int:
        conn = self._kernel._db_connection()
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return int(cur.rowcount or 0)

    # ---- 内部 ----

    @staticmethod
    def _columns(rows: Sequence[Mapping[str, Any]]) -> list[str]:
        first = rows[0]
        cols = [validate_table_name(column) for column in first.keys()]
        for row in rows:
            if list(row.keys()) != cols:
                raise ValueError("写入行之间列不一致")
        return cols

    def _upsert_chunk(
        self,
        target: str,
        columns: list[str],
        chunk: Sequence[Mapping[str, Any]],
        key: tuple[str, ...],
        update_columns: tuple[str, ...],
        skip_noop: bool = False,
    ) -> int:
        col_list = ", ".join(columns)
        key_sql = ", ".join(key)
        if update_columns:
            set_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
            where = ""
            if skip_noop:
                changed = " OR ".join(
                    f"{target}.{column} IS DISTINCT FROM EXCLUDED.{column}"
                    for column in update_columns
                )
                where = f" WHERE {changed}"
            conflict = f"ON CONFLICT ({key_sql}) DO UPDATE SET {set_sql}{where}"
        else:
            conflict = f"ON CONFLICT ({key_sql}) DO NOTHING"
        staging = f"_staging_upsert_{uuid4().hex[:12]}"
        sql = f"INSERT INTO {target} ({col_list}) SELECT {col_list} FROM {staging} {conflict}"
        # COPY staging + one SET-based merge.  The connection and transaction
        # remain private to the Kernel transaction boundary.
        conn = self._kernel._db_connection()
        with conn.cursor() as cur:
            values = [[row[c] for c in columns] for row in chunk]
            create_projection_staging(cur, staging, target, columns)
            copy_or_execute_values(cur, staging, columns, values)
            cur.execute(sql)
            return int(cur.rowcount or 0)

    def _insert_chunk(self, target: str, columns: list[str], chunk: Sequence[Mapping[str, Any]]) -> int:
        conn = self._kernel._db_connection()
        with conn.cursor() as cur:
            values = [[row[c] for c in columns] for row in chunk]
            return copy_or_execute_values(cur, target, columns, values)

    def _insert_ignore_chunk(self, target: str, columns: list[str], chunk: Sequence[Mapping[str, Any]]) -> int:
        col_list = ", ".join(columns)
        staging = f"_staging_insert_{uuid4().hex[:12]}"
        sql = (
            f"INSERT INTO {target} ({col_list}) "
            f"SELECT {col_list} FROM {staging} ON CONFLICT DO NOTHING"
        )
        conn = self._kernel._db_connection()
        with conn.cursor() as cur:
            values = [[row[c] for c in columns] for row in chunk]
            create_projection_staging(cur, staging, target, columns)
            copy_or_execute_values(cur, staging, columns, values)
            cur.execute(sql)
            return int(cur.rowcount or 0)
