"""WriteGateway：Standard/PIT/Feature 增量写（§4.2 / §4.3）。

upsert 内部自动 ChunkPlanner：调用者不需要写 for batch 循环——循环属于基础设施。
bulk_insert 大量纯插入（staging / 新版本）。
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence
from uuid import uuid4

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
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
                lambda: self._upsert_chunk(t, columns, chunk, key, update_columns, skip_noop)
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
            written += self._kernel._run_chunk(lambda: self._insert_ignore_chunk(t, columns, chunk))
            self._kernel._checkpoint_progress(context.run_id, context.job_name, scope_key, idx)
        self._kernel._checkpoint_done(context.run_id, context.job_name, scope_key)
        return len(rows), written

    def claim_financial_queue(
        self, context: RunContext, scope: WorkScope, limit: int = 10000,
    ) -> tuple[list[int], list[str]]:
        """Claim a bounded financial change queue batch atomically."""
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError("claim_financial_queue requires RunContext and WorkScope")
        if limit <= 0 or limit > 10000:
            raise ValueError("financial queue claim limit must be in 1..10000")
        if limit > self._kernel.transaction_manager.config.max_rows_per_tx:
            raise ValueError("financial queue claim exceeds transaction row budget")
        plan = SqlPlan(operation=Operation.UPSERT, target="ops.financial_change_queue", scope=scope, expected_mode="ROW_BASED", estimated_rows=limit)
        self._kernel.query_guard.check(plan)

        def claim() -> tuple[list[int], list[str]]:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """WITH picked AS (
                           SELECT queue_id FROM ops.financial_change_queue
                            WHERE status IN ('PENDING','FAILED')
                            ORDER BY queue_id LIMIT %s FOR UPDATE SKIP LOCKED
                       )
                       UPDATE ops.financial_change_queue q
                          SET status='RUNNING', attempts=q.attempts+1, updated_at=CURRENT_TIMESTAMP
                         FROM picked
                        WHERE q.queue_id=picked.queue_id
                       RETURNING q.queue_id, q.ts_code""",
                    (limit,),
                )
                rows = cur.fetchall()
            return [int(row[0]) for row in rows], [str(row[1]) for row in rows]

        # Claiming changes queue state and increments attempts.  If the server
        # committed but the client lost the acknowledgement, replay could
        # claim a different batch, so this boundary is explicitly non-retryable.
        return self._kernel._run_chunk(claim, idempotent=False)

    def update_financial_queue(
        self, context: RunContext, scope: WorkScope, queue_ids: Sequence[int],
        status: str, error: str | None,
    ) -> int:
        """Complete/fail a typed financial queue batch."""
        ids = tuple(int(value) for value in queue_ids)
        if not ids:
            return 0
        if status not in {"DONE", "FAILED", "PENDING"}:
            raise ValueError(f"unsupported financial queue status: {status}")
        plan = SqlPlan(operation=Operation.UPSERT, target="ops.financial_change_queue", scope=scope, expected_mode="ROW_BASED", estimated_rows=len(ids))
        self._kernel.query_guard.check(plan)

        def update(chunk) -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE ops.financial_change_queue
                          SET status=%s, last_error=%s, updated_at=CURRENT_TIMESTAMP,
                              completed_at=CASE WHEN %s='DONE' THEN CURRENT_TIMESTAMP ELSE completed_at END
                        WHERE queue_id = ANY(%s)""",
                    (status, error, status, list(chunk)),
                )
                return int(cur.rowcount or 0)

        total = 0
        scope_key = self._kernel._checkpoint_batch_scope_key(
            "financial-queue", status, scope, ids
        )
        resume_after, done = self._kernel._checkpoint_cursor(context, scope_key)
        if done:
            return 0
        for index, chunk in enumerate(
            self._kernel.chunk_planner.iter_chunks(ids, bytes_per_row=8), 1
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
        """Atomically promote one release and supersede prior validated releases."""
        plan = SqlPlan(operation=Operation.UPSERT, target="ops.tushare_data_release", scope=scope, expected_mode="ROW_BASED", estimated_rows=2)
        self._kernel.query_guard.check(plan)

        def update() -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE ops.tushare_data_release SET status='superseded' "
                    "WHERE status='validated' AND release_tag<>%s",
                    (result["tag"],),
                )
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
                return int(cur.rowcount or 0)

        return self._kernel._run_chunk(update)

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
        return self._kernel._run_chunk(replace)

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
        assignments = ", ".join(
            f"{column}=v.{column}::jsonb" if column in {"quality_report", "code_identity"}
            else f"{column}=v.{column}::integer" if column == "date_count"
            else f"{column}=v.{column}::bigint" if column in {"row_count", "stock_count", "sector_count"}
            else f"{column}=v.{column}"
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
