"""PublishGateway：冻结快照 / 回测数据集发布（§4 PUBLISH）。

对应 DatasetPolicy.write_mode=PUBLISH_ONLY 的表（如 contract.stock_snapshot_daily）。
FROZEN 后绝对只读（由 DatasetPolicy.immutable_after_freeze + frozen 双重保证）。
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.query_guard import validate_qualified_name, validate_table_name


class PublishGateway:
    def __init__(self, kernel) -> None:
        self._kernel = kernel

    def snapshot(
        self,
        context: RunContext,
        scope: WorkScope,
        target: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        """发布冻结快照（PUBLISH_ONLY）。"""
        return self._publish_rows(context, scope, target, rows, "snapshot")

    def dataset(
        self,
        context: RunContext,
        scope: WorkScope,
        target: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        """发布回测数据集（PUBLISH_ONLY）。"""
        return self._publish_rows(context, scope, target, rows, "dataset")

    def _publish_rows(
        self,
        context: RunContext,
        scope: WorkScope,
        target: str,
        rows: Sequence[Mapping[str, Any]],
        kind: str,
    ) -> int:
        if not isinstance(context, RunContext):
            raise TypeError(f"publish.{kind} 必须携带 RunContext")
        if not isinstance(scope, WorkScope):
            raise TypeError(f"publish.{kind} 必须携带 WorkScope")
        if not rows:
            return 0
        t = validate_qualified_name(target)
        plan = SqlPlan(
            operation=Operation.PUBLISH, target=t, scope=scope,
            expected_mode="ROW_BASED", estimated_rows=len(rows),
        )
        self._kernel.query_guard.check(plan)
        policy = self._kernel.policy_registry.require(t)
        if policy.require_snapshot_id:
            snapshot_id = scope.dimensions.get("snapshot_id")
            if any(str(row.get("snapshot_id")) != str(snapshot_id) for row in rows):
                raise ValueError(
                    "published row snapshot_id must match WorkScope.snapshot_id"
                )

        # 走 bulk_insert 的 chunk 语义，但写入受 PUBLISH_ONLY 约束（query_guard 已校验）
        chunks = self._kernel.chunk_planner.plan(list(rows))
        written = 0
        scope_key = self._kernel._checkpoint_batch_scope_key(
            f"publish:{kind}", t, scope, rows
        )
        resume_after, done = self._kernel._checkpoint_cursor(context, scope_key)
        if done:
            return 0
        for idx, chunk in enumerate(chunks, start=1):
            if idx <= resume_after:
                continue
            self._kernel._reserve_atomic_chunk(chunk)
            written += self._kernel._run_chunk(lambda: self._insert_chunk(t, chunk))
            self._kernel._checkpoint_progress(context.run_id, context.job_name, scope_key, idx)
        self._kernel._checkpoint_done(context.run_id, context.job_name, scope_key)
        return written

    def _insert_chunk(self, target: str, chunk: Sequence[Mapping[str, Any]]) -> int:
        columns = [validate_table_name(column) for column in chunk[0].keys()]
        if any(list(row.keys()) != columns for row in chunk):
            raise ValueError("publish rows must have a stable column set")
        col_list = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {target} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        conn = self._kernel._db_connection()
        with conn.cursor() as cur:
            cur.executemany(sql, [[row[c] for c in columns] for row in chunk])
            return int(cur.rowcount or 0)
