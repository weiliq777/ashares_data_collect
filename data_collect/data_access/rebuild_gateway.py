"""RebuildGateway：大范围重建（§4.6 / §4.7）。

禁止业务层 DELETE + INSERT——大范围重建只能走 rebuild.publish()。
SET-based SQL 按语义拆分成 run_insert_select / run_scoped_merge /
run_partition_replace / run_table_replace，每个都强制携带 SqlPlan，
禁止泛化的 run_set_sql()。
"""
from __future__ import annotations

import hashlib
import re
import uuid
from datetime import date
from typing import Any, Callable, Iterable, Mapping, Sequence

from data_collect.data_access.context import (
    Operation,
    RebuildMode,
    RunContext,
    SqlPlan,
    WorkScope,
)
from data_collect.data_access.query_guard import validate_qualified_name, validate_table_name
from data_collect.data_access.bulk_loader import copy_or_execute_values


class RebuildGateway:
    def __init__(self, kernel) -> None:
        self._kernel = kernel

    # ---- 完整重建生命周期 ----

    def _create_like(self, context: RunContext, source: str, target: str, unlogged: bool = True) -> str:
        """创建隔离构建表；仅供 Repository 的 smoke/staging 生命周期使用。"""
        if not isinstance(context, RunContext):
            raise TypeError("_create_like 必须携带 RunContext")
        source = validate_qualified_name(source)
        target = validate_qualified_name(target)
        kind = "UNLOGGED TABLE" if unlogged else "TABLE"

        def create_tx() -> None:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(f"CREATE {kind} IF NOT EXISTS {target} (LIKE {source} INCLUDING ALL)")

        self._kernel._run_chunk(create_tx)
        return target

    def publish(
        self,
        context: RunContext,
        scope: WorkScope,
        target: str,
        producer: Callable[[WorkScope], Iterable[Mapping[str, Any]]],
        mode: RebuildMode = RebuildMode.PARTITION_REPLACE,
    ) -> int:
        """大范围重建：Staging → 分批计算/COPY → Checkpoint → Validate → Hash → Atomic Publish。"""
        if not isinstance(context, RunContext):
            raise TypeError("rebuild.publish 必须携带 RunContext")
        if not isinstance(scope, WorkScope):
            raise TypeError("rebuild.publish 必须携带 WorkScope")
        if not isinstance(mode, RebuildMode):
            raise TypeError("mode 必须是 RebuildMode")
        if not callable(producer):
            raise TypeError("producer 必须是可调用（接收 scope，产出数据行）")

        target = validate_qualified_name(target)
        plan = SqlPlan(
            operation=Operation.REBUILD, target=target, scope=scope,
            expected_mode="ROW_BASED",
            allow_delete=mode == RebuildMode.SCOPED_MERGE,
        )
        self._kernel.query_guard.check(plan)

        lock_scope = self._lock_scope_key(scope, mode)
        with self._kernel.lock_manager.session_exclusive(target, lock_scope):
            # SCOPED_MERGE only needs a session-local staging relation.  The
            # application role intentionally has no CREATE privilege on
            # business schemas; permanent staging is reserved for
            # TABLE_REPLACE/PARTITION_REPLACE, where catalog publication
            # requires an owned relation.
            temporary_staging = mode == RebuildMode.SCOPED_MERGE
            staging = (
                self._temporary_staging_name(target, context)
                if temporary_staging
                else self._staging_name(target, context)
            )
            written = 0
            attached = False
            published = False
            try:
                scope_key = self._kernel._checkpoint_batch_scope_key(
                    f"rebuild:{mode.value}", target, scope, ()
                )

                def create_staging() -> bool:
                    conn = self._kernel._db_connection()
                    with conn.cursor() as cur:
                        if temporary_staging:
                            # TEMP relations are connection-local. A durable
                            # chunk checkpoint is resumable only while its
                            # matching staging relation still exists in this
                            # session. Process/connection restart removes the
                            # relation but not the checkpoint.
                            cur.execute("SELECT to_regclass(%s)", (staging,))
                            existed = bool(cur.fetchone()[0])
                            if existed:
                                return False
                            cur.execute(
                                f"CREATE TEMP TABLE {staging} "
                                f"(LIKE {target} INCLUDING ALL) "
                                "ON COMMIT PRESERVE ROWS"
                            )
                            return True
                        else:
                            cur.execute(f"CREATE TABLE IF NOT EXISTS {staging} (LIKE {target})")
                            return False

                staging_created = self._kernel._run_chunk(create_staging)

                if temporary_staging and staging_created:
                    # A new empty TEMP table cannot contain chunks described
                    # by a checkpoint from an earlier database session. Reset
                    # only this bounded scope and stage it again from chunk 1.
                    self._kernel._checkpoint_clear(context, scope_key)

                # Consume lazily under both row and estimated-byte budgets.
                columns: list[str] | None = None
                resume_after, done = self._kernel._checkpoint_cursor(context, scope_key)
                if done:
                    return 0
                batches = self._kernel.chunk_planner.iter_chunks(producer(scope))
                for idx, batch in enumerate(batches, start=1):
                    if idx <= resume_after:
                        continue
                    batch_columns = [
                        validate_table_name(column) for column in batch[0].keys()
                    ]
                    if any(list(row.keys()) != batch_columns for row in batch):
                        raise ValueError("rebuild producer rows must have a stable column set")
                    if columns is None:
                        columns = batch_columns
                    elif batch_columns != columns:
                        raise ValueError("rebuild producer batches changed their column set")
                    written += self._bulk_insert_staging(staging, columns, batch)
                    self._kernel._checkpoint_progress(
                        context.run_id, context.job_name, scope_key, idx
                    )

                self._build_index(staging, target)
                self._kernel.quality.validate(
                    context, scope, staging, target,
                    staging_columns=tuple(columns or ()),
                )
                self._staging_hash(staging, target)

                attached = self._publish(
                    mode, target, staging, scope, context=context,
                )
                published = True
                self._kernel._checkpoint_done(
                    context.run_id, context.job_name, scope_key
                )
                return written
            finally:
                # Failed rebuilds retain their deterministic staging table so
                # the next invocation with the same run/scope can resume from
                # the last committed checkpoint.  Successful scoped merges do
                # not attach/rename staging and therefore must remove it.
                if published and not attached:
                    self._drop_staging(staging)

    # ---- SET-based 语义接口（强制 SqlPlan，禁止泛化 run_set_sql）----

    def run_insert_select(self, sql: str, plan: SqlPlan, context: RunContext, params=None) -> int:
        return self._run_set_based("run_insert_select", sql, plan, context, params)

    def run_snapshot_lineage_bundle(
        self, sql: str, plan: SqlPlan, context: RunContext, params=None,
    ) -> int:
        """Materialize one bounded compact lineage bundle.

        The statement has two deliberately fixed write targets: the
        content-addressed source dictionary and the per-stock bundle table.
        This named operation is the narrow exception to the ordinary
        single-target ``run_insert_select`` parser; it is not a general SQL
        escape hatch. Both targets are checked against concrete policies and
        the operation remains one statement/one short Kernel transaction.
        """
        return self._run_snapshot_lineage_bundle(
            sql, plan, context, params,
            operation_name="run_snapshot_lineage_bundle",
            required_source=None,
        )

    def run_snapshot_lineage_bundle_from_legacy(
        self, sql: str, plan: SqlPlan, context: RunContext, params=None,
    ) -> int:
        """Convert one bounded legacy lineage slice into a v2 bundle.

        This is a deliberately separate named operation.  It permits only the
        fixed two-target dictionary/bundle statement whose read source is the
        immutable ``research.snapshot_lineage`` table.  It never rebuilds or
        updates ``research.stock_snapshot_daily`` and is therefore a format
        migration, not a snapshot rebuild or a generic SQL escape hatch.
        """
        return self._run_snapshot_lineage_bundle(
            sql, plan, context, params,
            operation_name="run_snapshot_lineage_bundle_from_legacy",
            required_source="research.snapshot_lineage",
        )

    def _run_snapshot_lineage_bundle(
        self,
        sql: str,
        plan: SqlPlan,
        context: RunContext,
        params,
        *,
        operation_name: str,
        required_source: str | None,
    ) -> int:
        if not isinstance(plan, SqlPlan):
            raise TypeError(f"{operation_name} requires SqlPlan")
        if plan.target != "research.snapshot_lineage_bundle":
            raise ValueError(
                f"{operation_name} target must be research.snapshot_lineage_bundle"
            )
        if plan.operation != Operation.REBUILD or plan.expected_mode != "SET_BASED":
            raise ValueError(f"{operation_name} requires REBUILD/SET_BASED")
        if not isinstance(context, RunContext):
            raise TypeError(f"{operation_name} requires RunContext")
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError(f"{operation_name} SQL must not be empty")

        source_plan = SqlPlan(
            operation=Operation.REBUILD,
            target="research.lineage_source_record",
            scope=plan.scope,
            expected_mode="SET_BASED",
            estimated_rows=max(0, int(plan.estimated_rows or 0) * 10),
            allow_insert_select=True,
        )
        if required_source:
            code_scope = plan.scope.codes or ()
            if not code_scope:
                raise ValueError(
                    f"{operation_name} requires a non-empty bounded code scope"
                )
            if len(code_scope) > 5_000:
                raise ValueError(
                    f"{operation_name} code scope exceeds the 5000-code safety ceiling"
                )
        self._kernel.query_guard.check(plan)
        self._kernel.query_guard.check(source_plan)
        self._validate_snapshot_lineage_bundle_sql(
            sql, required_source=required_source,
        )

        def run_sql() -> int:
            # The dictionary is shared by every snapshot series.  A unique
            # conflict that races another transaction can wait, then remain
            # invisible to the current statement snapshot; the subsequent
            # bundle FK write would otherwise see a NULL source_ref_id.  A
            # transaction-scoped advisory lock makes the fixed two-target
            # statement deterministic without widening its data scope.
            self._kernel.lock_manager.acquire_exclusive(
                "research.lineage_source_record", "content-addressed-dictionary"
            )
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params) if params else ())
                affected = int(cur.rowcount or 0)
                if required_source and affected != len(plan.scope.codes or ()):
                    # Raise before Kernel commits so an incomplete legacy
                    # source slice cannot leave a visible partial bundle.
                    raise RuntimeError(
                        f"{operation_name} produced {affected} rows for "
                        f"{len(plan.scope.codes or ())} requested codes"
                    )
                return affected

        # Both writes are replay-safe: source rows use DO NOTHING and bundle
        # rows use a deterministic scoped upsert.
        return self._kernel._run_chunk(run_sql, idempotent=True)

    def swap_partitioned_table(
        self, context: RunContext, scope: WorkScope,
        legacy_table: str, partitioned_table: str, final_table: str,
    ) -> str:
        """Short, explicit catalog cutover for a validated partition target.

        This is intentionally a named schema operation, not a general DDL or
        execute escape hatch.  The old table is retained under a deterministic
        backup name so rollback remains possible until post-cutover checks pass.
        """
        names = tuple(validate_qualified_name(value) for value in (legacy_table, partitioned_table, final_table))
        schemas = {value.rsplit('.', 1)[0] for value in names if '.' in value}
        if len(schemas) != 1 or final_table.rsplit('.', 1)[-1] != legacy_table.rsplit('.', 1)[-1]:
            raise ValueError("partitioned table cutover requires same-schema tables and matching final name")
        schema = final_table.rsplit('.', 1)[0]
        final_name = final_table.rsplit('.', 1)[-1]
        suffix = hashlib.sha256(partitioned_table.encode("utf-8")).hexdigest()[:12]
        backup_name = f"_legacy_{final_name[:40]}_{suffix}"
        backup_table = f"{schema}.{backup_name}"
        plan = SqlPlan(operation=Operation.REBUILD, target=final_table, scope=scope, expected_mode="ROW_BASED")
        self._kernel.query_guard.check(plan)

        def swap() -> str:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(f"LOCK TABLE {legacy_table} IN ACCESS EXCLUSIVE MODE")
                cur.execute(f"ALTER TABLE {legacy_table} RENAME TO {backup_name}")
                cur.execute(f"ALTER TABLE {partitioned_table} RENAME TO {final_name}")
            return backup_table

        return self._kernel._run_chunk(swap, idempotent=False)

    def swap_research_snapshot_partitioned(
        self, context: RunContext, scope: WorkScope,
        migration_id: str, approved_by: str,
    ) -> str:
        """Cut over the validated research snapshot and install its freeze guard."""
        # This is a physical catalog cutover of the partitioned staging
        # object, not a row-level snapshot write.  Use the staging policy so
        # QueryGuard does not incorrectly require a business snapshot_id for
        # an operation that atomically renames whole tables.
        plan = SqlPlan(
            operation=Operation.REBUILD,
            target="research.stock_snapshot_daily_partitioned",
            scope=scope, expected_mode="ROW_BASED",
        )
        self._kernel.query_guard.check(plan)
        if not migration_id.strip() or not approved_by.strip():
            raise ValueError("snapshot cutover requires migration_id and approved_by")
        suffix = hashlib.sha256(context.run_id.encode("utf-8")).hexdigest()[:12]
        backup_name = f"_legacy_stock_snapshot_daily_{suffix}"

        def swap() -> str:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                # DDL remains inside this named Gateway operation, but the
                # table-owner portion is delegated to the narrowly scoped,
                # SECURITY DEFINER database function.  The runtime role keeps
                # ordinary DML privileges only.
                cur.execute(
                    "SELECT research.cutover_stock_snapshot_partitioned(%s, %s, %s)",
                    (migration_id, approved_by[:120], backup_name),
                )
                row = cur.fetchone()
            if not row or not row[0]:
                raise RuntimeError("snapshot cutover function returned no backup table")
            return str(row[0])

        return self._kernel._run_chunk(swap, idempotent=False)

    def swap_daily_kline_partitioned(
        self, context: RunContext, scope: WorkScope,
        migration_id: str, approved_by: str,
    ) -> str:
        """Retain the legacy public daily-kline table after validated cutover."""
        plan = SqlPlan(
            operation=Operation.REBUILD,
            target="std.daily_kline",
            scope=scope, expected_mode="ROW_BASED",
        )
        self._kernel.query_guard.check(plan)
        if not migration_id.strip() or not approved_by.strip():
            raise ValueError("daily_kline cutover requires migration_id and approved_by")
        suffix = hashlib.sha256(context.run_id.encode("utf-8")).hexdigest()[:12]
        backup_name = f"_legacy_daily_kline_{suffix}"

        def swap() -> str:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ops.cutover_daily_kline_partitioned(%s, %s, %s)",
                    (migration_id, approved_by[:120], backup_name),
                )
                row = cur.fetchone()
            if not row or not row[0]:
                raise RuntimeError("daily_kline cutover function returned no backup table")
            return str(row[0])

        return self._kernel._run_chunk(swap, idempotent=False)

    def swap_adj_factor_daily_partitioned(
        self, context: RunContext, scope: WorkScope,
        migration_id: str, approved_by: str,
    ) -> str:
        """Retain the legacy public adjusted-factor table after cutover."""
        plan = SqlPlan(
            operation=Operation.REBUILD,
            target="std.adj_factor_daily", scope=scope, expected_mode="ROW_BASED",
        )
        self._kernel.query_guard.check(plan)
        if not migration_id.strip() or not approved_by.strip():
            raise ValueError("adj_factor_daily cutover requires migration_id and approved_by")
        suffix = hashlib.sha256(context.run_id.encode("utf-8")).hexdigest()[:12]
        backup_name = f"_legacy_adj_factor_daily_{suffix}"

        def swap() -> str:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ops.cutover_adj_factor_daily_partitioned(%s, %s, %s)",
                    (migration_id, approved_by[:120], backup_name),
                )
                row = cur.fetchone()
            if not row or not row[0]:
                raise RuntimeError("adj_factor_daily cutover function returned no backup table")
            return str(row[0])

        return self._kernel._run_chunk(swap, idempotent=False)

    def swap_valuation_daily_partitioned(
        self, context: RunContext, scope: WorkScope,
        migration_id: str, approved_by: str,
    ) -> str:
        """Retain the legacy public valuation table after cutover."""
        plan = SqlPlan(
            operation=Operation.REBUILD,
            target="std.valuation_daily", scope=scope, expected_mode="ROW_BASED",
        )
        self._kernel.query_guard.check(plan)
        if not migration_id.strip() or not approved_by.strip():
            raise ValueError("valuation_daily cutover requires migration_id and approved_by")
        suffix = hashlib.sha256(context.run_id.encode("utf-8")).hexdigest()[:12]
        backup_name = f"_legacy_valuation_daily_{suffix}"

        def swap() -> str:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ops.cutover_valuation_daily_partitioned(%s, %s, %s)",
                    (migration_id, approved_by[:120], backup_name),
                )
                row = cur.fetchone()
            if not row or not row[0]:
                raise RuntimeError("valuation_daily cutover function returned no backup table")
            return str(row[0])

        return self._kernel._run_chunk(swap, idempotent=False)

    def retire_legacy_financial_standard(
        self, context: RunContext, scope: WorkScope,
        migration_id: str, source_table: str, approved_by: str,
    ) -> str:
        """Retain a validated public financial Standard table as a backup.

        The canonical ``std`` relation is already populated and is switched in
        Repository code before this operation.  The narrowly allowlisted
        SECURITY DEFINER function only renames the old public relation; it is
        not a general DDL or SQL execution entry point.
        """
        spec = {
            "financial_income_standard": "std.financial_income_standard",
            "financial_balance_sheet_standard": "std.financial_balance_sheet_standard",
            "financial_cash_flow_standard": "std.financial_cash_flow_standard",
            "fina_indicator_standard": "std.fina_indicator_standard",
            "stock_financial_indicator": "std.stock_financial_indicator",
        }
        target = spec.get(source_table)
        if target is None:
            raise ValueError(f"unsupported financial Standard source: {source_table}")
        if not migration_id.strip() or not approved_by.strip():
            raise ValueError("financial Standard cutover requires migration_id and approved_by")
        plan = SqlPlan(operation=Operation.REBUILD, target=target, scope=scope, expected_mode="ROW_BASED")
        self._kernel.query_guard.check(plan)
        suffix = hashlib.sha256(context.run_id.encode("utf-8")).hexdigest()[:12]
        backup_name = f"_legacy_{source_table[:38]}_{suffix}"

        def retire() -> str:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ops.cutover_financial_standard(%s, %s, %s, %s)",
                    (source_table, backup_name, approved_by[:120], migration_id),
                )
                row = cur.fetchone()
            if not row or not row[0]:
                raise RuntimeError("financial Standard cutover function returned no backup table")
            return str(row[0])

        return self._kernel._run_chunk(retire, idempotent=False)

    def run_scoped_merge(self, sql: str, plan: SqlPlan, context: RunContext, params=None) -> int:
        return self._run_set_based("run_scoped_merge", sql, plan, context, params)

    def clear_observation_date(
        self,
        context: RunContext,
        scope: WorkScope,
        snapshot_id: str,
        observation_date: date,
        codes: Sequence[str],
    ) -> int:
        """Clear one mutable Observation date through the named DB operation.

        Observation retries can produce a different tag set after an input
        correction.  A plain UPSERT cannot remove a stale tag, while the
        application role intentionally has no table-wide DELETE privilege.
        The migration installs one fixed, snapshot-checked security-definer
        function; this typed Gateway method is its only application entry
        point.  It is bounded by one date and the explicit eligible code list.
        """
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError("clear_observation_date requires RunContext and WorkScope")
        if scope.date_from != observation_date or scope.date_to != observation_date:
            raise ValueError("Observation cleanup requires a one-day WorkScope")
        if not str(snapshot_id).strip():
            raise ValueError("snapshot_id is required")
        scoped_snapshot_id = str(scope.dimensions.get("snapshot_id") or "").strip()
        if scoped_snapshot_id != str(snapshot_id).strip():
            raise ValueError("Observation cleanup scope snapshot_id does not match argument")
        normalized_codes = tuple(dict.fromkeys(str(code) for code in codes if str(code).strip()))
        if not normalized_codes:
            return 0
        # One stock row, one confirmation, up to seven governed tags and one
        # sector aggregate can be removed for each code.  The DB function is
        # deliberately called in small code batches, so a retry can never
        # accumulate a date-wide DELETE transaction.
        max_rows = self._kernel.transaction_manager.config.max_rows_per_tx
        if max_rows < 10:
            raise ValueError("Observation cleanup requires a transaction budget of at least 10 rows")
        max_codes_per_tx = min(500, (max_rows - 1) // 10)
        if max_codes_per_tx <= 0:
            raise ValueError("Observation cleanup cannot fit within the transaction row budget")

        total_deleted = 0
        for offset in range(0, len(normalized_codes), max_codes_per_tx):
            code_batch = normalized_codes[offset:offset + max_codes_per_tx]
            # Delete sector aggregates exactly once for this date; all later
            # batches are stock-scoped only.  The function itself remains
            # idempotent, so a failed retry safely repeats earlier batches.
            clear_sector = offset == 0
            batch_scope = WorkScope(
                date_from=scope.date_from,
                date_to=scope.date_to,
                codes=code_batch,
                partition=scope.partition,
                dimensions=scope.dimensions,
            )
            estimated_rows = len(code_batch) * 10 + (1 if clear_sector else 0)
            plan = SqlPlan(
                operation=Operation.REBUILD,
                target="observation.stock_anomaly_daily",
                scope=batch_scope,
                expected_mode="SET_BASED",
                estimated_rows=estimated_rows,
                allow_delete=True,
            )
            self._kernel.query_guard.check(plan)

            def clear() -> int:
                conn = self._kernel._db_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT observation.clear_snapshot_date_chunk(%s::uuid, %s::date, %s::text[], %s::boolean)",
                        (snapshot_id, observation_date, list(code_batch), clear_sector),
                    )
                    row = cur.fetchone()
                return int(row[0] or 0) if row else 0

            total_deleted += self._kernel._run_chunk(clear, idempotent=False)
        return total_deleted

    def run_scoped_merge_counted(
        self, sql: str, plan: SqlPlan, context: RunContext, params=None,
    ) -> tuple[int, int]:
        """Run a typed projection whose final SELECT returns attempted/affected counts."""
        if not isinstance(plan, SqlPlan) or plan.expected_mode != "SET_BASED":
            raise TypeError("run_scoped_merge_counted requires a SET_BASED SqlPlan")
        if plan.operation != Operation.REBUILD:
            raise ValueError("run_scoped_merge_counted requires Operation.REBUILD")
        if not isinstance(context, RunContext):
            raise TypeError("run_scoped_merge_counted requires RunContext")
        self._kernel.query_guard.check(plan)
        self._validate_set_sql("run_scoped_merge", sql, plan)

        def run_sql() -> tuple[int, int]:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params) if params else ())
                row = cur.fetchone()
                if not row or len(row) < 2:
                    raise ValueError("counted scoped merge must return attempted and affected counts")
                return int(row[0] or 0), int(row[1] or 0)

        event = self._kernel._build_release_change_event(
            context, "RUN_SCOPED_MERGE_COUNTED", plan.target, plan.scope,
            statement=sql, params=tuple(params) if params else (),
        )
        if event is not None:
            event["record_empty"] = True
        return self._kernel._run_chunk(
            run_sql, idempotent=False, change_event=event,
        )

    def run_partition_replace(self, sql: str, plan: SqlPlan, context: RunContext, params=None) -> int:
        return self._run_set_based("run_partition_replace", sql, plan, context, params)

    def replace_daily_partition_rows(
        self,
        context: RunContext,
        scope: WorkScope,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        """Replace one market day through staged Standard publication.

        This is intentionally a typed rebuild operation, not a SQL escape
        hatch. Chunks are committed while loading a private staging table,
        then one short transaction replaces the requested date in the
        canonical partitioned Standard table. A failure never publishes a
        partially loaded day.
        """
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError("replace_daily_partition_rows requires RunContext and WorkScope")
        if scope.date_from is None or scope.date_to != scope.date_from:
            raise ValueError("daily partition replacement requires one date")
        if not rows:
            return 0

        # std.daily_kline is the canonical Standard relation after validated
        # migration.  The former public.daily_kline remains a legacy backup;
        # keeping it out of this transaction prevents drift between two
        # mutable copies and halves the transaction/write budget.
        targets = ("std.daily_kline",)
        plans = []
        for target in targets:
            plan = SqlPlan(
                operation=Operation.REBUILD,
                target=target,
                scope=scope,
                expected_mode="ROW_BASED",
                estimated_rows=len(rows),
                allow_delete=True,
            )
            self._kernel.query_guard.check(plan)
            plans.append(plan)

        columns = [validate_table_name(column) for column in rows[0].keys()]
        if any(list(row.keys()) != columns for row in rows):
            raise ValueError("daily rows have inconsistent columns")
        if any(row.get("trade_date") != scope.date_from for row in rows):
            raise ValueError("daily rows must stay inside the requested date scope")
        if len(rows) * len(targets) > self._kernel.transaction_manager.config.max_rows_per_tx:
            raise ValueError("daily Standard publish exceeds one transaction row budget")

        staging = tuple(self._daily_staging_name(target, context) for target in targets)
        lock_scope = f"daily:{scope.date_from.isoformat()}"
        with self._kernel.lock_manager.session_exclusive("daily_kline", lock_scope):
            try:
                for target, stage in zip(targets, staging):
                    self._prepare_daily_staging(stage, target)
                    for chunk in self._kernel.chunk_planner.iter_chunks(
                        rows, max_rows_per_chunk=1_000
                    ):
                        self._bulk_insert_staging(stage, columns, chunk)

                def publish() -> int:
                    conn = self._kernel._db_connection()
                    affected = []
                    with conn.cursor() as cur:
                        for target, stage in zip(targets, staging):
                            cur.execute(
                                f"DELETE FROM {target} WHERE trade_date=%s",
                                (scope.date_from,),
                            )
                            cur.execute(
                                f"INSERT INTO {target} ({', '.join(columns)}) "
                                f"SELECT {', '.join(columns)} FROM {stage} "
                                "ON CONFLICT DO NOTHING"
                            )
                            affected.append(int(cur.rowcount or 0))
                    return affected[0]

                change_events = []
                for target in targets:
                    event = self._kernel._build_release_change_event(
                        context, "PARTITION_REPLACE", target, scope, rows=rows,
                    )
                    if event is not None:
                        event["record_empty"] = True
                        change_events.append(event)

                return self._kernel.run_atomic(
                    context,
                    scope,
                    publish,
                    estimated_rows=len(rows) * len(targets),
                    change_events=change_events,
                )
            finally:
                for stage in staging:
                    self._drop_staging(stage)

    def _prepare_daily_staging(self, staging: str, target: str) -> None:
        """Create a session-local staging table for chunked loading.

        The application role must not receive CREATE privilege on public/std
        schemas.  A temporary table keeps the same session across chunk
        commits while PostgreSQL places the object in pg_temp.
        """
        def prepare() -> None:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {staging}")
                cur.execute(
                    f"CREATE TEMP TABLE {staging} "
                    f"(LIKE {target} INCLUDING DEFAULTS) "
                    "ON COMMIT PRESERVE ROWS"
                )

        self._kernel._run_chunk(prepare, idempotent=False)

    @staticmethod
    def _daily_staging_name(target: str, context: RunContext) -> str:
        """Build an unqualified, collision-resistant pg_temp table name."""
        target_key = target.replace(".", "_")
        suffix = hashlib.sha256(context.run_id.encode("utf-8")).hexdigest()[:12]
        return f"_tmp_daily_{target_key}_{suffix}"

    def run_table_replace(self, fill_sql: str, plan: SqlPlan, context: RunContext, params=None) -> int:
        """整表替换（staging + 原子 swap，消除 TRUNCATE）。

        fill_sql 中 '{staging}' 占位符会被替换为实际 staging 表名；语义：
        INSERT INTO {staging} (...) SELECT ... （不写 target，staging 初始为空）。
        框架负责：建 staging → 填充 → swap → 清理。读者要么看到旧表、要么看到新表。
        """
        if not isinstance(plan, SqlPlan):
            raise TypeError("run_table_replace 必须携带 SqlPlan")
        if plan.operation != Operation.REBUILD:
            raise ValueError("run_table_replace requires Operation.REBUILD")
        if plan.expected_mode != "SET_BASED":
            raise ValueError("run_table_replace 要求 SqlPlan.expected_mode == 'SET_BASED'")
        if not isinstance(context, RunContext):
            raise TypeError("run_table_replace 必须携带 RunContext")
        if not isinstance(fill_sql, str) or "{staging}" not in fill_sql:
            raise ValueError("run_table_replace 的 fill_sql 必须含 '{staging}' 占位符")
        self._kernel.query_guard.check(plan)

        target = plan.target
        staging = self._staging_name(target, context)
        filled = fill_sql.replace("{staging}", staging)
        self._validate_table_fill_sql(filled, staging)
        if target not in {"std.security_lifecycle", "pit.financial_asof"}:
            raise ValueError(
                "run_table_replace only supports the registered lifecycle/as-of targets"
            )
        n = 0
        try:
            def create_staging() -> None:
                conn = self._kernel._db_connection()
                with conn.cursor() as cur:
                    # The application role intentionally has no CREATE/ALTER
                    # privilege on business schemas. These two registered
                    # TABLE_REPLACE workflows use a narrow database-side DDL
                    # bridge; the set-based fill SQL remains in the Repository.
                    cur.execute(
                        "SELECT ops.create_table_replace_staging(%s, %s)",
                        (target, staging),
                    )

            self._kernel._run_chunk(create_staging, idempotent=False)
            # 填充 staging（SET-based，statement_timeout 熔断）
            def fill_staging() -> int:
                conn = self._kernel._db_connection()
                with conn.cursor() as cur:
                    cur.execute(filled, tuple(params) if params else ())
                    return int(cur.rowcount or 0)
            n = self._kernel._run_chunk(fill_staging, idempotent=False)
            # 原子 swap（catalog-only，短事务）
            def swap_tables() -> None:
                conn = self._kernel._db_connection()
                with conn.cursor() as cur:
                    old = self._swap_old_name(target, staging)
                    cur.execute(
                        "SELECT ops.swap_table_replace_staging(%s, %s, %s)",
                        (target, staging, old),
                    )
            event = self._kernel._build_release_change_event(
                context, "TABLE_REPLACE", target, WorkScope(),
                statement=filled, params=tuple(params) if params else (),
                force_full_audit_reason="whole-table replacement requires a full audit",
            )
            if event is not None:
                event["record_empty"] = True
            self._kernel._run_chunk(
                swap_tables, idempotent=False, change_event=event,
            )
            return n
        finally:
            self._drop_staging(staging)

    def _run_set_based(
        self, method: str, sql: str, plan: SqlPlan, context: RunContext, params=None
    ) -> int:
        """受控执行 SET-based SQL：QueryGuard 检查 + statement_timeout 熔断（不 ChunkPlanner 切分）。"""
        if not isinstance(plan, SqlPlan):
            raise TypeError(f"{method} 必须携带 SqlPlan")
        if plan.operation != Operation.REBUILD:
            raise ValueError(f"{method} requires Operation.REBUILD")
        if plan.expected_mode != "SET_BASED":
            raise ValueError(f"{method} 要求 SqlPlan.expected_mode == 'SET_BASED'")
        if not isinstance(context, RunContext):
            raise TypeError(f"{method} 必须携带 RunContext")
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError(f"{method} 的 sql 不能为空")
        self._kernel.query_guard.check(plan)
        self._validate_set_sql(method, sql, plan)

        def run_sql() -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params) if params else ())
                return int(cur.rowcount or 0)

        event = self._kernel._build_release_change_event(
            context, method.upper(), plan.target, plan.scope,
            statement=sql, params=tuple(params) if params else (),
        )
        if event is not None:
            event["record_empty"] = True
        return self._kernel._run_chunk(
            run_sql, idempotent=False, change_event=event,
        )

    @staticmethod
    def _validate_set_sql(method: str, sql: str, plan: SqlPlan) -> None:
        """Enforce the semantic boundary of the four typed SET-based APIs.

        SqlPlan is the primary policy input, but it cannot be allowed to claim
        ``INSERT...SELECT`` while executing DDL or an unrelated UPDATE. This
        parser is intentionally conservative: unsupported/multi-purpose SQL is
        rejected and must be expressed through a dedicated Gateway method.
        """
        normalized = re.sub(r"--[^\n]*|/\*.*?\*/", " ", sql, flags=re.S).strip()
        statements = [part.strip() for part in normalized.split(";") if part.strip()]
        if not statements:
            raise ValueError(f"{method} 的 SQL 为空")
        if re.search(r"\b(DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|VACUUM)\b", normalized, re.I):
            raise ValueError(f"{method} 禁止执行 DDL/权限 SQL；请使用专用发布流程")
        target = plan.target.lower()
        collapsed = re.sub(r"\s+", " ", normalized)
        # ``ON CONFLICT DO UPDATE SET`` is not a second DML target. Extract
        # UPDATE separately so its SET keyword is not mistaken for a table.
        dml_targets = {
            relation.lower()
            for relation in re.findall(
                r"\b(?:INSERT\s+INTO|DELETE\s+FROM)\s+"
                r"([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)",
                collapsed,
                re.I,
            )
        }
        dml_targets.update(
            relation.lower()
            for relation in re.findall(
                r"(?<!DO )\bUPDATE\s+"
                r"([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)",
                collapsed,
                re.I,
            )
        )
        if not dml_targets or dml_targets != {target}:
            raise ValueError(
                f"{method} DML targets must equal SqlPlan.target={plan.target}; "
                f"found={sorted(dml_targets)}"
            )
        if target not in normalized.lower():
            raise ValueError(f"{method} SQL 未包含 SqlPlan.target={plan.target}")
        if method == "run_insert_select":
            if not plan.allow_insert_select:
                raise ValueError("run_insert_select 必须显式 allow_insert_select=True")
            insert_match = re.search(r"\bINSERT\s+INTO\b", statements[0], re.I)
            prefix = statements[0][:insert_match.start()] if insert_match else ""
            prefix_dml = re.search(
                r"\b(?:INSERT|UPDATE|DELETE)\s+(?:INTO|FROM)\b",
                prefix, re.I,
            )
            if (
                len(statements) != 1
                or insert_match is None
                or prefix_dml is not None
                or (prefix.strip() and not prefix.lstrip().upper().startswith("WITH"))
            ):
                raise ValueError("run_insert_select 只接受单条 INSERT ... SELECT")
            if not re.search(r"\bSELECT\b", statements[0], re.I):
                raise ValueError("run_insert_select 必须包含 SELECT")
        elif method in {"run_scoped_merge", "run_partition_replace"}:
            if method == "run_partition_replace" and len(statements) != 1:
                raise ValueError("run_partition_replace accepts one statement")
            has_insert = bool(re.search(r"\bINSERT\s+INTO\b", normalized, re.I))
            has_delete = bool(re.search(r"\bDELETE\b", normalized, re.I))
            if not has_insert and not (method == "run_scoped_merge" and has_delete):
                raise ValueError(f"{method} 必须包含 INSERT INTO 或受控 DELETE")
            if method == "run_scoped_merge" and not re.match(
                r"^(DELETE|WITH|INSERT)\b", statements[0], re.I
            ):
                raise ValueError("run_scoped_merge accepts DELETE/WITH/INSERT only")
            if method == "run_scoped_merge" and has_delete:
                if not plan.allow_delete:
                    raise ValueError("run_scoped_merge 含 DELETE 时必须显式 allow_delete=True")
                for statement in statements:
                    if re.search(r"\bDELETE\b", statement, re.I) and not re.search(
                        r"\bWHERE\b", statement, re.I
                    ):
                        raise ValueError("run_scoped_merge 的每个 DELETE 必须带 WHERE")
            if method == "run_partition_replace" and not plan.scope.partition:
                raise ValueError("run_partition_replace 必须携带 scope.partition")
            if method == "run_partition_replace":
                if not (has_insert and has_delete and plan.allow_delete):
                    raise ValueError(
                        "run_partition_replace requires one scoped DELETE+INSERT CTE "
                        "and allow_delete=True"
                    )
                if not re.match(r"^WITH\b", statements[0], re.I) or not re.search(
                    r"\bWHERE\b", statements[0], re.I
                ):
                    raise ValueError(
                        "run_partition_replace must be one WITH DELETE ... WHERE ... INSERT"
                    )
        else:  # pragma: no cover - guarded by the public methods
            raise ValueError(f"未知 SET-based 方法: {method}")

    # ---- 内部 ----

    @staticmethod
    def _validate_snapshot_lineage_bundle_sql(
        sql: str, *, required_source: str | None = None,
    ) -> None:
        """Accept only the fixed two-target lineage bundle statement.

        ``required_source`` narrows the named operation further for the
        legacy-format migration.  The current snapshot builder has a
        different, fixed source relation and continues to use the original
        operation without this restriction.
        """
        normalized = re.sub(r"--[^\n]*|/\*.*?\*/", " ", sql, flags=re.S).strip()
        statements = [part.strip() for part in normalized.split(";") if part.strip()]
        if len(statements) != 1:
            raise ValueError("snapshot lineage bundle must be one statement")
        # The fixed bundle statement legitimately uses ``ON CONFLICT DO
        # UPDATE`` for replay-safe replacement of the same stock/date row.
        # Remove only that clause keyword before rejecting any standalone
        # UPDATE/DELETE/DDL escape hatch.
        forbidden_scan = re.sub(
            r"\bON\s+CONFLICT\b(?:(?!\bINSERT\s+INTO\b).)*?\bDO\s+UPDATE\b",
            "ON CONFLICT DO ", normalized, flags=re.I | re.S,
        )
        if re.search(
            r"\b(DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|VACUUM|UPDATE|DELETE)\b",
            forbidden_scan,
            re.I,
        ):
            raise ValueError("snapshot lineage bundle forbids DDL/UPDATE/DELETE")
        if not re.match(r"^WITH\b", normalized, re.I):
            raise ValueError("snapshot lineage bundle must use a WITH statement")
        targets = {
            relation.lower()
            for relation in re.findall(
                r"\bINSERT\s+INTO\s+"
                r"([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)",
                normalized,
                re.I,
            )
        }
        expected = {
            "research.lineage_source_record",
            "research.snapshot_lineage_bundle",
        }
        if targets != expected:
            raise ValueError(
                "snapshot lineage bundle DML targets must equal "
                f"{sorted(expected)}; found={sorted(targets)}"
            )
        if not re.search(r"\bON\s+CONFLICT\b", normalized, re.I):
            raise ValueError("snapshot lineage bundle must be replay-safe")
        if required_source:
            source_pattern = rf"\bFROM\s+{re.escape(required_source)}\b"
            if not re.search(source_pattern, normalized, re.I):
                raise ValueError(
                    "snapshot lineage bundle migration must read "
                    f"{required_source}"
                )
            if re.search(
                r"\b(?:FROM|JOIN)\s+research\.stock_snapshot_daily\b",
                normalized,
                re.I,
            ):
                raise ValueError(
                    "legacy lineage migration must not read or rebuild "
                    "research.stock_snapshot_daily"
                )
            # This named operation is a format conversion, not a general
            # INSERT ... SELECT seam.  Keep its read-side relation set just
            # as explicit as its two write targets: the immutable legacy
            # lineage table, the content-addressed dictionary, and CTEs that
            # belong to this statement.  A future caller cannot quietly add a
            # wide PIT/Feature or unrelated business-table join to the
            # migration path and recreate the original write amplification.
            cte_names = {
                name.lower()
                for name in re.findall(
                    r"(?:\bWITH\s+|,\s*)([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(",
                    normalized,
                    re.I,
                )
            }
            read_relations = {
                relation.lower()
                for relation in re.findall(
                    r"\b(?:FROM|JOIN)\s+"
                    r"([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)",
                    normalized,
                    re.I,
                )
            }
            allowed_relations = {
                required_source.lower(),
                "research.lineage_source_record",
                *cte_names,
            }
            unexpected_relations = sorted(read_relations - allowed_relations)
            if unexpected_relations:
                raise ValueError(
                    "legacy lineage migration read relations must be limited to "
                    f"{sorted(allowed_relations)}; found={unexpected_relations}"
                )

    @staticmethod
    def _validate_table_fill_sql(sql: str, staging: str) -> None:
        normalized = re.sub(r"--[^\n]*|/\*.*?\*/", " ", sql, flags=re.S).strip()
        statements = [part.strip() for part in normalized.split(";") if part.strip()]
        if len(statements) != 1 or re.search(
            r"\b(DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|VACUUM|UPDATE|DELETE)\b",
            normalized, re.I,
        ):
            raise ValueError("run_table_replace fill_sql must be one INSERT ... SELECT")
        if not re.match(r"^INSERT\s+INTO\b", normalized, re.I) or not re.search(r"\bSELECT\b", normalized, re.I):
            raise ValueError("run_table_replace fill_sql must be INSERT ... SELECT")
        if staging.lower() not in normalized.lower():
            raise ValueError("run_table_replace fill_sql must target staging")

    @staticmethod
    def _staging_name(target: str, context: RunContext) -> str:
        suffix = hashlib.sha256(context.run_id.encode("utf-8")).hexdigest()[:12]
        if "." in target:
            schema, table = target.split(".", 1)
            return f"{schema}._staging_{table}_{suffix}"
        return f"_staging_{target}_{suffix}"

    @staticmethod
    def _swap_old_name(target: str, staging: str) -> str:
        """Return a same-schema temporary name for an atomic table swap.

        PostgreSQL's ``ALTER TABLE ... RENAME TO`` accepts only the new
        unqualified table name.  The returned value is qualified for DROP,
        while callers must use its final component in the RENAME statement.
        """
        if "." in target:
            schema, table = target.rsplit(".", 1)
            prefix = f"{schema}."
        else:
            table, prefix = target, ""
        suffix = hashlib.sha256(staging.encode("utf-8")).hexdigest()[:12]
        old_table = f"_old_{table[:40]}_{suffix}"
        return f"{prefix}{old_table}"

    @staticmethod
    def _batched(rows: Iterable[Mapping[str, Any]], size: int = 10_000):
        buf: list[Mapping[str, Any]] = []
        for row in rows:
            buf.append(row)
            if len(buf) >= size:
                yield buf
                buf = []
        if buf:
            yield buf

    @staticmethod
    def _lock_scope_key(scope: WorkScope, mode: RebuildMode) -> str:
        if mode == RebuildMode.TABLE_REPLACE:
            return "table"
        if mode == RebuildMode.PARTITION_REPLACE:
            return f"partition:{scope.partition}"
        return (
            f"scope:{scope.date_from}:{scope.date_to}:"
            f"{','.join(scope.codes or ())}"
        )

    def _bulk_insert_staging(
        self, staging: str, columns: list[str], rows: list[Mapping[str, Any]]
    ) -> int:
        """框架内部写 staging：每批一个独立事务（chunk 语义），不经业务 DatasetPolicy。"""
        def insert_staging() -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                return copy_or_execute_values(
                    cur, staging, columns,
                    [[row[c] for c in columns] for row in rows],
                )
        return self._kernel._run_chunk(insert_staging, idempotent=False)

    def _build_index(self, staging: str, target: str) -> None:
        """占位：按 DatasetPolicy.partition_key 建索引（生产实现）。"""
        policy = self._kernel.policy_registry.require(target)
        columns = tuple(policy.partition_key)
        if not columns:
            return
        if any(not column.isidentifier() for column in columns):
            raise ValueError("DatasetPolicy.partition_key contains an invalid identifier")
        index_name = f"_idx_{hashlib.sha256(staging.encode()).hexdigest()[:12]}"
        def build_index() -> None:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS {index_name} "
                    f"ON {staging} ({', '.join(columns)})"
                )

        self._kernel._run_chunk(build_index, idempotent=False)

    def _staging_hash(self, staging: str, target: str | None = None) -> str:
        """Hash staging deterministically without forcing a JSON sort spill.

        Quality validation runs immediately before this method and blocks
        duplicate primary keys.  For governed targets with a primary key, the
        key order is therefore a deterministic row order and can use the
        staging primary-key index.  The legacy JSON ordering remains the
        fail-safe fallback for unkeyed test/migration relations.
        """
        staging = validate_qualified_name(staging)
        order_by = "row_to_json(t)::text"
        if target is not None:
            target = validate_qualified_name(target)
            policy = self._kernel.policy_registry.require(target)
            if policy.primary_key:
                order_by = ", ".join(
                    f"t.{validate_table_name(column)}"
                    for column in policy.primary_key
                )

        def hash_staging() -> str:
            conn = self._kernel._db_connection()
            digest = hashlib.sha256()
            with conn.cursor(name=f"rebuild_hash_{uuid.uuid4().hex}") as cur:
                cur.itersize = 10_000
                cur.execute(
                    "SELECT row_to_json(t)::text FROM %s t ORDER BY %s" %
                    (staging, order_by)
                )
                while True:
                    rows = cur.fetchmany(10_000)
                    if not rows:
                        break
                    for row in rows:
                        payload = str(row[0]).encode("utf-8")
                        digest.update(len(payload).to_bytes(8, "big"))
                        digest.update(payload)
            return digest.hexdigest()

        return self._kernel._run_chunk(hash_staging)

    def _publish(
        self, mode: RebuildMode, target: str, staging: str, scope: WorkScope,
        *, context: RunContext | None = None,
    ) -> bool:
        def publish_tx() -> bool:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                if mode == RebuildMode.TABLE_REPLACE:
                    old = self._swap_old_name(target, staging)
                    cur.execute(f"ALTER TABLE {target} RENAME TO {old.rsplit('.', 1)[-1]}")
                    cur.execute(f"ALTER TABLE {staging} RENAME TO {target.rsplit('.', 1)[-1]}")
                    cur.execute(f"DROP TABLE {old}")
                    return True
                elif mode == RebuildMode.PARTITION_REPLACE:
                    if not scope.partition:
                        raise ValueError("PARTITION_REPLACE 必须携带 scope.partition")
                    bounds = self._partition_bounds(scope)
                    if bounds is None:
                        raise ValueError("PARTITION_REPLACE requires partition bounds")
                    date_column = scope.dimensions.get("date_column", "trade_date")
                    if not isinstance(date_column, str) or not date_column.isidentifier():
                        raise ValueError("scope.dimensions.date_column invalid")
                    constraint = f"_partition_scope_{hashlib.sha256(staging.encode()).hexdigest()[:10]}"
                    cur.execute(
                        f"ALTER TABLE {staging} ADD CONSTRAINT {constraint} "
                        f"CHECK ({date_column} >= DATE '{bounds[0].isoformat()}' "
                        f"AND {date_column} < DATE '{bounds[1].isoformat()}')"
                    )
                    existing = scope.dimensions.get("existing_partition")
                    if existing:
                        existing = validate_qualified_name(str(existing))
                        if existing == staging or existing == target:
                            raise ValueError("existing_partition must be a child partition")
                        cur.execute(
                            f"ALTER TABLE {target} DETACH PARTITION {existing}"
                        )
                    cur.execute(f"ALTER TABLE {target} ATTACH PARTITION {staging}")
                    if existing:
                        cur.execute(f"DROP TABLE {existing}")
                    return True
                elif mode == RebuildMode.SCOPED_MERGE:
                    self._delete_scope(cur, target, scope)
                    cur.execute(f"INSERT INTO {target} SELECT * FROM {staging} ON CONFLICT DO NOTHING")
                    # False means the staging relation is not attached or
                    # renamed; the caller removes it after publication.
                else:  # pragma: no cover
                    raise ValueError(f"未知 RebuildMode: {mode}")

        # Publish is a short catalog/data transaction. It must be committed;
        # previously this method relied on connection context-manager behavior.
        event = None
        if context is not None:
            event = self._kernel._build_release_change_event(
                context, f"PUBLISH_{mode.value}", target, scope,
                statement=f"{mode.value}|{target}|{scope!r}",
                force_full_audit_reason=(
                    "whole-table replacement requires a full audit"
                    if mode == RebuildMode.TABLE_REPLACE else None
                ),
            )
        if event is not None:
            # A scoped replacement can remove rows while publishing an empty
            # staging slice. Record the committed publication regardless of
            # the count reported by the final INSERT.
            event["record_empty"] = True
        return bool(self._kernel._run_chunk(
            publish_tx, idempotent=False, change_event=event,
        ))

    @staticmethod
    def _partition_bounds(scope: WorkScope) -> tuple[date, date] | None:
        start = scope.dimensions.get("partition_from")
        end = scope.dimensions.get("partition_to")
        if isinstance(start, date) and isinstance(end, date) and start < end:
            return start, end
        if not scope.partition or not isinstance(scope.partition, str):
            return None
        match = re.fullmatch(r"(\d{4})(?:-(\d{2}))?", scope.partition)
        if not match:
            return None
        year = int(match.group(1))
        month = int(match.group(2) or 1)
        if not 1 <= month <= 12:
            return None
        start = date(year, month, 1)
        end = date(year + 1, 1, 1) if not match.group(2) else (
            date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
        )
        return start, end

    @staticmethod
    def _delete_scope(cur, target: str, scope: WorkScope) -> None:
        date_column = scope.dimensions.get("date_column", "trade_date")
        code_column = scope.dimensions.get("code_column", "ts_code")
        if not isinstance(date_column, str) or not date_column.isidentifier():
            raise ValueError("scope.dimensions.date_column 非法")
        if not isinstance(code_column, str) or not code_column.isidentifier():
            raise ValueError("scope.dimensions.code_column 非法")
        clauses = []
        params = []
        if scope.date_from is not None:
            clauses.append(f"{date_column} >= %s")
            params.append(scope.date_from)
        if scope.date_to is not None:
            clauses.append(f"{date_column} <= %s")
            params.append(scope.date_to)
        if scope.codes:
            clauses.append(f"{code_column} = ANY(%s)")
            params.append(list(scope.codes))
        if not clauses:
            raise ValueError("SCOPED_MERGE 禁止无范围发布")
        cur.execute(f"DELETE FROM {target} WHERE " + " AND ".join(clauses), tuple(params))

    def _drop_staging(self, staging: str) -> None:
        def drop_tx() -> None:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                if staging.startswith(("std._staging_", "pit._staging_")):
                    cur.execute(
                        "SELECT ops.drop_table_replace_staging(%s)",
                        (staging,),
                    )
                else:
                    cur.execute(f"DROP TABLE IF EXISTS {staging}")
        self._kernel._run_chunk(drop_tx)

    @staticmethod
    def _temporary_staging_name(target: str, context: RunContext) -> str:
        """Build an unqualified, session-local staging name for scoped merges."""
        table = target.rsplit(".", 1)[-1]
        suffix = hashlib.sha256(context.run_id.encode("utf-8")).hexdigest()[:12]
        return f"_staging_{table}_{suffix}"
