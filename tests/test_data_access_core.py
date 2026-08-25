"""Data Access Framework 纯逻辑层行为测试。

覆盖：WorkScope/RunContext/SqlPlan 校验、DatasetPolicy、ChunkPlanner（防大事务）、
RetryPolicy（重试边界）、Checkpoint（断点续传）、QueryGuard（FROZEN/scan_policy/write_mode/delete）、
TransactionManager（超时回滚）。
"""
from __future__ import annotations

from datetime import date

import pytest

from data_collect.data_access import (
    DataAccessError,
    GuardViolation,
    PolicyViolation,
    TransactionBudgetExceeded,
)
from data_collect.data_access.checkpoint import (
    Checkpoint,
    CheckpointState,
    InMemoryCheckpointStore,
    PostgresCheckpointStore,
)
from data_collect.data_access.chunk_planner import ChunkConfig, ChunkPlanner
from data_collect.data_access.bulk_loader import (
    copy_or_execute_values,
    create_projection_staging,
)
from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.dataset_policy import (
    DatasetPolicy,
    DatasetPolicyRegistry,
    ScanPolicy,
    WriteMode,
)
from data_collect.data_access.query_guard import QueryGuard
from data_collect.data_access.retry_policy import RetryPolicy
from data_collect.data_access.kernel import DataAccessKernel
from data_collect.data_access.transaction_manager import TransactionConfig, TransactionManager
from tests._fake_db import FakeConnection


# ---- 构造辅助 ----

def ctx() -> RunContext:
    return RunContext(
        run_id="run-1", job_name="job", code_version="git:abc",
        config_hash="cfg", pipeline_version="v1",
    )


def scope(days: int = 0) -> WorkScope:
    if days:
        return WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, days))
    return WorkScope()


def raw_policy() -> DatasetPolicy:
    return DatasetPolicy(
        name="raw.tushare_daily",
        write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
        update_allowed=False, delete_allowed=False,
        scan_policy=ScanPolicy.SCOPE_REQUIRED,
    )


def feature_policy() -> DatasetPolicy:
    return DatasetPolicy(
        name="feature.price_daily_v2",
        write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD]),
        primary_key=("ts_code", "trade_date"),
        partition_key=("trade_date",),
        require_scope=True,
        scan_policy=ScanPolicy.SCOPE_REQUIRED,
    )


def frozen_policy() -> DatasetPolicy:
    return DatasetPolicy(
        name="contract.stock_snapshot_daily",
        write_modes=frozenset([WriteMode.PUBLISH_ONLY]),
        immutable_after_freeze=True,
        frozen=True,
        require_snapshot_id=True,
        scan_policy=ScanPolicy.SCOPE_REQUIRED,
    )


def frozen_upsertable_policy() -> DatasetPolicy:
    """原本可 upsert、但已冻结（immutable_after_freeze + frozen）的表：
    用于验证规则 3（FROZEN 表禁止 UPDATE/DELETE）—— 即使 write_mode 允许 UPSERT 也要拦。"""
    return DatasetPolicy(
        name="contract.some_upsertable",
        write_modes=frozenset([WriteMode.UPSERT]),
        immutable_after_freeze=True,
        frozen=True,
        scan_policy=ScanPolicy.SCOPE_REQUIRED,
    )


def dim_policy() -> DatasetPolicy:
    return DatasetPolicy(
        name="ops.source_registry",
        write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
        scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
    )


def registry() -> DatasetPolicyRegistry:
    r = DatasetPolicyRegistry()
    r.register_many(
        [raw_policy(), feature_policy(), frozen_policy(), frozen_upsertable_policy(), dim_policy()]
    )
    return r


# ---- 核心对象校验 ----

def test_workscope_rejects_inverted_dates():
    with pytest.raises(ValueError):
        WorkScope(date_from=date(2026, 2, 1), date_to=date(2026, 1, 1))


def test_workscope_freezes_codes_to_tuple():
    s = WorkScope(codes=["a", "b"])
    assert isinstance(s.codes, tuple)


def test_workscope_is_scoped():
    assert WorkScope().is_scoped() is False
    assert WorkScope(date_from=date(2026, 1, 1)).is_scoped() is True
    assert WorkScope(codes=("600519.SH",)).is_scoped() is True


def test_runcontext_requires_all_fields():
    with pytest.raises(ValueError):
        RunContext(run_id="", job_name="j", code_version="c", config_hash="h", pipeline_version="p")


def test_sqlplan_rejects_bad_mode():
    with pytest.raises(ValueError):
        SqlPlan(operation=Operation.READ, target="t", scope=WorkScope(), expected_mode="BAD")


# ---- DatasetPolicy ----

def test_dataset_policy_allows_write():
    assert raw_policy().allows_write(WriteMode.APPEND_VERSIONED)
    assert not raw_policy().allows_write(WriteMode.UPSERT)


def test_registry_rejects_duplicate():
    r = DatasetPolicyRegistry()
    r.register(raw_policy())
    with pytest.raises(ValueError):
        r.register(raw_policy())


def test_registry_fail_closed_on_unknown():
    r = DatasetPolicyRegistry()
    with pytest.raises(PolicyViolation):
        r.require("unknown.table")


# ---- ChunkPlanner（防大事务：按实际输入切 chunk）----

def test_chunk_planner_splits_by_rows():
    planner = ChunkPlanner(ChunkConfig(max_rows_per_tx=50_000))
    rows = list(range(120_000))
    chunks = planner.plan(rows)
    assert [len(c) for c in chunks] == [50_000, 50_000, 20_000]


def test_chunk_planner_supports_a_smaller_operation_limit():
    planner = ChunkPlanner(ChunkConfig(max_rows_per_tx=50_000))
    chunks = planner.plan(list(range(2_501)), max_rows_per_chunk=1_000)
    assert [len(chunk) for chunk in chunks] == [1_000, 1_000, 501]


def test_chunk_planner_respects_bytes():
    planner = ChunkPlanner(ChunkConfig(max_rows_per_tx=50_000, max_bytes_per_tx=1000))
    rows = list(range(100))
    chunks = planner.plan(rows, bytes_per_row=100)
    # 1000 / 100 = 10 行每 chunk
    assert all(len(c) <= 10 for c in chunks)
    assert sum(len(c) for c in chunks) == 100


def test_chunk_planner_uses_actual_wide_row_bytes():
    planner = ChunkPlanner(ChunkConfig(max_rows_per_tx=50_000, max_bytes_per_tx=150))
    rows = [{"payload": "x" * 100}, {"payload": "y" * 100}]

    chunks = planner.plan(rows)

    assert [len(chunk) for chunk in chunks] == [1, 1]


def test_chunk_planner_rejects_single_row_over_byte_budget():
    planner = ChunkPlanner(ChunkConfig(max_bytes_per_tx=100))

    with pytest.raises(TransactionBudgetExceeded, match="single row exceeds"):
        planner.plan([{"payload": "x" * 200}])


def test_chunk_planner_empty():
    assert ChunkPlanner().plan([]) == []


# ---- RetryPolicy（重试边界）----

def test_retry_policy_rejects_policy_violation():
    rp = RetryPolicy()
    assert rp.should_retry(PolicyViolation("x")) is False
    assert rp.should_retry(GuardViolation("x")) is False
    assert rp.should_retry(TransactionBudgetExceeded("x")) is False


def test_retry_policy_rejects_data_access_error():
    rp = RetryPolicy()
    assert rp.should_retry(DataAccessError("generic")) is False


def test_retry_policy_retries_operational_error():
    try:
        from psycopg2 import OperationalError
    except ImportError:
        pytest.skip("psycopg2 未安装")
    rp = RetryPolicy()
    assert rp.should_retry(OperationalError("connection reset")) is True


# ---- Checkpoint（断点续传）----

def test_checkpoint_save_load():
    cp = Checkpoint(InMemoryCheckpointStore())
    cp.mark_progress("run-1", "job", "scope", 3)
    state = cp.load("run-1", "job", "scope")
    assert state.cursor == 3
    assert state.done is False


def test_checkpoint_mark_done():
    cp = Checkpoint(InMemoryCheckpointStore())
    cp.mark_progress("run-1", "job", "scope", 3)
    cp.mark_done("run-1", "job", "scope")
    state = cp.load("run-1", "job", "scope")
    assert state.done is True
    assert state.cursor == 3


# ---- QueryGuard ----

def test_guard_rejects_upsert_on_append_only():
    guard = QueryGuard(registry())
    plan = SqlPlan(operation=Operation.UPSERT, target="raw.tushare_daily", scope=WorkScope())
    with pytest.raises(PolicyViolation):
        guard.check(plan)


def test_guard_rejects_append_on_feature():
    guard = QueryGuard(registry())
    plan = SqlPlan(operation=Operation.APPEND, target="feature.price_daily_v2", scope=scope(1))
    with pytest.raises(PolicyViolation):
        guard.check(plan)


def test_guard_rejects_frozen_write():
    # 已冻结（frozen）的表，即使 write_mode 允许 UPSERT，也禁止 UPDATE/DELETE（规则 3）
    guard = QueryGuard(registry())
    plan = SqlPlan(operation=Operation.UPSERT, target="contract.some_upsertable", scope=scope(1))
    with pytest.raises(GuardViolation):
        guard.check(plan)


def test_guard_rejects_write_mode_mismatch_on_publish_only():
    # PUBLISH_ONLY 表拒绝 UPSERT（规则 1，write_mode 不匹配）→ PolicyViolation
    guard = QueryGuard(registry())
    plan = SqlPlan(operation=Operation.UPSERT, target="contract.stock_snapshot_daily", scope=scope(1))
    with pytest.raises(PolicyViolation):
        guard.check(plan)


def test_guard_rejects_unscoped_delete():
    guard = QueryGuard(registry())
    plan = SqlPlan(
        operation=Operation.REBUILD, target="feature.price_daily_v2",
        scope=WorkScope(), allow_delete=True,
    )
    with pytest.raises(GuardViolation):
        guard.check(plan)


def test_guard_rejects_delete_when_policy_disallows_it():
    guard = QueryGuard(registry())
    plan = SqlPlan(
        operation=Operation.REBUILD, target="raw.tushare_daily",
        scope=scope(1), allow_delete=True,
    )
    with pytest.raises(PolicyViolation):
        guard.check(plan)


def test_guard_requires_snapshot_id_for_publish_only_dataset():
    guard = QueryGuard(registry())
    policy = DatasetPolicy(
        name="contract.publishable", write_modes=frozenset([WriteMode.PUBLISH_ONLY]),
        require_snapshot_id=True, scan_policy=ScanPolicy.SCOPE_REQUIRED,
    )
    reg = DatasetPolicyRegistry(); reg.register(policy)
    guard = QueryGuard(reg)
    plan = SqlPlan(operation=Operation.PUBLISH, target="contract.publishable", scope=scope(1))
    with pytest.raises(GuardViolation):
        guard.check(plan)
    scoped = WorkScope(date_from=date(2026, 1, 1), dimensions={"snapshot_id": "s1"})
    guard.check(SqlPlan(operation=Operation.PUBLISH, target="contract.publishable", scope=scoped))


def test_registry_freeze_is_irreversible_for_immutable_policy():
    r = DatasetPolicyRegistry()
    r.register(DatasetPolicy(
        name="contract.publishable", write_modes=frozenset([WriteMode.PUBLISH_ONLY]),
        immutable_after_freeze=True,
    ))
    assert r.freeze("contract.publishable").frozen is True
    with pytest.raises(GuardViolation):
        QueryGuard(r).check(SqlPlan(
            operation=Operation.PUBLISH, target="contract.publishable", scope=scope(1),
        ))


def test_guard_rejects_unscoped_scan_on_scope_required():
    guard = QueryGuard(registry())
    plan = SqlPlan(operation=Operation.READ, target="feature.price_daily_v2", scope=WorkScope())
    with pytest.raises(GuardViolation):
        guard.check(plan)


def test_guard_allows_full_scan_on_dimension():
    guard = QueryGuard(registry())
    plan = SqlPlan(operation=Operation.READ, target="ops.source_registry", scope=WorkScope())
    guard.check(plan)  # 不应抛异常


def test_guard_allows_scoped_read():
    guard = QueryGuard(registry())
    plan = SqlPlan(operation=Operation.READ, target="feature.price_daily_v2", scope=scope(10))
    guard.check(plan)


def test_guard_allows_valid_upsert():
    guard = QueryGuard(registry())
    plan = SqlPlan(operation=Operation.UPSERT, target="feature.price_daily_v2", scope=scope(10))
    guard.check(plan)


def test_guard_unknown_target_fail_closed():
    guard = QueryGuard(registry())
    plan = SqlPlan(operation=Operation.READ, target="unknown.x", scope=scope(10))
    with pytest.raises(PolicyViolation):
        guard.check(plan)


# ---- TransactionManager（超时只能中止，不能执行中 commit）----

def test_transaction_commit_within_deadline():
    conn = FakeConnection()
    tm = TransactionManager(TransactionConfig(transaction_deadline=100.0), connection_factory=lambda: conn)
    tm.begin_chunk()
    tm.commit_chunk()
    assert conn.committed == 1
    assert conn.rolled_back == 0


def test_transaction_begin_chunk_sets_local_statement_timeout():
    conn = FakeConnection()
    tm = TransactionManager(
        TransactionConfig(transaction_deadline=2.5), connection_factory=lambda: conn
    )

    tm.begin_chunk()

    assert conn.internal_executed == [{
        "sql": "SET LOCAL statement_timeout = %s",
        "params": (2500,),
        "kind": "execute",
    }]


def test_transaction_deadline_rolls_back():
    import time

    conn = FakeConnection()
    tm = TransactionManager(TransactionConfig(transaction_deadline=1.0), connection_factory=lambda: conn)
    _ = tm._connection()  # framework-private：业务层没有 public connection 入口
    tm.begin_chunk()
    tm._tx_started_at = time.monotonic() - 10  # 模拟超时
    with pytest.raises(TransactionBudgetExceeded):
        tm.commit_chunk()
    assert conn.rolled_back == 1
    assert conn.committed == 0


def test_atomic_repository_workflow_commits_once_and_child_chunks_join():
    conn = FakeConnection()
    kernel = DataAccessKernel(
        connection_factory=lambda: conn,
        checkpoint_store=InMemoryCheckpointStore(),
    )
    seen = []
    result = kernel.run_atomic(
        ctx(), scope(),
        lambda: (seen.append(kernel.transaction_manager.in_atomic), kernel._run_chunk(lambda: "ok"))[1],
        estimated_rows=2,
    )
    assert result == "ok"
    assert seen == [True]
    assert conn.committed == 1
    assert conn.rolled_back == 0


def test_atomic_repository_workflow_rolls_back_all_child_writes_on_failure():
    conn = FakeConnection()
    kernel = DataAccessKernel(
        connection_factory=lambda: conn,
        checkpoint_store=InMemoryCheckpointStore(),
    )

    def fail():
        kernel._db_connection()
        kernel._run_chunk(lambda: "first")
        raise RuntimeError("workflow failed")

    with pytest.raises(RuntimeError, match="workflow failed"):
        kernel.run_atomic(ctx(), scope(), fail, estimated_rows=2)
    assert conn.committed == 0
    assert conn.rolled_back == 1


def test_atomic_gateway_checkpoints_are_deferred_until_commit():
    store = InMemoryCheckpointStore()
    kernel = DataAccessKernel(
        policy_registry=registry(), connection_factory=lambda: FakeConnection(),
        checkpoint_store=store,
    )
    failed_scope_key = kernel._checkpoint_batch_scope_key(
        "bulk_insert", "ops.source_registry", scope(), [{"source_id": "source-1"}]
    )
    committed_scope_key = kernel._checkpoint_batch_scope_key(
        "bulk_insert", "ops.source_registry", scope(), [{"source_id": "source-2"}]
    )

    def fail_after_gateway_write():
        kernel.write.bulk_insert(
            context=ctx(), scope=scope(), target="ops.source_registry",
            rows=[{"source_id": "source-1"}],
        )
        raise RuntimeError("workflow failed")

    with pytest.raises(RuntimeError, match="workflow failed"):
        kernel.run_atomic(ctx(), scope(), fail_after_gateway_write, estimated_rows=1)
    assert store.load("run-1", "job", failed_scope_key) is None

    kernel.run_atomic(
        ctx(), scope(),
        lambda: kernel.write.bulk_insert(
            context=ctx(), scope=scope(), target="ops.source_registry",
            rows=[{"source_id": "source-2"}],
        ),
        estimated_rows=1,
    )
    state = store.load("run-1", "job", committed_scope_key)
    assert state is not None
    assert state.done is True
    assert state.cursor == 1


def test_retry_policy_rolls_back_before_retry_and_stops_at_max_attempts():
    class Transient(Exception):
        pass

    policy = RetryPolicy(max_attempts=3, backoff_seconds=0)
    policy.RETRYABLE_PG_EXCEPTIONS = (Transient,)
    attempts = []
    rollbacks = []

    def eventually_succeeds():
        attempts.append(1)
        if len(attempts) < 3:
            raise Transient("retry")
        return "ok"

    assert policy.run(eventually_succeeds, rollback=lambda: rollbacks.append(1)) == "ok"
    assert len(attempts) == 3
    assert len(rollbacks) == 2

    attempts.clear()
    with pytest.raises(Transient):
        policy.run(
            lambda: (
                attempts.append(1),
                (_ for _ in ()).throw(Transient("retry")),
            )[1]
        )
    assert len(attempts) == 3


def test_retry_discards_connection_when_rollback_itself_fails():
    class Transient(Exception):
        pass

    class BrokenRollbackConnection(FakeConnection):
        def rollback(self):
            self.rolled_back += 1
            self.closed = True
            raise RuntimeError("connection is gone")

    connections = [BrokenRollbackConnection(), FakeConnection()]
    created = []

    def factory():
        conn = connections[len(created)]
        created.append(conn)
        return conn

    kernel = DataAccessKernel(connection_factory=factory)
    kernel.retry_policy.RETRYABLE_PG_EXCEPTIONS = (Transient,)
    attempts = []

    def operation():
        conn = kernel._db_connection()
        attempts.append(conn)
        if len(attempts) == 1:
            raise Transient("server disconnected")
        return "ok"

    assert kernel._run_chunk(operation) == "ok"
    assert len(created) == 2
    assert attempts[0] is not attempts[1]


def test_plain_bulk_insert_is_not_automatically_retried():
    class Transient(Exception):
        pass

    kernel = DataAccessKernel(
        policy_registry=registry(),
        connection_factory=lambda: FakeConnection(),
        checkpoint_store=InMemoryCheckpointStore(),
    )
    kernel.retry_policy.RETRYABLE_PG_EXCEPTIONS = (Transient,)
    attempts = []

    def fail_once(*_args):
        attempts.append(1)
        raise Transient("ambiguous commit")

    kernel.write._insert_chunk = fail_once
    with pytest.raises(Transient, match="ambiguous commit"):
        kernel.write.bulk_insert(
            ctx(), scope(), "ops.source_registry", [{"source_id": "s1"}]
        )
    assert len(attempts) == 1


def test_postgres_checkpoint_upsert_is_monotonic_at_database_boundary():
    class Cursor:
        def __init__(self, statements):
            self.statements = statements

        def execute(self, sql, params):
            self.statements.append((sql, params))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Connection:
        def __init__(self):
            self.statements = []

        def cursor(self):
            return Cursor(self.statements)

        def commit(self):
            pass

        def close(self):
            pass

    conn = Connection()
    store = PostgresCheckpointStore(connection_factory=lambda: conn)
    store.save(CheckpointState("run", "job", "scope", 3, False))

    sql = conn.statements[0][0]
    assert "GREATEST" in sql
    assert "data_access_checkpoint.done OR EXCLUDED.done" in sql


def test_atomic_child_gateway_cannot_accumulate_past_transaction_budget():
    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=registry(),
        transaction_config=TransactionConfig(max_rows_per_tx=2),
        connection_factory=lambda: conn,
        checkpoint_store=InMemoryCheckpointStore(),
    )

    def oversized_workflow():
        kernel.write.bulk_insert(
            ctx(), scope(), "ops.source_registry",
            [{"source_id": "a"}, {"source_id": "b"}, {"source_id": "c"}],
        )

    with pytest.raises(TransactionBudgetExceeded):
        kernel.run_atomic(ctx(), scope(), oversized_workflow, estimated_rows=2)
    assert conn.committed == 0
    assert conn.rolled_back == 1


def test_kernel_manifest_hook_records_and_finalizes_production_run():
    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=registry(), connection_factory=lambda: conn,
        checkpoint_store=InMemoryCheckpointStore(),
    )
    # Unit tests normally disable production control-plane writes because they
    # use a FakeConnection. Explicitly enable it here to verify the behavior.
    kernel._manifest_enabled = True
    kernel._install_manifest_hooks()
    try:
        kernel.write.upsert(
            context=ctx(), scope=WorkScope(partition="manifest-test"),
            target="ops.source_registry",
            rows=[{
                "source_id": "manifest-test", "provider": "test",
                "api_name": "test", "dataset_name": "test",
                "business_key": [], "frequency": "once",
                "supports_incremental": True, "supports_backfill": False,
                "available_time_rule": "now", "rate_limit": "{}",
                "enabled": True,
            }],
            key=("source_id",),
            update_columns=(
                "provider", "api_name", "dataset_name", "business_key",
                "frequency", "supports_incremental", "supports_backfill",
                "available_time_rule", "rate_limit", "enabled",
            ),
        )
    finally:
        kernel.close()
    manifest_sql = [
        entry["sql"] for entry in conn.executed
        if "ops.run_manifest" in entry["sql"]
    ]
    assert any("INSERT INTO ops.run_manifest" in sql for sql in manifest_sql)
    assert any("UPDATE ops.run_manifest" in sql for sql in manifest_sql)


def test_kernel_manifest_hook_counts_rows_from_stream_iteration():
    conn = FakeConnection(stream_rows=[("a",), ("b",), ("c",)])
    kernel = DataAccessKernel(
        policy_registry=registry(), connection_factory=lambda: conn,
        checkpoint_store=InMemoryCheckpointStore(),
    )
    kernel._manifest_enabled = True
    kernel._install_manifest_hooks()
    try:
        batches = list(kernel.read.stream(
            context=ctx(),
            source="feature.price_daily_v2",
            columns=["ts_code"],
            where={"trade_date": date(2026, 1, 1)},
            scope=WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 1)),
            batch_rows=2,
        ))
    finally:
        kernel.close()

    assert [len(batch) for batch in batches] == [2, 1]
    finish = [
        entry for entry in conn.executed
        if "UPDATE ops.run_manifest" in entry["sql"]
    ]
    assert finish
    assert finish[-1]["params"][2:] == (3, 0, "run-1")


def test_custom_checkpoint_store_does_not_disable_production_manifest_hooks():
    kernel = DataAccessKernel(checkpoint_store=InMemoryCheckpointStore())
    try:
        assert kernel._manifest_enabled is True
    finally:
        kernel.close()


def test_kernel_manifest_marks_stream_failure_during_iteration():
    class FailingStreamConnection(FakeConnection):
        def cursor(self, name=None):
            cursor = super().cursor(name)
            if name is not None:
                calls = 0
                original_fetchmany = cursor.fetchmany

                def fetchmany(size):
                    nonlocal calls
                    calls += 1
                    if calls >= 2:
                        raise RuntimeError("stream broke")
                    return original_fetchmany(size)

                cursor.fetchmany = fetchmany
            return cursor

    conn = FailingStreamConnection(stream_rows=[("a",), ("b",), ("c",)])
    kernel = DataAccessKernel(
        policy_registry=registry(), connection_factory=lambda: conn,
        checkpoint_store=InMemoryCheckpointStore(),
    )
    kernel._manifest_enabled = True
    kernel._install_manifest_hooks()
    try:
        with pytest.raises(RuntimeError, match="stream broke"):
            list(kernel.read.stream(
                context=ctx(),
                source="feature.price_daily_v2",
                columns=["ts_code"],
                where={"trade_date": date(2026, 1, 1)},
                scope=WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 1)),
                batch_rows=2,
            ))
    finally:
        kernel.close()

    finish = [
        entry for entry in conn.executed
        if "UPDATE ops.run_manifest" in entry["sql"]
    ]
    assert finish
    assert finish[-1]["params"][0] == "FAILED"
    assert finish[-1]["params"][1] == "stream broke"


def test_bulk_loader_prefers_copy_for_scalar_rows_and_encodes_nulls():
    class CopyCursor:
        def __init__(self):
            self.copy_sql = None
            self.copy_payload = None

        def copy_expert(self, sql, stream):
            self.copy_sql = sql
            self.copy_payload = stream.read()

    cursor = CopyCursor()
    count = copy_or_execute_values(
        cursor,
        "feature.test_fact",
        ("ts_code", "trade_date", "amount", "active"),
        [("000001.SZ", date(2026, 8, 20), None, True)],
    )

    assert count == 1
    assert cursor.copy_sql.startswith("COPY feature.test_fact")
    assert cursor.copy_payload == "000001.SZ,2026-08-20,\\N,true\n"


def test_bulk_loader_uses_typed_fallback_for_non_scalar_values():
    class TypedCursor:
        rowcount = 1

        def __init__(self):
            self.copied = False
            self.executed = None

        def copy_expert(self, _sql, _stream):
            self.copied = True

        def executemany(self, sql, rows):
            self.executed = (sql, rows)

    cursor = TypedCursor()
    count = copy_or_execute_values(
        cursor, "ops.test_payload", ("tags",), [(["a", "b"],)]
    )

    assert count == 1
    assert cursor.copied is False
    assert cursor.executed is not None


def test_bulk_loader_does_not_turn_literal_null_marker_into_sql_null():
    class Cursor:
        rowcount = 1

        def __init__(self):
            self.copied = False
            self.executed = None

        def copy_expert(self, _sql, _stream):
            self.copied = True

        def executemany(self, sql, rows):
            self.executed = (sql, rows)

    cursor = Cursor()
    assert copy_or_execute_values(
        cursor, "ops.test_payload", ("value",), [(r"\N",)]
    ) == 1
    assert cursor.copied is False
    assert cursor.executed[1] == [[r"\N"]]


def test_projection_staging_does_not_inherit_unprovided_not_null_columns():
    class Cursor:
        def __init__(self):
            self.sql = None

        def execute(self, sql):
            self.sql = sql

    cursor = Cursor()
    create_projection_staging(
        cursor,
        "_staging_migration",
        "ops.foundation_table_migration",
        ("migration_id", "status"),
    )

    assert "LIKE ops.foundation_table_migration" not in cursor.sql
    assert (
        "SELECT migration_id, status FROM ops.foundation_table_migration WITH NO DATA"
        in cursor.sql
    )
