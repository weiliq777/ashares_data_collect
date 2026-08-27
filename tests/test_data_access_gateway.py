"""Gateway 行为测试（fake connection，验证行为而非类名）。

重点验证：
- N+1（raw.append 版本判断是【一次】 SET-based，不是逐条 SELECT）
- 防大事务（write.upsert 自动 chunk，每批 ≤ 预算）
- 幂等（ON CONFLICT DO UPDATE）
- Kernel 不公开万能 execute()
- run_insert_select 强制 SqlPlan
"""
from __future__ import annotations

import hashlib
from datetime import date

import pytest

from data_collect.data_access import (
    DataAccessKernel,
    DatasetPolicy,
    DatasetPolicyRegistry,
    GuardViolation,
    PolicyViolation,
    ScanPolicy,
    WriteMode,
)
from data_collect.data_access.context import (
    Operation,
    RebuildMode,
    RunContext,
    SqlPlan,
    WorkScope,
)
from data_collect.data_access.chunk_planner import ChunkConfig
from data_collect.data_access.policy_catalog import build_default_registry
from tests._fake_db import FakeConnection

from tests.test_data_access_core import (
    ctx,
    feature_policy,
    frozen_upsertable_policy,
    raw_policy,
    registry,
)


def make_kernel(reg: DatasetPolicyRegistry | None = None, max_rows: int = 50_000):
    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=reg or registry(),
        chunk_config=ChunkConfig(max_rows_per_tx=max_rows),
        connection_factory=lambda: conn,
    )
    return kernel, conn


# ---- Kernel 组合根 ----

def test_kernel_exposes_six_gateways_not_execute():
    kernel, _ = make_kernel()
    for name in ("raw", "read", "write", "rebuild", "publish", "quality"):
        assert hasattr(kernel, name), f"缺少 gateway: {name}"
    # 不公开万能 execute()
    assert not hasattr(kernel, "execute")


def test_kernel_rejects_missing_runcontext():
    kernel, _ = make_kernel()
    with pytest.raises(TypeError):
        kernel.raw.append(context=None, dataset="raw.tushare_daily", rows=[], business_key=["k"])


def test_public_read_gateway_rejects_mutable_feature_consumption():
    kernel, conn = make_kernel(build_default_registry())
    with pytest.raises(ValueError, match="direct ReadGateway access is forbidden"):
        kernel.read.query(
            context=ctx(), source="feature.price_daily_v2",
            columns=("ts_code",),
            where={"trade_date": date(2026, 1, 1)},
            scope=WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 1)),
        )
    assert conn.executed == []


# ---- raw.append：N+1 与幂等（行为验证）----

def _rows(n: int) -> list[dict]:
    return [
        {"ts_code": f"60000{i % 10}.SH", "trade_date": date(2026, 1, 1),
         "payload_hash": f"h{i}"}
        for i in range(n)
    ]


def test_append_version_judgement_is_set_based_not_n_plus_1():
    kernel, conn = make_kernel()
    n = 100
    kernel.raw.append(
        context=ctx(), dataset="raw.tushare_daily", rows=_rows(n),
        business_key=["ts_code", "trade_date"],
    )
    # 版本判断：一次 SET-based（WHERE NOT EXISTS），而不是 n 次 SELECT
    judgement = [e for e in conn.executed if "WHERE NOT EXISTS" in e["sql"]]
    assert len(judgement) == 1


def test_raw_append_uses_small_indexed_chunks():
    kernel, conn = make_kernel()
    kernel.raw.append(
        context=ctx(), dataset="raw.tushare_daily", rows=_rows(2_001),
        business_key=["ts_code", "trade_date"],
    )
    judgement = [e for e in conn.executed if "WHERE NOT EXISTS" in e["sql"]]
    assert len(judgement) == 3
    # 逐条 SELECT 的条数应为 0（没有单独逐行查询）
    plain_selects = [
        e for e in conn.executed
        if e["kind"] == "execute" and e["sql"].lstrip().upper().startswith("SELECT")
    ]
    assert len(plain_selects) == 0


def test_append_uses_batch_executemany():
    kernel, conn = make_kernel()
    kernel.raw.append(
        context=ctx(), dataset="raw.tushare_daily", rows=_rows(100),
        business_key=["ts_code", "trade_date"],
    )
    inserts = [e for e in conn.executed if e["kind"] == "executemany"]
    # 批量插入 staging（executemany），而非 100 条 execute
    assert len(inserts) == 1
    assert inserts[0]["rowcount"] == 100


def test_append_chunks_large_input():
    kernel, conn = make_kernel(max_rows=50_000)
    kernel.raw.append(
        context=ctx(), dataset="raw.tushare_daily", rows=_rows(120_000),
        business_key=["ts_code", "trade_date"],
    )
    inserts = [e for e in conn.executed if e["kind"] == "executemany"]
    # Indexed Raw conflict probes use the smaller operation limit; the global
    # 50k transaction budget remains an upper bound, not a required batch size.
    assert len(inserts) == 120
    assert all(e["rowcount"] == 1_000 for e in inserts)
    assert conn.committed == 120


def test_raw_append_checkpoints_distinguish_batches_in_same_run():
    kernel, conn = make_kernel()
    first = [{
        "ts_code": "000001.SZ", "trade_date": date(2026, 8, 19),
        "payload_hash": "hash-1",
    }]
    second = [{
        "ts_code": "000002.SZ", "trade_date": date(2026, 8, 19),
        "payload_hash": "hash-2",
    }]

    assert kernel.raw.append(
        ctx(), "raw.tushare_daily", first, ("ts_code", "trade_date")
    ) == 1
    assert kernel.raw.append(
        ctx(), "raw.tushare_daily", second, ("ts_code", "trade_date")
    ) == 1
    # Exact replay is skipped, but a distinct second batch is not swallowed by
    # the first batch's completed checkpoint.
    assert kernel.raw.append(
        ctx(), "raw.tushare_daily", first, ("ts_code", "trade_date")
    ) == 0
    judgements = [e for e in conn.executed if "WHERE NOT EXISTS" in e["sql"]]
    assert len(judgements) == 2


def test_append_rejects_unknown_dataset():
    kernel, _ = make_kernel()
    with pytest.raises(PolicyViolation):
        kernel.raw.append(
            context=ctx(), dataset="unknown.tbl", rows=_rows(1),
            business_key=["ts_code"],
        )


def test_append_rejects_non_append_target():
    kernel, _ = make_kernel()
    with pytest.raises(PolicyViolation):
        kernel.raw.append(
            context=ctx(), dataset="feature.price_daily_v2", rows=_rows(1),
            business_key=["ts_code"],
        )


# ---- write.upsert / bulk_insert：防大事务 + 幂等 ----

def _feature_rows(n: int) -> list[dict]:
    return [
        {"ts_code": f"60{i:04d}.SH", "trade_date": date(2026, 1, 1),
         "ma20": 10.0, "ma60": 9.0}
        for i in range(n)
    ]


def test_upsert_chunks_and_uses_on_conflict():
    kernel, conn = make_kernel(max_rows=50_000)
    kernel.write.upsert(
        context=ctx(), scope=WorkScope(date_from=date(2026, 1, 1)),
        target="feature.price_daily_v2", rows=_feature_rows(120_000),
        key=["ts_code", "trade_date"], update_columns=["ma20", "ma60"],
    )
    inserts = [e for e in conn.executed if e["kind"] == "executemany"]
    assert [e["rowcount"] for e in inserts] == [50_000, 50_000, 20_000]
    publishes = [
        e for e in conn.executed
        if e["kind"] == "execute" and e["sql"].startswith("INSERT INTO feature.price_daily_v2")
    ]
    assert len(publishes) == 3
    assert all("ON CONFLICT" in e["sql"] for e in publishes)
    assert conn.committed == 3


def test_upsert_skip_noop_generates_distinct_change_guard():
    kernel, conn = make_kernel()
    kernel.write.upsert(
        context=ctx(), scope=WorkScope(date_from=date(2026, 1, 1)),
        target="feature.price_daily_v2", rows=_feature_rows(1),
        key=["ts_code", "trade_date"], update_columns=["ma20", "ma60"],
        skip_noop=True,
    )
    sql = next(
        entry["sql"] for entry in conn.executed
        if entry["kind"] == "execute" and entry["sql"].startswith("INSERT INTO feature.price_daily_v2")
    )
    assert "IS DISTINCT FROM EXCLUDED.ma20" in sql
    assert "IS DISTINCT FROM EXCLUDED.ma60" in sql


def test_upsert_checkpoints_distinguish_batches_in_same_run_and_scope():
    kernel, conn = make_kernel()
    write_scope = WorkScope(date_from=date(2026, 1, 1))
    first = _feature_rows(1)
    second = _feature_rows(1)
    second[0] = {**second[0], "ts_code": "000999.SZ"}

    assert kernel.write.upsert(
        ctx(), write_scope, "feature.price_daily_v2", first,
        ("ts_code", "trade_date"), ("ma20", "ma60"),
    ) == 1
    assert kernel.write.upsert(
        ctx(), write_scope, "feature.price_daily_v2", second,
        ("ts_code", "trade_date"), ("ma20", "ma60"),
    ) == 1
    assert kernel.write.upsert(
        ctx(), write_scope, "feature.price_daily_v2", first,
        ("ts_code", "trade_date"), ("ma20", "ma60"),
    ) == 0
    publishes = [
        entry for entry in conn.executed
        if entry["sql"].startswith("INSERT INTO feature.price_daily_v2")
    ]
    assert len(publishes) == 2


def test_upsert_rejects_append_only_target():
    kernel, _ = make_kernel()
    with pytest.raises(PolicyViolation):
        kernel.write.upsert(
            context=ctx(), scope=WorkScope(date_from=date(2026, 1, 1)),
            target="raw.tushare_daily", rows=_feature_rows(1),
            key=["ts_code"], update_columns=["ma20"],
        )


def test_bulk_insert_chunks():
    kernel, conn = make_kernel(max_rows=50_000)
    kernel.write.bulk_insert(
        context=ctx(), scope=WorkScope(date_from=date(2026, 1, 1)),
        target="ops.source_registry", rows=_feature_rows(120_000),
    )
    inserts = [e for e in conn.executed if e["kind"] == "executemany"]
    assert [e["rowcount"] for e in inserts] == [50_000, 50_000, 20_000]


def test_bulk_insert_resumes_after_last_committed_checkpoint(monkeypatch):
    from data_collect.data_access.checkpoint import InMemoryCheckpointStore

    conn = FakeConnection()
    store = InMemoryCheckpointStore()
    kernel = DataAccessKernel(
        policy_registry=registry(),
        chunk_config=ChunkConfig(max_rows_per_tx=2),
        connection_factory=lambda: conn,
        checkpoint_store=store,
    )
    rows = [{"source_id": f"s{i}"} for i in range(5)]
    write_scope = WorkScope(date_from=date(2026, 1, 1))
    original = kernel.write._insert_chunk
    calls = []

    def fail_second(target, columns, chunk):
        calls.append(tuple(row["source_id"] for row in chunk))
        if len(calls) == 2:
            raise RuntimeError("chunk two failed")
        return original(target, columns, chunk)

    monkeypatch.setattr(kernel.write, "_insert_chunk", fail_second)
    with pytest.raises(RuntimeError, match="chunk two failed"):
        kernel.write.bulk_insert(ctx(), write_scope, "ops.source_registry", rows)

    scope_key = kernel._checkpoint_batch_scope_key(
        "bulk_insert", "ops.source_registry", write_scope, rows
    )
    state = store.load(ctx().run_id, ctx().job_name, scope_key)
    assert state is not None and state.cursor == 1 and state.done is False

    replayed = []
    def record_replay(target, columns, chunk):
        replayed.append(tuple(row["source_id"] for row in chunk))
        return original(target, columns, chunk)

    monkeypatch.setattr(kernel.write, "_insert_chunk", record_replay)
    kernel.write.bulk_insert(ctx(), write_scope, "ops.source_registry", rows)
    assert replayed == [("s2", "s3"), ("s4",)]
    state = store.load(ctx().run_id, ctx().job_name, scope_key)
    assert state is not None and state.cursor == 3 and state.done is True


def test_snapshot_manifest_transition_is_guarded_and_typed():
    kernel, conn = make_kernel(build_default_registry())
    scope = WorkScope(
        date_from=date(2026, 1, 1), date_to=date(2026, 1, 1),
        partition="snapshot-1", dimensions={"snapshot_id": "snapshot-1"},
    )
    changed = kernel.write.transition_snapshot_manifest(
        ctx(), scope, "snapshot-1", ("BUILDING",), "VALIDATED", {},
    )
    assert changed == 0
    sql = next(item["sql"] for item in conn.executed if item["kind"] == "execute")
    assert "research.snapshot_manifest" in sql
    assert "m.status = ANY" in sql
    assert "m.status <> 'FROZEN'" in sql
    assert "m.snapshot_id=v.snapshot_id::uuid" in sql


def test_snapshot_manifest_allows_building_metadata_update():
    kernel, _ = make_kernel(build_default_registry())
    changed = kernel.write.transition_snapshot_manifest(
        ctx(), WorkScope(partition="snapshot-1"), "snapshot-1",
        ("BUILDING",), "BUILDING", {"input_manifest_hash": "a" * 64},
    )
    assert changed == 0


def test_snapshot_manifest_transition_casts_json_quality_report():
    kernel, conn = make_kernel(build_default_registry())
    kernel.write.transition_snapshot_manifest(
        ctx(), WorkScope(partition="snapshot-1"), "snapshot-1",
        ("BUILDING",), "VALIDATED", {"quality_report": "{}"},
    )
    sql = next(item["sql"] for item in conn.executed if item["kind"] == "execute")
    assert "quality_report=v.quality_report::jsonb" in sql


def test_snapshot_manifest_transition_casts_archive_size():
    kernel, conn = make_kernel(build_default_registry())
    kernel.write.transition_snapshot_manifest(
        ctx(), WorkScope(partition="snapshot-1"), "snapshot-1",
        ("VALIDATED",), "FROZEN",
        {"code_archive_size_bytes": 1234},
    )
    sql = next(item["sql"] for item in conn.executed if item["kind"] == "execute")
    assert "code_archive_size_bytes=v.code_archive_size_bytes::bigint" in sql


def test_snapshot_manifest_transition_rejects_unapproved_column():
    kernel, _ = make_kernel(build_default_registry())
    scope = WorkScope(partition="snapshot-1", dimensions={"snapshot_id": "snapshot-1"})
    with pytest.raises(ValueError, match="unsupported snapshot manifest fields"):
        kernel.write.transition_snapshot_manifest(
            ctx(), scope, "snapshot-1", ("BUILDING",), "VALIDATED",
            {"arbitrary_sql": "x"},
        )


def test_snapshot_manifest_transition_rejects_state_machine_bypass():
    kernel, _ = make_kernel(build_default_registry())
    with pytest.raises(ValueError, match="invalid snapshot state transition"):
        kernel.write.transition_snapshot_manifest(
            ctx(), WorkScope(partition="snapshot-1", dimensions={"snapshot_id": "snapshot-1"}),
            "snapshot-1", ("DRAFT",), "FROZEN", {},
        )


# ---- read.query：scan_policy + where→SQL ----

def test_read_requires_scope_on_scope_required():
    kernel, _ = make_kernel()
    with pytest.raises(GuardViolation):
        kernel.read.query(
            context=ctx(), source="feature.price_daily_v2",
            columns=["ts_code", "ma20"], where={},  # 无范围
        )


def test_read_builds_where_sql():
    kernel, conn = make_kernel()
    kernel.read.query(
        context=ctx(), source="feature.price_daily_v2",
        columns=["ts_code", "ma20"],
        where={"trade_date__gte": date(2026, 1, 1), "trade_date__lte": date(2026, 1, 31)},
        limit=100,
    )
    last = conn.executed[-1]
    assert "WHERE" in last["sql"]
    assert "LIMIT 100" in last["sql"]
    assert last["params"] == (date(2026, 1, 1), date(2026, 1, 31))


def test_read_accepts_plain_column_lookup():
    kernel, conn = make_kernel()
    day = date(2026, 1, 8)
    kernel.read.query(
        context=ctx(), source="feature.price_daily_v2", columns=["ts_code"],
        where={"trade_date": day},
    )
    assert "trade_date = %s" in conn.executed[-1]["sql"]
    assert conn.executed[-1]["params"] == (day,)


def test_read_stream_uses_distinct_server_cursor_names():
    kernel, conn = make_kernel()
    scope = WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 1))
    where = {"trade_date": date(2026, 1, 1)}
    list(kernel.read.stream(ctx(), "feature.price_daily_v2", ["ts_code"], where=where, scope=scope))
    list(kernel.read.stream(ctx(), "feature.price_daily_v2", ["ts_code"], where=where, scope=scope))
    names = [name for name in conn.cursor_names if name is not None]
    assert len(names) == 2
    assert names[0] != names[1]


def test_repository_stream_reads_cursor_metadata_after_first_fetch():
    from data_collect.data_access.checkpoint import InMemoryCheckpointStore

    class MetadataAfterFetchConnection(FakeConnection):
        def cursor(self, name=None, withhold=False):
            cursor = super().cursor(name, withhold=withhold)
            if name is not None:
                original_fetchmany = cursor.fetchmany
                def fetchmany(size):
                    rows = original_fetchmany(size)
                    if rows and not cursor.description:
                        from types import SimpleNamespace
                        cursor.description = [SimpleNamespace(name="ts_code")]
                    return rows
                cursor.fetchmany = fetchmany
            return cursor

    conn = MetadataAfterFetchConnection(stream_rows=[("000001.SZ",)])
    kernel = DataAccessKernel(
        policy_registry=registry(), connection_factory=lambda: conn,
        checkpoint_store=InMemoryCheckpointStore(),
    )
    try:
        batches = list(kernel.read._repository_stream(
            ctx(), "feature.price_daily_v2",
            "SELECT ts_code FROM feature.price_daily_v2 WHERE trade_date=%s",
            (date(2026, 1, 1),), WorkScope(date_from=date(2026, 1, 1)),
        ))
    finally:
        kernel.close()
    assert batches == [[{"ts_code": "000001.SZ"}]]


def test_repository_stream_commits_each_batch_with_holdable_cursor():
    class MetadataConnection(FakeConnection):
        def cursor(self, name=None, withhold=False):
            cursor = super().cursor(name, withhold=withhold)
            if name is not None:
                from types import SimpleNamespace
                cursor.description = [SimpleNamespace(name="ts_code")]
            return cursor

    conn = MetadataConnection(stream_rows=[("a",), ("b",), ("c",)])
    kernel = DataAccessKernel(
        policy_registry=registry(), connection_factory=lambda: conn,
    )
    try:
        batches = list(kernel.read._repository_stream(
            ctx(), "feature.price_daily_v2",
            "SELECT ts_code FROM feature.price_daily_v2 WHERE trade_date=%s",
            (date(2026, 1, 1),),
            WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 1)),
            batch_rows=2,
        ))
    finally:
        kernel.close()

    assert [len(batch) for batch in batches] == [2, 1]
    assert conn.committed == 3


def test_read_operations_close_their_own_transaction_boundary():
    kernel, conn = make_kernel()
    scope = WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 1))
    kernel.read.query(
        context=ctx(), source="feature.price_daily_v2", columns=["ts_code"],
        where={"trade_date": date(2026, 1, 1)},
    )
    kernel.read.scalar(
        context=ctx(), source="ops.source_registry", aggregate="COUNT(source_id)",
        scope=WorkScope(partition="test-read"),
    )
    list(kernel.read.stream(
        ctx(), "feature.price_daily_v2", ["ts_code"],
        where={"trade_date": date(2026, 1, 1)}, scope=scope,
    ))
    assert conn.committed == 3
    assert conn.rolled_back == 0


def test_read_stream_inside_repository_atomic_workflow_commits_once():
    kernel, conn = make_kernel()
    scope = WorkScope(partition="test-read-atomic")

    kernel.run_atomic(
        ctx(), scope,
        lambda: list(kernel.read.stream(ctx(), "ops.source_registry", ["source_id"], scope=scope)),
        estimated_rows=1,
    )

    assert conn.committed == 1
    assert conn.rolled_back == 0


def test_read_allows_full_scan_on_dimension():
    kernel, _ = make_kernel()
    kernel.read.query(
        context=ctx(), source="ops.source_registry",
        columns=["source_id"], where={},  # FULL_SCAN_ALLOWED
    )


def test_scope_required_read_rejects_fake_scope_without_where():
    kernel, _ = make_kernel()
    with pytest.raises(ValueError, match="bounded where"):
        kernel.read.query(
            ctx(), "feature.price_daily_v2", ["ts_code"], where={},
            scope=WorkScope(date_from=date(2026, 1, 1)),
        )


# ---- rebuild：强制 SqlPlan + 三种模式 ----

def test_run_insert_select_requires_sqlplan():
    kernel, _ = make_kernel()
    with pytest.raises(TypeError):
        kernel.rebuild.run_insert_select(sql="SELECT 1", plan=None, context=ctx())


def test_daily_partition_replace_stages_chunks_before_standard_publish():
    kernel, conn = make_kernel(build_default_registry())
    rows = [{
        "stock_code": "000001.SZ", "trade_date": date(2026, 8, 6),
        "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05,
        "volume": 100.0, "amount": 105.0,
    }]
    scope = WorkScope(
        date_from=date(2026, 8, 6), date_to=date(2026, 8, 6),
        partition="daily-kline",
    )

    assert kernel.rebuild.replace_daily_partition_rows(ctx(), scope, rows) == 0

    sqls = [entry["sql"] for entry in conn.executed]
    assert sum("CREATE TEMP TABLE" in sql for sql in sqls) == 1
    assert any("INSERT INTO std.daily_kline (" in sql and "FROM _tmp_daily_std_daily_kline_" in sql for sql in sqls)
    assert sqls.index(next(sql for sql in sqls if sql.startswith("DELETE FROM std.daily_kline"))) < sqls.index(next(sql for sql in sqls if sql.startswith("INSERT INTO std.daily_kline (")))


def test_run_insert_select_requires_set_based_mode():
    kernel, _ = make_kernel()
    plan = SqlPlan(
        operation=Operation.REBUILD, target="pit.universe_daily",
        scope=WorkScope(date_from=date(2026, 1, 1)),
        expected_mode="ROW_BASED",  # 错误
    )
    with pytest.raises(ValueError):
        kernel.rebuild.run_insert_select(sql="SELECT 1", plan=plan, context=ctx())


def test_rebuild_rejects_read_plan_before_sql_execution():
    kernel, conn = make_kernel()
    plan = SqlPlan(
        operation=Operation.READ,
        target="feature.price_daily_v2",
        scope=WorkScope(date_from=date(2026, 1, 1)),
        expected_mode="SET_BASED",
        allow_insert_select=True,
    )
    with pytest.raises(ValueError, match="Operation.REBUILD"):
        kernel.rebuild.run_insert_select(
            "INSERT INTO feature.price_daily_v2 SELECT 1", plan, ctx()
        )
    assert conn.executed == []


def test_gateways_reject_dynamic_identifier_injection_before_db_access():
    kernel, conn = make_kernel()
    with pytest.raises(ValueError):
        list(kernel.read.stream(
            ctx(), "feature.price_daily_v2",
            ["close) FROM secret_table; --"],
            scope=WorkScope(date_from=date(2026, 1, 1)),
        ))
    with pytest.raises(ValueError):
        kernel.write.upsert(
            ctx(), WorkScope(date_from=date(2026, 1, 1)),
            "feature.price_daily_v2",
            [{"ts_code": "x", "trade_date": date(2026, 1, 1), "bad;drop": 1}],
            ("ts_code", "trade_date"), (),
        )
    assert conn.executed == []


def test_run_insert_select_rejects_missing_allow_flag_and_ddl():
    kernel, _ = make_kernel()
    plan = SqlPlan(
        operation=Operation.REBUILD, target="feature.price_daily_v2",
        scope=WorkScope(date_from=date(2026, 1, 1)), expected_mode="SET_BASED",
    )
    with pytest.raises(ValueError):
        kernel.rebuild.run_insert_select(
            sql="INSERT INTO feature.price_daily_v2 SELECT 1", plan=plan, context=ctx()
        )
    plan = SqlPlan(
        operation=Operation.REBUILD, target="feature.price_daily_v2",
        scope=WorkScope(date_from=date(2026, 1, 1)), expected_mode="SET_BASED",
        allow_insert_select=True,
    )
    with pytest.raises(ValueError):
        kernel.rebuild.run_insert_select(
            sql="DROP TABLE feature.price_daily_v2", plan=plan, context=ctx()
        )


def test_run_insert_select_executes():
    kernel, conn = make_kernel()
    plan = SqlPlan(
        operation=Operation.REBUILD, target="feature.price_daily_v2",
        scope=WorkScope(date_from=date(2026, 1, 1)),
        expected_mode="SET_BASED", allow_insert_select=True,
    )
    kernel.rebuild.run_insert_select(
        sql="INSERT INTO feature.price_daily_v2 SELECT 1", plan=plan, context=ctx()
    )
    assert conn.committed == 1
    assert any("INSERT INTO feature.price_daily_v2" in e["sql"] for e in conn.executed)


def test_set_based_rebuild_does_not_enter_automatic_retry_boundary():
    kernel, conn = make_kernel()
    kernel.retry_policy.run = lambda *_args, **_kwargs: pytest.fail(
        "non-idempotent SET-based SQL must not be replayed automatically"
    )
    plan = SqlPlan(
        operation=Operation.REBUILD,
        target="feature.price_daily_v2",
        scope=WorkScope(date_from=date(2026, 1, 1)),
        expected_mode="SET_BASED",
        allow_insert_select=True,
    )

    kernel.rebuild.run_insert_select(
        sql="INSERT INTO feature.price_daily_v2 SELECT 1",
        plan=plan,
        context=ctx(),
    )

    assert conn.committed == 1


def test_scoped_merge_rejects_dml_against_a_different_target():
    kernel, _ = make_kernel()
    plan = SqlPlan(
        operation=Operation.REBUILD,
        target="feature.price_daily_v2",
        scope=WorkScope(date_from=date(2026, 1, 1)),
        expected_mode="SET_BASED",
        allow_delete=True,
    )
    with pytest.raises(ValueError, match="DML targets"):
        kernel.rebuild.run_scoped_merge(
            "DELETE FROM ops.source_registry WHERE source_id='feature.price_daily_v2'",
            plan, ctx(),
        )


def test_rebuild_producer_rejects_invalid_or_changing_columns():
    kernel, _ = make_kernel()
    with pytest.raises(ValueError):
        kernel.rebuild.publish(
            ctx(), WorkScope(date_from=date(2026, 1, 1)),
            "feature.price_daily_v2",
            lambda _scope: [{"ts_code": "x", "bad);drop": 1}],
            RebuildMode.SCOPED_MERGE,
        )


def test_rebuild_mode_enum():
    assert {m.value for m in RebuildMode} == {
        "TABLE_REPLACE", "PARTITION_REPLACE", "SCOPED_MERGE"
    }


def test_rebuild_publish_lifecycle_scoped_merge():
    """rebuild.publish 完整生命周期：staging 内部写入（不经过业务 DatasetPolicy）→ 按 mode 发布 → 清理。"""
    kernel, conn = make_kernel()

    def producer(s):
        yield from _feature_rows(100)

    kernel.rebuild.publish(
        context=ctx(), scope=WorkScope(date_from=date(2026, 1, 1)),
        target="feature.price_daily_v2", producer=producer,
        mode=RebuildMode.SCOPED_MERGE,
    )
    sqls = [e["sql"] for e in conn.executed]
    # 生命周期：建 staging → 写 staging → 发布 → 清理 staging
    assert any("CREATE TEMP TABLE" in s and "_staging_" in s for s in sqls)
    assert any("DROP TABLE" in s for s in sqls)
    # SCOPED_MERGE → INSERT INTO target SELECT FROM staging
    assert any("INSERT INTO feature.price_daily_v2 SELECT" in s for s in sqls)
    assert any("DELETE FROM feature.price_daily_v2 WHERE trade_date >=" in s for s in sqls)


def test_rebuild_hash_uses_named_cursor_and_streaming_fetchmany_without_string_agg():
    conn = FakeConnection(stream_rows=[("alpha",), ("beta",), ("gamma",)])
    kernel = DataAccessKernel(
        policy_registry=registry(), connection_factory=lambda: conn,
    )

    result = kernel.rebuild._staging_hash("feature._staging_price_daily_v2_test")

    digest = hashlib.sha256()
    for value in ("alpha", "beta", "gamma"):
        payload = value.encode("utf-8")
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    assert result == digest.hexdigest()
    assert any(name and name.startswith("rebuild_hash_") for name in conn.cursor_names)
    assert conn.fetchmany_calls == [10_000, 10_000]
    assert all("string_agg" not in entry["sql"].lower() for entry in conn.executed)


def test_rebuild_publish_acquires_and_releases_advisory_session_lock():
    kernel, conn = make_kernel()
    kernel.rebuild.publish(
        context=ctx(), scope=WorkScope(date_from=date(2026, 1, 1)),
        target="feature.price_daily_v2", producer=lambda _scope: [],
        mode=RebuildMode.SCOPED_MERGE,
    )

    sqls = [entry["sql"] for entry in conn.executed]
    acquire = next(i for i, sql in enumerate(sqls) if "pg_try_advisory_lock" in sql)
    create = next(i for i, sql in enumerate(sqls) if "CREATE TEMP TABLE" in sql)
    release = next(i for i, sql in enumerate(sqls) if "pg_advisory_unlock" in sql)
    assert acquire < create < release


def test_rebuild_publish_rejects_unscoped_scoped_merge():
    kernel, _ = make_kernel()
    with pytest.raises(GuardViolation):
        kernel.rebuild.publish(
            context=ctx(), scope=WorkScope(), target="feature.price_daily_v2",
            producer=lambda _scope: [], mode=RebuildMode.SCOPED_MERGE,
        )


def test_table_replace_rejects_non_insert_fill_sql():
    kernel, _ = make_kernel()
    plan = SqlPlan(
        operation=Operation.REBUILD, target="feature.price_daily_v2",
        scope=WorkScope(date_from=date(2026, 1, 1)), expected_mode="SET_BASED",
        allow_insert_select=True,
    )
    with pytest.raises(ValueError):
        kernel.rebuild.run_table_replace(
            "DROP TABLE {staging}", plan=plan, context=ctx()
        )


def test_table_replace_uses_unqualified_rename_and_atomic_swap():
    kernel, conn = make_kernel()
    plan = SqlPlan(
        operation=Operation.REBUILD, target="feature.price_daily_v2",
        scope=WorkScope(date_from=date(2026, 1, 1)), expected_mode="SET_BASED",
        allow_insert_select=True,
    )
    kernel.rebuild.run_table_replace(
        "INSERT INTO {staging} (ts_code) SELECT ts_code FROM feature.price_daily_v2",
        plan=plan, context=ctx(),
    )
    sqls = [entry["sql"] for entry in conn.executed]
    assert any("ALTER TABLE feature.price_daily_v2 RENAME TO _old_price_daily_v2_" in sql for sql in sqls)
    assert any("ALTER TABLE feature._staging_price_daily_v2_" in sql and "RENAME TO price_daily_v2" in sql for sql in sqls)
    assert not any("DROP TABLE IF EXISTS feature.price_daily_v2" in sql for sql in sqls)


def test_table_replace_publish_mode_keeps_target_present_during_swap():
    kernel, conn = make_kernel()
    assert kernel.rebuild._publish(
        RebuildMode.TABLE_REPLACE, "feature.price_daily_v2",
        "feature._staging_price_daily_v2_test", WorkScope(date_from=date(2026, 1, 1)),
    ) is True
    sqls = [entry["sql"] for entry in conn.executed]
    assert any("ALTER TABLE feature.price_daily_v2 RENAME TO _old_price_daily_v2_" in sql for sql in sqls)
    assert any("DROP TABLE feature._old_price_daily_v2_" in sql for sql in sqls)


def test_partition_replace_builds_bounds_and_keeps_attached_partition():
    kernel, conn = make_kernel()
    kernel.rebuild.publish(
        context=ctx(),
        scope=WorkScope(partition="2026-01"),
        target="feature.price_daily_v2",
        producer=lambda _scope: _feature_rows(1),
        mode=RebuildMode.PARTITION_REPLACE,
    )
    sqls = [entry["sql"] for entry in conn.executed]
    assert any("ADD CONSTRAINT" in sql and "2026-01-01" in sql for sql in sqls)
    assert any("ATTACH PARTITION" in sql for sql in sqls)
    assert not any("DROP TABLE IF EXISTS feature._staging_" in sql for sql in sqls)


def test_rebuild_publish_table_replace_swaps():
    kernel, conn = make_kernel()

    def producer(s):
        yield from _feature_rows(10)

    kernel.rebuild.publish(
        context=ctx(), scope=WorkScope(date_from=date(2026, 1, 1)),
        target="feature.price_daily_v2", producer=producer,
        mode=RebuildMode.TABLE_REPLACE,
    )
    sqls = [e["sql"] for e in conn.executed]
    assert any("ALTER TABLE feature.price_daily_v2 RENAME TO _old_price_daily_v2_" in s for s in sqls)
    assert any("ALTER TABLE feature._staging_price_daily_v2_" in s and "RENAME TO price_daily_v2" in s for s in sqls)
    assert any("DROP TABLE feature._old_price_daily_v2_" in s for s in sqls)


# ---- publish：FROZEN 只读 ----

def test_publish_rejects_frozen_upsert_via_guard():
    from data_collect.data_access.query_guard import QueryGuard

    guard = QueryGuard(registry())
    plan = SqlPlan(
        operation=Operation.UPSERT, target="contract.some_upsertable",
        scope=WorkScope(date_from=date(2026, 1, 1)),
    )
    with pytest.raises(GuardViolation):
        guard.check(plan)


def test_immutable_manifest_generic_upsert_cannot_update_rows():
    kernel, _ = make_kernel(build_default_registry())
    with pytest.raises(ValueError, match="typed transition"):
        kernel.write.upsert(
            ctx(), WorkScope(partition="snapshot-1"),
            "contract.snapshot_manifest",
            [{"snapshot_id": "snapshot-1", "status": "FAILED"}],
            key=("snapshot_id",), update_columns=("status",),
        )


def test_publish_snapshot_rows_must_match_scope_snapshot_id():
    kernel, _ = make_kernel(build_default_registry())
    with pytest.raises(ValueError, match="snapshot_id"):
        kernel.publish.snapshot(
            ctx(),
            WorkScope(
                date_from=date(2026, 1, 1), date_to=date(2026, 1, 1),
                partition="snapshot-1", dimensions={"snapshot_id": "snapshot-1"},
            ),
            "contract.stock_snapshot_daily",
            [{"snapshot_id": "snapshot-2", "research_date": date(2026, 1, 1)}],
        )


def test_repository_query_accepts_schema_qualified_form_of_logical_source():
    kernel, _ = make_kernel(build_default_registry())

    rows = kernel.read._repository_query(
        ctx(),
        "std.daily_kline",
        "SELECT COUNT(*) AS row_count FROM std.daily_kline WHERE trade_date=%s",
        (date(2026, 8, 20),),
        WorkScope(date_from=date(2026, 8, 20), date_to=date(2026, 8, 20)),
        2,
    )

    assert rows == [{"row_count": 0}]


def test_repository_query_does_not_accept_different_qualified_relation():
    kernel, _ = make_kernel(build_default_registry())

    with pytest.raises(ValueError, match="source=std.daily_kline"):
        kernel.read._repository_query(
            ctx(),
            "std.daily_kline",
            "SELECT COUNT(*) AS row_count FROM public.valuation_daily",
            (),
            WorkScope(date_from=date(2026, 8, 20), date_to=date(2026, 8, 20)),
            2,
        )

    with pytest.raises(ValueError, match="source=std.daily_kline"):
        kernel.read._repository_query(
            ctx(), "std.daily_kline",
            "SELECT COUNT(*) AS row_count FROM secret.daily_kline",
            (), WorkScope(date_from=date(2026, 8, 20)), 2,
        )
