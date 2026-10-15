"""Repository 迁移样板行为测试（fake connection）。

验证 SET-based SQL 从 job 迁到 Repository 后：
- 通过 Kernel 受控通道执行（run_scoped_merge）
- 强制 SqlPlan（scope 缺日期 → 拒绝）
- params 正确传递
"""
from __future__ import annotations

from datetime import date

import pytest

from data_collect.data_access import DataAccessKernel
from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.security_repository import SecurityRepository
from data_collect.data_access.repository.pit_repository import PitRepository
from data_collect.data_access.repository.market_repository import MarketRepository
from data_collect.data_access.repository.release_repository import ReleaseRepository
from data_collect.data_access.repository.research_snapshot_repository import (
    ResearchBaseline,
    ResearchSnapshotRepository,
)
from tests._fake_db import FakeConnection


def ctx() -> RunContext:
    return RunContext(
        run_id="run-1", job_name="job", code_version="git:abc",
        config_hash="cfg", pipeline_version="v1",
    )


def make_kernel_and_repo():
    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    return SecurityRepository(kernel), kernel, conn


@pytest.mark.parametrize(
    ("schema_version", "expected_domain"),
    [
        ("market-activity-v1", "MARKET_ACTIVITY"),
        ("feature-contract-v2.0", "RESEARCH"),
        ("", "RESEARCH"),
    ],
)
def test_release_schema_maps_to_an_isolated_baseline_domain(schema_version, expected_domain):
    assert ReleaseRepository._baseline_domain(schema_version) == expected_domain


def test_research_snapshot_baseline_exposes_named_fields_and_keeps_tuple_access(monkeypatch):
    cutoff = date(2026, 8, 6)
    row = {
        "release_tag": "release-1",
        "baseline_domain": "RESEARCH",
        "baseline_cutoff_date": cutoff,
        "status": "validated",
        "quality_report_hash": "quality-hash",
        "source_manifest_hash": "source-hash",
        "source_manifest": {"append_only_sources": {}},
        "schema_version": "feature-contract-v2.0",
        "quality_report": {"failures": {}},
    }
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=FakeConnection,
    )
    repository = ResearchSnapshotRepository(kernel)
    monkeypatch.setattr(
        kernel.read, "_repository_query", lambda *args, **kwargs: [row],
    )

    baseline = repository.baseline(cutoff, "release-1", ctx(), verify_source_manifest=False)

    assert isinstance(baseline, ResearchBaseline)
    assert baseline.release_tag == "release-1"
    assert baseline.baseline_cutoff_date == cutoff
    assert baseline.quality_report_hash == "quality-hash"
    assert baseline.source_manifest_hash == "source-hash"
    assert baseline.source_manifest == {"append_only_sources": {}}
    assert baseline.quality_report == {"failures": {}}
    assert baseline[3] == baseline.quality_report_hash

    kernel.close()


def test_research_release_gate_blocks_implemented_dividends_without_ex_date(monkeypatch):
    from data_collect.data_access.repository.quality_inspection_repository import (
        QualityInspectionRepository,
    )

    monkeypatch.setattr(
        QualityInspectionRepository,
        "phase2_invariants",
        lambda self, context, cutoff, skip_names: {},
    )
    monkeypatch.setattr(
        QualityInspectionRepository,
        "phase2_checkpoint_completeness",
        lambda self, context, cutoff: {},
    )
    values = {
        "release:lifecycle": 0,
        "release:dividend-duplicates": 0,
        "release:dividend-ex-date": 1,
    }
    repository = ReleaseRepository(object())
    monkeypatch.setattr(
        repository,
        "_scalar",
        lambda table, sql, context, partition, params=(): values[partition],
    )

    report = repository.quality_gate(date(2026, 9, 25), False, ctx())

    assert report["implementation_without_ex_date"] == 1
    assert report["failures"]["implementation_without_ex_date"] == 1


def test_research_observation_baseline_lookup_is_domain_scoped():
    from data_collect.data_access.repository.observation_repository import (
        StockAnomalyObservationRepository,
    )

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    with pytest.raises(ValueError, match="validated data baseline not found"):
        StockAnomalyObservationRepository(kernel).validated_baseline("baseline-x", ctx())
    query = next(item["sql"] for item in conn.executed if "ops.tushare_data_release" in item["sql"])
    assert "baseline_domain='RESEARCH'" in query


def test_incremental_watermarks_fall_back_to_non_revoked_research_releases():
    from data_collect.data_access.repository.phase2_raw_repository import Phase2RawRepository

    for repository, method_name in (
        (MarketRepository, "baseline_cutoff"),
        (Phase2RawRepository, "baseline_cutoff"),
    ):
        conn = FakeConnection()
        kernel = DataAccessKernel(
            policy_registry=build_default_registry(), connection_factory=lambda: conn
        )
        with pytest.raises(RuntimeError, match="RESEARCH"):
            getattr(repository(kernel), method_name)(ctx())
        query = next(
            item["sql"] for item in conn.executed
            if "ops.tushare_data_release" in item["sql"]
        )
        assert "status IN ('validated', 'superseded')" in query
        assert "baseline_domain='RESEARCH'" in query
        assert "CASE WHEN status='validated' THEN 0 ELSE 1 END" in query


def test_security_repository_requires_scope_dates():
    repo, _, _ = make_kernel_and_repo()
    with pytest.raises(ValueError):
        repo.rebuild_status_daily(WorkScope(), ctx())  # 无日期范围


@pytest.mark.parametrize(
    ("logical_target", "canonical_target"),
    [
        ("daily_kline", "std.daily_kline"),
        ("valuation_daily", "std.valuation_daily"),
        ("adj_factor_daily", "std.adj_factor_daily"),
    ],
)
def test_market_standard_count_uses_registered_standard_relation(
    logical_target, canonical_target,
):
    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    captured = {}

    def fake_query(**kwargs):
        captured.update(kwargs)
        return [{"count": 0}]

    kernel.read._repository_query = fake_query
    MarketRepository(kernel).standard_count(logical_target, date(2026, 9, 10), ctx())
    assert captured["source"] == canonical_target
    assert f"FROM {canonical_target}" in captured["sql"]


def test_security_repository_executes_set_based_sql():
    repo, kernel, conn = make_kernel_and_repo()
    scope = WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 2))
    repo.rebuild_status_daily(scope, ctx())

    sqls = [e["sql"] for e in conn.executed]
    # SET-based 重建：scoped DELETE + INSERT...ON CONFLICT
    assert any("DELETE FROM std.security_status_daily" in s for s in sqls)
    assert any("ON CONFLICT" in s for s in sqls)
    # Two dates, one governed transaction per date.
    assert conn.committed == 2


def test_security_repository_passes_params():
    repo, kernel, conn = make_kernel_and_repo()
    scope = WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 2))
    repo.rebuild_status_daily(scope, ctx())

    # params 应为 6 个（DELETE 2 + dates 2 + limits 2）
    executed_with_params = [e for e in conn.executed if e["params"]]
    assert executed_with_params
    params = executed_with_params[0]["params"]
    assert params == (
        date(2026, 1, 1), date(2026, 1, 1),
        date(2026, 1, 1), date(2026, 1, 1),
        date(2026, 1, 1), date(2026, 1, 1),
    )


def test_security_repository_splits_wide_status_scope_by_day():
    repo, kernel, conn = make_kernel_and_repo()
    scope = WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 2))

    repo.rebuild_status_daily(scope, ctx())

    statements = [
        item for item in conn.executed
        if "DELETE FROM std.security_status_daily" in item["sql"]
    ]
    assert len(statements) == 2
    assert [item["params"][:2] for item in statements] == [
        (date(2026, 1, 1), date(2026, 1, 1)),
        (date(2026, 1, 2), date(2026, 1, 2)),
    ]
    assert conn.committed == 2


def test_security_repository_guard_rejects_unscoped_delete():
    """无范围的 scoped merge 会被 QueryGuard 拒绝（allow_delete 但 scope 空）。"""
    from data_collect.data_access.context import Operation, SqlPlan
    from data_collect.data_access.errors import GuardViolation

    _, kernel, _ = make_kernel_and_repo()
    plan = SqlPlan(
        operation=Operation.REBUILD, target="std.security_status_daily",
        scope=WorkScope(), expected_mode="SET_BASED",
        allow_insert_select=True, allow_delete=True,
    )
    with pytest.raises(GuardViolation):
        kernel.query_guard.check(plan)


def test_pit_rebuild_splits_join_heavy_targets_by_date():
    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    repo = PitRepository(kernel)
    scope = WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 2))

    repo.rebuild_universe(scope, ctx())

    merge_sql = [
        item for item in conn.executed
        if "DELETE FROM pit.universe_daily" in item["sql"]
    ]
    assert len(merge_sql) == 2
    assert [item["params"][:2] for item in merge_sql] == [
        (date(2026, 1, 1), date(2026, 1, 1)),
        (date(2026, 1, 2), date(2026, 1, 2)),
    ]
    # Two bounded reads plus one governed write per day.
    assert conn.committed == 6


def test_pit_rebuild_can_split_a_day_by_code_batch():
    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    repo = PitRepository(kernel)
    codes = tuple(f"{index:06d}.SZ" for index in range(501))

    repo.rebuild_universe(
        WorkScope(
            date_from=date(2026, 1, 1),
            date_to=date(2026, 1, 1),
            codes=codes,
        ),
        ctx(),
    )

    merge_sql = [
        item for item in conn.executed
        if "DELETE FROM pit.universe_daily" in item["sql"]
    ]
    assert len(merge_sql) == 2
    assert all("ts_code = ANY(%s)" in item["sql"] for item in merge_sql)
    assert all(len(item["params"]) == 6 for item in merge_sql)
    assert merge_sql[0]["params"][2] == list(codes[:500])
    assert merge_sql[1]["params"][2] == list(codes[500:])
    assert conn.committed == 2


def test_security_repository_lifecycle_uses_staging_swap_not_truncate():
    """rebuild_lifecycle 用 staging+swap（TABLE_REPLACE），消除 TRUNCATE。"""
    repo, kernel, conn = make_kernel_and_repo()
    repo.rebuild_lifecycle(ctx())
    sqls = [e["sql"] for e in conn.executed]
    # 建 staging（带 schema）+ fill + swap
    assert any("ops.create_table_replace_staging" in s for s in sqls)
    # PostgreSQL 的 RENAME TO 右侧必须是未限定的表名；swap 期间旧表仍在，
    # 因而不会出现 DROP target 后再 RENAME 的短暂 relation 缺失窗口。
    assert any("ops.swap_table_replace_staging" in s for s in sqls)
    assert any("ops.drop_table_replace_staging" in s for s in sqls)
    # 无 TRUNCATE（文档 §5.2 禁止）
    assert not any("TRUNCATE" in s for s in sqls)


def test_backtest_run_repository_uses_bulk_insert_for_immutable_strategy():
    from data_collect.data_access.repository.backtest_run_repository import BacktestRunRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(policy_registry=build_default_registry(), connection_factory=lambda: conn)
    repository = BacktestRunRepository(kernel)
    repository.register_strategy({
        "strategy_id": "strategy-v1", "strategy_name": "strategy-v1", "strategy_version": "1",
        "description": "test", "parameters": "{}", "parameters_hash": "hash", "code_version": "git",
    }, ctx())
    inserts = [item for item in conn.executed if item["kind"] == "executemany"]
    assert len(inserts) == 1
    assert "backtest.app_strategy_version" in inserts[0]["sql"]
    assert "ON CONFLICT" not in inserts[0]["sql"]


def test_backtest_run_repository_requires_exact_approved_strategy_version():
    from data_collect.data_access.repository.backtest_run_repository import BacktestRunRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(policy_registry=build_default_registry(), connection_factory=lambda: conn)
    repository = BacktestRunRepository(kernel)
    assert repository.strategy("strategy-v1", "1", ctx()) is None
    query = conn.executed[-1]
    assert "strategy_id=%s AND strategy_version=%s" in query["sql"]
    assert query["params"] == ("strategy-v1", "1")


def test_backtest_run_status_transition_uses_named_update_not_partial_upsert():
    from data_collect.data_access.repository.backtest_run_repository import BacktestRunRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(policy_registry=build_default_registry(), connection_factory=lambda: conn)
    repository = BacktestRunRepository(kernel)
    changed = repository.kernel.write.update_backtest_run_status(
        ctx(), WorkScope(partition="run-1"), "run-1", "FAILED", "test failure"
    )
    assert changed == 0
    query = conn.executed[-1]
    assert query["sql"].startswith("UPDATE backtest.run_manifest SET status=%s")
    assert "WHERE run_id=%s AND status='RUNNING'" in query["sql"]
    assert query["params"] == ("FAILED", "test failure", "run-1")


def test_dataset_runtime_pages_are_bounded_by_requested_evaluation_window():
    from data_collect.data_access.repository.dataset_repository import DatasetRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    repository = DatasetRepository(kernel)
    start, end = date(2025, 1, 2), date(2025, 3, 31)

    repository.bar_page(
        "dataset-1", ctx(), (date(2025, 1, 31), "000001.SZ"), 100,
        date_from=start, date_to=end,
    )
    repository.observation_page(
        "dataset-1", ("research_date", "ts_code", "raw_close"), ctx(),
        (date(2025, 1, 31), "000001.SZ"), 100,
        date_from=start, date_to=end,
    )

    bar_query = next(
        row for row in conn.executed
        if "FROM backtest.dataset_execution_bar" in row["sql"]
    )
    observation_query = next(
        row for row in conn.executed
        if "JOIN research.stock_snapshot_daily s" in row["sql"]
    )
    assert "trade_date >= %s" in bar_query["sql"]
    assert "trade_date <= %s" in bar_query["sql"]
    assert bar_query["params"] == (
        "dataset-1", start, end, date(2025, 1, 31), "000001.SZ",
    )
    assert "s.research_date >= %s" in observation_query["sql"]
    assert "s.research_date <= %s" in observation_query["sql"]
    assert "s.research_date=m.research_date" in observation_query["sql"]
    assert "snapshot_id IN" not in observation_query["sql"]
    assert observation_query["params"] == (
        "dataset-1", start, end, date(2025, 1, 31), "000001.SZ",
    )


def test_dataset_research_snapshot_contracts_are_joined_in_one_bounded_read():
    from data_collect.data_access.repository.dataset_repository import DatasetRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    rows = DatasetRepository(kernel).research_snapshot_contracts("dataset-1", ctx())
    assert rows == []
    query = conn.executed[-1]
    assert "JOIN research.snapshot_manifest" in query["sql"]
    assert "WHERE m.dataset_id=%s" in query["sql"]
    assert query["params"] == ("dataset-1",)


def test_instrument_repository_keeps_legacy_raw_first_seen_only():
    from data_collect.data_access.repository.instrument_repository import InstrumentRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    InstrumentRepository(kernel).persist_stock_basic(
        [{
            "ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行",
            "area": "深圳", "industry": "银行", "market": "主板",
            "list_date": "19910403", "delist_date": None, "list_status": "L",
        }],
        "stock-basic-test",
        ctx(),
    )
    raw_sql = next(
        entry["sql"] for entry in conn.executed
        if "INSERT INTO tushare_stock_basic_raw (" in entry["sql"]
    )
    assert "ON CONFLICT DO NOTHING" in raw_sql
    assert "DO UPDATE" not in raw_sql


def test_financial_vip_page_queue_lookup_is_bounded_to_page_hashes(monkeypatch):
    from data_collect.data_access.repository.financial_vip_repository import FinancialVipRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    repository = FinancialVipRepository(kernel)
    records = [{
        "ts_code": "000001.SZ", "end_date": "20251231", "ann_date": "20260301",
        "report_type": "1", "comp_type": "1", "end_type": "1",
        "f_ann_date": "20260301", "update_flag": "0", "revenue": 1,
    }]

    def lookup(*, context, source, sql, params, scope, max_rows):
        conn.executed.append({"kind": "execute", "sql": sql, "params": params})
        return [{
            "record_id": 1,
            "business_key": params[0][0],
                "ts_code": "000001.SZ",
                "end_date": date(2025, 12, 31),
                "batch_id": "batch-1",
                "payload_hash": params[1][0],
                "available_date": date(2026, 3, 1),
        }]

    monkeypatch.setattr(kernel.read, "_repository_query", lookup)
    repository.write_versions_and_queue("income", records, "batch-1", ctx())
    lookup = next(
        entry for entry in conn.executed
        if entry["kind"] == "execute" and "SELECT record_id" in entry["sql"]
    )
    assert "FROM unnest(%s::text[], %s::text[])" in lookup["sql"]
    assert "v.business_key=r.business_key" in lookup["sql"]
    assert "v.payload_hash=r.payload_hash" in lookup["sql"]
    assert "AND r.dataset=%s" in lookup["sql"]
    assert lookup["params"][0][0] == "000001.SZ:20251231:1:1:1"
    assert len(lookup["params"][0]) == 1
    assert len(lookup["params"][1]) == 1


def test_financial_vip_queue_lookup_fails_closed_for_missing_pair():
    from data_collect.data_access.repository.financial_vip_repository import FinancialVipRepository

    with pytest.raises(RuntimeError, match="queue lookup incomplete"):
        FinancialVipRepository._assert_queue_lookup_complete(
            [{"business_key": "000001.SZ:20251231:1:1:1", "payload_hash": "a" * 64}],
            [],
        )


def test_financial_vip_write_rolls_back_on_incomplete_queue_lookup():
    from data_collect.data_access.repository.financial_vip_repository import FinancialVipRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    repository = FinancialVipRepository(kernel)
    with pytest.raises(RuntimeError, match="queue lookup incomplete"):
        repository.write_versions_and_queue(
            "income",
            [{
                "ts_code": "000001.SZ", "end_date": "20251231", "ann_date": "20260301",
                "report_type": "1", "comp_type": "1", "end_type": "1",
                "f_ann_date": "20260301", "update_flag": "0", "revenue": 1,
            }],
            "batch-1",
            ctx(),
        )
    assert conn.committed == 0
    assert conn.rolled_back == 1


def test_financial_queue_claim_uses_lease_owner_and_reclaims_expired_rows():
    class QueueConnection(FakeConnection):
        def cursor(self, name=None, withhold=False):
            cursor = super().cursor(name, withhold)
            original_execute = cursor.execute

            def execute(sql, params=None):
                original_execute(sql, params)
                if "RETURNING q.queue_id, q.ts_code" in sql:
                    cursor._fetch_rows = [(11, "000001.SZ")]

            cursor.execute = execute
            return cursor

    conn = QueueConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    ids, codes = kernel.write.claim_financial_queue(
        ctx(), WorkScope(partition="vip-queue-claim"),
        limit=10, lease_seconds=600, max_attempts=3,
        through_date=date(2026, 9, 5),
    )
    assert ids == [11]
    assert codes == ["000001.SZ"]
    reclaim = next(
        entry for entry in conn.executed
        if "WITH reclaimable AS" in entry["sql"]
    )
    claim = next(
        entry for entry in conn.executed
        if "RETURNING q.queue_id, q.ts_code" in entry["sql"]
    )
    assert "claim_run_id=NULL" in reclaim["sql"]
    assert "claim_owner_id=NULL" in reclaim["sql"]
    assert "lease_expires_at=NULL" in reclaim["sql"]
    assert "claim_run_id=%s" in claim["sql"]
    assert "claim_owner_id=%s" in claim["sql"]
    assert "lease_expires_at=CURRENT_TIMESTAMP" in claim["sql"]
    assert claim["params"] == (3, date(2026, 9, 5), 10, "run-1", "run-1", 600)


def test_financial_queue_finish_does_not_reuse_cross_attempt_checkpoint():
    from data_collect.data_access.write_gateway import WriteGateway

    source = __import__("inspect").getsource(WriteGateway.update_financial_queue)
    assert "_checkpoint_cursor" not in source
    assert "_checkpoint_done" not in source
    assert "claim_owner_id=%s" in source


def test_phase2_raw_serialization_normalizes_non_finite_api_numbers():
    from data_collect.data_access.repository.phase2_raw_repository import Phase2RawRepository

    assert Phase2RawRepository._json_safe(float("nan")) is None
    assert Phase2RawRepository._json_safe(float("inf")) is None
    assert Phase2RawRepository._json_safe({"nested": [1.0, float("-inf")]}) == {
        "nested": [1.0, None]
    }


def test_financial_event_reads_are_split_into_policy_bounded_code_batches():
    from data_collect.data_access.repository.financial_repository import FinancialRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    repository = FinancialRepository(kernel)
    rows = repository.load_events(
        ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ", "000006.SZ"],
        date(2020, 1, 1), date(2026, 8, 21), ctx(),
    )

    reads = [entry for entry in conn.executed if entry["kind"] == "execute"]
    assert rows == []
    assert len(reads) == 2
    assert len(reads[0]["params"][0]) == 5
    assert len(reads[1]["params"][0]) == 1
    assert "max_rows=500_000" not in __import__(
        "inspect"
    ).getsource(FinancialRepository.load_events)


def test_foundation_checkpoint_invalidation_is_one_set_based_write():
    from data_collect.data_access.repository.financial_repository import FinancialRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    repository = FinancialRepository(kernel)
    repository.kernel.write.invalidate_foundation_checkpoints(
        ctx(),
        WorkScope(partition="foundation-run:run-1", dimensions={"run_id": "run-1"}),
        "run-1",
        "invalidated: code identity changed",
    )
    updates = [
        entry for entry in conn.executed
        if entry["kind"] == "execute" and "UPDATE ops.foundation_rebuild_checkpoint" in entry["sql"]
    ]
    assert len(updates) == 1
    assert "status='PENDING'" in updates[0]["sql"]
    assert conn.committed == 1


def test_foundation_checkpoint_invalidation_can_target_changed_stages_only():
    from data_collect.data_access.repository.financial_repository import FinancialRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    FinancialRepository(kernel).kernel.write.invalidate_foundation_checkpoints(
        ctx(),
        WorkScope(partition="foundation-run:run-1", dimensions={"run_id": "run-1"}),
        "run-1",
        "invalidated: stage computation identity changed",
        stages=["valuation_daily_v2"],
    )
    update = next(
        entry for entry in conn.executed
        if entry["kind"] == "execute"
        and "UPDATE ops.foundation_rebuild_checkpoint" in entry["sql"]
    )
    assert "stage=ANY(%s)" in update["sql"]
    assert update["params"][-1] == ["valuation_daily_v2"]


def test_foundation_checkpoint_state_is_reapplied_after_invalidation():
    """An earlier write cursor must not swallow a later state transition."""
    from data_collect.data_access.repository.financial_repository import FinancialRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    repository = FinancialRepository(kernel)
    for _ in range(2):
        repository.save_checkpoint(
            "run-1", "company_financial_v2", "000001.SZ..000005.SZ",
            "COMPLETED", ctx(), attempted=80, written=80,
            result_hash="a" * 64,
        )

    statements = [
        entry for entry in conn.executed
        if entry["kind"] == "execute"
        and entry["sql"].startswith(
            "INSERT INTO ops.foundation_rebuild_checkpoint"
        )
    ]
    assert len(statements) == 2
    assert all(
        "ON CONFLICT (run_id, stage, partition_key)" in entry["sql"]
        for entry in statements
    )


def test_foundation_completed_checkpoints_are_loaded_in_one_bounded_read():
    from data_collect.data_access.repository.financial_repository import FinancialRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    repository = FinancialRepository(kernel)
    assert repository.completed_checkpoint_keys("run-1", "company_financial_v2", ctx()) == set()
    reads = [
        entry for entry in conn.executed
        if entry["kind"] == "execute"
        and "SELECT partition_key FROM ops.foundation_rebuild_checkpoint" in entry["sql"]
    ]
    assert len(reads) == 1
    assert reads[0]["params"] == ("run-1", "company_financial_v2")


def test_valuation_code_discovery_deduplicates_before_detail_row_limit():
    from data_collect.data_access.repository.financial_repository import FinancialRepository

    source = __import__("inspect").getsource(FinancialRepository.valuation_codes)
    assert "SELECT DISTINCT stock_code FROM std.valuation_daily" in source
    assert "self.kernel.read._repository_query" in source
    assert "self.kernel.read.query" not in source
    assert "max_rows=self.kernel.policy_registry.require" in source


def test_financial_feature_jsonb_is_encoded_at_repository_boundary():
    from data_collect.data_access.repository.financial_repository import FinancialRepository

    lineage = {"income": {"report_date": "2025-12-31"}, "sources": ["raw-1"]}
    rows = FinancialRepository._normalize_feature_rows([{
        "ts_code": "000001.SZ", "component_lineage": lineage,
        "quality_status": "VALID",
    }])

    assert isinstance(rows[0]["component_lineage"], str)
    assert __import__("json").loads(rows[0]["component_lineage"]) == lineage
    assert lineage["sources"] == ["raw-1"]


def test_release_manifest_aggregates_each_source_once_through_cutoff():
    from datetime import datetime
    from data_collect.data_access.repository.release_repository import ReleaseRepository

    class ProbeReleaseRepository(ReleaseRepository):
        def __init__(self):
            super().__init__(None)
            self.calls = []

        def _aggregate(self, table, sql, params, context, partition):
            self.calls.append((table, sql, params, partition))
            if "MIN(" in sql:
                return {"min_date": date(2026, 8, 1)}
            return {
                "count": 7, "max_record_id": 9,
                "max_fetched_at": datetime(2026, 8, 21, 12, 0),
            }

    repository = ProbeReleaseRepository()
    cutoff = date(2026, 8, 21)
    result = repository._manifest_date_ranges(
        "tushare_market_raw_version", "trade_date", cutoff, None,
        dataset="daily", include_record_id=True, partition="release:market:daily",
    )

    assert len(repository.calls) == 2
    _, sql, params, partition = repository.calls[1]
    assert "trade_date BETWEEN %s AND %s" in sql
    assert params == (date(2026, 8, 1), cutoff, "daily")
    assert partition.endswith(":2026-08-01:2026-08-21")
    assert result == {
        "row_count": 7, "max_record_id": 9,
        "max_fetched_at": "2026-08-21T12:00:00",
    }


def test_release_manifest_splits_only_a_timed_out_window():
    from datetime import datetime
    from data_collect.data_access.repository.release_repository import ReleaseRepository

    class TimeoutProbe(ReleaseRepository):
        def __init__(self):
            super().__init__(None)
            self.windows = []

        def _aggregate(self, table, sql, params, context, partition):
            if "MIN(" in sql:
                return {"min_date": date(2026, 8, 1)}
            start, end = params[:2]
            self.windows.append((start, end))
            if (end - start).days > 5:
                raise RuntimeError("canceling statement due to statement timeout")
            return {
                "count": (end - start).days + 1,
                "max_record_id": end.day,
                "max_fetched_at": datetime.combine(end, datetime.min.time()),
            }

    repository = TimeoutProbe()
    result = repository._manifest_date_ranges(
        "tushare_market_raw_version", "trade_date", date(2026, 8, 21), None,
        dataset="daily", include_record_id=True, partition="release:market:daily",
    )

    assert len(repository.windows) > 1
    assert any((end - start).days > 5 for start, end in repository.windows)
    assert any((end - start).days <= 5 for start, end in repository.windows)
    assert result["row_count"] == 21
    assert result["max_record_id"] == 21


def test_release_manifest_groups_versioned_market_datasets_in_one_scan():
    from datetime import datetime
    from types import SimpleNamespace
    from data_collect.data_access.repository.release_repository import ReleaseRepository

    calls = []

    class ReadProbe:
        def _repository_query(self, context, source, sql, params=(), scope=None, max_rows=None):
            calls.append((source, sql, params))
            if "SELECT dataset, min_date" in sql:
                return [
                    {"dataset": "daily", "min_date": date(2026, 8, 1)},
                    {"dataset": "daily_basic", "min_date": date(2026, 8, 1)},
                    {"dataset": "adj_factor", "min_date": date(2026, 8, 1)},
                ]
            return [
                {"dataset": dataset, "count": 3, "max_record_id": index,
                 "max_fetched_at": datetime(2026, 8, 21, 12, 0)}
                for index, dataset in enumerate(("daily", "daily_basic", "adj_factor"), 1)
            ]

    repository = ReleaseRepository(SimpleNamespace(read=ReadProbe()))
    result = repository._market_versions_manifest(date(2026, 8, 21), None)

    assert len(calls) == 2  # one grouped minimum query + one grouped aggregate
    assert all(item["row_count"] == 3 for item in result.values())
    assert all(item["max_record_id"] in {1, 2, 3} for item in result.values())
    assert all(item["max_fetched_at"] == "2026-08-21T12:00:00" for item in result.values())
    shared_sql = calls[1][1]
    minimum_sql = calls[0][1]
    assert "MIN(CASE" not in minimum_sql
    assert minimum_sql.count("SELECT MIN(trade_date)") == 3
    assert "trade_date BETWEEN %s AND %s" in shared_sql
    assert "cal_date" not in shared_sql
    assert " OR " not in shared_sql


def test_market_activity_release_gate_is_scoped_to_market_inputs():
    from types import SimpleNamespace
    from data_collect.data_access.repository.release_repository import ReleaseRepository

    calls = []

    class ReadProbe:
        def _repository_query(self, context, source, sql, params=(), scope=None, max_rows=None):
            calls.append(source)
            if source == "ops.market_data_quality_gate":
                return [{
                    "dataset": "daily",
                    "trade_date": date(2026, 9, 10),
                    "actual_rows": 5549,
                    "expected_rows": 5549,
                    "status": "PASS",
                    "batch_id": "batch-1",
                    "reason": "coverage within threshold",
                    "created_at": None,
                }]
            if source == "std.daily_kline":
                return [{"row_count": 5549, "stock_count": 5549, "invalid_count": 0}]
            raise AssertionError(f"unexpected source: {source}")

    repository = ReleaseRepository(SimpleNamespace(read=ReadProbe()))
    result = repository.market_activity_quality_gate(date(2026, 9, 10), None)

    assert result["failures"] == {}
    assert result["standard_daily_kline"]["stock_count"] == 5549
    assert calls == ["ops.market_data_quality_gate", "std.daily_kline"]


def test_latest_daily_gate_query_is_bounded_and_returns_one_row():
    from types import SimpleNamespace
    from datetime import timedelta
    from data_collect.data_access.repository.market_coverage_repository import (
        MarketCoverageRepository,
    )

    calls = []

    class ReadProbe:
        def _repository_query(self, context, source, sql, params=(), scope=None, max_rows=None):
            calls.append((source, sql, params, scope, max_rows))
            return [{"trade_date": date(2026, 9, 25), "status": "PASS"}]

    end_date = date(2026, 9, 27)
    repository = MarketCoverageRepository(SimpleNamespace(read=ReadProbe()))
    result = repository.latest_daily_gate(end_date, 31, ctx())

    assert result["status"] == "PASS"
    source, sql, params, scope, max_rows = calls[0]
    assert source == "ops.market_data_quality_gate"
    assert "trade_date BETWEEN %s AND %s" in sql
    assert "ORDER BY trade_date DESC, created_at DESC, gate_id DESC" in sql
    assert "LIMIT 1" in sql
    assert params == (end_date - timedelta(days=30), end_date)
    assert (scope.date_from, scope.date_to) == params
    assert max_rows == 1


@pytest.mark.parametrize("lookback_days", [0, 32])
def test_latest_daily_gate_rejects_unbounded_lookback(lookback_days):
    from types import SimpleNamespace
    from data_collect.data_access.repository.market_coverage_repository import (
        MarketCoverageRepository,
    )

    repository = MarketCoverageRepository(SimpleNamespace(read=SimpleNamespace()))
    with pytest.raises(ValueError, match="1..31"):
        repository.latest_daily_gate(date(2026, 9, 27), lookback_days, ctx())


def test_market_activity_source_manifest_hashes_exact_calculation_inputs():
    from datetime import timedelta
    from data_collect.market_activity.contracts import MarketActivityRequest
    from data_collect.market_activity.metrics import build_daily_report

    cutoff = date(2026, 9, 10)
    bars = []
    for offset in range(20, 0, -1):
        bars.append({
            "stock_code": "600000.SH", "trade_date": cutoff - timedelta(days=offset),
            "close": 10.0, "volume": 100.0, "amount": 1000.0,
        })
    bars.append({
        "stock_code": "600000.SH", "trade_date": cutoff,
        "close": 11.0, "volume": 200.0, "amount": 2500.0,
    })
    report = build_daily_report([bars], MarketActivityRequest(trade_date=cutoff))

    manifest, manifest_hash = ReleaseRepository(None).market_activity_source_manifest(
        cutoff, report
    )

    assert manifest["schema_version"] == "market-activity-input-v2"
    assert manifest["input_window"] == {
        "source": "std.daily_kline",
        "date_from": report.lookback_start.isoformat(),
        "date_to": cutoff.isoformat(),
        "row_count": report.input_row_count,
    }
    assert manifest["report"]["input_manifest_hash"] == report.input_manifest_hash
    assert manifest["report"]["activity_hash"] == report.activity_hash
    assert len(manifest_hash) == 64


def test_market_activity_full_release_audit_uses_one_hard_limited_input_stream():
    from datetime import timedelta
    from types import SimpleNamespace
    from data_collect.market_activity.contracts import MarketActivityRequest
    from data_collect.market_activity.metrics import build_daily_report

    cutoff = date(2026, 9, 10)
    bars = []
    for offset in range(20, 0, -1):
        bars.append({
            "stock_code": "600000.SH", "trade_date": cutoff - timedelta(days=offset),
            "close": 10.0, "volume": 100.0, "amount": 1000.0,
        })
    bars.append({
        "stock_code": "600000.SH", "trade_date": cutoff,
        "close": 11.0, "volume": 200.0, "amount": 2500.0,
    })
    expected_report = build_daily_report([bars], MarketActivityRequest(trade_date=cutoff))
    reads = []
    streams = []

    class ReadProbe:
        def _repository_query(self, context, source, sql, params=(), scope=None, max_rows=None):
            reads.append((source, sql, params, max_rows))
            if source == "ops.tushare_data_release":
                return [{
                    "baseline_domain": "MARKET_ACTIVITY",
                    "baseline_cutoff_date": cutoff,
                    "schema_version": "market-activity-v1",
                    "data_cutoff_timestamp": None,
                    "temporal_evidence_level": "WAREHOUSE_OBSERVED",
                    "visibility_policy_version": "business-pit-v2",
                }]
            if source == "ops.market_data_quality_gate":
                return [{
                    "dataset": "daily", "trade_date": cutoff,
                    "actual_rows": 1, "expected_rows": 1, "status": "PASS",
                    "batch_id": "batch-1", "reason": "ok", "created_at": None,
                }]
            if source == "std.daily_kline":
                return [{"row_count": 1, "stock_count": 1, "invalid_count": 0}]
            raise AssertionError(f"unexpected fact/control query: {source}")

        def _repository_stream(self, **kwargs):
            streams.append(kwargs)
            return iter([bars])

    kernel = SimpleNamespace(
        read=ReadProbe(),
        write=SimpleNamespace(begin_release_validation_boundary=lambda *args: 77),
    )
    result = ReleaseRepository(kernel).validate("market-release", "", ctx())

    assert result["release_change_watermark"] == 77
    assert result["gate"]["market_activity_report"]["activity_hash"] == expected_report.activity_hash
    assert result["source_manifest"]["report"]["input_manifest_hash"] == expected_report.input_manifest_hash
    assert len(streams) == 1
    assert streams[0]["source"] == "std.daily_kline"
    assert "LIMIT %s" in streams[0]["sql"]
    assert streams[0]["params"] == (cutoff - timedelta(days=45), cutoff, 250_001)
    assert all("raw" not in source and "tushare_market_raw_version" not in sql
               for source, sql, _, _ in reads)


def test_market_activity_release_audit_stops_before_lookback_stream_when_daily_gate_fails():
    import pytest
    from types import SimpleNamespace

    cutoff = date(2026, 9, 10)
    streams = []

    class ReadProbe:
        def _repository_query(self, context, source, sql, params=(), scope=None, max_rows=None):
            if source == "ops.tushare_data_release":
                return [{
                    "baseline_domain": "MARKET_ACTIVITY",
                    "baseline_cutoff_date": cutoff,
                    "schema_version": "market-activity-v1",
                    "data_cutoff_timestamp": None,
                    "temporal_evidence_level": "WAREHOUSE_OBSERVED",
                    "visibility_policy_version": "business-pit-v2",
                }]
            if source == "ops.market_data_quality_gate":
                return [{
                    "dataset": "daily", "trade_date": cutoff,
                    "actual_rows": 0, "expected_rows": 0, "status": "BLOCKED",
                    "batch_id": "batch-1", "reason": "empty source", "created_at": None,
                }]
            if source == "std.daily_kline":
                return [{"row_count": 0, "stock_count": 0, "invalid_count": 0}]
            raise AssertionError(f"unexpected source: {source}")

        def _repository_stream(self, **kwargs):
            streams.append(kwargs)
            return iter(())

    kernel = SimpleNamespace(
        read=ReadProbe(),
        write=SimpleNamespace(begin_release_validation_boundary=lambda *args: 77),
    )
    with pytest.raises(RuntimeError, match="release quality gate failed"):
        ReleaseRepository(kernel).validate("market-release", "unused", ctx())

    assert streams == []


def test_feature_repository_rebuild_price_v2_scoped_merge():
    """rebuild_price_v2 迁移：分批 scoped merge（DELETE + INSERT...ON CONFLICT），无 TRUNCATE。"""
    from data_collect.data_access.repository.feature_repository import FeatureRepository

    repo, kernel, conn = make_kernel_and_repo_feature()
    scope = WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 31))
    repo.rebuild_price_v2(scope, ctx(), codes=["600000.SH", "600001.SH"])
    sqls = [e["sql"] for e in conn.executed]
    assert any("DELETE FROM feature.price_daily_v2" in s for s in sqls)
    assert any("INSERT INTO feature.price_adjustment_factor_asof" in s for s in sqls)
    assert any("INSERT INTO feature.price_daily_v2" in s for s in sqls)
    assert any("INTERVAL '400 days'" in s for s in sqls)
    assert any("DISTINCT ON (d.stock_code)" in s for s in sqls)
    assert not any("TRUNCATE" in s for s in sqls)


def test_price_v2_checkpoint_identity_accepts_execution_id():
    from pathlib import Path

    source = Path("data_collect/jobs/tushare_phase2_features.py").read_text(encoding="utf-8")
    assert "execution_id=None" in source
    assert "logical_execution_id = str(execution_id or base_context.run_id)" in source
    assert "logical_execution_id," in source


def test_feature_repository_load_codes_deduplicates_before_read_ceiling(monkeypatch):
    """Large history must not truncate the stock universe at daily-row limit."""
    repo, kernel, _ = make_kernel_and_repo_feature()
    calls = []

    def fake_repository_query(**kwargs):
        calls.append(kwargs)
        return [
            {"stock_code": "600001.SH"},
            {"stock_code": "000001.SZ"},
            {"stock_code": "600001.SH"},
        ]

    monkeypatch.setattr(kernel.read, "_repository_query", fake_repository_query)
    codes = repo.load_codes_in_window(
        WorkScope(date_from=date(2016, 8, 1), date_to=date(2026, 8, 21)), ctx()
    )

    assert codes == ["000001.SZ", "600001.SH"]
    assert len(calls) == 1
    assert "SELECT DISTINCT stock_code" in calls[0]["sql"]
    assert calls[0]["params"] == (date(2016, 8, 1), date(2026, 8, 21))
    assert calls[0]["max_rows"] == 100_000


def make_kernel_and_repo_feature():
    from data_collect.data_access.repository.feature_repository import FeatureRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    return FeatureRepository(kernel), kernel, conn
