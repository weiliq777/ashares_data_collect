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


def test_security_repository_requires_scope_dates():
    repo, _, _ = make_kernel_and_repo()
    with pytest.raises(ValueError):
        repo.rebuild_status_daily(WorkScope(), ctx())  # 无日期范围


def test_security_repository_executes_set_based_sql():
    repo, kernel, conn = make_kernel_and_repo()
    scope = WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 31))
    repo.rebuild_status_daily(scope, ctx())

    sqls = [e["sql"] for e in conn.executed]
    # SET-based 重建：scoped DELETE + INSERT...ON CONFLICT
    assert any("DELETE FROM std.security_status_daily" in s for s in sqls)
    assert any("ON CONFLICT" in s for s in sqls)
    # 提交了一次（单事务）
    assert conn.committed == 1


def test_security_repository_passes_params():
    repo, kernel, conn = make_kernel_and_repo()
    scope = WorkScope(date_from=date(2026, 1, 1), date_to=date(2026, 1, 31))
    repo.rebuild_status_daily(scope, ctx())

    # params 应为 6 个（DELETE 2 + dates 2 + limits 2）
    executed_with_params = [e for e in conn.executed if e["params"]]
    assert executed_with_params
    params = executed_with_params[0]["params"]
    assert params == (
        date(2026, 1, 1), date(2026, 1, 31),
        date(2026, 1, 1), date(2026, 1, 31),
        date(2026, 1, 1), date(2026, 1, 31),
    )


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
    assert conn.committed == 2


def test_security_repository_lifecycle_uses_staging_swap_not_truncate():
    """rebuild_lifecycle 用 staging+swap（TABLE_REPLACE），消除 TRUNCATE。"""
    repo, kernel, conn = make_kernel_and_repo()
    repo.rebuild_lifecycle(ctx())
    sqls = [e["sql"] for e in conn.executed]
    # 建 staging（带 schema）+ fill + swap
    assert any("CREATE TABLE std._staging_" in s for s in sqls)
    # PostgreSQL 的 RENAME TO 右侧必须是未限定的表名；swap 期间旧表仍在，
    # 因而不会出现 DROP target 后再 RENAME 的短暂 relation 缺失窗口。
    assert any(
        "ALTER TABLE std.security_lifecycle RENAME TO _old_security_lifecycle_" in s
        for s in sqls
    )
    assert any(
        "ALTER TABLE std._staging_" in s and "RENAME TO security_lifecycle" in s
        for s in sqls
    )
    assert any("DROP TABLE std._old_security_lifecycle_" in s for s in sqls)
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


def test_financial_vip_page_queue_lookup_is_bounded_to_page_hashes():
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
    repository.write_versions_and_queue("income", records, "batch-1", ctx())
    lookup = next(
        entry for entry in conn.executed
        if entry["kind"] == "execute" and "SELECT record_id" in entry["sql"]
    )
    assert "WHERE batch_id=%s" in lookup["sql"]
    assert "payload_hash = ANY(%s)" in lookup["sql"]
    assert "AND dataset=%s" in lookup["sql"]
    assert lookup["params"][0] == "batch-1"
    assert len(lookup["params"][1]) == 1


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

    assert len(repository.calls) == 1
    _, sql, params, partition = repository.calls[0]
    assert "trade_date <= %s" in sql
    assert "BETWEEN" not in sql
    assert params == (cutoff, "daily")
    assert partition.endswith(":through:2026-08-21")
    assert result == {
        "row_count": 7, "max_record_id": 9,
        "max_fetched_at": "2026-08-21T12:00:00",
    }


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


def make_kernel_and_repo_feature():
    from data_collect.data_access.repository.feature_repository import FeatureRepository

    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn
    )
    return FeatureRepository(kernel), kernel, conn
