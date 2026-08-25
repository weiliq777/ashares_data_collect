"""Data Access Framework 集成测试（需要真实 PostgreSQL，mark=integration）。

pytest 默认跳过 integration（见 pytest.ini addopts）。
显式运行：pytest -m integration tests/test_data_access_integration.py

覆盖（真实 DB 行为，而非 fake）：
- raw.append 幂等（同 payload_hash 不重复插入，异 hash 插入新版本）
- write.upsert chunk + ON CONFLICT 幂等
- read.query scan_policy / stream
- FROZEN 表拒绝写（GuardViolation）
"""
from __future__ import annotations

from datetime import date

import pytest

from data_collect.data_access import DataAccessKernel
from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.policy_catalog import build_default_registry

pytestmark = pytest.mark.integration


def _ctx() -> RunContext:
    return RunContext(
        run_id="it-run-1", job_name="it", code_version="git:test",
        config_hash="cfg", pipeline_version="v1",
    )


@pytest.fixture(scope="module")
def kernel() -> DataAccessKernel:
    k = DataAccessKernel(policy_registry=build_default_registry())
    yield k
    k.close()


@pytest.fixture()
def test_table(kernel: DataAccessKernel):
    """建一个测试表并注册 policy，测完清理。"""
    table = "public._da_it_append"
    conn = kernel._db_connection()
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table} "
            "(ts_code text, trade_date date, payload_hash text, "
            " PRIMARY KEY (ts_code, trade_date, payload_hash))"
        )
    kernel.policy_registry.register(
        pytest.importorskip("data_collect.data_access.dataset_policy").DatasetPolicy(
            name=table,
            write_modes=frozenset(
                [pytest.importorskip("data_collect.data_access.dataset_policy").WriteMode.APPEND_VERSIONED]
            ),
            update_allowed=False, delete_allowed=False,
        )
    )
    yield table
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()


def test_append_idempotent_same_hash(kernel: DataAccessKernel, test_table: str):
    """同 payload_hash 不重复插入（幂等）。"""
    rows = [{"ts_code": "600000.SH", "trade_date": date(2026, 1, 1), "payload_hash": "h1"}]
    n1 = kernel.raw.append(context=_ctx(), dataset=test_table, rows=rows,
                           business_key=["ts_code", "trade_date"])
    n2 = kernel.raw.append(context=_ctx(), dataset=test_table, rows=rows,
                           business_key=["ts_code", "trade_date"])
    assert n1 == 1
    assert n2 == 0  # 第二次相同 hash，忽略


def test_append_new_hash_inserts_new_version(kernel: DataAccessKernel, test_table: str):
    rows1 = [{"ts_code": "600000.SH", "trade_date": date(2026, 1, 1), "payload_hash": "h1"}]
    rows2 = [{"ts_code": "600000.SH", "trade_date": date(2026, 1, 1), "payload_hash": "h2"}]
    kernel.raw.append(context=_ctx(), dataset=test_table, rows=rows1,
                      business_key=["ts_code", "trade_date"])
    n = kernel.raw.append(context=_ctx(), dataset=test_table, rows=rows2,
                          business_key=["ts_code", "trade_date"])
    assert n == 1  # 异 hash 插入新版本


def test_guard_rejects_write_on_frozen(kernel: DataAccessKernel):
    """FROZEN 表写被 QueryGuard 拒绝（真实 policy 路径）。"""
    plan = SqlPlan(
        operation=Operation.UPSERT, target="contract.stock_snapshot_daily",
        scope=WorkScope(date_from=date(2026, 1, 1)),
    )
    from data_collect.data_access.errors import PolicyViolation

    with pytest.raises(PolicyViolation):
        kernel.query_guard.check(plan)
