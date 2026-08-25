from __future__ import annotations

from datetime import date

from data_collect.data_access import DataAccessKernel
from data_collect.data_access.context import RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.foundation_contract_repository import FoundationContractRepository
from tests._fake_db import FakeConnection


def ctx() -> RunContext:
    return RunContext(
        run_id="stage1-run",
        job_name="stage1-contract",
        code_version="git:test",
        config_hash="cfg",
        pipeline_version="foundation-contract-v1",
    )


def make_repo():
    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(),
        connection_factory=lambda: conn,
    )
    return FoundationContractRepository(kernel), conn


def test_contract_metadata_uses_publish_gateway_and_is_idempotent():
    repo, conn = make_repo()
    repo.publish_contract_version(ctx(), "foundation-contract-test-v1.0.0")
    repo.publish_data_dictionary(ctx(), "foundation-contract-test-v1.0.0", [{
        "schema_name": "feature",
        "table_name": "price_daily_v2",
        "column_name": "ma20",
        "business_name": "20日均线",
        "definition": "最近20个交易日复权收盘价均值",
        "source": "feature pipeline",
    }])

    inserts = [item for item in conn.executed if item["kind"] == "executemany"]
    assert len(inserts) == 2
    assert all("ON CONFLICT DO NOTHING" in item["sql"] for item in inserts)
    assert conn.committed == 2


def test_trading_day_build_is_keyed_upsert_not_unbounded_sql():
    repo, conn = make_repo()
    repo.record_trading_day_build(
        ctx(), trade_date=date(2026, 8, 18), pipeline_version="v1",
        run_id="run-1", final_status="PARTIAL",
    )
    sql = next(
        item["sql"] for item in conn.executed
        if item["kind"] == "execute"
        and item["sql"].startswith("INSERT INTO ops.trading_day_build")
    )
    assert "INSERT INTO ops.trading_day_build" in sql
    assert "ON CONFLICT (trade_date, pipeline_version) DO UPDATE" in sql
    assert "SELECT *" not in sql
    assert conn.committed == 1


def test_repository_has_no_public_sql_escape_hatch():
    assert not hasattr(FoundationContractRepository, "execute")
    assert not hasattr(FoundationContractRepository, "get_connection")
