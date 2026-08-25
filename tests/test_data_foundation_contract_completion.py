from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from data_collect.data_access import DataAccessKernel
from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.quality_gateway import QualityGateway, QualityViolation
from data_collect.data_access.repository.contract_product_repository import (
    ContractProductRepository,
)
from data_collect.data_access.repository.foundation_metadata_repository import (
    FoundationMetadataRepository,
)
from tests._fake_db import FakeConnection


def ctx() -> RunContext:
    return RunContext(
        run_id="foundation-completion-run",
        job_name="foundation-completion",
        code_version="git:test",
        config_hash="config-hash",
        pipeline_version="foundation-v2",
    )


def make_kernel(connection=None):
    connection = connection or FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(),
        connection_factory=lambda: connection,
    )
    return kernel, connection


def last_staging_write(connection):
    return next(
        entry for entry in reversed(connection.executed)
        if entry["kind"] == "executemany" and "_staging_" in entry["sql"]
    )


def last_publish(connection, target):
    return next(
        entry for entry in reversed(connection.executed)
        if entry["kind"] == "execute" and entry["sql"].startswith(f"INSERT INTO {target}")
    )


def source_values() -> dict:
    return {
        "source_id": "tushare.daily",
        "provider": "tushare",
        "api_name": "daily",
        "dataset_name": "stock_daily",
        "business_key": ["ts_code", "trade_date"],
        "frequency": "daily",
        "supports_incremental": True,
        "supports_backfill": True,
        "available_time_rule": "T+1 08:00",
        "rate_limit": {"calls_per_minute": 200, "rows_per_call": 5000},
        "enabled": True,
    }


def feature_values() -> dict:
    return {
        "feature_name": "price_daily_v2",
        "feature_version": "2.0.0",
        "business_name": "daily price features",
        "definition": "point-in-time daily price and return features",
        "grain": "ts_code/research_date",
        "source_datasets": ["stock_daily", "adj_factor"],
        "dependency_features": ["price_daily_v1"],
        "lookback_rule": "120 trading days",
        "asof_rule": "available by research_date",
        "adjustment_rule": "forward-adjusted only",
        "impact_rule": {"change_types": ["CORRECTION"], "rebuild": "date"},
        "quality_rule": {"missing_source": "BLOCK"},
        "code_version": "git:test",
        "status": "ACTIVE",
    }


def snapshot_values(status: str = "DRAFT") -> dict:
    return {
        "snapshot_id": "snapshot-1",
        "research_date": date(2026, 8, 19),
        "data_baseline": "baseline-1",
        "contract_version": "foundation-contract-v2",
        "feature_version": "price-v2",
        "pit_version": "pit-v2",
        "code_version": "git:test",
        "status": status,
    }


def dataset_values(status: str = "DRAFT") -> dict:
    return {
        "dataset_id": "dataset-1",
        "start_date": date(2026, 8, 1),
        "end_date": date(2026, 8, 19),
        "trading_dates": [date(2026, 8, 19)],
        "snapshot_ids": ["snapshot-1"],
        "feature_version": "price-v2",
        "pit_version": "pit-v2",
        "data_baseline": "baseline-1",
        "contract_version": "foundation-contract-v2",
        "status": status,
    }


def test_foundation_registry_requires_exact_fields_and_serializes_json():
    kernel, conn = make_kernel()
    repo = FoundationMetadataRepository(kernel)

    assert repo.register_source(ctx(), source_values()) == 1
    source_insert = last_staging_write(conn)
    assert "_staging_" in source_insert["sql"]
    source_row = source_insert["params"][0]
    assert source_row[4] == ["ts_code", "trade_date"]
    assert json.loads(source_row[9]) == source_values()["rate_limit"]

    assert repo.register_feature_version(ctx(), feature_values()) == 1
    feature_insert = last_staging_write(conn)
    feature_publish = last_publish(conn, "feature.feature_registry")
    assert "ON CONFLICT DO NOTHING" in feature_publish["sql"]
    feature_row = feature_insert["params"][0]
    assert feature_row[5] == ["stock_daily", "adj_factor"]
    assert json.loads(feature_row[10]) == feature_values()["impact_rule"]
    assert json.loads(feature_row[11]) == feature_values()["quality_rule"]

    with pytest.raises(ValueError, match="source registry fields mismatch"):
        repo.register_source(ctx(), {**source_values(), "unexpected": True})
    incomplete_feature = feature_values()
    incomplete_feature.pop("quality_rule")
    with pytest.raises(ValueError, match="feature registry fields mismatch"):
        repo.register_feature_version(ctx(), incomplete_feature)


def test_change_events_validate_fields_and_impact_resolver_is_active_registry_query():
    kernel, conn = make_kernel()
    repo = FoundationMetadataRepository(kernel)
    scope = WorkScope(
        date_from=date(2026, 8, 1),
        date_to=date(2026, 8, 19),
        codes=("000001.SZ",),
        partition="daily",
    )
    event = {
        "event_id": "event-1",
        "dataset": "stock_daily",
        "business_key": "000001.SZ|2026-08-19",
        "ts_code": "000001.SZ",
        "effective_date": date(2026, 8, 19),
        "old_hash": "a" * 64,
        "new_hash": "b" * 64,
        "source_run_id": ctx().run_id,
        "change_type": "CORRECTION",
        "status": "PENDING",
    }

    assert repo.record_change_events(ctx(), scope, []) == 0
    assert repo.record_change_events(ctx(), scope, [event]) == 1
    assert "INSERT INTO ops.change_event" in last_publish(conn, "ops.change_event")["sql"]

    pending = repo.pending_change_events(ctx(), scope, limit=25)
    assert pending == []
    pending_sql = conn.executed[-1]
    assert "status='PENDING'" in pending_sql["sql"]
    assert "effective_date >= %s" in pending_sql["sql"]
    assert "ts_code = ANY(%s)" in pending_sql["sql"]
    assert pending_sql["params"][-1] == 25

    impacts = repo.resolve_impacts(
        ctx(), dataset="stock_daily", ts_code="000001.SZ", effective_date=date(2026, 8, 19)
    )
    assert impacts == []
    impact_sql = conn.executed[-1]
    assert "FROM feature.feature_registry" in impact_sql["sql"]
    assert "ANY(source_datasets)" in impact_sql["sql"]
    assert "status='ACTIVE'" in impact_sql["sql"]

    bad_event = dict(event)
    bad_event.pop("new_hash")
    with pytest.raises(ValueError, match="change event fields mismatch"):
        repo.record_change_events(ctx(), scope, [bad_event])
    with pytest.raises(ValueError, match="limit must be in 1..10000"):
        repo.pending_change_events(ctx(), scope, limit=0)


def test_run_manifest_requires_start_and_status_and_upserts_context_scope_metadata():
    kernel, conn = make_kernel()
    repo = FoundationMetadataRepository(kernel)
    scope = WorkScope(
        date_from=date(2026, 8, 1),
        date_to=date(2026, 8, 19),
        codes=("000001.SZ", "600000.SH"),
        partition="foundation",
        dimensions={"market": "A"},
    )
    start = datetime(2026, 8, 20, 1, 2, tzinfo=timezone.utc)

    written = repo.save_run_manifest(
        ctx(),
        scope,
        {
            "status": "SUCCEEDED",
            "start_time": start,
            "end_time": start,
            "input_manifest_hash": "i" * 64,
            "output_manifest_hash": "o" * 64,
            "rows_read": 10,
            "rows_written": 9,
            "rows_rejected": 1,
        },
    )
    assert written == 1
    manifest_publish = last_publish(conn, "ops.run_manifest")
    assert "INSERT INTO ops.run_manifest" in manifest_publish["sql"]
    manifest_insert = last_staging_write(conn)
    columns = [
        "run_id", "job_name", "pipeline_version", "code_commit", "config_hash",
        "scope", "input_manifest_hash", "output_manifest_hash", "start_time",
        "end_time", "rows_read", "rows_written", "rows_rejected", "status", "error",
    ]
    row = dict(zip(columns, manifest_insert["params"][0], strict=True))
    assert row["run_id"] == ctx().run_id
    assert row["job_name"] == ctx().job_name
    assert row["code_commit"] == ctx().code_version
    assert json.loads(row["scope"]) == {
        "codes": ["000001.SZ", "600000.SH"],
        "date_from": "2026-08-01",
        "date_to": "2026-08-19",
        "dimensions": {"market": "A"},
        "partition": "foundation",
    }

    with pytest.raises(ValueError, match="requires status and start_time"):
        repo.save_run_manifest(ctx(), scope, {"status": "RUNNING"})
    with pytest.raises(ValueError, match="unsupported run manifest fields"):
        repo.save_run_manifest(
            ctx(), scope, {"status": "RUNNING", "start_time": start, "not_allowed": 1}
        )


def test_contract_products_only_create_complete_draft_rows():
    kernel, conn = make_kernel()
    repo = ContractProductRepository(kernel)

    assert repo.create_snapshot(ctx(), snapshot_values()) == 1
    snapshot_publish = last_publish(conn, "contract.snapshot_manifest")
    assert "ON CONFLICT (snapshot_id) DO NOTHING" in snapshot_publish["sql"]

    assert repo.create_dataset_release(ctx(), dataset_values()) == 1
    assert "contract.dataset_release" in last_publish(conn, "contract.dataset_release")["sql"]

    with pytest.raises(ValueError, match="complete DRAFT"):
        repo.create_snapshot(ctx(), snapshot_values(status="BUILDING"))
    incomplete = dataset_values()
    incomplete.pop("pit_version")
    with pytest.raises(ValueError, match="complete DRAFT"):
        repo.create_dataset_release(ctx(), incomplete)


def test_contract_frozen_transitions_require_hashes_and_reject_invalid_state_changes():
    kernel, conn = make_kernel()
    repo = ContractProductRepository(kernel)
    snap_scope = WorkScope(
        date_from=date(2026, 8, 19), date_to=date(2026, 8, 19), partition="snapshot-1"
    )
    dataset_scope = WorkScope(
        date_from=date(2026, 8, 1), date_to=date(2026, 8, 19), partition="dataset-1"
    )

    with pytest.raises(ValueError, match="FROZEN requires fields"):
        repo.transition_snapshot(
            ctx(), "snapshot-1", date(2026, 8, 19), ("VALIDATED",), "FROZEN",
            {"manifest_hash": "m", "quality_report_hash": "q", "frozen_at": "now"},
        )
    with pytest.raises(ValueError, match="FROZEN requires fields"):
        repo.transition_dataset_release(
            ctx(), "dataset-1", dataset_scope, ("VALIDATED",), "FROZEN", {}
        )
    with pytest.raises(ValueError, match="invalid contract transition"):
        repo.transition_snapshot(
            ctx(), "snapshot-1", date(2026, 8, 19), ("DRAFT",), "FROZEN", {}
        )

    assert repo.transition_snapshot(
        ctx(), "snapshot-1", date(2026, 8, 19), ("VALIDATED",), "FROZEN",
        {
            "input_manifest_hash": "i",
            "manifest_hash": "m",
            "quality_report_hash": "q",
            "frozen_at": "now",
        },
    ) == 0
    assert repo.transition_dataset_release(
        ctx(), "dataset-1", dataset_scope, ("VALIDATED",), "FROZEN",
        {"manifest_hash": "m", "quality_report_hash": "q", "frozen_at": "now"},
    ) == 0
    assert "status <> 'FROZEN'" in conn.executed[-1]["sql"]
    assert "status=ANY(%s::text[])" in conn.executed[-1]["sql"]


def test_sql_058_declares_foundation_contract_tables_checks_and_frozen_triggers():
    sql = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "058_data_foundation_contract_completion.sql"
    ).read_text(encoding="utf-8")

    for table in (
        "feature.feature_registry",
        "ops.source_registry",
        "ops.change_event",
        "ops.run_manifest",
        "contract.snapshot_manifest",
        "contract.stock_snapshot_daily",
        "contract.dataset_release",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
    assert "CHECK (status <> 'FROZEN' OR" in sql
    assert "CREATE OR REPLACE FUNCTION contract.guard_frozen_product()" in sql
    assert "CREATE TRIGGER trg_contract_snapshot_frozen" in sql
    assert "CREATE TRIGGER trg_contract_dataset_frozen" in sql
    assert "CREATE TRIGGER trg_contract_snapshot_rows" in sql
    assert "FROZEN contract product is immutable" in sql
    assert "invalid contract product state transition" in sql


class _QualityCursor:
    def __init__(self, conn: "_QualityConnection") -> None:
        self.conn = conn
        self.rowcount = 0
        self._row = None

    def execute(self, sql, params=None):
        self.conn.executed.append({"kind": "execute", "sql": sql, "params": params})
        staged_rowcount = self.rowcount
        self.rowcount = 0
        if sql.startswith("SET LOCAL statement_timeout"):
            self._row = None
        elif sql.startswith("CREATE TEMP TABLE _staging_"):
            self._row = None
        elif sql.startswith("INSERT INTO ops.data_quality_result") and " FROM _staging_" in sql:
            self.rowcount = staged_rowcount
            self._row = None
        elif sql == "SELECT COUNT(*) FROM contract.stock_snapshot_daily":
            self._row = (self.conn.row_count,)
        elif "HAVING COUNT(*) > 1" in sql:
            self._row = (self.conn.duplicate_groups,)
        elif "snapshot_id IS NULL OR research_date IS NULL OR ts_code IS NULL" in sql:
            self._row = (self.conn.null_key_count,)
        elif "to_jsonb(t)->>'version_available_date'" in sql:
            self._row = (self.conn.pit_violations,)
        elif "research_date < %s" in sql or "research_date > %s" in sql:
            self._row = (self.conn.outside_scope_rows,)
        elif sql == "SELECT MAX(research_date) FROM contract.stock_snapshot_daily":
            self._row = (self.conn.max_observed,)
        else:  # pragma: no cover - protects this fake from silently accepting new SQL
            raise AssertionError(f"unexpected quality SQL: {sql}")

    def executemany(self, sql, params_list):
        self.conn.executed.append(
            {"kind": "executemany", "sql": sql, "params": list(params_list)}
        )
        self.rowcount = len(self.conn.executed[-1]["params"])

    def fetchone(self):
        return self._row

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _QualityConnection:
    supports_statement_deadline = True

    def __init__(self, *, row_count=1, pit_violations=0):
        self.row_count = row_count
        self.duplicate_groups = 0
        self.null_key_count = 0
        self.outside_scope_rows = 0
        self.max_observed = date(2026, 8, 19) if row_count else None
        self.pit_violations = pit_violations
        self.executed = []
        self.committed = 0
        self.rolled_back = 0

    def cursor(self, name=None):
        return _QualityCursor(self)

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1

    def close(self):
        pass


def quality_scope() -> WorkScope:
    return WorkScope(date_from=date(2026, 8, 19), date_to=date(2026, 8, 19))


def test_quality_gateway_returns_six_results_and_persists_report():
    connection = _QualityConnection()
    kernel, _ = make_kernel(connection)
    gateway = QualityGateway(kernel)

    gateway.validate(
        ctx(), quality_scope(),
        "contract.stock_snapshot_daily",
        "contract.stock_snapshot_daily",
    )

    persisted = [x for x in connection.executed if x["kind"] == "executemany"][0]
    checks = json.loads(persisted["params"][0][5])
    assert set(checks) == {
        "completeness", "freshness", "uniqueness", "validity", "consistency", "pit_safety"
    }
    assert len([x for x in connection.executed if x["kind"] == "executemany"]) == 1
    assert "INSERT INTO ops.data_quality_result" in last_publish(
        connection, "ops.data_quality_result"
    )["sql"]
    assert persisted["params"][0][-2] == 0
    assert len(persisted["params"][0][-1]) == 64


def test_quality_gateway_blocks_empty_table_and_pit_violation_and_persists_each_result():
    for connection, expected_name in (
        (_QualityConnection(row_count=0), "completeness"),
        (_QualityConnection(pit_violations=1), "pit_safety"),
    ):
        kernel, _ = make_kernel(connection)
        gateway = QualityGateway(kernel)
        with pytest.raises(QualityViolation) as exc_info:
            gateway.validate(
                ctx(), quality_scope(),
                "contract.stock_snapshot_daily",
                "contract.stock_snapshot_daily",
            )
        assert expected_name in exc_info.value.blocks
        persisted = [x for x in connection.executed if x["kind"] == "executemany"]
        assert len(persisted) == 1
        assert persisted[0]["params"][0][-2] == 1
