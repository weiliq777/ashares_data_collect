from datetime import date, datetime
from pathlib import Path

import pytest

from data_collect.data_access import DataAccessKernel, WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.data_access.repository.financial_standard_repository import FinancialStandardRepository
from tests._fake_db import FakeConnection


ROOT = Path(__file__).resolve().parents[1]


def _context():
    from data_collect.data_access import RunContext

    return RunContext("financial-contract-test", "test", "test", "test", "test")


def test_financial_standard_policies_match_deployed_idempotency_keys():
    registry = build_default_registry()
    statement_key = ("source", "source_record_key", "record_version")
    for target in (
        "std.financial_income_standard",
        "std.financial_balance_sheet_standard",
        "std.financial_cash_flow_standard",
    ):
        assert registry.require(target).primary_key == statement_key
    assert registry.require("std.fina_indicator_standard").primary_key == (
        "source", "source_record_key", "record_version", "payload_hash"
    )
    assert registry.require("std.stock_financial_indicator").primary_key == (
        "stock_code", "report_date", "announce_date", "source"
    )


def test_financial_standard_and_compatibility_upserts_pass_kernel_policy_check():
    connection = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: connection
    )
    context = _context()
    statement = FinancialStandardRepository(kernel)
    statement.upsert_standard_rows(
        "std.financial_income_standard",
        [{
            "source": "tushare", "source_record_key": "000001.SZ:2025-12-31:1:1:4",
            "record_version": 10, "stock_code": "000001.SZ",
        }],
        context,
    )
    FinancialRepository(kernel).upsert_legacy_indicator(
        [{
            "stock_code": "000001.SZ", "report_date": "2025-12-31",
            "announce_date": "2026-03-31", "source": "tushare", "roe": 12.0,
        }],
        ["000001.SZ"],
        context,
    )
    assert any("std.financial_income_standard" in item["sql"] for item in connection.executed)
    assert any("std.stock_financial_indicator" in item["sql"] for item in connection.executed)


def test_financial_standard_repository_reads_exact_bounded_raw_versions(monkeypatch):
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=FakeConnection
    )
    repository = FinancialStandardRepository(kernel)
    captured = {}
    monkeypatch.setattr(
        kernel.read,
        "_repository_query",
        lambda *args: captured.update(args=args) or [{
            "record_id": 123,
            "logical_business_key": "000001.SZ:20210630:1:1:2",
            "payload": {"ts_code": "000001.SZ", "end_date": "20210630"},
            "payload_hash": "a" * 64,
            "fetched_at": datetime(2021, 8, 1),
            "ambiguous_same_day_revision": False,
        }],
    )

    rows = repository.raw_versions_by_ids(
        "income", [123, 123], _context(), date(2021, 6, 30), date(2021, 6, 30),
    )

    assert len(rows) == 1
    assert rows[0]["_record_id"] == 123
    assert rows[0]["_payload_hash"] == "a" * 64
    assert rows[0]["_ambiguous_same_day_revision"] is False
    assert captured["args"][1] == "tushare_financial_raw_version"
    assert captured["args"][3] == ("income", [123])
    assert "r.record_id=ANY(%s)" in captured["args"][2]
    assert "other.payload_hash <> r.payload_hash" in captured["args"][2]
    assert captured["args"][4].date_from == date(2021, 6, 30)
    assert captured["args"][4].date_to == date(2021, 6, 30)
    assert captured["args"][5] == 200
    kernel.close()


def test_financial_standard_repository_rejects_unbounded_raw_lookup():
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=FakeConnection
    )
    repository = FinancialStandardRepository(kernel)
    with pytest.raises(ValueError):
        repository.raw_versions_by_ids(
            "fina_indicator", [123], _context(), date(2021, 6, 30), date(2021, 6, 30),
        )
    with pytest.raises(ValueError):
        repository.raw_versions_by_ids(
            "income", list(range(1, 202)), _context(), date(2021, 6, 30), date(2021, 6, 30),
        )
    with pytest.raises(ValueError):
        repository.raw_versions_by_ids(
            "income", [0], _context(), date(2021, 6, 30), date(2021, 6, 30),
        )
    with pytest.raises(ValueError):
        repository.raw_versions_by_ids(
            "income", [123], _context(), None, None,
        )
    kernel.close()


def test_fina_indicator_same_day_revision_is_not_dropped():
    source = (ROOT / "data_collect" / "data_access" / "repository" / "fina_indicator_repository.py").read_text(encoding="utf-8")
    assert "WHEN ambiguous_same_day_revision THEN 'WARNING'" in source
    assert "ON CONFLICT (source, source_record_key, record_version, payload_hash)" in source


def test_financial_writers_and_migrations_share_one_lock_domain():
    migration = (ROOT / "data_collect" / "jobs" / "migrate_financial_standard.py").read_text(encoding="utf-8")
    compatibility = (ROOT / "data_collect" / "jobs" / "migrate_stock_financial_indicator.py").read_text(encoding="utf-8")
    repository = (ROOT / "data_collect" / "data_access" / "repository" / "financial_repository.py").read_text(encoding="utf-8")
    assert 'session_exclusive("warehouse-write", WAREHOUSE_WRITE_LOCK)' in migration
    assert 'session_exclusive("warehouse-write", WAREHOUSE_WRITE_LOCK)' in compatibility
    assert 'session_exclusive("warehouse-write", "foundation-rebuild")' in repository
