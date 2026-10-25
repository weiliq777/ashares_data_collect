from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_projection_repository import (
    FinancialProjectionRepository,
    FinancialProjectionSelection,
    projection_row_hash,
)
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.jobs.tushare_foundation_features import (
    _lineage,
    calculate_debt_to_assets_features,
)
from tests._fake_db import FakeConnection
from tools.replay_financial_pit_projection import normalize


def test_projection_selection_binds_version_to_manifest_hash():
    selection = FinancialProjectionSelection("projection-v2", "a" * 64)

    assert selection.projection_version == "projection-v2"
    assert selection.manifest_sha256 == "a" * 64
    with pytest.raises(ValueError):
        FinancialProjectionSelection("projection-v2", "not-a-sha")


def test_financial_projection_rows_are_append_only_policy_targets():
    registry = build_default_registry()
    for table in (
        "pit.financial_standard_projection_run",
        "pit.financial_standard_projection_batch",
        "pit.financial_standard_projection",
    ):
        policy = registry.require(table)
        assert policy.primary_key
        assert policy.update_allowed is False
        assert policy.delete_allowed is False


def test_projection_reader_requires_complete_versioned_overlay_and_keeps_raw_identity():
    connection = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(),
        connection_factory=lambda: connection,
    )
    repository = FinancialRepository(kernel)
    calls = []
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        "data_collect.data_access.repository.financial_projection_repository.FinancialProjectionRepository.assert_complete",
        lambda self, selection, context, start, end: calls.append((selection, start, end)),
    )
    monkeypatch.setattr(
        kernel.read,
        "_repository_query",
        lambda **kwargs: calls.append(kwargs) or [],
    )
    context = RunContext("projection-reader", "test", "test", "test", "test")
    selection = FinancialProjectionSelection("projection-v2", "a" * 64)
    assert "p.projection_version=%s" in repository._PROJECTED_EVENTS_SQL
    assert "p.standard_id=s.financial_id" in repository._PROJECTED_EVENTS_SQL
    assert "COALESCE(p.data_available_timestamp,s.data_available_timestamp)" in repository._PROJECTED_EVENTS_SQL
    assert "s.source_record_key, s.record_version, s.payload_hash" in repository._PROJECTED_EVENTS_SQL
    assert repository.load_events(
        ["000001.SZ"], date(2015, 1, 1), date(2021, 8, 1), context, selection,
    ) == []
    query_call = next(call for call in calls if isinstance(call, dict))
    assert query_call["sql"] == repository._PROJECTED_EVENTS_SQL
    assert query_call["params"][0] == "projection-v2"
    assert calls[0] == (selection, date(2015, 1, 1), date(2021, 8, 1))
    monkeypatch.undo()
    kernel.close()


def test_feature_lineage_carries_projection_version_and_content_identity():
    event = {
        "dataset": "income",
        "source_record_key": "000001.SZ:2023-06-30:1:1:2",
        "record_version": 123,
        "payload_hash": "b" * 64,
        "vintage_quality": "SOURCE_ANNOUNCEMENT_ASSUMED",
        "quality_status": "VALID",
        "source_observed_at": datetime(2023, 8, 20, 10, 0),
        "projection_version": "projection-v2",
        "projection_content_sha256": "c" * 64,
    }

    lineage, fingerprint, _ = _lineage(
        [event], factor_inputs={"revenue_growth_yoy": [event]},
    )

    item = next(value for key, value in lineage.items() if key.startswith("income:"))
    assert item["projection_version"] == "projection-v2"
    assert item["projection_content_sha256"] == "c" * 64
    assert item["used_by"] == ["revenue_growth_yoy"]
    assert len(fingerprint) == 64


def test_debt_to_assets_fingerprint_distinguishes_projection_version():
    source = {
        "dataset": "balance",
        "ts_code": "000001.SZ",
        "report_date": date(2023, 6, 30),
        "data_available_timestamp": datetime(2023, 8, 21, tzinfo=ZoneInfo("Asia/Shanghai")),
        "source_observed_at": datetime(2023, 8, 20, 10, 0),
        "report_type": "1",
        "comp_type": "1",
        "end_type": "2",
        "source_record_key": "000001.SZ:2023-06-30:1:1:2",
        "record_version": 123,
        "payload_hash": "d" * 64,
        "vintage_quality": "SOURCE_ANNOUNCEMENT_ASSUMED",
        "quality_status": "VALID",
        "values": {"total_assets": 1_000.0, "total_liab": 450.0},
    }
    base = calculate_debt_to_assets_features([source], date(2023, 6, 30), date(2023, 6, 30))[0]
    projected = calculate_debt_to_assets_features(
        [{**source, "projection_version": "projection-v2", "projection_content_sha256": "e" * 64}],
        date(2023, 6, 30), date(2023, 6, 30),
    )[0]

    assert base["source_fingerprint"] != projected["source_fingerprint"]
    assert projected["source_projection_version"] == "projection-v2"


def test_projection_row_hash_changes_when_projection_content_changes():
    row = {
        "projection_version": "projection-v2",
        "source_dataset": "income",
        "standard_id": 7,
        "raw_record_id": 123,
        "data_available_timestamp": datetime(2023, 8, 21, tzinfo=ZoneInfo("Asia/Shanghai")),
    }
    original = projection_row_hash(row)
    changed = projection_row_hash({**row, "data_available_timestamp": datetime(
        2023, 8, 22, tzinfo=ZoneInfo("Asia/Shanghai"),
    )})

    assert original != changed


def test_projection_normalizes_database_nan_text_to_null():
    assert normalize("NaN") is None
    assert normalize(float("nan")) is None


def test_projection_rejects_timestamp_that_does_not_match_announcement_rule():
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(),
        connection_factory=lambda: FakeConnection(),
    )
    repository = FinancialProjectionRepository(kernel)
    announcement = date(2023, 8, 21)
    row = {
        "projection_version": "projection-v2",
        "source_dataset": "income",
        "batch_number": 1,
        "standard_id": 7,
        "source_standard_id": 7,
        "raw_dataset": "income",
        "raw_record_id": 123,
        "raw_business_key": "000001.SZ:2023-06-30:1:1:2",
        "raw_payload_sha256": "a" * 64,
        "standard_source": "tushare",
        "source_record_key": "000001.SZ:2023-06-30:1:1:2",
        "source_record_version": 123,
        "standard_payload_sha256": "a" * 64,
        "standard_content_sha256": "b" * 64,
        "ts_code": "000001.SZ",
        "report_date": date(2023, 6, 30),
        "report_type": "1",
        "comp_type": "1",
        "projected_end_type": "2",
        "projected_announce_date": announcement,
        "data_available_timestamp": datetime(2023, 8, 23, tzinfo=ZoneInfo("Asia/Shanghai")),
        "vintage_quality": "SOURCE_ANNOUNCEMENT_ASSUMED",
        "quality_status": "VALID",
        "source_observed_at": datetime(2026, 8, 22, 12, 0),
        "projection_rule_version": "announcement-next-day-shanghai-midnight-v1",
        "created_run_id": "test-run",
    }
    row["projection_content_sha256"] = projection_row_hash(row)
    context = RunContext("projection-test", "projection-test", "test", "test", "test")

    with pytest.raises(ValueError, match=r"announcement date \+ 1 day"):
        repository.append_rows([row], context)

    kernel.close()
