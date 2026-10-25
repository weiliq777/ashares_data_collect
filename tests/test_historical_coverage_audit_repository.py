from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.historical_coverage_audit_repository import (
    HistoricalCoverageAuditRepository,
)
from tests._fake_db import FakeConnection


def test_missing_timestamp_lineage_audit_is_bounded_and_exposes_identity_conflicts(monkeypatch):
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=FakeConnection,
    )
    repository = HistoricalCoverageAuditRepository(kernel)
    context = RunContext(
        run_id="pit-lineage-test",
        job_name="historical_coverage_audit",
        code_version="test",
        config_hash="test",
        pipeline_version="test",
    )
    expected = [{"source": "income", "record_version_identity_conflicts": 1}]
    captured = {}

    def capture(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return expected

    monkeypatch.setattr(kernel.read, "_repository_query", capture)
    result = repository.missing_timestamp_raw_lineage_monthly(
        date(2015, 1, 1), date(2015, 12, 31), context,
    )

    sql = captured["args"][2]
    assert result == expected
    assert captured["args"][1] == "std.financial_balance_sheet_standard"
    assert captured["args"][3] == (
        date(2015, 1, 1), date(2015, 12, 31),
        date(2015, 1, 1), date(2015, 12, 31),
        date(2015, 1, 1), date(2015, 12, 31),
        date(2015, 1, 1), date(2015, 12, 31),
    )
    assert "raw_payload_hash_conflicts" in sql
    assert "raw_report_date_conflicts" in sql
    assert captured["args"][4].date_from == date(2015, 1, 1)
    assert captured["args"][4].date_to == date(2015, 12, 31)
    kernel.close()


def test_missing_timestamp_lineage_conflict_details_have_a_hard_row_limit(monkeypatch):
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=FakeConnection,
    )
    repository = HistoricalCoverageAuditRepository(kernel)
    context = RunContext(
        run_id="pit-lineage-conflicts-test",
        job_name="historical_coverage_audit",
        code_version="test",
        config_hash="test",
        pipeline_version="test",
    )
    captured = {}
    monkeypatch.setattr(
        kernel.read, "_repository_query",
        lambda *args, **kwargs: captured.update(args=args, kwargs=kwargs) or [],
    )

    repository.missing_timestamp_raw_lineage_conflicts(
        date(2015, 1, 1), date(2016, 12, 31), context, limit=25,
    )

    assert captured["args"][3][-1] == 25
    assert captured["args"][4].date_from == date(2015, 1, 1)
    assert captured["args"][4].date_to == date(2016, 12, 31)
    assert captured["kwargs"] == {}
    with pytest.raises(ValueError):
        repository.missing_timestamp_raw_lineage_conflicts(
            date(2015, 1, 1), date(2016, 12, 31), context, limit=501,
        )
    kernel.close()


def test_financial_v2_rebuild_candidate_query_has_date_and_count_bounds(monkeypatch):
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=FakeConnection,
    )
    repository = HistoricalCoverageAuditRepository(kernel)
    context = RunContext(
        run_id="financial-candidate-test",
        job_name="historical_coverage_audit",
        code_version="test",
        config_hash="test",
        pipeline_version="test",
    )
    captured = {}
    monkeypatch.setattr(
        kernel.read, "_repository_query",
        lambda *args, **kwargs: captured.update(args=args) or [],
    )

    repository.financial_v2_rebuild_candidates(
        date(2021, 6, 30), context, limit=20,
    )

    assert captured["args"][3] == (date(2021, 6, 30), 20)
    assert captured["args"][4].date_from == date(2021, 6, 30)
    assert captured["args"][4].date_to == date(2021, 6, 30)
    assert "NOT EXISTS" in captured["args"][2]
    kernel.close()


def test_financial_feature_cutoff_comparison_uses_the_explicit_local_cutoff(monkeypatch):
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=FakeConnection,
    )
    repository = HistoricalCoverageAuditRepository(kernel)
    context = RunContext(
        run_id="financial-feature-cutoff-test",
        job_name="historical_coverage_audit",
        code_version="test",
        config_hash="test",
        pipeline_version="test",
    )
    captured = {}
    monkeypatch.setattr(
        kernel.read, "_repository_query",
        lambda *args, **kwargs: captured.update(args=args) or [],
    )

    repository.financial_feature_cutoff_comparison(
        date(2021, 6, 30), date(2021, 8, 1), context,
    )

    sql = captured["args"][2]
    assert sql.count("AT TIME ZONE 'Asia/Shanghai'") == 8
    assert captured["args"][3] == (
        date(2021, 8, 1), date(2021, 8, 1), date(2021, 8, 1), date(2021, 8, 1),
        date(2021, 6, 30),
        date(2021, 8, 1), date(2021, 8, 1), date(2021, 8, 1), date(2021, 8, 1),
        date(2021, 6, 30),
    )
    assert captured["args"][4].date_from == date(2021, 6, 30)
    assert captured["args"][4].dimensions["pit_cutoff_date"] == "2021-08-01"
    kernel.close()


def test_missing_timestamp_projection_batch_uses_stable_bounded_cursor(monkeypatch):
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=FakeConnection,
    )
    repository = HistoricalCoverageAuditRepository(kernel)
    context = RunContext(
        run_id="pit-projection-page-test",
        job_name="historical_coverage_audit",
        code_version="test",
        config_hash="test",
        pipeline_version="test",
    )
    captured = {}
    monkeypatch.setattr(
        kernel.read, "_repository_query",
        lambda *args, **kwargs: captured.update(args=args, kwargs=kwargs) or [],
    )

    repository.missing_timestamp_projection_batch(
        "income", date(2021, 1, 1), date(2021, 12, 31), context,
        after_standard_id=1400, limit=250,
    )

    assert captured["args"][1] == "std.financial_income_standard"
    assert captured["args"][3] == (
        "income", "income", date(2021, 1, 1), date(2021, 12, 31), 1400, 250,
    )
    sql = captured["args"][2]
    assert "r.record_id=s.record_version" in sql
    assert "s.end_type='NaN' AND r.end_type IS NULL" in sql
    assert "s.financial_id>%s" in sql
    assert "ORDER BY s.financial_id" in sql
    assert captured["args"][4].date_from == date(2021, 1, 1)
    assert captured["args"][4].date_to == date(2021, 12, 31)
    with pytest.raises(ValueError):
        repository.missing_timestamp_projection_batch(
            "fina_indicator", date(2021, 1, 1), date(2021, 12, 31), context,
        )
    with pytest.raises(ValueError):
        repository.missing_timestamp_projection_batch(
            "income", date(2021, 1, 1), date(2021, 12, 31), context, limit=5001,
        )
    kernel.close()


def test_financial_report_period_codes_enforce_asof_cutoff_and_source_allowlist(monkeypatch):
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=FakeConnection,
    )
    repository = HistoricalCoverageAuditRepository(kernel)
    context = RunContext(
        run_id="report-period-codes-test",
        job_name="historical_coverage_audit",
        code_version="test",
        config_hash="test",
        pipeline_version="test",
    )
    captured = {}
    monkeypatch.setattr(
        kernel.read, "_repository_query",
        lambda *args, **kwargs: captured.update(args=args) or [{"ts_code": "000001.SZ"}],
    )

    result = repository.financial_report_period_codes(
        "balance_sheet", date(2021, 6, 30),
        datetime(2021, 8, 20, 15, tzinfo=ZoneInfo("Asia/Shanghai")), context,
    )

    assert result == ["000001.SZ"]
    assert captured["args"][1] == "std.financial_balance_sheet_standard"
    assert captured["args"][3] == (
        date(2021, 6, 30), datetime(2021, 8, 20, 15, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    assert "data_available_timestamp IS NOT NULL" in captured["args"][2]
    assert "data_available_timestamp < %s::timestamptz" in captured["args"][2]
    assert captured["args"][4].dimensions["cutoff_timestamp"] == "2021-08-20T15:00:00+08:00"
    with pytest.raises(ValueError):
        repository.financial_report_period_codes(
            "income; DROP TABLE x", date(2021, 6, 30),
            datetime(2021, 8, 20, 15, tzinfo=ZoneInfo("Asia/Shanghai")), context,
        )
    kernel.close()


def test_financial_feature_rows_are_source_allowlisted_and_cutoff_bounded(monkeypatch):
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=FakeConnection,
    )
    repository = HistoricalCoverageAuditRepository(kernel)
    context = RunContext(
        run_id="feature-rows-test",
        job_name="historical_coverage_audit",
        code_version="test",
        config_hash="test",
        pipeline_version="test",
    )
    captured = {}
    monkeypatch.setattr(
        kernel.read, "_repository_query",
        lambda *args, **kwargs: captured.update(args=args) or [],
    )

    repository.financial_feature_rows(
        "company_financial_v2", date(2021, 6, 30),
        datetime(2021, 8, 2, 15, tzinfo=ZoneInfo("Asia/Shanghai")), context,
    )

    assert captured["args"][1] == "feature.company_financial_v2"
    assert captured["args"][3] == (
        date(2021, 6, 30), datetime(2021, 8, 2, 15, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    assert "component_lineage" in captured["args"][2]
    assert "data_available_timestamp <" in captured["args"][2]
    with pytest.raises(ValueError):
        repository.financial_feature_rows(
            "std.financial_income_standard", date(2021, 6, 30),
            datetime(2021, 8, 2, 15, tzinfo=ZoneInfo("Asia/Shanghai")), context,
        )
    kernel.close()
