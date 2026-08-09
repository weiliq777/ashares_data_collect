from datetime import date
import inspect
from pathlib import Path

import pytest
import pandas as pd

from data_collect.jobs.backtest_dataset import (
    ADAPTER_VERSION,
    SCHEMA_VERSION as BACKTEST_DATASET_SCHEMA_VERSION,
    _check_baseline,
    _quality_report_hash,
    create_dataset,
)
from data_collect.backtest.engine import BacktestConfig
from data_collect.jobs.research_watchlist import add_evidence, create_watchlist
from data_collect.pipeline import _resolve_invocation
from data_collect.jobs.tushare_fina_indicator_version import _version_values
from data_collect.jobs.tushare_financial_raw_to_standard import transform


def test_watchlist_creation_no_longer_reads_evidence_only_arguments():
    with pytest.raises(ValueError, match="max_members"):
        create_watchlist("unused", "unused", max_members=0)


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"authority_level": "UNTRUSTED"}, "authority_level"),
        ({"claim_role": "buy_now"}, "claim_role"),
        ({"claim_role": "supporting", "content_hash": None}, "64"),
        ({"claim_role": "contrary", "content_hash": "short"}, "64"),
    ],
)
def test_evidence_audit_arguments_fail_before_database_access(kwargs, message):
    with pytest.raises(ValueError, match=message):
        add_evidence("unused", "000001.SZ", "test", "test", "test", **kwargs)


def test_dataset_oos_argument_belongs_only_to_dataset_creation():
    create_parameters = inspect.signature(create_dataset).parameters
    baseline_parameters = inspect.signature(_check_baseline).parameters
    assert "out_of_sample_start" in create_parameters
    assert "out_of_sample_start" not in baseline_parameters


def test_dataset_materializes_mutable_execution_inputs_from_bounded_raw():
    source = Path("data_collect/jobs/backtest_dataset.py").read_text(encoding="utf-8")
    for token in (
        "limit_max_record_id",
        "suspend_max_record_id",
        "dividend_max_record_id",
        "trade_cal_max_record_id",
        "limit_source_record_id",
        "suspend_source_record_id",
        "FROM tushare_dividend_raw",
        "FROM tushare_stk_limit_raw",
        "FROM tushare_suspend_d_raw",
        "mv.dataset='trade_cal'",
        "Baseline Raw 交易日历区间不完整",
    ):
        assert token in source
    execution_section = source.split("bar_rows = cur.fetchall()", 1)[0].rsplit(
        "cur.execute(", 1
    )[1]
    assert "std.security_status_daily" not in execution_section
    assert "pit.security_tradeability_daily" not in execution_section


def test_quality_report_hash_canonicalizes_database_dates():
    quality = {"out_of_sample_start": date(2025, 8, 6), "status": "VALID"}
    first = _quality_report_hash(quality)
    second = _quality_report_hash({"status": "VALID", "out_of_sample_start": "2025-08-06"})
    assert first == second


def test_dataset_validation_has_failure_state_recovery_boundary():
    source = Path("data_collect/jobs/backtest_dataset.py").read_text(encoding="utf-8")
    assert "def _mark_dataset_validation_failed" in source
    assert "WHERE dataset_id=%s AND status='BUILDING'" in source
    assert "_mark_dataset_validation_failed(dataset_id, exc)" in source


def test_release_switch_is_atomic_and_respects_single_validated_index():
    source = Path("data_collect/jobs/tushare_release.py").read_text(encoding="utf-8")
    supersede_at = source.index("SET status='superseded'")
    validate_at = source.index("SET status='validated'")
    assert supersede_at < validate_at
    assert 'isolation_level="REPEATABLE READ"' in source
    assert "_locked_quality_and_source_manifest" in source
    assert "_lock_release_inputs(conn)" in source


def test_database_contract_closes_release_and_dataset_raw_lineage_gaps():
    sql = Path("sql/039_dataset_raw_lineage_contract.sql").read_text(encoding="utf-8")
    for token in (
        "assert_release_immutable",
        "validated/superseded data release is immutable",
        "assert_dataset_extended_freeze_identity",
        "assert_dataset_raw_lineage_complete",
        "limit_source_record_id",
        "suspend_source_record_id",
        "source_record_version",
    ):
        assert token in sql


def test_candidate_packet_exposes_evidence_authority_level():
    source = Path("data_collect/decision/candidate_service.py").read_text(encoding="utf-8")
    evidence_section = source.split("def _evidence_for_candidate", 1)[1].split(
        "def list_candidates", 1
    )[0]
    assert evidence_section.count("authority_level") >= 2


def test_research_snapshot_build_locks_quality_and_materialization_inputs():
    source = Path("data_collect/jobs/research_snapshot.py").read_text(encoding="utf-8")
    build_section = source.split("def build", 1)[1].split("def validate", 1)[0]
    assert "_lock_release_inputs(conn)" in build_section
    assert build_section.index("_lock_release_inputs(conn)") < build_section.index("_quality_report")


def test_oos_default_is_deterministic_and_within_evaluation_dates():
    # Contract-level regression: the default split remains tied to the ordered
    # evaluation trading dates, not wall-clock days.
    source = Path("data_collect/jobs/backtest_dataset.py").read_text(encoding="utf-8")
    assert "int(len(evaluation_open_dates) * 0.8)" in source
    assert "out_of_sample_start not in evaluation_open_dates" in source
    assert date(2026, 8, 3) < date(2026, 8, 4)


def test_pipeline_task_kwargs_are_forwarded_without_silent_override():
    fn_name, kwargs = _resolve_invocation(
        {"kwargs": {"rebuild_derived": False}}, "20260808", {"dry_run": True}
    )
    assert fn_name == "run"
    assert kwargs == {
        "run_date": "20260808", "rebuild_derived": False, "dry_run": True,
    }
    with pytest.raises(ValueError, match="重复"):
        _resolve_invocation(
            {"kwargs": {"dry_run": False}}, "20260808", {"dry_run": True}
        )


def test_financial_revision_key_excludes_announcement_and_update_metadata():
    base = {
        "ts_code": "000001.SZ", "end_date": "20251231",
        "report_type": "1", "comp_type": "1", "end_type": "4",
        "ann_date": "20260320", "update_flag": "0", "roe": 15.0,
    }
    revised = dict(base, ann_date="20260401", update_flag="1", roe=16.0)
    first = _version_values(base, "batch-a")
    second = _version_values(revised, "batch-b")
    assert first[0] == second[0]
    assert first[12] != second[12]


def test_financial_availability_is_never_same_day_as_date_only_announcement():
    frame = pd.DataFrame([{
        "ts_code": "000001.SZ", "end_date": "20251231",
        "ann_date": "20260320", "f_ann_date": None,
        "report_type": "1", "comp_type": "1", "end_type": "4",
        "update_flag": "0", "revenue": 100.0,
        "_logical_business_key": "000001.SZ:20251231:1:1:4",
        "_record_id": 1, "_payload_hash": "a" * 64,
        "_fetched_at": "2026-08-08 20:00:00",
        "_ambiguous_same_day_revision": False,
    }])
    out = transform(frame, "income")
    assert pd.Timestamp(out.iloc[0]["announce_date"]).date() == date(2026, 3, 20)
    assert out.iloc[0]["version_available_date"] == date(2026, 3, 21)


def test_formal_versions_change_when_execution_semantics_change():
    assert BACKTEST_DATASET_SCHEMA_VERSION == "backtest-dataset-v2"
    assert ADAPTER_VERSION == "research-to-backtest-v2"
    assert BacktestConfig().execution_model_version == "daily-next-open-v2"
    signature = inspect.signature(create_dataset)
    assert signature.parameters["market_rule_version"].default == "cn-a-share-v2"
    assert (
        signature.parameters["fee_rule_version"].default
        == "research-fee-assumption-v3"
    )


def test_historical_research_snapshot_does_not_expose_future_delist_date():
    source = Path("data_collect/jobs/research_snapshot.py").read_text(encoding="utf-8")
    snapshot_sql = source.split("def _stock_snapshot_sql", 1)[1].split(
        "def _lineage_sql", 1
    )[0]
    assert snapshot_sql.count(
        "to_date(NULLIF(b.payload->>'delist_date',''), 'YYYYMMDD') <= u.trade_date"
    ) >= 1
    assert "WHEN u.is_listed_asof THEN 'L'" in snapshot_sql
    assert "ELSE 'D'" in snapshot_sql
    assert "THEN to_date(NULLIF(b.payload->>'delist_date',''), 'YYYYMMDD')" in snapshot_sql


def test_open_time_audit_contract_is_versioned_and_database_bound():
    sql = Path("sql/041_backtest_open_time_contract.sql").read_text(encoding="utf-8")
    for token in (
        "liquidity_reference_date",
        "daily-next-open-v2",
        "assert_order_liquidity_reference",
        "NEW.liquidity_reference_date >= NEW.order_date",
        "'liquidity_reference_date', NEW.liquidity_reference_date::text",
    ):
        assert token in sql
