"""End-to-end Dataset and Alpha Reader checks for the isolated test database."""
from __future__ import annotations

import os
from collections import defaultdict
from datetime import date

import pytest

from data_collect.alpha_research.factors import factor_values
from data_collect.alpha_research.labels import build_forward_labels
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.alpha_research_repository import (
    AlphaResearchRepository,
)
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.jobs import tushare_release
from tests.integration_fixtures.alpha_research_v21 import (
    DATASET_ID,
    FUTURE_SESSIONS,
    SIGNAL_SESSIONS,
    ensure_alpha_research_dataset,
)
from tests.integration_fixtures.real_golden_sample import (
    DATASET_ID as REAL_SAMPLE_DATASET_ID,
)
from tests.integration_fixtures.real_golden_sample import (
    E2E_CONTRACT_VERSION as REAL_SAMPLE_E2E_CONTRACT_VERSION,
)
from tests.integration_fixtures.real_golden_sample import (
    RESEARCH_SCHEMA as REAL_SAMPLE_RESEARCH_SCHEMA,
)
from tests.integration_fixtures.real_golden_sample import (
    SAMPLE_VERSION as REAL_SAMPLE_VERSION,
)
from tests.integration_fixtures.real_golden_sample import (
    ensure_real_golden_dataset,
    import_real_golden_sample,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session", autouse=True)
def require_isolated_integration_profile():
    assert os.getenv("ASHARES_ENV") == "integration_test", (
        "Run through tools/integration_test_db.ps1 -Action test; refusing to "
        "seed research fixtures without the isolated integration profile."
    )
    assert os.getenv("ASHARES_TEST_ENVIRONMENT_ID")
    assert os.getenv("DATA_COLLECT_CONFIG")


def test_synthetic_v21_dataset_lifecycle_and_alpha_reader():
    # Given a synthetic TEST_FIXTURE universe with a 23-session signal block
    # and 60 future sessions, create and freeze the real Dataset artifact.
    result = ensure_alpha_research_dataset()
    assert result["dataset_id"] == DATASET_ID
    assert result["dataset_status"] == "FROZEN"
    assert result["snapshot_count"] == SIGNAL_SESSIONS + FUTURE_SESSIONS
    assert result["signal_session_count"] == SIGNAL_SESSIONS
    assert result["forward_tail_session_count"] == FUTURE_SESSIONS

    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        connection = kernel._db_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT current_database(), identity.environment_kind
                     FROM integration_test.environment_identity AS identity
                    WHERE identity.singleton=TRUE"""
            )
            database_name, environment_kind = cursor.fetchone()
        assert (database_name, environment_kind) == ("ashares_test", "TEST_FIXTURE")

        repository = DatasetRepository(kernel)
        context = RunContext(
            run_id="integration-alpha-reader",
            job_name="integration_alpha_reader",
            code_version="test",
            config_hash="test",
            pipeline_version="integration-test-v1",
        )

        manifest = repository.manifest(DATASET_ID, context)
        assert manifest["status"] == "FROZEN"
        assert manifest["schema_version"] == "backtest-dataset-v2"

        # Use the Dataset's own open calendar to define the first 23 signals.
        calendar = repository.open_calendar(DATASET_ID, context)
        assert len(calendar) == SIGNAL_SESSIONS + FUTURE_SESSIONS
        signal_end = calendar[SIGNAL_SESSIONS - 1]
        observations = repository.observation_page(
            DATASET_ID,
            (
                "research_date", "ts_code", "pe_pct_5y", "pe_ttm", "roe_ttm",
                "debt_to_assets", "revenue_growth_yoy", "profit_growth_yoy",
                "return_20d", "return_60d", "adj_close_base", "ma60",
                "volume_ratio_20d", "is_pe_meaningful", "has_full_pe_5y_history",
                "financial_feature_contract_version", "financial_vintage_quality",
                "financial_component_lineage", "financial_data_available_timestamp",
                "roe_quality_status", "growth_quality_status", "price_feature_version",
                "price_adjustment_mode",
            ),
            context,
            date_from=calendar[0],
            date_to=signal_end,
        )
        assert len(observations) == SIGNAL_SESSIONS * 20
        values = [factor_values(row) for row in observations]
        assert all(item["roe_ttm"].status == "VALID" for item in values)
        assert all(item["revenue_growth_yoy"].status == "VALID" for item in values)
        assert all(item["profit_growth_yoy"].status == "VALID" for item in values)
        assert all(item["return_20d"].status == "VALID" for item in values)
        assert all(item["return_60d"].status == "VALID" for item in values)

        bars = repository.bar_page(DATASET_ID, context)
        assert len(bars) == len(calendar) * 20
        bars_by_day = defaultdict(dict)
        for row in bars:
            bars_by_day[row["trade_date"]][row["ts_code"]] = row

        labels = build_forward_labels(
            observations,
            calendar=calendar,
            bars=bars_by_day,
            actions={},
            horizons=(20, 40, 60),
        )
        assert len(labels) == SIGNAL_SESSIONS * 20 * 3
        assert {label.status for label in labels} == {"COMPLETE"}
        assert {label.horizon for label in labels} == {20, 40, 60}
        assert all(label.prediction_total_return is not None for label in labels)
        assert all(label.entry_reference_total_return is not None for label in labels)

        # The test fixture cannot satisfy or masquerade as the production
        # Foundation release gate, even though the fixture Dataset is usable.
        production_gate = tushare_release._quality_gate(
            calendar[-1], include_foundation_v2=True,
        )
        assert production_gate["failures"]
        assert production_gate["foundation_v2_validated_run_count"] == 0
    finally:
        kernel.close()


def test_real_golden_sample_import_is_repeatable_and_fails_closed():
    # Given a deterministic real sample bundle imported only into ashares_test
    first = import_real_golden_sample()
    second = import_real_golden_sample()
    assert first["sample_version"] == second["sample_version"] == REAL_SAMPLE_VERSION
    assert first["fixture_marker"] == second["fixture_marker"] == "REAL_SAMPLE_TEST_ONLY"
    assert first["loader_contract_version"] == second["loader_contract_version"] == REAL_SAMPLE_E2E_CONTRACT_VERSION
    assert first["bundle_sha256"] == second["bundle_sha256"]
    assert second["status"] == "REUSED"

    # When the actual test-only Snapshot → Dataset path runs, keep the full
    # sample and compare every factor and forward label with immutable oracles.
    result = ensure_real_golden_dataset()
    expected_snapshot_count = 83 if REAL_SAMPLE_VERSION == "real-financial-window-v1" else 61
    expected_signal_observations = 690 if REAL_SAMPLE_VERSION == "real-financial-window-v1" else 30
    expected_label_rows = expected_signal_observations * 3
    assert result["snapshot_count"] == expected_snapshot_count
    assert result["snapshot_statuses"] == ["FROZEN"]
    assert result["fixture_marker"] == "REAL_SAMPLE_TEST_ONLY"
    assert result["calendar_sessions"] == expected_snapshot_count
    assert result["independent_reference_comparison"]["signal_observations"] == expected_signal_observations
    assert result["independent_reference_comparison"]["label_rows"] == expected_label_rows
    assert result["independent_reference_comparison"]["factor_rows"] > 0
    comparison = result["independent_reference_comparison"]
    expected_factor_status = (
        "FAIL"
        if REAL_SAMPLE_VERSION == "real-golden-v1"
        or (
            REAL_SAMPLE_VERSION == "real-financial-window-v1"
            and REAL_SAMPLE_E2E_CONTRACT_VERSION == "real-financial-window-e2e-v6"
        )
        else "PASS"
    )
    assert comparison["factor_match_status"] == expected_factor_status
    assert comparison["label_match_status"] == "PASS"
    if REAL_SAMPLE_VERSION == "real-golden-v1":
        # The v1 fixture is research-v2.1 and has no independent D/A feature
        # version evidence. Keep its historical reference values, but require
        # the v4 reader to reject every legacy D/A value rather than accept an
        # unversioned source as a valid research-v2.2 factor.
        mismatches = comparison["factor_mismatches"]
        assert comparison["factor_mismatch_count"] == len(mismatches) == 30
        assert {row["factor"] for row in mismatches} == {"debt_to_assets"}
        assert all(
            row["actual"] == {
                "status": "INVALID",
                "reason": "DEBT_TO_ASSETS_FEATURE_VERSION_NOT_SUPPORTED",
                "value": None,
            }
            for row in mismatches
        )
        assert {
            row["ts_code"]: row["expected"]["value"]
            for row in mismatches
            if row["ts_code"] in {"688426.SH", "688448.SH"}
        } == {"688426.SH": 8.619, "688448.SH": 27.9137}
        assert result["end_to_end_status"] == "FAIL"
    elif (
        REAL_SAMPLE_VERSION == "real-financial-window-v1"
        and REAL_SAMPLE_E2E_CONTRACT_VERSION == "real-financial-window-e2e-v6"
    ):
        assert comparison["factor_mismatch_count"] == 23
        assert {
            (row["ts_code"], row["factor"], row["difference"])
            for row in comparison["factor_mismatches"]
        } == {("000666.SZ", "debt_to_assets", "STATUS_OR_REASON")}
        assert comparison["financial_feature_presence_mismatch_count"] == 0
        assert result["dataset_freeze_gate"] == "BLOCKED_REFERENCE_MISMATCH"
        assert result["dataset_freeze_action"] == "NOT_APPLICABLE_FROZEN"
        assert result["alpha_reader_comparison"]["factor_match_status"] == "FAIL"
        assert result["alpha_reader_comparison"]["factor_mismatch_count"] == 23
        assert result["end_to_end_status"] == "FAIL"
    elif REAL_SAMPLE_VERSION == "real-financial-window-v1":
        assert comparison["factor_mismatches"] == []
        assert result["dataset_freeze_gate"] == "PASS"
        assert result["dataset_freeze_action"] in {
            "FROZEN_AFTER_REFERENCE_PASS",
            "NOT_APPLICABLE_FROZEN",
        }
        assert result["end_to_end_status"] == "PASS"
        assert result["alpha_reader_comparison"]["factor_match_status"] == "PASS"
        assert result["alpha_reader_comparison"]["factor_mismatch_count"] == 0
        assert result["alpha_reader_comparison"]["label_match_status"] == "PASS"
        gate = result["reference_mismatch_freeze_gate"]
        assert gate["status_before"] == "VALIDATED"
        assert gate["status_after"] == "VALIDATED"
        assert gate["injected_factor_mismatch_count"] == 1
        assert gate["freeze_action"] == "BLOCKED_REFERENCE_MISMATCH"
    elif REAL_SAMPLE_VERSION == "real-golden-v3":
        assert comparison["factor_mismatches"] == []
        assert result["dataset_freeze_gate"] == "PASS"
        assert result["dataset_freeze_action"] in {
            "FROZEN_AFTER_REFERENCE_PASS",
            "NOT_APPLICABLE_FROZEN",
        }
        assert result["end_to_end_status"] == "PASS"
        alpha_reader = result["alpha_reader_comparison"]
        assert alpha_reader["factor_match_status"] == "PASS"
        assert alpha_reader["label_match_status"] == "PASS"
        identity = alpha_reader["debt_to_assets_source_identity_check"]
        assert identity == {
            "ts_code": "000666.SZ",
            "report_date": "2023-03-31",
            "source_record_key": "000666.SZ:2023-03-31:1:1:1",
            "source_record_version": 491097,
            "source_payload_hash": "0e768431ffc85182c5b9d8cb2314e633a83e9d22c07df723ea143f859b6a60d8",
            "status": "VALID",
            "reason": "PIT_SOURCE_ANNOUNCEMENT_ASSUMED",
            "value": 36.619,
        }
        gate = result["reference_mismatch_freeze_gate"]
        assert gate["status_before"] == "VALIDATED"
        assert gate["status_after"] == "VALIDATED"
        assert gate["injected_factor_mismatch_count"] == 1
        assert gate["freeze_action"] == "BLOCKED_REFERENCE_MISMATCH"
    else:
        assert comparison["factor_mismatches"] == []
        assert result["end_to_end_status"] == "PASS"
    if REAL_SAMPLE_VERSION == "real-financial-window-v1":
        assert result["independent_reference_comparison"]["missing_pit_factor_rows"] > 0
        assert result["independent_reference_comparison"]["suspended_label_rows"] >= 5
        assert result["independent_reference_comparison"]["unknown_delist_terminal_rows"] >= 3
        assert result["import"]["recomputed_features"]["standard_events_verified"] == 4329
        assert result["alpha_reader_comparison"]["dataset_status"] == "FROZEN"
        assert result["alpha_reader_comparison"]["snapshot_count"] == 83
        assert result["alpha_reader_comparison"]["signal_observations"] == 690
        assert result["alpha_reader_comparison"]["label_rows"] == 2070
        assert result["alpha_reader_comparison"]["label_match_status"] == "PASS"
        if REAL_SAMPLE_E2E_CONTRACT_VERSION == "real-financial-window-e2e-v6":
            assert result["alpha_reader_comparison"]["factor_match_status"] == "FAIL"
        else:
            assert result["alpha_reader_comparison"]["factor_match_status"] == "PASS"
    else:
        assert result["independent_reference_comparison"]["missing_pit_factor_rows"] == 1
        assert result["independent_reference_comparison"]["suspended_label_rows"] == 5
        assert result["independent_reference_comparison"]["unknown_delist_terminal_rows"] == 3
    assert result["dataset_id"] == REAL_SAMPLE_DATASET_ID
    if REAL_SAMPLE_VERSION == "real-golden-v1":
        assert result["dataset_status"] == "VALIDATED"
        assert result["dataset_freeze_gate"] == "BLOCKED_REFERENCE_MISMATCH"
        assert result["dataset_freeze_action"] == "BLOCKED_REFERENCE_MISMATCH"
    else:
        assert result["dataset_status"] == "FROZEN"
    assert result["dataset_quality_report"]["status"] == "VALID"
    assert result["dataset_quality_report"]["validation_counts"]["missing_execution_rows"] == 0
    assert result["independent_reference_comparison"]["unresolved_execution_cases"] == []

    # The Alpha Reader must reject the unfrozen legacy v1 Dataset, and accept
    # frozen single-schema samples for all positive research paths.
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        repository = DatasetRepository(kernel)
        context = RunContext(
            run_id="integration-real-golden-alpha-reader",
            job_name="integration_real_golden_alpha_reader",
            code_version="test", config_hash="test",
            pipeline_version="integration-test-v1",
        )
        if REAL_SAMPLE_VERSION == "real-golden-v1":
            with pytest.raises(ValueError, match="Alpha research requires a FROZEN Backtest Dataset"):
                AlphaResearchRepository(repository, REAL_SAMPLE_DATASET_ID, context)
            return
        reader = AlphaResearchRepository(repository, REAL_SAMPLE_DATASET_ID, context)
        assert len(reader.identity.schema_versions) == 1
        assert reader.identity.schema_versions == (REAL_SAMPLE_RESEARCH_SCHEMA,)
        signal_start = date.fromisoformat(result["signal_dates"][0])
        signal_end = date.fromisoformat(result["signal_dates"][-1])
        columns = (
            "research_date", "ts_code", "delist_date", "debt_to_assets",
            "roe_ttm", "revenue_growth_yoy", "profit_growth_yoy",
            "financial_feature_contract_version", "financial_vintage_quality",
            "financial_component_lineage", "financial_data_available_timestamp",
            "roe_quality_status", "growth_quality_status", "return_20d",
            "return_60d", "price_feature_version", "price_adjustment_mode",
            "debt_to_assets_quality_status", "debt_to_assets_quality_reason",
            "debt_to_assets_data_available_timestamp", "debt_to_assets_source_observed_at",
            "debt_to_assets_vintage_quality", "debt_to_assets_source_quality_status",
            "debt_to_assets_source_record_key", "debt_to_assets_source_record_version",
            "debt_to_assets_source_payload_hash", "debt_to_assets_feature_contract_version",
            "debt_to_assets_report_date",
        )
        signal_rows = [
            row
            for page in reader.observation_pages(signal_start, signal_end, columns)
            for row in page
        ]
        assert len(signal_rows) == expected_signal_observations
        if REAL_SAMPLE_VERSION == "real-golden-v2":
            da_by_code = {str(row["ts_code"]): factor_values(row)["debt_to_assets"] for row in signal_rows}
            assert da_by_code["688426.SH"].value == 8.619
            assert da_by_code["688448.SH"].value == 27.9137
            assert all(da_by_code[code].status == "VALID" for code in ("688426.SH", "688448.SH"))
        calendar = reader.identity.calendar
        bars = reader.bars(calendar[0], calendar[-1])
        actions = reader.actions(calendar[0], calendar[-1])
        assert sum(map(len, bars.values())) == comparison["bar_rows"]
        assert sum(len(events) for by_code in actions.values() for events in by_code.values()) == comparison["action_rows"]
    finally:
        kernel.close()
