from __future__ import annotations

import pytest

from data_collect.alpha_research.contracts import SOURCE_QUALITY_EVIDENCE_VERSION
from data_collect.alpha_research.factors import factor_values
from data_collect.alpha_research.golden_sample import (
    EDGE_CASES,
    GOLDEN_SAMPLE_VERSION,
    SAMPLE_SEED,
    content_hash,
    select_codes,
    validate_manifest,
)
from data_collect.data_access.corporate_action_evidence import (
    quality_report_has_current_ex_date_evidence,
)
from tests.integration_fixtures.real_golden_sample import (
    _assert_debt_to_assets_source_identity,
    _financial_features_for_fixture,
    _load_bundle,
    _quality_report,
)


def test_golden_sample_selection_is_stable_and_keeps_boundary_cases():
    candidates = [f"{index:06d}.SZ" for index in range(1, 101)]
    first = select_codes(candidates)
    second = select_codes(reversed(candidates))

    assert first == second
    assert len(first) == 30
    assert len({row["ts_code"] for row in first}) == 30
    assert {row["ts_code"] for row in first[:len(EDGE_CASES)]} == set(EDGE_CASES)
    assert all(row["selection_role"] == "EDGE_CASE" for row in first[:len(EDGE_CASES)])
    assert all(row["selection_role"] == "HASH_SAMPLE" for row in first[len(EDGE_CASES):])
    assert SAMPLE_SEED == GOLDEN_SAMPLE_VERSION


def test_golden_sample_selection_allows_fixed_cases_outside_price_candidates():
    candidates = [f"{index:06d}.SZ" for index in range(1, 101)]
    selected = select_codes(candidates)

    assert "300965.SZ" in {row["ts_code"] for row in selected}
    assert "300965.SZ" not in candidates


def test_golden_sample_selection_rejects_too_small_candidate_universe():
    with pytest.raises(ValueError, match="only 3 ordinary securities"):
        select_codes(("A.SZ", "B.SZ", "C.SZ"))


def test_golden_sample_manifest_is_versioned_and_requires_30_unique_codes():
    securities = select_codes([f"{index:06d}.SZ" for index in range(1, 101)])
    manifest = {
        "sample_version": GOLDEN_SAMPLE_VERSION,
        "signal_date": "2023-09-22",
        "target_date": "2023-12-20",
        "sample_seed": SAMPLE_SEED,
        "securities": securities,
        "source_dataset": {"dataset_id": "dataset"},
        "source_snapshots": [{"snapshot_id": "snapshot"}],
        "bundle_sha256": content_hash({"bundle": "test"}),
    }

    validate_manifest(manifest)

    manifest["securities"] = securities[:-1] + [securities[0]]
    with pytest.raises(ValueError, match="non-empty and unique"):
        validate_manifest(manifest)


def test_real_fixture_release_evidence_is_current_and_bounded_to_imported_actions():
    report = _quality_report()

    assert quality_report_has_current_ex_date_evidence(report)
    assert report["full_market_quality_claim"] is False
    assert report["bounded_fixture_action_count"] == 2
    assert report["implementation_without_ex_date"] == 0


def test_debt_to_assets_feature_rejects_source_report_period_mismatch():
    source = {
        "dataset": "balance",
        "ts_code": "000046.SZ",
        "report_date": "2016-09-30",
        "source_record_key": "000046.SZ:2016-09-30:1:1:3",
        "record_version": 739123,
        "payload_hash": "b" * 64,
    }
    feature = {
        "ts_code": "000046.SZ",
        "report_date": "2023-06-30",
        "source_record_key": source["source_record_key"],
        "record_version": source["record_version"],
        "payload_hash": source["payload_hash"],
    }

    with pytest.raises(RuntimeError, match="report_date does not match Standard source identity"):
        _assert_debt_to_assets_source_identity([feature], [source])


def test_real_financial_warning_source_component_fails_closed_at_factor_gate():
    bundle, manifest, _, _ = _load_bundle()
    case = manifest.get("quality_cases", {}).get("financial_source_warning_case")
    if case is None:
        pytest.skip("this immutable sample version has no dedicated warning-case oracle")
    event = next(
        row for row in bundle["financial_standard_events"]
        if row["source_record_key"] == "000046.SZ:2016-12-31:1:1:4"
        and int(row["record_version"]) == 739122
    )
    feature = next(
        row for row in _financial_features_for_fixture(bundle)
        if row["ts_code"] == case["ts_code"]
        and str(row["report_date"])[:10] == case["report_date"]
    )
    assert content_hash(feature) == case["feature_sha256"]
    row = {
        **feature,
        "research_date": case["research_date"],
        "financial_feature_contract_version": "feature-contract-v2.0",
        "financial_vintage_quality": "SOURCE_ANNOUNCEMENT_ASSUMED",
        "financial_data_available_timestamp": case["data_available_timestamp"],
        "financial_component_lineage": {
            "__quality_evidence__": {"version": SOURCE_QUALITY_EVIDENCE_VERSION},
            case["warning_components"][0]: {
                "source_record_key": event["source_record_key"],
                "record_version": event["record_version"],
                "payload_hash": event["payload_hash"],
                "source_quality_status": event["quality_status"],
                "used_by": ["roe_ttm"],
            },
        },
    }

    roe = factor_values(row)["roe_ttm"]

    assert event["quality_status"] == "WARNING"
    assert roe.status == "INVALID"
    assert roe.reason == case["factor_reasons"]["roe_ttm"]
