from datetime import date

import pytest

from data_collect.backtest.dataset_composition import (
    DatasetComponent,
    assert_resumable_component_prefix,
    assert_compatible_manifests,
    composition_hash,
    validate_component_coverage,
)
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.jobs.backtest_dataset import _assert_resumable_composite_manifest


D1 = date(2026, 8, 21)
D2 = date(2026, 8, 24)
D3 = date(2026, 8, 25)


def _component(dataset_id, start, end, token):
    return DatasetComponent(
        dataset_id=dataset_id,
        effective_from=start,
        effective_to=end,
        input_manifest_hash=token * 64,
        manifest_hash=(token.lower() * 64),
        baseline_tag=f"baseline-{dataset_id}",
        baseline_quality_report_hash=(token * 64),
    )


def _manifest(dataset_id, *, baseline="b1", status="FROZEN", **overrides):
    row = {
        "dataset_id": dataset_id,
        "dataset_kind": "MATERIALIZED",
        "dataset_name": "daily",
        "dataset_version": "1",
        "status": status,
        "baseline": baseline,
        "baseline_quality_report_hash": "a" * 64,
        "baseline_source_manifest_hash": "b" * 64,
        "schema_version": "dataset-v2",
        "adapter_version": "adapter-v2",
        "feature_version": "feature-v2",
        "price_adjustment_mode": "adj_close_base_for_features",
        "calendar_rule_version": "calendar-v1",
        "market_rule_version": "market-v1",
        "fee_rule_version": "fees-v1",
        "benchmark_code": "000300.SH",
        "selected_columns": {"research": ["research_date", "ts_code"]},
        "availability_rules": {"research": "asof"},
        "warmup_start": D1,
        "evaluation_start": D1,
        "evaluation_end": D2,
        "input_manifest_hash": "c" * 64,
        "calendar_hash": "d" * 64,
        "rules_hash": "e" * 64,
        "execution_hash": "f" * 64,
        "corporate_action_hash": "1" * 64,
        "benchmark_hash": "2" * 64,
    }
    row.update(overrides)
    return row


def test_component_coverage_accepts_adjacent_open_days_across_weekend():
    components = [
        _component("base", D1, D1, "A"),
        _component("extension", D2, D3, "B"),
    ]
    validate_component_coverage(components, (D1, D2, D3))


@pytest.mark.parametrize(
    "components,calendar",
    [
        (
            [_component("base", D1, D1, "A"), _component("tail", D3, D3, "B")],
            (D1, D2, D3),
        ),
        (
            [_component("base", D1, D2, "A"), _component("tail", D2, D3, "B")],
            (D1, D2, D3),
        ),
        (
            [_component("base", D1, D2, "A")],
            (D1, D2, D3),
        ),
    ],
)
def test_component_coverage_rejects_gap_overlap_or_uncovered_end(components, calendar):
    with pytest.raises(ValueError):
        validate_component_coverage(components, calendar)


def test_component_retry_accepts_only_an_exact_prefix():
    first = {
        "ordinal": 0,
        "component_dataset_id": "base",
        "effective_from": D1,
        "effective_to": D1,
        "component_input_manifest_hash": "a" * 64,
        "component_manifest_hash": "b" * 64,
        "component_baseline_tag": "baseline-a",
        "component_baseline_quality_report_hash": "c" * 64,
    }
    second = {**first, "ordinal": 1, "component_dataset_id": "tail", "effective_from": D2, "effective_to": D3}
    assert_resumable_component_prefix([first], [first, second])
    with pytest.raises(ValueError, match="精确前缀"):
        assert_resumable_component_prefix(
            [{**first, "component_dataset_id": "other"}], [first, second],
        )
    with pytest.raises(ValueError, match="多于"):
        assert_resumable_component_prefix([first, second], [first])


def test_composite_manifest_retry_requires_same_draft_identity():
    intended = {
        "dataset_id": "logical",
        "status": "DRAFT",
        "dataset_name": "daily",
        "dataset_version": "2",
        "dataset_kind": "COMPOSITE",
        "selected_columns": '{"research":["research_date","ts_code"]}',
        "availability_rules": '{"research":"asof"}',
        "evaluation_end": D3,
    }
    existing = {
        **intended,
        "selected_columns": {"research": ["research_date", "ts_code"]},
        "availability_rules": {"research": "asof"},
    }
    _assert_resumable_composite_manifest(existing, intended)
    with pytest.raises(RuntimeError, match="身份与本次输入不一致"):
        _assert_resumable_composite_manifest(
            {**existing, "evaluation_end": date(2026, 8, 26)}, intended,
        )


def test_compatibility_allows_baseline_change_but_rejects_contract_drift():
    base = _manifest("base", baseline="old")
    extension = _manifest("tail", baseline="new")
    assert_compatible_manifests((base, extension))

    extension["feature_version"] = "different-feature-version"
    with pytest.raises(ValueError, match="契约不兼容"):
        assert_compatible_manifests((base, extension))


def test_composition_hash_is_stable_and_commits_date_partition_and_inputs():
    base = _component("base", D1, D1, "A")
    tail = _component("tail", D2, D3, "B")
    first = composition_hash((base, tail))
    assert first == composition_hash((base, tail))
    assert first != composition_hash((base, _component("tail", D2, D3, "C")))


class _FakeReadGateway:
    def __init__(self):
        self.calls = []

    def _repository_query(self, context, source, sql, params, scope, max_rows):
        self.calls.append((source, sql, params, scope, max_rows))
        if source == "backtest.dataset_component":
            if params == ("logical-id",):
                return [{
                    "ordinal": 0,
                    "component_dataset_id": "physical-id",
                    "effective_from": D1,
                    "effective_to": D3,
                    "component_input_manifest_hash": "a" * 64,
                    "component_manifest_hash": "b" * 64,
                    "component_baseline_tag": "base-v1",
                    "component_baseline_quality_report_hash": "c" * 64,
                }]
            return []
        if source == "backtest.dataset_manifest":
            kind = "MATERIALIZED" if params == ("physical-id",) else "COMPOSITE"
            return [{"dataset_kind": kind}]
        if source == "backtest.dataset_execution_bar":
            return [{"trade_date": D2, "ts_code": "600000.SH"}]
        return []


class _FakeKernel:
    def __init__(self):
        self.read = _FakeReadGateway()


def test_materialized_dataset_skips_component_manifest_lookup():
    kernel = _FakeKernel()
    repository = DatasetRepository(kernel)

    repository.manifest("physical-id", object())
    assert repository.composition_components("physical-id", object()) == []
    assert not any(
        call[0] == "backtest.dataset_component" for call in kernel.read.calls
    )


def test_composite_single_day_bar_read_routes_directly_to_physical_dataset():
    kernel = _FakeKernel()
    repository = DatasetRepository(kernel)
    rows = repository.bar_page(
        "logical-id", object(), date_from=D2, date_to=D2,
    )
    assert rows == [{"trade_date": D2, "ts_code": "600000.SH"}]
    data_call = kernel.read.calls[-1]
    assert data_call[0] == "backtest.dataset_execution_bar"
    assert data_call[2][0] == "physical-id"


def test_composite_range_read_is_date_bounded_and_joins_components():
    kernel = _FakeKernel()
    repository = DatasetRepository(kernel)
    repository.bar_page(
        "logical-id", object(), date_from=D1, date_to=D3,
    )
    data_call = kernel.read.calls[-1]
    assert data_call[0] == "backtest.dataset_execution_bar"
    assert "JOIN backtest.dataset_execution_bar" in data_call[1]
    assert "b.trade_date >= %s" in data_call[1]
    assert "b.trade_date <= %s" in data_call[1]
    assert data_call[2] == ("logical-id", D1, D3)


def test_composite_execution_hash_scan_is_rejected_before_reading_fact_rows():
    kernel = _FakeKernel()
    repository = DatasetRepository(kernel)
    with pytest.raises(RuntimeError, match="must not rescan merged execution facts"):
        repository.execution_page("logical-id", object(), limit=100)
    assert not any(
        call[0] == "backtest.dataset_execution_bar" for call in kernel.read.calls
    )


def test_composite_without_components_fails_closed_for_reads():
    kernel = _FakeKernel()
    repository = DatasetRepository(kernel)
    with pytest.raises(RuntimeError, match="has no component rows"):
        repository.composition_components("empty-composite", object())
