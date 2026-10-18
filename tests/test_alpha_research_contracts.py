from datetime import date, timedelta

import pytest

from data_collect.alpha_research.contracts import (
    LABEL_ACTION_FIELDS,
    LABEL_BAR_FIELDS,
    AlphaResearchSpec,
    FACTORS,
    ResearchRunIdentity,
)
from data_collect.data_access.repository.alpha_research_repository import AlphaResearchRepository


class _FrozenRepository:
    def __init__(self, *, status="FROZEN", schema="research-v2.1", selected=None):
        start = date(2024, 1, 2)
        self.calendar = tuple(start + timedelta(days=index) for index in range(4))
        self.row = {
            "dataset_id": "dataset-a",
            "status": status,
            "dataset_kind": "MATERIALIZED",
            "baseline": "baseline-a",
            "input_manifest_hash": "a" * 64,
            "quality_report_hash": "b" * 64,
            "quality_report": {"status": "VALID"},
            "selected_columns": selected or {"research": [
                "research_date", "ts_code", "pe_pct_5y", "is_pe_meaningful",
            ], "execution": sorted(LABEL_BAR_FIELDS),
                "corporate_action": sorted(LABEL_ACTION_FIELDS)},
            "warmup_start": start,
            "evaluation_start": start,
            "evaluation_end": self.calendar[-1],
        }
        self.contracts = [{
            "research_date": start,
            "snapshot_status": "FROZEN",
            "research_schema_version": schema,
            "research_baseline": "baseline-a",
        }]

    def manifest(self, dataset_id, context):
        return dict(self.row)

    def research_snapshot_contracts(self, dataset_id, context):
        return list(self.contracts)

    def open_calendar(self, dataset_id, context):
        return self.calendar


def test_research_adapter_requires_frozen_quality_and_supported_schema():
    context = object()
    with pytest.raises(ValueError, match="FROZEN"):
        AlphaResearchRepository(_FrozenRepository(status="VALIDATED"), "dataset-a", context)
    with pytest.raises(ValueError, match="schema"):
        AlphaResearchRepository(_FrozenRepository(schema="research-v1.3"), "dataset-a", context)


def test_composite_dataset_requires_contiguous_component_proofs():
    class CompositeRepository(_FrozenRepository):
        def __init__(self):
            super().__init__()
            self.row["dataset_kind"] = "COMPOSITE"

        def composition_components(self, dataset_id, context):
            return [{"ordinal": 1, "component_dataset_id": "child-a"}]

    with pytest.raises(ValueError, match="ordinals"):
        AlphaResearchRepository(CompositeRepository(), "dataset-a", object())


def test_factor_availability_respects_dataset_selected_columns():
    adapter = AlphaResearchRepository(_FrozenRepository(), "dataset-a", object())
    available, reason = adapter.identity.factor_availability(FACTORS[0].input_fields)
    assert available is False
    assert reason == "NOT_INCLUDED_IN_DATASET_CONTRACT:has_full_pe_5y_history"
    assert adapter.identity.factor_availability(("pe_pct_5y",))[0] is True


def test_observation_adapter_advances_keyset_pages_without_reordering():
    class PagedRepository(_FrozenRepository):
        def __init__(self):
            super().__init__(selected={
                "research": ["research_date", "ts_code", "pe_pct_5y"],
                "execution": sorted(LABEL_BAR_FIELDS),
                "corporate_action": sorted(LABEL_ACTION_FIELDS),
            })
            self.page_keys = []

        def observation_page(
            self, dataset_id, columns, context, *, last_key=None, limit=10_000,
            date_from=None, date_to=None,
        ):
            self.page_keys.append(last_key)
            if last_key is None:
                return [{"research_date": date_from, "ts_code": "000001.SZ", "pe_pct_5y": 0.1}]
            if last_key[1] == "000001.SZ":
                return [{"research_date": date_from, "ts_code": "000002.SZ", "pe_pct_5y": 0.2}]
            return []

    repository = PagedRepository()
    adapter = AlphaResearchRepository(repository, "dataset-a", object(), page_size=1)
    pages = list(adapter.observation_pages(
        repository.calendar[0], repository.calendar[0], ("pe_pct_5y",),
    ))
    assert [page[0]["ts_code"] for page in pages] == ["000001.SZ", "000002.SZ"]
    assert repository.page_keys == [None, (repository.calendar[0], "000001.SZ"), (repository.calendar[0], "000002.SZ")]


def test_alpha_run_identity_is_stable_and_parameters_change_identity():
    start = date(2024, 1, 2)
    first = AlphaResearchSpec("dataset-a", start, start + timedelta(days=2))
    same = AlphaResearchSpec("dataset-a", start, start + timedelta(days=2))
    changed = AlphaResearchSpec("dataset-a", start, start + timedelta(days=3))
    one = ResearchRunIdentity.build(
        dataset_id="dataset-a", input_hash="input", spec=first,
        selected_fields=("pe_ttm", "research_date"), schema_versions=("research-v2.1",),
        code_hash="code-v1",
    )
    two = ResearchRunIdentity.build(
        dataset_id="dataset-a", input_hash="input", spec=same,
        selected_fields=("research_date", "pe_ttm"), schema_versions=("research-v2.1",),
        code_hash="code-v1",
    )
    three = ResearchRunIdentity.build(
        dataset_id="dataset-a", input_hash="input", spec=changed,
        selected_fields=("pe_ttm", "research_date"), schema_versions=("research-v2.1",),
        code_hash="code-v1",
    )
    assert one.run_id == two.run_id
    assert one.run_id != three.run_id
    code_changed = ResearchRunIdentity.build(
        dataset_id="dataset-a", input_hash="input", spec=same,
        selected_fields=("research_date", "pe_ttm"), schema_versions=("research-v2.1",),
        code_hash="code-v2",
    )
    assert one.run_id != code_changed.run_id
    assert AlphaResearchSpec(
        "dataset-a", start, start + timedelta(days=2), horizons=(60, 20),
    ).horizons == (20, 60)


def test_alpha_spec_rejects_unsupported_horizon_and_invalid_range():
    start = date(2024, 1, 2)
    with pytest.raises(ValueError, match="horizons"):
        AlphaResearchSpec("dataset-a", start, start, horizons=(30,))
    with pytest.raises(ValueError, match="signal_start"):
        AlphaResearchSpec("dataset-a", start + timedelta(days=1), start)
