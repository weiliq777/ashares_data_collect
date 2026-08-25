from pathlib import Path

import pytest

from data_collect.data_access.research_contract import (
    FOUNDATION_SCHEMA_VERSIONS,
    STRATEGY_NEUTRAL_SCHEMA_VERSION,
    assert_strategy_neutral_inputs,
)
from data_collect.jobs.research_snapshot import _inputs_for_schema


def test_research_v21_uses_foundation_inputs_and_is_strategy_neutral():
    definitions = _inputs_for_schema(STRATEGY_NEUTRAL_SCHEMA_VERSION)
    assert definitions
    assert STRATEGY_NEUTRAL_SCHEMA_VERSION in FOUNDATION_SCHEMA_VERSIONS
    assert all("predicate" in definition for definition in definitions)
    assert_strategy_neutral_inputs(definitions)


def test_research_v21_rejects_strategy_columns_but_allows_membership_metadata():
    with pytest.raises(ValueError, match="strategy/application columns"):
        assert_strategy_neutral_inputs(({
            "schema": "research",
            "table": "stock_snapshot_daily",
            "columns": ("ts_code", "signal_score"),
            "predicate": "dataset membership",
        },))


def test_research_v21_snapshot_orchestration_has_no_strategy_fields():
    source = Path("data_collect/jobs/research_snapshot.py").read_text(encoding="utf-8")
    assert "STRATEGY_NEUTRAL_SCHEMA_VERSION" in source
    assert "assert_strategy_neutral_inputs" in source
