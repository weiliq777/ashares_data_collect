from datetime import date

import pytest

from data_collect.jobs import research_snapshot
from data_collect.jobs.research_snapshot import (
    INPUTS,
    REQUIRED_LINEAGE_GROUPS,
    _manifest_hash,
    _row_bytes,
    _lineage_sql,
    _stock_snapshot_sql,
)
from data_collect.data_access.repository.research_snapshot_repository import ResearchSnapshotRepository


def test_snapshot_freeze_rechecks_baseline_and_quality_gate():
    source = __import__("pathlib").Path("data_collect/jobs/research_snapshot.py").read_text(encoding="utf-8")
    repository = __import__("pathlib").Path(
        "data_collect/data_access/repository/research_snapshot_repository.py"
    ).read_text(encoding="utf-8")
    assert "def baseline" in repository
    assert 'quality_hash != manifest["quality_hash"]' in source
    assert "def manifest(" in repository
    assert "for_update" in repository


def test_snapshot_input_manifest_precompute_preserves_order_and_hash():
    repository = ResearchSnapshotRepository.__new__(ResearchSnapshotRepository)
    calls = []

    def fake_input_stats(definition, cutoff, source_manifest, context, row_bytes):
        calls.append(definition)
        return {"table_name": definition, "row_count": 1}

    repository.input_stats = fake_input_stats
    stats, digest = repository._precompute_input_manifest(
        ["first", "second"], date(2026, 8, 18), {"manifest": True}, "ctx", "row-bytes",
        lambda items: "hash:" + ",".join(item["table_name"] for item in items),
    )

    assert calls == ["first", "second"]
    assert [item["table_name"] for item in stats] == ["first", "second"]
    assert digest == "hash:first,second"


def test_snapshot_input_stats_uses_one_stream_for_count_range_and_hash():
    source = __import__("pathlib").Path(
        "data_collect/data_access/repository/research_snapshot_repository.py"
    ).read_text(encoding="utf-8")
    input_stats = source.split("    def input_stats(", 1)[1].split("    def baseline(", 1)[0]
    assert "aggregate_sql" not in input_stats
    assert "row_count += 1" in input_stats
    assert "batch_rows=self.INPUT_STATS_BATCH_ROWS" in input_stats


def test_snapshot_validation_reuses_persisted_input_evidence():
    source = __import__("pathlib").Path(
        "data_collect/data_access/repository/research_snapshot_repository.py"
    ).read_text(encoding="utf-8")
    check = source.split("    def check_snapshot(", 1)[1]
    assert '"research.snapshot_input"' in check
    assert "snapshot_input_manifest_incomplete" in check
    assert "input_manifest_hash_changed" in check
    assert "self.input_stats(" not in check


def test_foundation_snapshot_rejects_legacy_release_contract():
    baseline = (
        "legacy", date(2026, 8, 6), "validated", "quality", "source",
        {}, "feature-contract-v1.0",
    )
    with pytest.raises(RuntimeError, match="feature-contract-v2.0"):
        research_snapshot._assert_baseline_schema(
            baseline, research_snapshot.FOUNDATION_SCHEMA_VERSION
        )


def test_series_quality_evidence_keeps_its_real_coverage_cutoff():
    stored = {"cutoff_date": "2026-08-06", "failures": {}}
    assert research_snapshot._stored_quality_cutoff(
        date(2026, 7, 31), stored
    ) == date(2026, 8, 6)
    with pytest.raises(RuntimeError, match="质量证据截止日早于研究截止日"):
        research_snapshot._stored_quality_cutoff(date(2026, 8, 7), stored)


def test_foundation_historical_snapshot_uses_release_quality_boundary():
    baseline = (
        "foundation", date(2026, 8, 6), "validated", "quality", "source",
        {}, research_snapshot.FOUNDATION_FEATURE_CONTRACT,
    )


    assert research_snapshot._quality_evidence_cutoff(
        baseline,
        date(2026, 8, 3),
        research_snapshot.FOUNDATION_SCHEMA_VERSION,
    ) == date(2026, 8, 6)
    assert research_snapshot._quality_evidence_cutoff(
        baseline,
        date(2026, 8, 3),
        research_snapshot.SCHEMA_VERSION,
    ) == date(2026, 8, 3)


def test_single_snapshot_build_reuses_manifest_quality_evidence_boundary():
    source = __import__("pathlib").Path(
        "data_collect/jobs/research_snapshot.py"
    ).read_text(encoding="utf-8")
    build_section = source.split("def build", 1)[1].split("def validate", 1)[0]
    assert "_current_quality_for_manifest(manifest)" in build_section
    assert "_quality_report(\n            manifest[\"cutoff\"]" not in build_section


def test_series_freeze_reuses_one_precomputed_quality_report():
    source = __import__("pathlib").Path(
        "data_collect/jobs/research_snapshot_series.py"
    ).read_text(encoding="utf-8")
    assert "quality_report=base_report" in source


def test_foundation_snapshot_missing_lineage_is_jsonb():
    source = __import__("pathlib").Path(
        "data_collect/data_access/repository/research_snapshot_repository.py"
    ).read_text(encoding="utf-8")
    assert "to_jsonb(s.ts_code || ':' || s.research_date::text || ':missing')" in source


def test_pit_validation_allows_building_before_validate_transition():
    source = __import__("pathlib").Path(
        "data_collect/data_access/repository/pit_validation_repository.py"
    ).read_text(encoding="utf-8")
    assert '{"BUILDING", "VALIDATED", "FROZEN"}' in source


def test_row_hash_is_deterministic_for_same_values():
    row = (date(2026, 8, 3), "000001.SZ", 12.34, None)
    assert _row_bytes(row) == _row_bytes(row)


def test_row_hash_changes_when_selected_value_changes():
    assert _row_bytes(("000001.SZ", 1.0)) != _row_bytes(("000001.SZ", 1.1))


def test_input_manifest_hash_includes_selection_and_columns():
    base = {
        "schema_name": "pit", "table_name": "universe_daily",
        "selection_predicate": "trade_date = :research_cutoff_date",
        "selected_columns": ["trade_date", "ts_code"],
        "hash_algorithm": "sha256", "row_count": 1,
        "min_date": date(2026, 8, 3), "max_date": date(2026, 8, 3),
        "content_hash": "a" * 64,
    }
    changed_columns = dict(base, selected_columns=["trade_date", "ts_code", "is_st_asof"])
    changed_predicate = dict(base, selection_predicate="trade_date <= :research_cutoff_date")
    assert _manifest_hash([base]) != _manifest_hash([changed_columns])
    assert _manifest_hash([base]) != _manifest_hash([changed_predicate])


def test_snapshot_input_definitions_are_explicit_and_unique():
    names = [(item["schema"], item["table"]) for item in INPUTS]
    assert len(names) == len(set(names))
    assert all(item["predicate"] and item["columns"] and item["order"] for item in INPUTS)


def test_lineage_contract_has_all_required_groups():
    assert set(REQUIRED_LINEAGE_GROUPS) == {
        "universe", "tradeability", "price", "valuation", "financial",
        "shareholder_return", "lifecycle", "instrument", "market_facts",
        "fundamental_history",
    }


def test_lineage_sql_uses_real_source_metadata_for_derived_groups():
    sql = _lineage_sql()
    for source_table in (
        "feature.price_daily_v2",
        "feature.valuation_daily_v1",
        "feature.shareholder_return_v1",
        "std.security_lifecycle",
        "tushare_stock_basic_raw",
    ):
        assert source_table in sql
    assert "source_record_version" in sql
    assert "source_payload_hash" in sql
    assert "digest(" in sql


def test_snapshot_uses_asof_lifecycle_and_not_current_status_for_history():
    sql = _stock_snapshot_sql()
    assert "u.is_listed_asof" in sql
    assert "SELECT l.name, l.market" in sql
    assert "l.effective_from <= u.trade_date" in sql
    assert "b.payload->>'list_date'" in sql


def test_snapshot_code_contains_adjusted_factor_vintage_gate():
    source = __import__("pathlib").Path(
        "data_collect/data_access/repository/research_snapshot_repository.py"
    ).read_text(encoding="utf-8")
    assert "def price_vintage" in source
    assert "a.fetched_at::date > %s" in source
    assert "price_vintage_dates" in source
