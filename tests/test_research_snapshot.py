from datetime import date
from types import SimpleNamespace

import pytest

from data_collect.jobs import research_snapshot
from data_collect.jobs.research_snapshot import (
    INPUTS,
    REQUIRED_LINEAGE_GROUPS,
    _manifest_hash,
    _row_bytes,
    _lineage_sql,
    _inputs_for_schema,
    _stock_snapshot_sql,
)
from data_collect.data_access.repository.research_snapshot_repository import ResearchSnapshotRepository
from data_collect.jobs.research_snapshot import freeze_snapshots_batch


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


def test_snapshot_input_stats_uses_bounded_keyset_pages_for_wide_inputs():
    calls = []
    repository = ResearchSnapshotRepository.__new__(ResearchSnapshotRepository)
    repository.INPUT_STATS_PAGE_ROWS = 2

    def fake_query(context, source, sql, params, scope, max_rows):
        calls.append((source, sql, params, max_rows))
        last_code = params[-2]
        pages = {
            "": [
                {"ts_code": "000001.SZ", "trade_date": date(2021, 8, 10)},
                {"ts_code": "000002.SZ", "trade_date": date(2021, 8, 10)},
            ],
            "000002.SZ": [
                {"ts_code": "000003.SZ", "trade_date": date(2021, 8, 10)},
            ],
        }
        return pages[last_code]

    repository.kernel = SimpleNamespace(
        read=SimpleNamespace(_repository_query=fake_query),
    )
    definition = {
        "schema": "feature",
        "table": "price_daily_v2",
        "columns": ("ts_code", "trade_date"),
        "where": "trade_date = %s",
        "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code",
        "date_column": "trade_date",
        "predicate": "trade_date = :research_cutoff_date",
    }

    result = repository.input_stats(
        definition, date(2021, 8, 10), None, "context", _row_bytes
    )

    assert result["row_count"] == 3
    assert len(calls) == 2
    assert all(call[3] == 2 for call in calls)
    assert calls[0][2] == (date(2021, 8, 10), "", 2)
    assert calls[1][2] == (date(2021, 8, 10), "000002.SZ", 2)
    assert '"ts_code" > %s' in calls[0][1]


def test_snapshot_code_batches_stay_below_statement_deadline_budget():
    source = __import__("pathlib").Path(
        "data_collect/data_access/repository/research_snapshot_repository.py"
    ).read_text(encoding="utf-8")
    assert "SNAPSHOT_CODE_BATCH_SIZE = 100" in source


def test_snapshot_uses_contract_base_adjusted_price_and_lineage_hash():
    repository_source = __import__("pathlib").Path(
        "data_collect/data_access/repository/research_snapshot_repository.py"
    ).read_text(encoding="utf-8")
    job_source = __import__("pathlib").Path(
        "data_collect/jobs/research_snapshot.py"
    ).read_text(encoding="utf-8")

    assert "p.adj_close_base, p.feature_set_version" in repository_source
    assert "p.asof_close, p.feature_set_version" not in repository_source
    assert "'adj_close_base', p.adj_close_base" in repository_source
    assert '"raw_close", "adj_close_base"' in job_source
    assert '"asof_close"' not in job_source


def test_snapshot_cleanup_uses_cutoff_for_partitioned_children():
    calls = []
    repository = ResearchSnapshotRepository.__new__(ResearchSnapshotRepository)

    def fake_run(sql, plan, context, params):
        calls.append((sql, params))
        return 0

    repository.kernel = SimpleNamespace(
        rebuild=SimpleNamespace(run_scoped_merge=fake_run),
    )
    cutoff = date(2022, 12, 2)

    repository._delete_snapshot_rows(
        "snapshot-id", "context", "research.stock_snapshot_daily", cutoff
    )
    repository._delete_snapshot_rows(
        "snapshot-id", "context", "research.snapshot_lineage", cutoff
    )
    repository._delete_snapshot_rows(
        "snapshot-id", "context", "research.snapshot_input", cutoff
    )

    assert "research_date=%s" in calls[0][0]
    assert "WITH victims AS" in calls[0][0]
    assert calls[0][1][:2] == ("snapshot-id", cutoff)
    assert calls[0][1][2] == repository.SNAPSHOT_DELETE_CHUNK_SIZE
    assert "research_date=%s" in calls[1][0]
    assert calls[1][1][:2] == ("snapshot-id", cutoff)
    assert calls[1][1][2] == repository.SNAPSHOT_DELETE_CHUNK_SIZE
    assert "research_date=%s" not in calls[2][0]
    assert calls[2][1] == ("snapshot-id",)


def test_snapshot_cleanup_repeats_short_delete_transactions_until_empty():
    calls = []
    repository = ResearchSnapshotRepository.__new__(ResearchSnapshotRepository)
    repository.SNAPSHOT_DELETE_CHUNK_SIZE = 2
    results = iter((2, 2, 1))

    def fake_run(sql, plan, context, params):
        calls.append((sql, params))
        return next(results)

    repository.kernel = SimpleNamespace(
        rebuild=SimpleNamespace(run_scoped_merge=fake_run),
    )

    deleted = repository._delete_snapshot_rows(
        "snapshot-id", "context", "research.snapshot_lineage", date(2022, 12, 2)
    )

    assert deleted == 5
    assert len(calls) == 3
    assert all(call[1][2] == 2 for call in calls)


def test_snapshot_batch_sql_binds_code_scope_for_each_materialization_layer():
    stock_sql = ResearchSnapshotRepository.stock_snapshot_batch_sql()
    foundation_sql = ResearchSnapshotRepository.foundation_snapshot_batch_sql()
    lineage_sql = ResearchSnapshotRepository.lineage_batch_sql()

    assert "u.ts_code=ANY(%s::text[])" in stock_sql
    assert stock_sql.count("%s") == 3
    assert "s.snapshot_id=ANY(%s::uuid[])" in foundation_sql
    assert "s.research_date=ANY(%s::date[])" in foundation_sql
    assert "s.ts_code=ANY(%s::text[])" in foundation_sql
    assert foundation_sql.count("%s") == 3
    assert "codes AS (SELECT unnest(%s::text[]) AS ts_code)" in lineage_sql
    assert lineage_sql.count("%s") == 2
    assert lineage_sql.count("AND s.ts_code IN (SELECT ts_code FROM codes)") == 11


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


def test_batch_freeze_accepts_precomputed_validation_evidence():
    parameters = __import__("inspect").signature(freeze_snapshots_batch).parameters
    assert "baseline" in parameters
    assert "quality_report" in parameters
    assert "verify_source_manifest" in parameters


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


def test_foundation_inputs_declare_v1_compatibility_and_v2_financial_sources():
    definitions = _inputs_for_schema("research-v2.0")
    names = [(item["schema"], item["table"]) for item in definitions]
    assert ("feature", "company_financial_v2") in names
    assert ("feature", "company_financial_v1") in names
    assert len(names) == len(set(names))
    # The base materialization still reads V1 compatibility fields; keeping
    # that source in the manifest prevents a hidden input dependency.
    assert "FROM feature.company_financial_v1 c" in _stock_snapshot_sql()


def test_foundation_lineage_points_financial_group_to_selected_v2_record():
    sql = ResearchSnapshotRepository.lineage_sql(foundation=True)
    assert "'feature.company_financial_v2', COALESCE" in sql
    assert "'feature.company_financial_v1', COALESCE" not in sql
    # The historical-summary compatibility group remains explicitly V1.
    assert "feature.company_financial_v1:pit-visible-summary" in sql
    enrichment = ResearchSnapshotRepository.foundation_snapshot_sql()
    for column in (
        "financial_record_version", "financial_source_record_key",
        "financial_payload_hash", "financial_source_fingerprint",
        "financial_vintage_quality",
        "financial_source_observed_at",
    ):
        assert column in enrichment


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
