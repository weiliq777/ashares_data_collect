from datetime import date

from data_collect.jobs.research_snapshot import (
    INPUTS,
    REQUIRED_LINEAGE_GROUPS,
    _manifest_hash,
    _row_bytes,
    _lineage_sql,
    _stock_snapshot_sql,
)


def test_snapshot_freeze_rechecks_baseline_and_quality_gate():
    source = __import__("pathlib").Path("data_collect/jobs/research_snapshot.py").read_text(encoding="utf-8")
    assert '_baseline(manifest["cutoff"], manifest["baseline"], conn)' in source
    assert 'quality_hash != manifest["quality_hash"]' in source
    assert "def _manifest(conn, snapshot_id: str, *, for_update: bool = False)" in source
    assert "_manifest(conn, snapshot_id, for_update=True)" in source


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
    source = __import__("pathlib").Path("data_collect/jobs/research_snapshot.py").read_text(encoding="utf-8")
    assert "def _assert_adj_factor_vintage" in source
    assert "a.fetched_at::date > %s" in source
    assert "_assert_price_vintage_for_schema" in source
