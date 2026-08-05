from datetime import date

from data_collect.jobs.research_snapshot import (
    INPUTS,
    REQUIRED_LINEAGE_GROUPS,
    _manifest_hash,
    _row_bytes,
    _lineage_sql,
)


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
        "shareholder_return", "lifecycle", "instrument",
    }


def test_lineage_sql_uses_real_source_metadata_for_derived_groups():
    sql = _lineage_sql()
    for source_table in (
        "feature.price_daily_v1",
        "feature.valuation_daily_v1",
        "feature.shareholder_return_v1",
        "std.security_lifecycle",
        "tushare_stock_basic_raw",
    ):
        assert source_table in sql
    assert "source_record_version" in sql
    assert "source_payload_hash" in sql
    assert "digest(" in sql
