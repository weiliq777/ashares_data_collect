from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_snapshot_child_guards_are_statement_level_and_preserve_frozen_gate():
    sql = (ROOT / "sql" / "082_snapshot_statement_guards.sql").read_text(
        encoding="utf-8"
    )

    assert "FROZEN research snapshot child is immutable" in sql
    assert "REFERENCING NEW TABLE AS new_rows" in sql
    assert "REFERENCING OLD TABLE AS old_rows" in sql
    assert "FOR EACH STATEMENT" in sql
    assert "FOR EACH ROW EXECUTE FUNCTION research.assert_snapshot_child_mutable" not in sql
    for table_name in (
        "snapshot_input", "stock_snapshot_daily", "snapshot_lineage",
        "snapshot_lineage_bundle",
    ):
        assert f"'{table_name}'" in sql


def test_snapshot_date_guard_is_statement_level():
    sql = (ROOT / "sql" / "082_snapshot_statement_guards.sql").read_text(
        encoding="utf-8"
    )

    assert "assert_snapshot_date_new_rows" in sql
    assert "trg_research_stock_snapshot_date_stmt_ins" in sql
    assert "trg_research_stock_snapshot_date_stmt_upd" in sql
    assert "n.research_date <> m.research_cutoff_date" in sql
