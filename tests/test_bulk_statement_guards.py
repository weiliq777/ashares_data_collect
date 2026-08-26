from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_existing_snapshot_partitions_get_statement_guards():
    sql = (ROOT / "sql" / "083_snapshot_partition_guard_repair.sql").read_text(
        encoding="utf-8"
    )
    assert "install_stock_snapshot_partition_guards" in sql
    assert "pg_inherits" in sql
    assert "DROP TRIGGER IF EXISTS trg_research_stock_snapshot_mutable" in sql
    assert "FOR EACH STATEMENT" in sql


def test_large_backtest_dataset_children_do_not_use_row_guards():
    sql = (ROOT / "sql" / "084_backtest_dataset_statement_guards.sql").read_text(
        encoding="utf-8"
    )
    assert "dataset_execution_bar" in sql
    assert "REFERENCING NEW TABLE AS new_rows" in sql
    assert "REFERENCING OLD TABLE AS old_rows" in sql
    assert "FOR EACH STATEMENT" in sql
    assert "FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_child_mutable" not in sql
