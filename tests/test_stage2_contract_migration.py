from pathlib import Path


def test_stage2_runtime_consumers_do_not_read_legacy_rule_table():
    repository_files = (
        "data_collect/data_access/repository/candidate_repository.py",
        "data_collect/data_access/repository/dataset_repository.py",
        "data_collect/data_access/repository/backtest_run_repository.py",
        "data_collect/data_access/repository/backtest_rules_repository.py",
    )
    for filename in repository_files:
        source = Path(filename).read_text(encoding="utf-8")
        assert "backtest.market_rule_effective" not in source
    assert "foundation.market_rule_effective" in Path(
        "data_collect/data_access/repository/candidate_repository.py"
    ).read_text(encoding="utf-8")
    assert "backtest.app_execution_params" in Path(
        "data_collect/data_access/repository/dataset_repository.py"
    ).read_text(encoding="utf-8")


def test_stage2_split_migration_is_additive_and_append_only():
    sql = Path("sql/052_split_market_execution_contract.sql").read_text(encoding="utf-8")
    assert "CREATE SCHEMA IF NOT EXISTS foundation" in sql
    assert "CREATE TABLE IF NOT EXISTS foundation.market_rule_effective" in sql
    assert "CREATE TABLE IF NOT EXISTS foundation.tax_rule_effective" in sql
    assert "ON CONFLICT" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    assert "DROP TABLE" not in sql
