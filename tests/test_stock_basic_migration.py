from pathlib import Path


def test_stock_basic_pilot_is_explicit_and_retains_legacy_source():
    sql = Path("sql/055_stock_basic_pilot.sql").read_text(encoding="utf-8").lower()
    assert "create table if not exists std.stock_basic_info" in sql
    assert "primary key (stock_code)" in sql
    assert "drop table" not in sql
    source = Path("data_collect/jobs/migrate_stock_basic.py").read_text(encoding="utf-8")
    assert "SchemaMigrationRepository" in source
    assert "automated-validation" in source


def test_stock_basic_pilot_has_deterministic_copy_and_validation():
    source = Path("data_collect/data_access/repository/schema_migration_repository.py").read_text(encoding="utf-8")
    assert "def prepare_stock_basic" in source
    assert "def validate_stock_basic" in source
    assert "source_content_hash" in source
    assert "target_content_hash" in source
    assert "run_insert_select" in source


def test_stock_basic_reads_and_writes_use_standard_target_only():
    paths = (
        "data_collect/data_access/repository/financial_repository.py",
        "data_collect/data_access/repository/history_repository.py",
        "data_collect/data_access/repository/pit_repository.py",
        "data_collect/data_access/repository/phase2_raw_repository.py",
        "data_collect/data_access/repository/raw_repository.py",
        "data_collect/data_access/repository/quality_inspection_repository.py",
        "data_collect/data_access/repository/security_repository.py",
    )
    for path in paths:
        text = Path(path).read_text(encoding="utf-8")
        assert "std.stock_basic_info" in text
    instrument = Path("data_collect/data_access/repository/instrument_repository.py").read_text(encoding="utf-8")
    assert '"std.stock_basic_info"' in instrument
    assert '"stock_basic_info"' not in instrument
