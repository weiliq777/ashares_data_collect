from pathlib import Path


def test_trading_calendar_pilot_retains_legacy_source_and_uses_kernel_repository():
    sql = Path("sql/054_trading_calendar_pilot.sql").read_text(encoding="utf-8").lower()
    assert "create table if not exists std.trading_calendar" in sql
    assert "drop table" not in sql
    source = Path("data_collect/jobs/migrate_trading_calendar.py").read_text(encoding="utf-8")
    assert "SchemaMigrationRepository" in source
    assert "public source retained" in source


def test_trading_calendar_pilot_hashes_source_and_target():
    source = Path("data_collect/data_access/repository/schema_migration_repository.py").read_text(encoding="utf-8")
    assert "source_content_hash" in source
    assert "target_content_hash" in source
    assert "run_insert_select" in source


def test_pilot_record_keeps_cutover_as_a_separate_gate():
    doc = Path("dev_doc/data_foundation_stage3_pilot_20260818.md").read_text(encoding="utf-8")
    assert "CUTOVER" in doc
    assert "读取路径 | 已切换到 `std.trading_calendar`" in doc


def test_calendar_writers_use_the_canonical_standard_target_only():
    for path in (
        "data_collect/data_access/repository/market_repository.py",
        "data_collect/data_access/repository/raw_standard_repository.py",
    ):
        source = Path(path).read_text(encoding="utf-8")
        assert "std.trading_calendar" in source
        assert 'target="trading_calendar"' not in source
        assert "dual-write count mismatch" not in source
