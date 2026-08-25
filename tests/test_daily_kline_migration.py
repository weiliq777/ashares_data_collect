from pathlib import Path


def test_daily_kline_target_is_partitioned_and_non_destructive():
    sql = Path("sql/057_daily_kline_partitioned_pilot.sql").read_text(encoding="utf-8").lower()
    assert "create table if not exists std.daily_kline" in sql
    assert "partition by range (trade_date)" in sql
    assert "std.daily_kline_2026" in sql
    assert "drop table" not in sql
    assert "truncate" not in sql


def test_daily_kline_migration_is_windowed_hash_validated_and_resumable():
    repository = Path("data_collect/data_access/repository/schema_migration_repository.py").read_text(encoding="utf-8")
    job = Path("data_collect/jobs/migrate_daily_kline.py").read_text(encoding="utf-8")
    assert "prepare_daily_kline_window" in repository
    assert "validate_daily_kline_window" in repository
    assert "source_content_hash" in repository
    assert "kernel.checkpoint.load" in job
    assert "WINDOW_DAYS = 7" in job
    assert "record_validation" in job
    assert "SOURCE_START" in job and "SOURCE_END" in job
    assert "terminal state" in job
    assert job.count("validate_daily_kline_window(") >= 3
    assert "repaired window mismatch" in job
    assert "estimated_rows=50_000" in repository
    assert "session_exclusive" in job
    assert '"std.daily_kline.partition-migration"' in job
    assert "cutover_daily_kline_partitioned" in job
    assert "auto_cutover" in job


def test_daily_kline_cutover_is_owner_safe_and_retains_legacy_source():
    sql = Path("sql/066_daily_kline_cutover_definer.sql").read_text(encoding="utf-8").lower()
    assert "security definer" in sql
    assert "alter table public.daily_kline rename to" in sql
    assert "drop table" not in sql
    assert "grant execute" in sql
    assert "revoke all on table" in sql


def test_daily_kline_runtime_consumers_are_qualified_to_standard_schema():
    paths = (
        "data_collect/data_access/repository/collector_repository.py",
        "data_collect/data_access/repository/feature_repository.py",
        "data_collect/data_access/repository/market_repository.py",
        "data_collect/data_access/repository/pit_repository.py",
        "data_collect/data_access/repository/quality_inspection_repository.py",
        "data_collect/data_access/repository/research_snapshot_repository.py",
        "data_collect/data_access/repository/security_repository.py",
        "data_collect/data_access/repository/wave3_repository.py",
        "data_collect/jobs/research_snapshot.py",
    )
    forbidden = ("FROM DAILY_KLINE", "JOIN DAILY_KLINE", "FROM PUBLIC.DAILY_KLINE")
    for path in paths:
        source = Path(path).read_text(encoding="utf-8").upper()
        assert not any(token in source for token in forbidden), path


def test_adj_factor_migration_is_bounded_and_uses_the_standard_target():
    sql = Path("sql/067_adj_factor_daily_partitioned_target.sql").read_text(encoding="utf-8").lower()
    repository = Path("data_collect/data_access/repository/schema_migration_repository.py").read_text(encoding="utf-8")
    job = Path("data_collect/jobs/migrate_adj_factor_daily.py").read_text(encoding="utf-8")
    assert "partition by range (trade_date)" in sql
    assert "std.adj_factor_daily_2026" in sql
    assert "prepare_adj_factor_daily_window" in repository
    assert "validate_adj_factor_daily_window" in repository
    assert "kernel.checkpoint.load" in job
    assert "WINDOW_DAYS = 7" in job
    assert "public.adj_factor_daily" in job
    assert "cutover_only" in job


def test_daily_kline_writers_use_the_canonical_standard_target():
    for path in (
        "data_collect/data_access/repository/collector_repository.py",
        "data_collect/data_access/repository/market_repository.py",
        "data_collect/data_access/repository/raw_standard_repository.py",
    ):
        source = Path(path).read_text(encoding="utf-8")
        assert "std.daily_kline" in source
        assert "target=\"daily_kline\"" not in source
