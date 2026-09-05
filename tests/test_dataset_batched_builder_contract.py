from pathlib import Path

from data_collect.jobs.backtest_dataset_batched import (
    _aggregate_execution_quality,
    _quality_counter_key,
)

ROOT = Path(__file__).resolve().parents[1]


def test_batch_migration_is_date_scoped_and_resumable():
    source = (ROOT / "sql" / "043_backtest_dataset_batch_checkpoint.sql").read_text(encoding="utf-8")
    assert "backtest.dataset_build_batch_checkpoint" in source
    assert "batch_start DATE" in source
    assert "batch_end DATE" in source
    assert "CHECK (status IN ('RUNNING','COMPLETED','FAILED'))" in source
    assert "tushare_" not in source


def test_batched_builder_filters_source_query_by_date_range():
    source = (ROOT / "data_collect" / "jobs" / "backtest_dataset_batched.py").read_text(encoding="utf-8")
    repository_source = (ROOT / "data_collect" / "data_access" / "repository" / "dataset_repository.py").read_text(encoding="utf-8")
    assert "m.research_date BETWEEN %s AND %s" in source
    assert "DELETE FROM backtest.dataset_execution_bar" in source
    assert "trade_date BETWEEN %s AND %s" in source
    assert "_materialized_hashes" in source
    assert "WITH base AS MATERIALIZED" in repository_source
    assert "market AS MATERIALIZED" in repository_source
    assert "market_source AS MATERIALIZED" in repository_source
    assert "limit_source AS MATERIALIZED" in repository_source
    assert "suspend_source AS MATERIALIZED" in repository_source
    assert "mv.ts_code=k.code AND mv.trade_date=k.day" in repository_source
    assert "limits AS MATERIALIZED" in repository_source
    assert "execution_row_count(" in source
    assert "actual_rows == expected_rows" in source
    repository_source = (ROOT / "data_collect" / "data_access" / "repository" / "dataset_repository.py").read_text(encoding="utf-8")
    assert "restart_checkpoint=True" in repository_source
    assert repository_source.count("restart_checkpoint=True") >= 5


def test_batched_builder_counts_all_row_quality_states():
    assert _quality_counter_key("VALID") == "valid"
    assert _quality_counter_key("WARNING") == "warning"
    assert _quality_counter_key("INVALID") == "invalid"
    assert _quality_counter_key("MISSING_SOURCE") == "missing"


def test_finalize_preserves_worst_quality_from_materialized_rows():
    assert _aggregate_execution_quality({"VALID": 10}) == "VALID"
    assert _aggregate_execution_quality({"VALID": 9, "WARNING": 1}) == "WARNING"
    assert _aggregate_execution_quality({"VALID": 9, "MISSING_SOURCE": 1}) == "MISSING_SOURCE"
    assert _aggregate_execution_quality({"WARNING": 9, "INVALID": 1}) == "INVALID"
    assert _aggregate_execution_quality({}) == "MISSING_SOURCE"


def test_batched_builder_does_not_use_global_execution_delete_or_truncate():
    source = (ROOT / "data_collect" / "jobs" / "backtest_dataset_batched.py").read_text(encoding="utf-8")
    assert "TRUNCATE" not in source.upper()
    assert 'DELETE FROM backtest.dataset_execution_bar WHERE dataset_id=%s"' not in source


def test_handoff_defaults_to_batched_builder():
    source = (ROOT / "data_collect" / "jobs" / "backtest_dataset_handoff.py").read_text(encoding="utf-8")
    assert "build_dataset_batched" in source
    assert "--batch-days" in source
    assert "force_rebuild=args.force_rebuild" in source


def test_batched_builder_holds_warehouse_write_lock_at_public_boundary():
    source = (ROOT / "data_collect" / "jobs" / "backtest_dataset_batched.py").read_text(
        encoding="utf-8"
    )
    assert "from data_collect.jobs.tushare_job_lock import warehouse_write_job" in source
    assert "@warehouse_write_job\ndef build_dataset_batched" in source


def test_handoff_holds_same_lock_across_build_validate_and_freeze():
    source = (ROOT / "data_collect" / "jobs" / "backtest_dataset_handoff.py").read_text(
        encoding="utf-8"
    )
    assert "from data_collect.jobs.tushare_job_lock import warehouse_write_job" in source
    assert "@warehouse_write_job\ndef main" in source
    assert "_build_dataset_batched(" in source


def test_benchmark_repair_is_explicit_and_does_not_mutate_baseline():
    repository = (ROOT / "data_collect" / "data_access" / "repository" / "dataset_repository.py").read_text(encoding="utf-8")
    job = (ROOT / "data_collect" / "jobs" / "repair_backtest_benchmark.py").read_text(encoding="utf-8")
    migration = (ROOT / "sql" / "094_dataset_benchmark_source_repair.sql").read_text(encoding="utf-8")
    assert "def bind_benchmark_source_repair" in repository
    assert "record_id > %s AND record_id <= %s" in repository
    assert "benchmark_source_repair_rows" in repository
    assert "base_source_manifest_hash" in repository
    assert "def update_dataset_member_stats_batch" in (ROOT / "data_collect" / "data_access" / "write_gateway.py").read_text(encoding="utf-8")
    assert "Dataset member stats update missed rows" in repository
    assert "@warehouse_write_job" in job
    assert "benchmark_source_manifest" in migration
    assert "DROP TABLE" not in migration.upper()
