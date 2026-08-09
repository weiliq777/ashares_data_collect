from pathlib import Path


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
    assert "m.research_date BETWEEN %s AND %s" in source
    assert "DELETE FROM backtest.dataset_execution_bar" in source
    assert "trade_date BETWEEN %s AND %s" in source
    assert "_materialized_hashes" in source


def test_batched_builder_does_not_use_global_execution_delete_or_truncate():
    source = (ROOT / "data_collect" / "jobs" / "backtest_dataset_batched.py").read_text(encoding="utf-8")
    assert "TRUNCATE" not in source.upper()
    assert 'DELETE FROM backtest.dataset_execution_bar WHERE dataset_id=%s"' not in source


def test_handoff_defaults_to_batched_builder():
    source = (ROOT / "data_collect" / "jobs" / "backtest_dataset_handoff.py").read_text(encoding="utf-8")
    assert "build_dataset_batched" in source
    assert "--batch-days" in source
    assert "force_rebuild=args.force_rebuild" in source

