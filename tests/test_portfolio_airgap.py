from pathlib import Path


def test_backtest_portfolio_airgap_migration_is_append_only_and_manual_only():
    sql = Path("sql/032_backtest_portfolio_airgap.sql").read_text(encoding="utf-8")
    for token in (
        "source_backtest_run_id",
        "source_backtest_order_id",
        "source_dataset_id",
        "source_signal_id",
        "source_order_hash",
        "BACKTEST",
        "PENDING_APPROVAL",
        "SUCCEEDED/FROZEN",
        "FILLED",
        "trg_portfolio_backtest_airgap",
        "uq_portfolio_intent_backtest_order",
        "does not match Portfolio intent",
    ):
        assert token in sql
    assert "DROP TABLE" not in sql
    assert "TRUNCATE" not in sql
    assert "DELETE FROM tushare" not in sql


def test_import_job_never_approves_or_fills_backtest_orders():
    source = Path("data_collect/jobs/portfolio_simulation.py").read_text(encoding="utf-8")
    assert "def import_backtest_order(" in source
    assert "BACKTEST_ORDER_IMPORTED_PENDING_APPROVAL" in source
    assert "source_order_hash" in source
    assert "'PENDING_APPROVAL'" in source
    assert "status='APPROVED'" not in source[source.index("def import_backtest_order("):]
    assert "record_manual_fill" not in source[source.index("def import_backtest_order("):source.index("def set_intent_status(")]


def test_database_order_audit_hash_is_bound_to_airgap_source():
    source = Path("sql/033_backtest_order_audit_hash.sql").read_text(encoding="utf-8")
    assert "set_order_audit_hash" in source
    assert "btrim(NEW.source_order_hash) <> btrim(order_audit_hash)" in source
    assert "digest(convert_to(payload::text, 'UTF8'), 'sha256')" in source
