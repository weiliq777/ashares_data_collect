from pathlib import Path


def test_portfolio_migration_is_auditable_and_does_not_touch_raw():
    sql = Path("sql/029_create_portfolio_model.sql").read_text(encoding="utf-8")
    for token in (
        "portfolio.account_manifest",
        "portfolio.order_intent",
        "portfolio.trade_fill",
        "portfolio.position_lot",
        "portfolio.cash_ledger",
        "portfolio.performance_daily",
        "portfolio.audit_event",
        "portfolio.assert_account_status",
        "portfolio.assert_trade_fill",
        "PAPER_MANUAL",
        "FROZEN",
        "cash_settlement_days",
        "source_snapshot_id",
        "valuation_input_hash",
        "assert_valuation_append_only",
        "new portfolio accounts must start in DRAFT status",
        "new portfolio intents must start in PENDING_APPROVAL status",
        "APPROVED portfolio intent requires approved_by",
        "portfolio intent decision date must equal snapshot cutoff",
        "T+1 portfolio intent requires execution date after decision date",
        "portfolio fill fees do not match bound account rules",
        "portfolio intent decision date is not an SSE open date",
        "portfolio fill date is not an SSE open date",
        "portfolio cash dates must be SSE open dates",
        "portfolio position dates must be SSE open dates",
        "first portfolio cash ledger row must initialize account cash",
        "portfolio cash ledger can contain only one INIT row",
        "portfolio cash ledger balance does not reconcile",
        "uq_portfolio_cash_ledger_init",
        "portfolio cash ledger requires an existing account",
        "FOR UPDATE",
        "portfolio position lot remaining shares cannot increase",
        "EXECUTED portfolio intent requires a trade fill",
    ):
        assert token in sql
    assert "DROP TABLE" not in sql
    assert "CREATE TABLE IF NOT EXISTS tushare_" not in sql


def test_portfolio_job_is_manual_only_and_requires_frozen_snapshot():
    source = Path("data_collect/jobs/portfolio_simulation.py").read_text(encoding="utf-8")
    assert "FROZEN Research Snapshot" in source
    assert "人工批准的模拟成交" in source
    assert "PAPER_MANUAL" in Path("sql/029_create_portfolio_model.sql").read_text(encoding="utf-8")
    assert "不生成买卖信号" in source
    assert "不访问券商" in source
    assert "卖出资金结算日必须显式提供" in source
    assert "portfolio intent stock is absent from snapshot" in Path("sql/029_create_portfolio_model.sql").read_text(encoding="utf-8")
    assert "portfolio intent must use the account watchlist" in Path("sql/029_create_portfolio_model.sql").read_text(encoding="utf-8")
    assert "未来成交已写入后回补过去估值" in source
    assert "同一组合/日期已有不同估值输入" in source
    assert "必须严格对应 FROZEN Snapshot 截止日" in source
    assert "FROZEN Snapshot 原始收盘价不一致" in source
