from pathlib import Path


def test_daily_candidate_migration_has_manual_airgap_and_daily_binding():
    sql = Path("sql/034_daily_candidate_ai_audit.sql").read_text(encoding="utf-8")
    for marker in (
        "research.candidate_run",
        "research.candidate",
        "research.ai_audit",
        "research.human_decision",
        "portfolio.snapshot_binding",
        "DAILY_SIGNAL",
        "PENDING_APPROVAL",
        "requires an APPROVE human decision",
        "audit_risk_level = 'HIGH'",
    ):
        assert marker in sql
    assert "trading_calendar" in sql


def test_skill_is_strict_and_does_not_claim_to_trade():
    skill = Path("skills/ashares-decision-audit/SKILL.md").read_text(encoding="utf-8")
    assert "ai-audit-v1" in skill
    assert "broker, scheduler" in skill
    assert "PENDING_APPROVAL" in skill
    assert "candidate_id" in skill


def test_forward_execution_resizes_only_pending_intents():
    service = Path("data_collect/decision/audit_service.py").read_text(encoding="utf-8")
    assert "prepare_daily_signal_execution" in service
    assert "status='PENDING_APPROVAL'" in service
    assert "resized_at_execution" in service
    assert "never approves or fills" in service
    assert "available_cash_balance - equity * reserve_cash_pct" in service
    portfolio_job = Path("data_collect/jobs/portfolio_simulation.py").read_text(encoding="utf-8")
    assert "available_cash - equity_base * float(reserve_pct)" in portfolio_job
    assert "FROM portfolio.performance_daily" in portfolio_job


def test_candidate_generation_binds_frozen_observations_portfolio_and_rules():
    service = Path("data_collect/decision/candidate_service.py").read_text(encoding="utf-8")
    observation_section = service.split("def _observations", 1)[1].split("def _portfolio_state", 1)[0]
    assert "daily_kline" not in observation_section
    assert "portfolio_state_hash" in service
    assert "observation_input_hash" in service
    assert "rule_snapshot_hash" in service
    assert "strategy_version=%s AND input_hash=%s" in service
    assert "current_cash - equity_base * reserve_cash_pct" in service
    assert "available_date<=%s" in service
    assert '"available_cash": available_cash' in service
    assert "account_reserve" in service
    assert "必须与已绑定 Portfolio 账户一致" in service
