from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_observation_schema_requires_frozen_audit_fields():
    sql = (ROOT / "sql" / "090_market_observation_domain.sql").read_text(encoding="utf-8")
    assert "calculation_run_id VARCHAR(160) NOT NULL" in sql
    assert "observation_snapshot_frozen_fields" in sql
    assert "input_manifest_hash IS NOT NULL" in sql
    assert "quality_report_hash IS NOT NULL" in sql
    assert "frozen_at IS NOT NULL" in sql


def test_frozen_guard_covers_child_inserts_and_checks_new_rows():
    sql = (ROOT / "sql" / "091_market_observation_guardrails.sql").read_text(encoding="utf-8")
    for table in (
        "observation.stock_anomaly_daily",
        "observation.stock_anomaly_tag",
        "observation.sector_anomaly_daily",
        "observation.stock_sector_confirmation",
        "observation.event_link",
    ):
        assert f"BEFORE INSERT OR UPDATE OR DELETE ON {table}" in sql
    assert "row_data := to_jsonb(NEW);" in sql
    assert "observation.clear_snapshot_date" in sql
    assert "observation.clear_snapshot_date_chunk" in sql
    assert "trg_observation_stock_identity" in sql
    assert "trg_observation_sector_scope" in sql
    assert "cardinality(p_ts_codes) > 10000" in sql


def test_observation_cleanup_is_a_named_bounded_operation():
    gateway = (ROOT / "data_collect" / "data_access" / "rebuild_gateway.py").read_text(encoding="utf-8")
    policy = (ROOT / "data_collect" / "data_access" / "policy_catalog.py").read_text(encoding="utf-8")
    assert "def clear_observation_date(" in gateway
    assert "observation.clear_snapshot_date" in gateway
    assert "allow_delete=True" in gateway
    assert "name=\"observation.stock_anomaly_daily\"" in policy
    assert "WriteMode.REBUILD" in policy


def test_observation_scope_and_run_identity_are_database_enforced():
    sql = (ROOT / "sql" / "091_market_observation_guardrails.sql").read_text(encoding="utf-8")
    assert "ALTER COLUMN calculation_run_id SET NOT NULL" in sql
    assert "parent_run_id <> NEW.calculation_run_id" in sql
    assert "NEW.observation_date < parent_start" in sql
    assert "p_clear_sector BOOLEAN" in sql


def test_observation_reads_require_valid_universe_asof_input():
    repository = (ROOT / "data_collect" / "data_access" / "repository" / "observation_repository.py").read_text(encoding="utf-8")
    assert "AND quality_status='VALID'" in repository
    assert "AND source_asof_date IS NOT NULL" in repository
    assert "AND u.source_asof_date <= u.trade_date" in repository
    assert "s.status='FROZEN'" in repository
