from datetime import date
from pathlib import Path

from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.jobs.migrate_research_snapshot_partitioned import windows


def test_release_contract_has_revocation_and_temporal_evidence():
    sql = Path("sql/063_formal_release_temporal_contract.sql").read_text(encoding="utf-8")
    assert "data_cutoff_timestamp" in sql
    assert "revoked" in sql
    assert "ops.data_release_event" in sql
    assert "DELETE FROM" not in sql


def test_snapshot_partition_target_is_additive_and_partitioned():
    sql = Path("sql/064_research_snapshot_partition_target.sql").read_text(encoding="utf-8")
    assert "PARTITION BY RANGE (research_date)" in sql
    assert "stock_snapshot_daily_partitioned" in sql
    assert "DROP TABLE" not in sql


def test_snapshot_migration_uses_one_day_windows_and_target_policy():
    assert list(windows(date(2026, 1, 1), date(2026, 1, 3))) == [
        (date(2026, 1, 1), date(2026, 1, 2)),
        (date(2026, 1, 2), date(2026, 1, 3)),
        (date(2026, 1, 3), date(2026, 1, 4)),
    ]
    policy = build_default_registry().require("research.stock_snapshot_daily_partitioned")
    assert policy.partition_key == ("research_date",)
    assert policy.require_snapshot_id is False


def test_snapshot_migration_repair_is_snapshot_scoped_without_partition_wide_conflict_probe():
    source = Path(
        "data_collect/data_access/repository/schema_migration_repository.py"
    ).read_text(encoding="utf-8")
    repair_sql = source.split("def repair_research_snapshot_window", 1)[1].split(
        "def validate_research_snapshot_window", 1
    )[0]
    assert "USING research.snapshot_manifest m" in repair_sql
    assert "m.snapshot_id=t.snapshot_id" in repair_sql
    assert "ON CONFLICT" not in repair_sql
    job = Path(
        "data_collect/jobs/migrate_research_snapshot_partitioned.py"
    ).read_text(encoding="utf-8")
    assert 'evidence["target_row_count"] == 0' in job
    assert "validate_research_snapshot_window" in job


def test_snapshot_migration_is_single_instance_locked():
    job = Path(
        "data_collect/jobs/migrate_research_snapshot_partitioned.py"
    ).read_text(encoding="utf-8")
    assert "session_exclusive" in job
    assert '"research.snapshot.partition-migration"' in job


def test_snapshot_cutover_uses_staging_policy_for_physical_swap():
    gateway = Path("data_collect/data_access/rebuild_gateway.py").read_text(
        encoding="utf-8"
    )
    section = gateway.split(
        "def swap_research_snapshot_partitioned", 1
    )[1].split("def _run_set_based", 1)[0]
    assert 'target="research.stock_snapshot_daily_partitioned"' in section
    sql = Path("sql/065_research_snapshot_cutover_definer.sql").read_text(
        encoding="utf-8"
    )
    assert "SECURITY DEFINER" in sql
    assert "GRANT EXECUTE" in sql
    assert "ALTER TABLE research.stock_snapshot_daily" in sql
