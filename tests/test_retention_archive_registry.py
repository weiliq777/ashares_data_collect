from pathlib import Path


def test_retention_registry_is_metadata_only_and_fail_closed_for_physical_cleanup():
    sql = Path("sql/100_retention_archive_registry_20260907.sql").read_text(
        encoding="utf-8"
    ).lower()
    assert "create table if not exists ops.retention_archive_registry" in sql
    assert "archive_state" in sql
    assert "candidate" in sql
    assert "on conflict (object_type, object_name, object_key) do nothing" in sql
    assert "drop table" not in sql
    assert "delete from" not in sql
    assert "truncate table" not in sql


def test_retention_registry_covers_all_snapshot_consumer_reference_tables():
    sql = Path("sql/100_retention_archive_registry_20260907.sql").read_text(
        encoding="utf-8"
    ).lower()
    for relation in (
        "backtest.dataset_member",
        "backtest.signal",
        "research.watchlist_member",
        "research.evidence_reference",
        "research.candidate",
        "research.candidate_run",
    ):
        assert relation in sql


def test_retention_registry_requires_explicit_approval_before_archive_state():
    sql = Path("sql/100_retention_archive_registry_20260907.sql").read_text(
        encoding="utf-8"
    ).lower()
    assert "archive_state in ('candidate', 'approved', 'archived', 'restored', 'rejected')" in sql
    assert "approved_by" in sql
    assert "approved_at" in sql
    assert "restored_at" in sql
