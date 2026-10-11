from pathlib import Path


def test_table_replace_staging_includes_source_indexes_and_constraints():
    sql = Path("sql/101_table_replace_definer.sql").read_text(encoding="utf-8").lower()

    assert "like %s including defaults including indexes" in sql
    assert "but not indexes" not in sql


def test_repair_migration_restores_lifecycle_keys_and_lookup_indexes():
    sql = Path("sql/112_restore_security_lifecycle_indexes.sql").read_text(
        encoding="utf-8"
    ).lower()

    assert "create unique index concurrently if not exists security_lifecycle_pkey" in sql
    assert "primary key using index security_lifecycle_pkey" in sql
    assert "uq_security_lifecycle_effective_interval" in sql
    assert "idx_security_lifecycle_code_date" in sql
    assert "idx_security_lifecycle_source_lineage" in sql
