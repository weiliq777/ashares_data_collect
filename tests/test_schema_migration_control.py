from pathlib import Path

import pytest


def test_schema_cutover_sql_is_metadata_only_and_has_terminal_guards():
    sql = Path("sql/053_schema_cutover_control.sql").read_text(encoding="utf-8").lower()
    assert "ops.foundation_table_migration" in sql
    assert "status = 'validated'" in sql
    assert "cutover requires a previously validated migration" in sql
    assert "rollback requires a previously cutover migration" in sql
    assert "invalid migration transition" in sql
    assert "alter table" not in sql
    assert "drop table" not in sql
    assert "insert into" not in sql


def test_schema_migration_repository_requires_matching_validation_hashes():
    from data_collect.data_access.repository.schema_migration_repository import SchemaMigrationRepository

    with pytest.raises(ValueError, match="hashes differ"):
        SchemaMigrationRepository.record_validation(
            object(), None, migration_id="not-used", source_row_count=1,
            target_row_count=1, source_content_hash="a" * 64,
            target_content_hash="b" * 64,
        )
