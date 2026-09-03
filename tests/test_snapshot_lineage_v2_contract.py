from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_compact_lineage_migration_is_partitioned_and_does_not_touch_upstream_data():
    sql = (ROOT / "sql" / "092_snapshot_lineage_bundle_v2.sql").read_text(
        encoding="utf-8"
    )
    lower = sql.lower()
    assert "partition by range (research_date)" in lower
    assert "snapshot_lineage_bundle_default" in lower
    assert "lineage_source_record" in lower
    assert "research.snapshot_lineage_bundle" in lower
    assert "raw" in lower and "standard" in lower and "pit" in lower
    assert "drop table" not in lower
    assert "truncate" not in lower


def test_compact_lineage_runtime_permissions_are_narrow():
    sql = (ROOT / "sql" / "092_snapshot_lineage_bundle_v2.sql").read_text(
        encoding="utf-8"
    )
    assert "GRANT SELECT, INSERT ON research.lineage_source_record TO ashares_app" in sql
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON research.snapshot_lineage_bundle" in sql
    assert "GRANT ALL" not in sql.upper()
