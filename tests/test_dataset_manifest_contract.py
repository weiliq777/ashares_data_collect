from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_dataset_manifest_read_includes_quality_report_for_foundation_freeze_gate():
    source = (
        ROOT / "data_collect" / "data_access" / "repository" /
        "dataset_repository.py"
    ).read_text(encoding="utf-8")
    projection = source.split("def manifest(", 1)[1].split(
        "def frozen_snapshot_ids(", 1
    )[0]
    assert "benchmark_source_manifest_hash,quality_report,quality_report_hash" in projection


def test_freeze_validation_proof_avoids_child_table_rescan():
    sql = (ROOT / "sql" / "104_backtest_dataset_freeze_validation_proof.sql").read_text(encoding="utf-8")
    assert "report->'validation_counts'" in sql
    assert "FROZEN Dataset validation proof is missing or failed" in sql
    assert "FROM backtest.dataset_execution_bar" not in sql
