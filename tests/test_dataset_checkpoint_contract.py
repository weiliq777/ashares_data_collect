from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_checkpoint_migration_defines_stage_contract():
    source = (ROOT / "sql" / "042_backtest_dataset_build_checkpoint.sql").read_text(encoding="utf-8")
    assert "backtest.dataset_build_checkpoint" in source
    assert "CHECK (stage IN ('BUILD','VALIDATE','FREEZE'))" in source
    assert "CHECK (status IN ('RUNNING','COMPLETED','FAILED'))" in source


def test_handoff_supports_stage_resume_without_changing_build_engine():
    source = (ROOT / "data_collect" / "jobs" / "backtest_dataset_handoff.py").read_text(encoding="utf-8")
    assert "--force-rebuild" in source
    assert '"BUILD"' in source and '"VALIDATE"' in source and '"FREEZE"' in source
    assert "BUILD=SKIPPED" in source
    assert "status='BUILDING', failure_reason=NULL, failed_at=NULL" in source
    assert "manifest[\"status\"] == \"VALIDATED\"" in source


def test_force_rebuild_reopens_only_validated_dataset():
    source = (ROOT / "data_collect" / "jobs" / "backtest_dataset_handoff.py").read_text(encoding="utf-8")
    assert 'if args.force_rebuild and manifest["status"] == "VALIDATED":' in source
    assert '("VALIDATED",),\n                "FAILED"' in source
    assert '"explicit force rebuild requested after validation"' in source


def test_checkpoint_does_not_touch_raw_or_standard_layers():
    source = (ROOT / "sql" / "042_backtest_dataset_build_checkpoint.sql").read_text(encoding="utf-8")
    assert "tushare_" not in source
    assert "DELETE FROM standard" not in source
    assert "DELETE FROM pit" not in source


def test_freeze_checkpoint_recovery_allows_only_failed_or_running_to_completed():
    source = (ROOT / "sql" / "044_backtest_freeze_checkpoint_recovery.sql").read_text(encoding="utf-8")
    assert "OLD.status IN ('RUNNING','FAILED')" in source
    assert "NEW.status = 'COMPLETED'" in source
    assert "tushare_" not in source
