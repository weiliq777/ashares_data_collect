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


def test_checkpoint_does_not_touch_raw_or_standard_layers():
    source = (ROOT / "sql" / "042_backtest_dataset_build_checkpoint.sql").read_text(encoding="utf-8")
    assert "tushare_" not in source
    assert "DELETE FROM standard" not in source
    assert "DELETE FROM pit" not in source
