from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_production_pipeline_is_raw_first_and_serial():
    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    tasks = config["pipelines"]["tushare_production_daily"]["tasks"]
    names = [task["name"] for task in tasks]
    assert names == [
        "production_instrument_raw", "production_market_raw", "production_phase2_raw",
        "production_financial_raw", "production_financial_derived",
        "production_market_derived", "production_quality",
    ]
    assert tasks[1]["kwargs"]["rebuild_derived"] is False
    assert tasks[2]["kwargs"]["rebuild_standard"] is False
    assert tasks[3]["kwargs"]["rebuild_derived"] is False
    assert "schedule" not in config["pipelines"]["tushare_production_daily"]


def test_production_pipeline_quality_is_bounded_and_does_not_full_scan():
    source = (ROOT / "data_collect/data_access/repository/production_quality_repository.py").read_text(encoding="utf-8")
    assert "BETWEEN %s AND %s" in source
    assert "DISTINCT ON (dataset, trade_date)" in source
    assert "created_at DESC, gate_id DESC" in source
    assert "TRUNCATE" not in source
    assert "GROUP BY" not in source


def test_pipeline_entrypoint_exposes_dry_run():
    source = (ROOT / "tools/run_pipeline_once.py").read_text(encoding="utf-8")
    assert 'parser.add_argument("--dry-run"' in source
    assert 'base_kwargs["dry_run"] = True' in source


def test_foundation_pipeline_can_reuse_existing_standard_layer():
    source = (ROOT / "data_collect/jobs/tushare_foundation_pipeline.py").read_text(encoding="utf-8")
    assert 'parser.add_argument(\n        "--reuse-standard"' in source
    assert "if not reuse_standard:" in source
    assert '"reuse_standard": reuse_standard' in source


def test_quality_gate_has_window_indexes_for_status_counts():
    sql = (ROOT / "sql/061_quality_gate_window_indexes.sql").read_text(encoding="utf-8")
    assert sql.count("CREATE INDEX CONCURRENTLY IF NOT EXISTS") == 5
    assert sql.count("quality_status") >= 5


def test_phase2_calendar_refresh_is_date_bounded():
    source = (ROOT / "data_collect/jobs/tushare_phase2_incremental.py").read_text(encoding="utf-8")
    standard = (ROOT / "data_collect/jobs/tushare_raw_to_standard.py").read_text(encoding="utf-8")
    assert "rebuild_calendar(start_date=_date(start), end_date=_date(end))" in source
    assert "def rebuild_calendar(start_date=None, end_date=None)" in standard
    assert "start_date=start_date, end_date=end_date" in standard
