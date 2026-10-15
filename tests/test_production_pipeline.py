from pathlib import Path
from datetime import date
from inspect import signature
import importlib
from types import SimpleNamespace

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_production_pipeline_is_one_scheduled_chain_with_final_gate():
    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    pipelines = config["pipelines"]
    production = pipelines["tushare_production_daily"]
    tasks = production["tasks"]
    names = [task["name"] for task in tasks]
    assert names == [
        "production_instrument_raw", "production_market_raw", "production_phase2_raw",
        "production_market_derived", "production_financial_raw",
        "production_financial_derived", "production_quality",
        "production_market_activity",
    ]
    assert production["schedule"] == {
        "hour": 20, "minute": 0, "timezone": "Asia/Shanghai",
    }
    assert tasks[1]["kwargs"]["rebuild_derived"] is False
    assert tasks[2]["kwargs"]["rebuild_standard"] is False
    assert tasks[4]["kwargs"]["rebuild_derived"] is False
    assert tasks[3]["depends_on"] == ["production_phase2_raw"]
    assert tasks[5]["depends_on"] == ["production_financial_raw"]
    assert tasks[6]["depends_on"] == [
        "production_market_derived", "production_financial_derived",
    ]
    assert tasks[7]["depends_on"] == ["production_quality"]
    assert tasks[3]["kwargs"]["lookback_days"] == tasks[6]["kwargs"]["lookback_days"]
    assert "schedule" not in pipelines["tushare_daily"]
    assert "schedule" not in pipelines["tushare_financial"]


def test_versioned_example_config_contains_the_reviewed_production_dag():
    example = yaml.safe_load((ROOT / "config.example.yaml").read_text(encoding="utf-8"))
    pipelines = example["pipelines"]
    production = pipelines["tushare_production_daily"]
    from data_collect.pipeline import _validate_production_pipeline_contract, _validate_production_schedule_contract

    _validate_production_pipeline_contract("tushare_production_daily", production["tasks"])
    _validate_production_schedule_contract(pipelines)
    assert production["schedule"] == {
        "hour": 20, "minute": 0, "timezone": "Asia/Shanghai",
    }
    assert [task["name"] for task in production["tasks"]][-2:] == [
        "production_quality", "production_market_activity",
    ]


def test_production_pipeline_rejects_full_history_release_and_snapshot_jobs():
    from data_collect.pipeline import _validate_production_daily_tasks

    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    tasks = list(config["pipelines"]["tushare_production_daily"]["tasks"])
    tasks.append({
        "name": "unsafe_release",
        "job": "data_collect.jobs.tushare_release",
        "depends_on": ["production_quality"],
    })
    with pytest.raises(RuntimeError, match="without a bounded delta contract"):
        _validate_production_daily_tasks(tasks)

    duplicated = list(config["pipelines"]["tushare_production_daily"]["tasks"])
    duplicated.append({
        "name": "second_market_derived",
        "job": "data_collect.jobs.tushare_production_market_derived",
        "depends_on": ["production_phase2_raw"],
    })
    with pytest.raises(RuntimeError, match="topology is not explicitly reviewed"):
        _validate_production_daily_tasks(duplicated)


def test_production_market_derived_lookback_has_hard_upper_bound():
    from data_collect.jobs.tushare_production_market_derived import run

    with pytest.raises(ValueError, match="in 1..31"):
        run(run_date="20260925", lookback_days=32, dry_run=True)


@pytest.mark.parametrize(
    ("module_name", "kwargs"),
    [
        ("data_collect.jobs.tushare_market_incremental_by_date", {"lookback_days": 32}),
        ("data_collect.jobs.tushare_phase2_incremental", {"lookback_days": 32}),
        ("data_collect.jobs.tushare_phase2_incremental", {"event_lookback_days": 91}),
    ],
)
def test_incremental_fetch_jobs_reject_unbounded_lookback(module_name, kwargs):
    module = importlib.import_module(module_name)
    with pytest.raises(ValueError):
        module.run.__wrapped__(dry_run=True, **kwargs)


def test_production_quality_rejects_unbounded_lookback_before_database_access():
    from data_collect.jobs.tushare_production_quality import run

    with pytest.raises(ValueError, match="in 1..31"):
        run(run_date="20260925", lookback_days=32, dry_run=True)


def test_market_derived_rebuild_rejects_unbounded_output_window():
    from data_collect.jobs.tushare_derived_rebuild import rebuild_after_market

    with pytest.raises(ValueError, match="不能超过 45 个自然日"):
        rebuild_after_market("2026-01-01", "2026-03-01")


def test_production_pipeline_quality_is_bounded_and_does_not_full_scan():
    source = (ROOT / "data_collect/data_access/repository/production_quality_repository.py").read_text(encoding="utf-8")
    assert "BETWEEN %s AND %s" in source
    assert "DISTINCT ON (dataset, trade_date)" in source
    assert "created_at DESC, gate_id DESC" in source
    assert "TRUNCATE" not in source
    assert "GROUP BY table_name, trade_date" in source
    assert "ANY(%s::date[])" in source
    assert "SELECT DISTINCT trade_date, stock_code AS ts_code" in source
    assert "WHERE source='tushare'" in source


def test_scheduled_phase2_kwargs_bind_to_the_real_run_signature():
    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    task = next(
        task for task in config["pipelines"]["tushare_production_daily"]["tasks"]
        if task["job"] == "data_collect.jobs.tushare_phase2_incremental"
    )
    from data_collect.jobs.tushare_phase2_incremental import run

    signature(run).bind(run_date="20260905", **task.get("kwargs", {}))
    signature(run).bind(run_date="20260905", rebuild_derived=True)


def test_every_production_task_config_binds_to_its_real_run_signature():
    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    tasks = config["pipelines"]["tushare_production_daily"]["tasks"]
    for task in tasks:
        module = importlib.import_module(task["job"])
        signature(module.run).bind(
            run_date="20260925", **(task.get("kwargs") or {}),
        )


def test_production_quality_is_fail_closed_for_empty_window():
    from data_collect.data_access.repository.production_quality_repository import (
        ProductionQualityRepository,
    )

    class EmptyRead:
        def _repository_query(self, context, source, sql, params, scope, max_rows):
            if "FULL OUTER JOIN" in sql:
                return []
            if source == "ops.market_data_quality_gate":
                return []
            return [{"rows": 0, "observed_dates": 0, "observed_date_values": [], "missing_source": 0, "unexpected_status": 0}]

    repository = object.__new__(ProductionQualityRepository)
    repository.kernel = SimpleNamespace(read=EmptyRead())
    report = repository.inspect_window(
        date(2026, 9, 4), date(2026, 9, 4), None,
        expected_dates=(date(2026, 9, 4),),
    )
    assert report["failures"]
    assert any("no rows" in failure for failure in report["failures"])
    assert any("missing expected keys" in failure for failure in report["failures"])


def test_production_quality_expected_price_population_includes_standard_kline():
    source = (ROOT / "data_collect/data_access/repository/production_quality_repository.py").read_text(encoding="utf-8")
    assert "FROM std.daily_kline" in source
    assert "UNION" in source
    assert "has_price_history IS TRUE" in source
    assert "is_tradeable IS TRUE" in source


def test_production_quality_empty_calendar_is_fail_closed(monkeypatch):
    from data_collect.jobs import tushare_production_quality as quality

    class EmptyCalendar:
        def trade_dates(self, *args, **kwargs):
            return []

    class FakeKernel:
        def close(self):
            pass

        def record_run_failure(self, context, exc):
            pass

    monkeypatch.setattr(quality, "DataAccessKernel", lambda **kwargs: FakeKernel())
    monkeypatch.setattr(quality, "Phase2RawRepository", lambda kernel: EmptyCalendar())
    with pytest.raises(RuntimeError, match="no trading days"):
        quality.run(run_date="20260905")
    assert quality.run(run_date="20260905", allow_empty_window=True)["status"] == "no_trading_days"


def test_production_quality_rejects_partial_gate_key_set():
    from data_collect.data_access.repository.production_quality_repository import (
        ProductionQualityRepository,
    )

    expected_date = date(2026, 9, 4)

    class PartialRead:
        def _repository_query(self, context, source, sql, params, scope, max_rows):
            if "FULL OUTER JOIN" in sql:
                return []
            if source == "ops.market_data_quality_gate":
                return [{"dataset": "daily", "trade_date": expected_date, "status": "PASS"}]
            return [{
                "rows": 1,
                "observed_dates": 1,
                "observed_date_values": [expected_date],
                "missing_source": 0,
                "unexpected_status": 0,
            }]

    repository = object.__new__(ProductionQualityRepository)
    repository.kernel = SimpleNamespace(read=PartialRead())
    report = repository.inspect_window(
        expected_date, expected_date, None, expected_dates=(expected_date,),
    )
    gate = report["ops.market_data_quality_gate"]
    assert gate["expected_total"] == 3
    assert len(gate["missing_keys"]) == 2
    assert any("missing expected keys" in failure for failure in report["failures"])


def test_production_quality_rejects_code_fingerprint_mismatch():
    from data_collect.data_access.repository.production_quality_repository import (
        ProductionQualityRepository,
    )

    expected_date = date(2026, 9, 4)

    class FingerprintMismatchRead:
        def _repository_query(self, context, source, sql, params, scope, max_rows):
            if "FULL OUTER JOIN" in sql:
                return [{
                    "table_name": table,
                    "trade_date": expected_date,
                    "expected_rows": 1,
                    "actual_rows": 1,
                    "missing_rows": 0,
                    "extra_rows": 0,
                    "expected_fingerprint": "expected",
                    "actual_fingerprint": "actual",
                } for table in ProductionQualityRepository._WINDOWS]
            if source == "ops.market_data_quality_gate":
                return [{
                    "dataset": dataset,
                    "trade_date": expected_date,
                    "status": "PASS",
                } for dataset in ProductionQualityRepository._EXPECTED_GATE_DATASETS]
            return [{
                "rows": 1,
                "observed_dates": 1,
                "observed_date_values": [expected_date],
                "missing_source": 0,
                "unexpected_status": 0,
            }]

    repository = object.__new__(ProductionQualityRepository)
    repository.kernel = SimpleNamespace(read=FingerprintMismatchRead())
    report = repository.inspect_window(
        expected_date, expected_date, None, expected_dates=(expected_date,),
    )
    assert any("code coverage fingerprint mismatch" in failure for failure in report["failures"])


def test_production_quality_code_coverage_is_planned_one_trade_date_at_a_time():
    from data_collect.data_access.repository.production_quality_repository import (
        ProductionQualityRepository,
    )

    dates = (date(2026, 9, 3), date(2026, 9, 4))
    coverage_calls = []

    class RecordingRead:
        def _repository_query(self, context, source, sql, params, scope, max_rows):
            if "FULL OUTER JOIN" in sql:
                covered_date = params[0][0]
                coverage_calls.append((covered_date, params, scope, max_rows))
                return [{
                    "table_name": table,
                    "trade_date": covered_date,
                    "expected_rows": 1,
                    "actual_rows": 1,
                    "missing_rows": 0,
                    "extra_rows": 0,
                    "expected_fingerprint": "same",
                    "actual_fingerprint": "same",
                } for table in ProductionQualityRepository._WINDOWS]
            if source == "ops.market_data_quality_gate":
                return [{
                    "dataset": dataset,
                    "trade_date": trade_date,
                    "status": "PASS",
                } for dataset in ProductionQualityRepository._EXPECTED_GATE_DATASETS
                  for trade_date in dates]
            return [{
                "rows": 2,
                "observed_dates": len(dates),
                "observed_date_values": list(dates),
                "missing_source": 0,
                "unexpected_status": 0,
            }]

    repository = object.__new__(ProductionQualityRepository)
    repository.kernel = SimpleNamespace(read=RecordingRead())
    report = repository.inspect_window(
        dates[0], dates[-1], None, expected_dates=dates,
    )

    assert report["failures"] == []
    assert len(coverage_calls) == len(dates)
    assert [item[0] for item in coverage_calls] == list(dates)
    assert all(
        len(values) == 1 and values[0] == trade_date
        for trade_date, params, _, _ in coverage_calls
        for values in params
    )
    assert all(item[2].date_from == item[0] and item[2].date_to == item[0] for item in coverage_calls)
    assert all(item[3] == len(ProductionQualityRepository._WINDOWS) for item in coverage_calls)


def test_production_quality_repository_rejects_more_than_31_expected_dates():
    from data_collect.data_access.repository.production_quality_repository import (
        ProductionQualityRepository,
    )

    repository = object.__new__(ProductionQualityRepository)
    repository.kernel = SimpleNamespace(read=SimpleNamespace())
    dates = tuple(date.fromordinal(date(2026, 1, 1).toordinal() + offset) for offset in range(32))
    with pytest.raises(ValueError, match="limited to 31 trade dates"):
        repository.inspect_window(dates[0], dates[-1], None, expected_dates=dates)


def test_production_daily_chain_runs_derived_builders_before_final_quality():
    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    tasks = config["pipelines"]["tushare_production_daily"]["tasks"]
    names = [task["name"] for task in tasks]
    assert names[-2:] == ["production_quality", "production_market_activity"]
    quality = next(task for task in tasks if task["name"] == "production_quality")
    activity = next(task for task in tasks if task["name"] == "production_market_activity")
    assert quality["job"] == "data_collect.jobs.tushare_production_quality"
    assert set(quality["depends_on"]) == {
        "production_market_derived", "production_financial_derived",
    }
    assert activity["job"] == "data_collect.jobs.market_activity_production_daily"
    assert activity["depends_on"] == ["production_quality"]
    source = (ROOT / "data_collect/jobs/tushare_derived_rebuild.py").read_text(encoding="utf-8")
    assert 'features["technical_daily_v1"]' in source


def test_production_pipeline_contract_is_versioned_and_fail_closed():
    from data_collect.pipeline import (
        _validate_production_pipeline_contract,
        _validate_production_schedule_contract,
    )

    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    _validate_production_pipeline_contract(
        "tushare_production_daily",
        config["pipelines"]["tushare_production_daily"]["tasks"],
    )
    _validate_production_schedule_contract(config["pipelines"])
    with pytest.raises(RuntimeError, match="missing required producer task"):
        _validate_production_pipeline_contract(
            "tushare_production_daily",
            [
                {
                    "name": "production_instrument_raw",
                    "job": "data_collect.jobs.a_share_instrument_tushare",
                }
            ],
        )

    without_activity = [
        task for task in config["pipelines"]["tushare_production_daily"]["tasks"]
        if task["name"] != "production_market_activity"
    ]
    with pytest.raises(RuntimeError, match="missing required tasks"):
        _validate_production_pipeline_contract(
            "tushare_production_daily", without_activity,
        )

    invalid_schedules = {
        **config["pipelines"],
        "tushare_daily": {
            **config["pipelines"]["tushare_daily"],
            "schedule": {"hour": 18, "minute": 0},
        },
    }
    with pytest.raises(RuntimeError, match="must not have a schedule"):
        _validate_production_schedule_contract(invalid_schedules)

    missing_schedule = {
        **config["pipelines"],
        "tushare_production_daily": {
            **config["pipelines"]["tushare_production_daily"],
            "schedule": None,
        },
    }
    with pytest.raises(RuntimeError, match="must be the scheduled Tushare"):
        _validate_production_schedule_contract(missing_schedule)


def test_outcome_is_reviewed_optional_downstream_and_disabled_in_current_dag():
    from data_collect.pipeline import (
        _STABLE_RETRY_ARGUMENTS,
        _validate_production_pipeline_contract,
    )

    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    tasks = list(config["pipelines"]["tushare_production_daily"]["tasks"])
    outcome = {
        "name": "production_market_anomaly_outcome",
        "job": "data_collect.jobs.market_anomaly_outcome_daily",
        "depends_on": ["production_market_activity"],
        "kwargs": {"lookback_days": 31},
        "retries": 1,
    }
    tasks.append(outcome)
    _validate_production_pipeline_contract("tushare_production_daily", tasks)
    assert _STABLE_RETRY_ARGUMENTS[outcome["job"]] == "execution_id"

    invalid = [*tasks[:-1], {**outcome, "depends_on": ["production_quality"]}]
    with pytest.raises(RuntimeError, match="dependencies"):
        _validate_production_pipeline_contract("tushare_production_daily", invalid)

    names = [task["name"] for task in config["pipelines"]["tushare_production_daily"]["tasks"]]
    assert "production_market_anomaly_outcome" not in names


def test_outcome_job_dry_run_is_bounded_and_does_not_open_kernel():
    from data_collect.jobs.market_anomaly_outcome_daily import run

    report = run(run_date="20261010", dry_run=True, lookback_days=10)
    assert report == {
        "status": "dry_run",
        "upper_bound": "2026-10-10",
        "lookback_days": 10,
        "max_events_per_batch": 100,
        "max_batches_per_run": 5,
    }

    with pytest.raises(ValueError, match="1..31"):
        run(run_date="20261010", dry_run=True, lookback_days=32)


def test_split_tushare_pipeline_cannot_be_scheduled_individually(monkeypatch):
    import data_collect.config as app_config
    from run_job import start_scheduler

    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    monkeypatch.setattr(app_config, "get_pipeline_config", lambda: config["pipelines"])
    with pytest.raises(ValueError, match="manual/diagnostic"):
        start_scheduler(pipeline_name="tushare_daily")


def test_pipeline_entrypoint_exposes_dry_run():
    source = (ROOT / "tools/run_pipeline_once.py").read_text(encoding="utf-8")
    assert 'parser.add_argument("--dry-run"' in source
    assert 'base_kwargs["dry_run"] = True' in source


def test_foundation_pipeline_can_reuse_existing_standard_layer():
    source = (ROOT / "data_collect/jobs/tushare_foundation_pipeline.py").read_text(encoding="utf-8")
    assert 'parser.add_argument(\n        "--reuse-standard"' in source
    assert "if not reuse_standard:" in source
    assert '"reuse_standard": reuse_standard' in source


def test_foundation_stage_identity_is_explicit_and_fail_closed():
    from data_collect.jobs.foundation_stage_identity import stage_code_identities

    identities = stage_code_identities()
    assert {
        "company_financial_v2", "valuation_daily_v2", "standard_v2",
        "valuation_standard_v2", "quality",
    } <= identities.keys()
    assert all(len(value) == 64 for value in identities.values())


def test_foundation_resume_keeps_original_parameters_and_stage_identity():
    pipeline = (ROOT / "data_collect/jobs/tushare_foundation_pipeline.py").read_text(encoding="utf-8")
    repository = (ROOT / "data_collect/data_access/repository/financial_repository.py").read_text(encoding="utf-8")
    assert "requested_reuse_standard" in pipeline
    assert "missing immutable reuse_standard parameter" in pipeline
    assert 'parameters.get("_stage_code_identities")' in pipeline
    assert "_last_resume_stage_code_identities" in repository
    assert 'parameters["_stage_code_identities"]' not in repository


def test_foundation_pipeline_does_not_materialize_warmup_into_production_feature():
    source = (ROOT / "data_collect/jobs/tushare_foundation_pipeline.py").read_text(encoding="utf-8")
    assert '"reason": "warmup is read-only input, not published output"' in source
    assert 'rebuild_company_financial_v2(None, WARMUP_START' not in source


def test_valuation_rebuild_defaults_to_bounded_multi_stock_batch():
    source = (ROOT / "data_collect/jobs/tushare_foundation_features.py").read_text(encoding="utf-8")
    assert 'os.getenv("ASHARES_VALUATION_CODE_BATCH_SIZE", "15")' in source
    assert "MAX_VALUATION_CODE_BATCH_SIZE = 15" in source
    assert "MAX_VALUATION_ROWS_PER_TRANSACTION = 20_000" in source


def test_valuation_batch_size_override_cannot_widen_five_year_reads(monkeypatch):
    from data_collect.jobs import tushare_foundation_features

    monkeypatch.setenv("ASHARES_VALUATION_CODE_BATCH_SIZE", "16")
    with pytest.raises(ValueError, match="must be in 1..15"):
        tushare_foundation_features._valuation_code_batch_size()


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
