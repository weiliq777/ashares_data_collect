import json
from datetime import date, timedelta

import data_collect.jobs.alpha_research as alpha_job
from data_collect.alpha_research.contracts import (
    FACTOR_INPUT_FIELDS,
    SOURCE_QUALITY_EVIDENCE_VERSION,
)
from data_collect.data_access.repository.alpha_research_repository import (
    FrozenResearchIdentity,
)


class _Kernel:
    def __init__(self, **_kwargs):
        self.closed = False

    def close(self):
        self.closed = True


class _Adapter:
    instance = None
    future_delist_date = None

    def __init__(self, repository, dataset_id, context, *, page_size):
        start = date(2025, 1, 2)
        self.identity = FrozenResearchIdentity(
            dataset_id=dataset_id,
            dataset_kind="MATERIALIZED",
            baseline="baseline-a",
            input_manifest_hash="input-hash",
            quality_report_hash="quality-hash",
            frozen_start=start,
            evaluation_start=start,
            frozen_end=start + timedelta(days=60),
            selected_fields=frozenset({
                "research_date", "ts_code", "total_mv", "delist_date", *FACTOR_INPUT_FIELDS,
            }),
            schema_versions=("research-v2.1",),
            snapshot_count=1,
            research_dates=(start,),
            calendar=tuple(start + timedelta(days=index) for index in range(61)),
        )
        self._start = start
        self._bars = {
            start + timedelta(days=index): {
                "000001.SZ": {
                    "raw_open": 10 + index / 100,
                    "raw_close": 10 + index / 100,
                    "quality_status": "VALID",
                    "is_suspended": False,
                }
            }
            for index in range(61)
        }
        self._observation = {
            "research_date": start,
            "ts_code": "000001.SZ",
            "delist_date": None,
            "pe_pct_5y": 0.2,
            "is_pe_meaningful": True,
            "has_full_pe_5y_history": True,
            "pe_ttm": 10,
            "roe_ttm": 0.1,
            "ttm_complete": True,
            "roe_quality_status": "VALID",
            "debt_to_assets": 0.3,
            "revenue_growth_yoy": 0.1,
            "profit_growth_yoy": 0.2,
            "growth_quality_status": "VALID",
            "return_20d": 0.01,
            "return_60d": 0.03,
            "price_feature_version": "price_daily_v2",
            "price_adjustment_mode": "point_in_time_total_return",
            "adj_close_base": 100,
            "ma60": 99,
            "volume_ratio_20d": 1.2,
            "total_mv": 1_000_000_000,
            "financial_feature_contract_version": "feature-contract-v2.0",
            "financial_data_available_timestamp": "2025-01-01T17:00:00+08:00",
            "financial_vintage_quality": "SOURCE_ANNOUNCEMENT_ASSUMED",
            "financial_component_lineage": {
                "__quality_evidence__": {"version": SOURCE_QUALITY_EVIDENCE_VERSION},
                "income:report:1": {
                    "payload_hash": "a" * 64,
                    "source_quality_status": "VALID",
                    "used_by": ["roe_ttm", "revenue_growth_yoy", "profit_growth_yoy"],
                },
            },
        }
        _Adapter.instance = self

    def observation_pages(self, date_from, date_to, columns):
        rows = [self._observation]
        if self.future_delist_date is not None:
            rows.append({
                "research_date": self.future_delist_date,
                "ts_code": "000001.SZ",
                "delist_date": self.future_delist_date,
            })
        selected = [
            {key: value for key, value in row.items() if key in columns}
            for row in rows
            if date_from <= row["research_date"] <= date_to
        ]
        if selected:
            yield selected

    def bars(self, date_from, date_to):
        return {day: rows for day, rows in self._bars.items() if date_from <= day <= date_to}

    def actions(self, date_from, date_to):
        return {}

    def benchmark_closes(self):
        return {}


def test_offline_job_writes_reproducible_report_without_database_access(monkeypatch, tmp_path):
    monkeypatch.setattr(alpha_job, "DataAccessKernel", _Kernel)
    monkeypatch.setattr(alpha_job, "AlphaResearchRepository", _Adapter)
    monkeypatch.setattr(alpha_job, "build_run_context", lambda *args, **kwargs: object())
    start = date(2025, 1, 2)
    result = alpha_job.run_research(
        "dataset-a", start, start, tmp_path,
        bootstrap_repetitions=100,
    )
    report = json.loads((tmp_path / result["run_id"] / "report.json").read_text(encoding="utf-8"))
    assert report["quality"]["signal_observation_count"] == 1
    assert report["quality"]["label_status_counts"] == {"COMPLETE": 3}
    assert report["quality"]["size_control_input"] == "PREVIOUS_SESSION_TOTAL_MV"
    assert report["quality"]["factor_quality"]["roe_ttm"]["valid_count"] == 1
    assert report["status"] == "EXPLORATORY_ONLY_STAGE1_GATES_UNRESOLVED"
    assert report["research_validity_status"] == "NOT_FORMAL_FACTOR_EFFICACY_ASSESSMENT"
    assert report["strategy_incrementality"]["status"] == "NOT RUN_STAGE1_EVIDENCE_UNRESOLVED"
    assert len((tmp_path / result["run_id"] / "samples.jsonl").read_text(encoding="utf-8").splitlines()) == 3
    assert _Adapter.instance is not None


def test_previous_session_total_mv_uses_only_the_prior_open_date():
    first_day = date(2025, 1, 2)
    signal_day = date(2025, 1, 3)
    later_day = date(2025, 1, 6)
    calendar = (first_day, signal_day, later_day)
    indices = {day: index for index, day in enumerate(calendar)}
    values = {
        (first_day, "000001.SZ"): 100.0,
        (signal_day, "000001.SZ"): 200.0,
    }

    assert alpha_job._previous_session_total_mv(
        signal_day, "000001.SZ", indices, calendar, values,
    ) == 100.0
    assert alpha_job._previous_session_total_mv(
        first_day, "000001.SZ", indices, calendar, values,
    ) is None
    assert alpha_job._previous_session_total_mv(
        later_day, "000001.SZ", indices, calendar, values,
    ) == 200.0


def test_future_frozen_delist_evidence_is_reported_and_not_used_as_zero(monkeypatch, tmp_path):
    monkeypatch.setattr(alpha_job, "DataAccessKernel", _Kernel)
    monkeypatch.setattr(alpha_job, "AlphaResearchRepository", _Adapter)
    monkeypatch.setattr(alpha_job, "build_run_context", lambda *args, **kwargs: object())
    start = date(2025, 1, 2)
    monkeypatch.setattr(_Adapter, "future_delist_date", start + timedelta(days=5))

    result = alpha_job.run_research(
        "dataset-delisted", start, start, tmp_path,
        bootstrap_repetitions=100,
    )
    run_dir = tmp_path / result["run_id"]
    report = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
    samples = [json.loads(line) for line in (run_dir / "samples.jsonl").read_text(encoding="utf-8").splitlines()]

    assert report["quality"]["delist_evidence_status"] == "FROZEN_PIT_DELIST_DATE_AVAILABLE"
    assert report["quality"]["known_delisted_security_count"] == 1
    assert report["quality"]["unresolved_delist_label_count"] == 3
    assert report["quality"]["outcome_completeness_status"] == "BLOCKED_UNRESOLVED_DELISTING_TERMINAL_VALUE"
    assert report["quality"]["label_reason_counts"] == {
        "DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE": 3,
    }
    assert all(sample["label"]["prediction_total_return"] is None for sample in samples)
    assert all(
        sample["label"]["reason"] == "DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE"
        for sample in samples
    )
    assert "未解决退市标签 3 条" in (run_dir / "report.md").read_text(encoding="utf-8")
