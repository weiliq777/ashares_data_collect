from datetime import date
from types import SimpleNamespace

import pytest

from data_collect.jobs import market_anomaly_outcome_daily as job


T = date(2026, 10, 9)


class FakeKernel:
    def __init__(self, **_kwargs):
        self.closed = False

    def close(self):
        self.closed = True


def test_dry_run_does_not_construct_kernel():
    def unexpected_kernel(**_kwargs):
        raise AssertionError("dry run must not connect to the database")

    old = job.DataAccessKernel
    job.DataAccessKernel = unexpected_kernel
    try:
        result = job.run("20261009", dry_run=True)
    finally:
        job.DataAccessKernel = old

    assert result["status"] == "dry_run"
    assert result["upper_bound"] == "2026-10-09"
    assert result["max_events_per_batch"] == 100


def test_non_pass_market_gate_fails_closed_and_closes_kernel(monkeypatch):
    kernel = FakeKernel()
    monkeypatch.setattr(job, "DataAccessKernel", lambda **_kwargs: kernel)
    monkeypatch.setattr(
        job.MarketCoverageRepository, "latest_daily_gate",
        lambda *_args: {"trade_date": T, "status": "BLOCKED", "reason": "coverage"},
    )

    with pytest.raises(RuntimeError, match="not PASS"):
        job.run("20261009", execution_id="job-test")

    assert kernel.closed is True


def test_pass_gate_advances_only_to_gate_date(monkeypatch):
    kernel = FakeKernel()
    observed = {}

    monkeypatch.setattr(job, "DataAccessKernel", lambda **_kwargs: kernel)
    monkeypatch.setattr(
        job.MarketCoverageRepository, "latest_daily_gate",
        lambda *_args: {"trade_date": T, "status": "PASS"},
    )

    class FakeEvaluator:
        def __init__(self, repository, policy):
            observed["repository"] = repository
            observed["policy"] = policy

        def run(self, *, through_date, context):
            observed["through_date"] = through_date
            observed["run_id"] = context.run_id
            return SimpleNamespace(as_dict=lambda: {"status": "COMPLETE"})

    monkeypatch.setattr(job, "OutcomeEvaluator", FakeEvaluator)
    monkeypatch.setattr(job, "MarketAnomalyOutcomeRepository", lambda value: value)

    result = job.run("20261010", execution_id="stable-execution")

    assert result["status"] == "COMPLETE"
    assert result["gate_trade_date"] == T.isoformat()
    assert observed["through_date"] == T
    assert observed["run_id"] == "stable-execution"
    assert kernel.closed is True
