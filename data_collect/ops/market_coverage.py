"""Market coverage gate calculation and fail-closed publication checks."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from statistics import median
from typing import Iterable

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_coverage_repository import MarketCoverageRepository


@dataclass(frozen=True)
class CoverageGateResult:
    dataset: str
    trade_date: date
    actual_rows: int
    expected_rows: float | None
    baseline_days: int
    threshold_pct: float
    drop_pct: float | None
    status: str
    publishable: bool
    reason: str
    sample_mode: bool = False


def _as_date(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def evaluate_market_coverage(dataset: str, trade_date: date | str, actual_rows: int,
                             baseline_counts: Iterable[int], threshold_pct: float = 0.01,
                             sample_mode: bool = False) -> CoverageGateResult:
    if not dataset or not str(dataset).strip():
        raise ValueError("dataset must not be empty")
    if int(actual_rows) < 0:
        raise ValueError("actual_rows must be non-negative")
    if not 0 <= float(threshold_pct) < 1:
        raise ValueError("threshold_pct must be in [0, 1)")
    day = _as_date(trade_date)
    if sample_mode:
        return CoverageGateResult(str(dataset), day, int(actual_rows), None, 0, float(threshold_pct),
                                  None, "SKIPPED_SAMPLE", False, "sample is not full-market coverage", True)
    positive = [int(value) for value in baseline_counts if int(value) > 0]
    if not positive:
        return CoverageGateResult(str(dataset), day, int(actual_rows), None, 0, float(threshold_pct),
                                  None, "NO_BASELINE", False, "no historical full-market baseline", False)
    expected = float(median(positive))
    drop_pct = max(0.0, (expected - int(actual_rows)) / expected)
    passed = int(actual_rows) >= expected * (1.0 - float(threshold_pct))
    return CoverageGateResult(
        str(dataset), day, int(actual_rows), expected, len(positive), float(threshold_pct), drop_pct,
        "PASS" if passed else "BLOCKED", passed,
        "coverage within threshold" if passed else f"coverage drop {drop_pct:.2%} exceeds {threshold_pct:.2%}",
        False,
    )


def _context(job_name: str, token: str) -> RunContext:
    return RunContext(run_id=f"{job_name}-{token}", job_name=job_name, code_version="unknown",
                      config_hash="unknown", pipeline_version="v1")


def persist_market_coverage_gate(dataset: str, trade_date: date | str, actual_rows: int, batch_id: str,
                                 *, threshold_pct: float = 0.01, baseline_days: int = 5,
                                 sample_mode: bool = False) -> CoverageGateResult:
    if not batch_id:
        raise ValueError("batch_id must not be empty")
    if int(baseline_days) <= 0:
        raise ValueError("baseline_days must be positive")
    day = _as_date(trade_date)
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = _context("market-coverage", f"{dataset}-{day}")
    try:
        repository = MarketCoverageRepository(kernel)
        counts = [] if sample_mode else repository.previous_counts(dataset, day, baseline_days, context)
        result = evaluate_market_coverage(dataset, day, actual_rows, counts, threshold_pct, sample_mode)
        repository.save_gate(result, batch_id, context)
        return result
    finally:
        kernel.close()


def persist_market_coverage_blocked(dataset: str, trade_date: date | str, actual_rows: int,
                                    batch_id: str, reason: str) -> CoverageGateResult:
    if not batch_id:
        raise ValueError("batch_id must not be empty")
    if not reason or not str(reason).strip():
        raise ValueError("blocked reason must not be empty")
    day = _as_date(trade_date)
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = _context("market-coverage-blocked", f"{dataset}-{day}")
    try:
        repository = MarketCoverageRepository(kernel)
        result = evaluate_market_coverage(dataset, day, actual_rows,
                                          repository.previous_counts(dataset, day, 5, context))
        blocked = replace(result, status="BLOCKED", publishable=False,
                          reason=f"sync/materialization failure: {str(reason)[:500]}")
        repository.save_gate(blocked, batch_id, context)
        return blocked
    finally:
        kernel.close()


def latest_market_coverage_failures(trade_date: date | str) -> list[tuple]:
    day = _as_date(trade_date)
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        return MarketCoverageRepository(kernel).latest_failures(day, _context("market-coverage-read", str(day)))
    finally:
        kernel.close()


def latest_market_coverage_failures_in_range(start_date: date | str, end_date: date | str) -> list[tuple]:
    start, end = _as_date(start_date), _as_date(end_date)
    if start > end:
        raise ValueError("coverage range start must not be after end")
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        return MarketCoverageRepository(kernel).latest_failures_in_range(
            start, end, _context("market-coverage-range-read", f"{start}-{end}")
        )
    finally:
        kernel.close()


def assert_market_coverage_publishable(trade_date: date | str) -> None:
    failures = latest_market_coverage_failures(trade_date)
    if failures:
        raise RuntimeError(f"market coverage gate failed: {trade_date}: {failures}")


def assert_market_coverage_range_publishable(start_date: date | str, end_date: date | str) -> None:
    failures = latest_market_coverage_failures_in_range(start_date, end_date)
    if failures:
        raise RuntimeError(f"market coverage range gate failed: {failures[:20]}")
