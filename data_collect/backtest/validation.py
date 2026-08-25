"""Backtest dataset cutoff validation and in-memory truncation helpers."""
from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import date
from typing import Iterable
from uuid import uuid4

from .models import BacktestDataset, Observation


def validate_observation_cutoffs(observations: Iterable[Observation], decision_date: date) -> list[str]:
    issues = []
    for observation in observations:
        if observation.research_date > decision_date:
            issues.append(f"observation date after decision date: {observation.ts_code}/{observation.research_date}")
        effective = observation.get("effective_announce_date")
        if effective is not None and effective > observation.research_date:
            issues.append(f"financial availability after research date: {observation.ts_code}/{effective}>{observation.research_date}")
    return issues


def truncate_dataset_asof(dataset: BacktestDataset, cutoff_date: date) -> BacktestDataset:
    if cutoff_date < dataset.evaluation_start:
        raise ValueError("cutoff_date cannot be earlier than evaluation_start")
    calendar = tuple(day for day in dataset.calendar if day <= cutoff_date)
    if not calendar:
        raise ValueError("no trading date available before cutoff_date")
    truncated = replace(
        dataset,
        calendar=calendar,
        evaluation_end=min(dataset.evaluation_end, cutoff_date, calendar[-1]),
        observations={day: rows for day, rows in dataset.observations.items() if day <= cutoff_date},
        bars={key: value for key, value in dataset.bars.items() if key[0] <= cutoff_date},
        dividends={day: rows for day, rows in dataset.dividends.items() if day <= cutoff_date},
        member_roles={day: role for day, role in dataset.member_roles.items() if day <= cutoff_date},
        snapshot_ids={day: value for day, value in dataset.snapshot_ids.items() if day <= cutoff_date},
        rules={key: value for key, value in dataset.rules.items() if key[0] <= cutoff_date},
    )
    issues = validate_observation_cutoffs((observation for rows in truncated.observations.values() for observation in rows), cutoff_date)
    if issues:
        raise ValueError(f"cutoff validation failed: {issues[:5]}")
    return truncated


def assert_no_future_rows(dataset: BacktestDataset, cutoff_date: date) -> None:
    future = {
        "calendar": [day for day in dataset.calendar if day > cutoff_date],
        "bars": [key for key in dataset.bars if key[0] > cutoff_date],
        "observations": [day for day in dataset.observations if day > cutoff_date],
        "dividends": [day for day in dataset.dividends if day > cutoff_date],
    }
    if any(future.values()):
        raise AssertionError({key: value[:3] for key, value in future.items()})


def _service(label: str):
    from data_collect.data_access import DataAccessKernel, RunContext
    from data_collect.data_access.policy_catalog import build_default_registry
    from data_collect.data_access.repository.pit_validation_repository import PitValidationRepository
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"pit-validation-{label}-{uuid4().hex}", job_name="backtest_validation",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, PitValidationRepository(kernel), context


def verify_pit_cutoff_consistency(target_date: date | str, snapshot_id: str | None = None) -> dict:
    cutoff = target_date if isinstance(target_date, date) else date.fromisoformat(str(target_date))
    kernel, repository, context = _service(str(cutoff))
    try:
        return repository.verify(cutoff, snapshot_id, context)
    finally:
        kernel.close()


def assert_pit_cutoff_consistency(target_date: date | str, snapshot_id: str | None = None) -> None:
    report = verify_pit_cutoff_consistency(target_date, snapshot_id=snapshot_id)
    if not report["passed"]:
        raise RuntimeError(f"PIT cutoff validation failed: {report['issues']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="PIT cutoff read-only validation")
    parser.add_argument("--target-date", required=True)
    parser.add_argument("--snapshot-id")
    args = parser.parse_args()
    report = verify_pit_cutoff_consistency(args.target_date, args.snapshot_id)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
