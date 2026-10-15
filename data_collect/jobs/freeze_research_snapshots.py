"""Batch-freeze a validated Research Snapshot series with bounded transactions."""
from __future__ import annotations

import argparse
from datetime import date
from typing import Iterable, TypeVar
from uuid import uuid4

from data_collect.jobs.research_snapshot import (
    FOUNDATION_SCHEMA_VERSION,
    _baseline,
    _quality_report,
    freeze_snapshots_batch,
)
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.snapshot_series_repository import SnapshotSeriesRepository
from data_collect.data_access.research_contract import is_foundation_schema


T = TypeVar("T")


def _select_snapshots(baseline_tag: str, schema_version: str, start, end):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"freeze-selection-{uuid4().hex}", job_name="freeze_research_snapshots",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    try:
        return SnapshotSeriesRepository(kernel).select_snapshots(baseline_tag, schema_version, start, end, context)
    finally:
        kernel.close()


def _batches(values: Iterable[T], batch_size: int) -> tuple[tuple[T, ...], ...]:
    """Return bounded, deterministic batches for resumable snapshot freezing."""
    if batch_size < 1 or batch_size > 100:
        raise ValueError("batch_size must be between 1 and 100")
    items = tuple(values)
    return tuple(
        items[offset:offset + batch_size]
        for offset in range(0, len(items), batch_size)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="分批冻结已验证的 Research Snapshot")
    parser.add_argument("--baseline-tag", required=True)
    parser.add_argument("--schema-version", required=True)
    parser.add_argument("--code-archive-path")
    parser.add_argument("--start-date", help="可选的截止日期范围起点（含）")
    parser.add_argument("--end-date", help="可选的截止日期范围终点（含）")
    parser.add_argument(
        "--expected-snapshots",
        type=int,
        help="所选系列的快照数不等于该值时立即失败",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="每个事务冻结的快照数（1..100，默认 10）",
    )
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start_date) if args.start_date else None
    end_date = date.fromisoformat(args.end_date) if args.end_date else None
    if start_date and end_date and start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    if args.expected_snapshots is not None and args.expected_snapshots < 1:
        raise ValueError("expected_snapshots must be positive")
    _batches((), args.batch_size)  # fail before accessing the database

    rows = [
        (row["snapshot_id"], row["research_cutoff_date"], row["status"])
        for row in _select_snapshots(args.baseline_tag, args.schema_version, start_date, end_date)
    ]
    if args.expected_snapshots is not None and len(rows) != args.expected_snapshots:
        raise RuntimeError(
            f"expected {args.expected_snapshots} snapshots, got {len(rows)}"
        )
    if not rows:
        raise RuntimeError("no snapshots selected")
    unexpected = [
        (str(snapshot_id), cutoff.isoformat(), status)
        for snapshot_id, cutoff, status in rows
        if status not in {"VALIDATED", "FROZEN"}
    ]
    if unexpected:
        raise RuntimeError(
            "snapshot series contains non-publishable states: "
            f"{unexpected[:10]}"
        )

    pending = [str(row[0]) for row in rows if row[2] == "VALIDATED"]
    already_frozen = len(rows) - len(pending)
    chunks = _batches(pending, args.batch_size)
    print(
        f"selected={len(rows)} pending={len(pending)} "
        f"already_frozen={already_frozen} batches={len(chunks)}",
        flush=True,
    )
    if not pending:
        print(
            f"freeze complete selected={len(rows)} newly_frozen=0",
            flush=True,
        )
        return
    baseline = _baseline(
        max(row[1] for row in rows),
        args.baseline_tag,
        verify_source_manifest=False,
    )
    quality_cutoff = (
        baseline.baseline_cutoff_date
        if is_foundation_schema(args.schema_version)
        else max(row[1] for row in rows)
    )
    quality_report, _ = _quality_report(quality_cutoff, args.schema_version)
    if quality_report["failures"]:
        raise RuntimeError(
            f"quality gate failed before snapshot freeze: {quality_report['failures']}"
        )
    for index, chunk in enumerate(chunks, start=1):
        freeze_snapshots_batch(
            list(chunk),
            args.code_archive_path,
            baseline=baseline,
            quality_report=quality_report,
            verify_source_manifest=False,
        )
        print(
            f"freeze progress batch={index}/{len(chunks)} "
            f"completed={min(index * args.batch_size, len(pending))}/{len(pending)}",
            flush=True,
        )
    print(
        f"freeze complete selected={len(rows)} newly_frozen={len(pending)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
