"""Opt-in, read-only exercise of the long-history Snapshot lookup path.

Run from the repository root with:

    $env:ASHARES_RUN_SNAPSHOT_HISTORY_PERF = "1"
    $env:ASHARES_SNAPSHOT_HISTORY_BASELINE = "<existing baseline tag>"
    $env:ASHARES_SNAPSHOT_HISTORY_SCHEMA = "<existing schema version>"
    uv run pytest -o addopts= -m integration -s tests/test_snapshot_history_performance.py

The test reads the real trading calendar and snapshot manifests. Its PostgreSQL
connections are forced into read-only mode; it never creates or changes rows.
It reports timings instead of asserting a machine-dependent speed threshold.
"""
from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import os
from threading import Lock
from time import perf_counter
from uuid import uuid4

import pytest

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.checkpoint import InMemoryCheckpointStore
from data_collect.data_access.errors import LockNotAcquired
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.snapshot_series_repository import (
    SnapshotSeriesRepository,
)
from data_collect.data_access.schema import connect_with_safety_limits
from data_collect.data_access.snapshot_lifecycle import (
    MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE,
)


pytestmark = pytest.mark.integration


def _read_only_kernel() -> DataAccessKernel:
    def connect_read_only():
        connection = connect_with_safety_limits()
        connection.set_session(readonly=True)
        return connection

    return DataAccessKernel(
        policy_registry=build_default_registry(),
        connection_factory=connect_read_only,
        checkpoint_store=InMemoryCheckpointStore(),
    )


def _context(label: str) -> RunContext:
    return RunContext(
        run_id=f"snapshot-history-perf-{label}-{uuid4().hex}",
        job_name="snapshot_history_read_only_perf",
        code_version="test",
        config_hash="test",
        pipeline_version="data-access-v1",
    )


def _lookup(
    date_groups: tuple[tuple[date, ...], ...], baseline: str, schema: str,
) -> tuple[dict[date, tuple[str, str]], int, int, float]:
    """Read one worker's disjoint date groups through the real repository path."""
    kernel = _read_only_kernel()
    repository = SnapshotSeriesRepository(kernel)
    context = _context("worker")
    result: dict[date, tuple[str, str]] = {}
    query_count = 0
    largest_date_batch = 0
    repository_query = kernel.read._repository_query

    def count_queries(*args, **kwargs):
        nonlocal query_count, largest_date_batch
        params = kwargs.get("params")
        if params is not None and len(params) == 3 and isinstance(params[2], list):
            query_count += 1
            largest_date_batch = max(largest_date_batch, len(params[2]))
        return repository_query(*args, **kwargs)

    kernel.read._repository_query = count_queries
    started = perf_counter()
    try:
        for dates in date_groups:
            result.update(repository.existing_snapshots(dates, baseline, schema, context))
    finally:
        kernel.close()
    return result, query_count, largest_date_batch, perf_counter() - started


def _partition_by_month(trade_dates: tuple[date, ...]) -> tuple[tuple[date, ...], ...]:
    grouped: dict[tuple[int, int], list[date]] = defaultdict(list)
    for trade_date in trade_dates:
        grouped[(trade_date.year, trade_date.month)].append(trade_date)
    return tuple(tuple(grouped[key]) for key in sorted(grouped))


def test_long_history_snapshot_lookup_is_correct_and_bounded():
    if os.getenv("ASHARES_RUN_SNAPSHOT_HISTORY_PERF") != "1":
        pytest.skip("set ASHARES_RUN_SNAPSHOT_HISTORY_PERF=1 to run the live read-only test")

    baseline = os.getenv("ASHARES_SNAPSHOT_HISTORY_BASELINE", "").strip()
    schema = os.getenv("ASHARES_SNAPSHOT_HISTORY_SCHEMA", "").strip()
    if not baseline or not schema:
        pytest.fail(
            "set ASHARES_SNAPSHOT_HISTORY_BASELINE and "
            "ASHARES_SNAPSHOT_HISTORY_SCHEMA to an existing snapshot series"
        )

    start = date.fromisoformat(os.getenv("ASHARES_SNAPSHOT_HISTORY_START", "2016-01-01"))
    end = date.fromisoformat(os.getenv("ASHARES_SNAPSHOT_HISTORY_END", date.today().isoformat()))
    if start > end:
        pytest.fail("snapshot history start date must not be after end date")

    # Discover the actual rows and trading dates through read-only Repository APIs.
    kernel = _read_only_kernel()
    repository = SnapshotSeriesRepository(kernel)
    context = _context("inventory")
    try:
        trade_dates = repository.open_trade_dates(start, end, "SSE", context)
        inventory = repository.series_inventory(
            baseline, schema, start, end, context, max_rows=10_000,
        )
    finally:
        kernel.close()

    assert len(trade_dates) >= 1_000, (
        f"expected a long history (at least 1,000 open dates), got {len(trade_dates)}"
    )
    assert len(inventory) >= 500, (
        f"expected a populated historical series (at least 500 manifests), got {len(inventory)}"
    )
    trade_date_set = set(trade_dates)
    expected = {
        row["research_cutoff_date"]: (str(row["snapshot_id"]), row["status"])
        for row in inventory
        if row["research_cutoff_date"] in trade_date_set
    }

    # Current production lookup: one serial run over the whole date range.
    serial_result, serial_queries, serial_max_batch, serial_elapsed = _lookup(
        (trade_dates,), baseline, schema,
    )
    assert serial_result == expected
    assert 1 <= serial_max_batch <= MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE
    assert serial_queries == (
        len(trade_dates) + MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE - 1
    ) // MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE

    # Concurrency experiment: assign whole months to two workers, each with an
    # independent read-only connection and no overlapping cutoff dates.
    month_groups = _partition_by_month(trade_dates)
    worker_groups = (month_groups[::2], month_groups[1::2])
    lock = Lock()
    parallel_results: list[dict[date, tuple[str, str]]] = []
    parallel_queries = 0

    def run_worker(groups: tuple[tuple[date, ...], ...]) -> None:
        nonlocal parallel_queries
        result, query_count, max_batch, _elapsed = _lookup(groups, baseline, schema)
        assert 1 <= max_batch <= MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE
        with lock:
            parallel_results.append(result)
            parallel_queries += query_count

    started = perf_counter()
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run_worker, groups) for groups in worker_groups]
        for future in futures:
            future.result()
    parallel_elapsed = perf_counter() - started

    combined_parallel_result: dict[date, tuple[str, str]] = {}
    for result in parallel_results:
        overlap = combined_parallel_result.keys() & result.keys()
        assert not overlap, f"month partitions unexpectedly overlap: {sorted(overlap)[:3]}"
        combined_parallel_result.update(result)
    assert combined_parallel_result == expected

    month_query_floor = sum(
        (len(group) + MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE - 1)
        // MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE
        for group in month_groups
    )
    assert parallel_queries == month_query_floor

    speedup = serial_elapsed / parallel_elapsed if parallel_elapsed else float("inf")
    print(
        "snapshot-history read-only benchmark: "
        f"range={start}..{end}, open_dates={len(trade_dates)}, "
        f"matching_snapshots={len(expected)}, months={len(month_groups)}, "
        f"serial_queries={serial_queries}, serial_seconds={serial_elapsed:.3f}, "
        f"two_worker_month_queries={parallel_queries}, "
        f"two_worker_month_seconds={parallel_elapsed:.3f}, observed_speedup={speedup:.2f}x"
    )


def test_snapshot_lifecycle_locks_serialize_overlap_and_allow_disjoint_months():
    if os.getenv("ASHARES_RUN_SNAPSHOT_HISTORY_PERF") != "1":
        pytest.skip("set ASHARES_RUN_SNAPSHOT_HISTORY_PERF=1 to run live advisory-lock checks")

    first_kernel = _read_only_kernel()
    second_kernel = _read_only_kernel()
    target = "research.snapshot.history-perf"
    run_key = uuid4().hex
    first_month = f"{run_key}:2025-01"
    second_month = f"{run_key}:2025-02"
    try:
        # Separate sessions can hold locks for disjoint business partitions.
        with first_kernel.lock_manager.session_exclusive_many(
            target, [first_month], wait=False,
        ):
            with second_kernel.lock_manager.session_exclusive_many(
                target, [second_month], wait=False,
            ):
                pass

            # The same partition remains protected while its owner is active.
            with pytest.raises(LockNotAcquired):
                with second_kernel.lock_manager.session_exclusive_many(
                    target, [first_month], wait=False,
                ):
                    pytest.fail("overlapping snapshot partition unexpectedly acquired its lock")
    finally:
        first_kernel.close()
        second_kernel.close()
