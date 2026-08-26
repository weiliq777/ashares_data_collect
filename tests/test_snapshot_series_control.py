from __future__ import annotations

from datetime import date, timedelta
import inspect
from pathlib import Path

import pytest

from data_collect.data_access.checkpoint import Checkpoint, InMemoryCheckpointStore
from data_collect.jobs.research_snapshot_series_ops import build_parser
from data_collect.data_access.repository.snapshot_series_repository import SnapshotSeriesRepository
from data_collect.jobs.research_snapshot import (
    freeze_snapshots_batch,
    inspect_snapshots_batch,
)
from data_collect.jobs.snapshot_series_control import (
    MonitorMode,
    SeriesSelector,
    SnapshotSeriesFinalize,
    SnapshotSeriesMonitor,
    SnapshotSeriesResume,
    bounded_batches,
    scan_log_tails,
)


DATES = tuple(date(2026, 1, day) for day in (2, 5, 6, 7))


def _row(index: int, status: str) -> dict:
    return {
        "snapshot_id": f"00000000-0000-0000-0000-{index:012d}",
        "research_cutoff_date": DATES[index - 1],
        "status": status,
        "input_manifest_hash": "i" * 64,
        "quality_report_hash": "q" * 64,
        "baseline_quality_report_hash": "b" * 64,
        "baseline_source_manifest_hash": "s" * 64,
        "git_commit": "abc",
        "git_dirty": False,
        "diff_hash": None,
        "created_at": None,
        "built_at": None,
        "validated_at": None,
        "frozen_at": None,
        "failed_at": None,
        "failure_reason": None,
    }


class FakeRepository:
    def __init__(self, rows, dates=DATES):
        self.rows = list(rows)
        self.dates = tuple(dates)
        self.max_rows_seen = []

    def open_trade_dates(self, start, end, exchange, context):
        return self.dates

    def series_inventory(
        self, baseline_tag, schema_version, start, end, context, *, max_rows,
    ):
        self.max_rows_seen.append(max_rows)
        return [dict(row) for row in self.rows]

    def set_statuses(self, ids, status):
        for row in self.rows:
            if str(row["snapshot_id"]) in ids:
                row["status"] = status


def _selector(expected=4):
    return SeriesSelector(
        DATES[0], DATES[-1], "baseline", "research-v2.0", expected,
    )


def test_progress_monitor_reads_only_bounded_manifest_inventory():
    repository = FakeRepository([
        _row(1, "BUILDING"), _row(2, "FAILED"),
        _row(3, "DRAFT"), _row(4, "VALIDATED"),
    ])
    report = SnapshotSeriesMonitor(repository, object()).inspect(_selector())

    assert report.status_counts == {
        "BUILDING": 1, "DRAFT": 1, "FAILED": 1, "VALIDATED": 1,
    }
    assert not report.blocking
    assert repository.max_rows_seen == [5]
    assert any(issue.code == "failed_work_queue" for issue in report.issues)


def test_progress_inventory_uses_a_direct_cutoff_range_without_large_table_joins():
    source = inspect.getsource(SnapshotSeriesRepository.series_inventory)
    assert "research_cutoff_date BETWEEN %s AND %s" in source
    assert "stock_snapshot_daily" not in source
    assert "snapshot_lineage" not in source
    assert "IS NULL OR research_cutoff_date" not in source


def test_acceptance_monitor_rejects_gaps_duplicates_and_pending_statuses():
    duplicate = _row(2, "VALIDATED")
    duplicate["research_cutoff_date"] = DATES[0]
    repository = FakeRepository([
        _row(1, "VALIDATED"), duplicate, _row(3, "BUILDING"),
    ])
    report = SnapshotSeriesMonitor(repository, object()).inspect(
        _selector(), mode=MonitorMode.ACCEPTANCE,
    )

    codes = {issue.code for issue in report.issues}
    assert report.blocking
    assert {"manifest_count_mismatch", "duplicate_cutoff", "missing_cutoff", "non_publishable_status"} <= codes


def test_resume_validates_building_before_building_failed_and_draft():
    repository = FakeRepository([
        _row(1, "BUILDING"), _row(2, "FAILED"),
        _row(3, "DRAFT"), _row(4, "VALIDATED"),
    ])
    calls = []

    def build(ids):
        calls.append(("build", tuple(ids)))
        repository.set_statuses(ids, "BUILDING")

    def validate(ids):
        calls.append(("validate", tuple(ids)))
        repository.set_statuses(ids, "VALIDATED")

    runner = SnapshotSeriesResume(
        SnapshotSeriesMonitor(repository, object()),
        Checkpoint(InMemoryCheckpointStore()),
        build,
        validate,
    )
    report = runner.run(_selector(), batch_size=2)

    assert [kind for kind, _ in calls] == ["validate", "build", "validate"]
    assert calls[0][1] == (_row(1, "BUILDING")["snapshot_id"],)
    assert report.status_counts == {"VALIDATED": 4}


def test_resume_never_exposes_a_building_rebuild_flag():
    help_text = build_parser().format_help()
    assert "rebuild-building" not in help_text
    resume = build_parser().parse_args([
        "resume", "--start-date", "2026-01-02", "--end-date", "2026-01-07",
        "--baseline-tag", "baseline", "--schema-version", "research-v2.0",
        "--expected-snapshots", "4",
    ])
    assert not hasattr(resume, "rebuild_building")


def test_freeze_batch_accepts_precomputed_bounded_evidence():
    parameters = inspect.signature(freeze_snapshots_batch).parameters
    assert {"baseline", "quality_report", "verify_source_manifest"} <= set(parameters)


def test_inspection_helper_contains_no_lifecycle_write_calls():
    source = inspect.getsource(inspect_snapshots_batch)
    assert "transition_snapshot" not in source
    assert "mark_failed" not in source
    assert "freeze_snapshot" not in source


def test_finalize_completes_all_deep_checks_before_first_freeze():
    repository = FakeRepository([_row(index, "VALIDATED") for index in range(1, 5)])
    calls = []

    def inspect(ids):
        calls.append(("inspect", tuple(ids)))
        return {}

    def freeze(ids):
        calls.append(("freeze", tuple(ids)))
        repository.set_statuses(ids, "FROZEN")

    runner = SnapshotSeriesFinalize(
        SnapshotSeriesMonitor(repository, object()),
        Checkpoint(InMemoryCheckpointStore()),
        inspect,
        freeze,
    )
    report = runner.run(_selector(), batch_size=2)

    assert [kind for kind, _ in calls] == [
        "inspect", "inspect", "inspect", "freeze", "inspect", "freeze",
    ]
    assert report.status_counts == {"FROZEN": 4}


def test_finalize_does_not_freeze_when_any_deep_batch_fails():
    repository = FakeRepository([_row(index, "VALIDATED") for index in range(1, 5)])
    frozen = []
    runner = SnapshotSeriesFinalize(
        SnapshotSeriesMonitor(repository, object()),
        Checkpoint(InMemoryCheckpointStore()),
        lambda ids: {ids[0]: [{"lineage": "missing"}]} if DATES[0].isoformat() else {},
        lambda ids: frozen.extend(ids),
    )

    with pytest.raises(RuntimeError, match="deep acceptance"):
        runner.run(_selector(), batch_size=2)
    assert frozen == []


def test_finalize_accepts_only_a_frozen_prefix_for_resume():
    valid = FakeRepository([
        _row(1, "FROZEN"), _row(2, "FROZEN"),
        _row(3, "VALIDATED"), _row(4, "VALIDATED"),
    ])
    report = SnapshotSeriesMonitor(valid, object()).inspect(
        _selector(), mode=MonitorMode.ACCEPTANCE,
    )
    with pytest.raises(RuntimeError, match="completed preflight checkpoint"):
        SnapshotSeriesFinalize._assert_resumable_state(report)
    SnapshotSeriesFinalize._assert_resumable_state(
        report, partial_resume_authorized=True,
    )

    invalid = FakeRepository([
        _row(1, "FROZEN"), _row(2, "VALIDATED"),
        _row(3, "FROZEN"), _row(4, "VALIDATED"),
    ])
    report = SnapshotSeriesMonitor(invalid, object()).inspect(
        _selector(), mode=MonitorMode.ACCEPTANCE,
    )
    with pytest.raises(RuntimeError, match="contiguous"):
        SnapshotSeriesFinalize._assert_resumable_state(
            report, partial_resume_authorized=True,
        )


def test_log_scanner_reads_bounded_tail_and_classifies_database_failures(tmp_path: Path):
    path = tmp_path / "snapshot.err.log"
    path.write_text(
        "normal progress\nERROR: deadlock detected\nDETAIL: process waits\n",
        encoding="utf-8",
    )
    findings = scan_log_tails([path], tail_bytes=1024)
    assert [(item.category, Path(item.path).name) for item in findings] == [
        ("deadlock", "snapshot.err.log"),
    ]


@pytest.mark.parametrize("batch_size", [0, 21])
def test_operational_batches_are_strictly_bounded(batch_size):
    with pytest.raises(ValueError):
        bounded_batches(range(5), batch_size)


def test_stale_building_is_warning_and_never_an_automatic_reset():
    row = _row(1, "BUILDING")
    row["built_at"] = date(2020, 1, 1)  # deliberately old naive temporal value
    repository = FakeRepository([row, _row(2, "VALIDATED"), _row(3, "VALIDATED"), _row(4, "VALIDATED")])
    # Use a datetime in the real row shape for the actual check.
    from datetime import datetime
    repository.rows[0]["built_at"] = datetime(2020, 1, 1)
    report = SnapshotSeriesMonitor(repository, object()).inspect(
        _selector(), stale_building_after=timedelta(hours=1),
    )
    issue = next(item for item in report.issues if item.code == "stale_building_candidate")
    assert issue.severity == "WARNING"
