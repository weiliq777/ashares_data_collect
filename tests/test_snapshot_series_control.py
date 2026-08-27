from __future__ import annotations

from datetime import date, datetime, timedelta
import inspect
from pathlib import Path

import pytest

from data_collect.jobs import research_snapshot as research_snapshot_job
from data_collect.data_access.checkpoint import Checkpoint, InMemoryCheckpointStore
from data_collect.jobs.research_snapshot_series_ops import build_parser
from data_collect.data_access.repository.snapshot_series_repository import SnapshotSeriesRepository
from data_collect.jobs.research_snapshot import (
    freeze_snapshots_batch,
    inspect_snapshots_batch,
)
from data_collect.data_access.repository.research_snapshot_repository import ResearchSnapshotRepository
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
    built_at = datetime(2026, 1, 8) if status in {"BUILDING", "VALIDATED", "FROZEN"} else None
    validated_at = datetime(2026, 1, 8) if status in {"VALIDATED", "FROZEN"} else None
    frozen_at = datetime(2026, 1, 8) if status == "FROZEN" else None
    return {
        "snapshot_id": f"00000000-0000-0000-0000-{index:012d}",
        "research_cutoff_date": DATES[index - 1],
        "status": status,
        "input_manifest_hash": "i" * 64,
        "quality_report_hash": "q" * 64,
        "baseline_quality_report_hash": "b" * 64,
        "baseline_source_manifest_hash": "s" * 64,
        "code_version": "abc",
        "git_commit": "abc",
        "git_dirty": False,
        "diff_hash": None,
        "code_archive_path": None,
        "created_at": None,
        "built_at": built_at,
        "validated_at": validated_at,
        "frozen_at": frozen_at,
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
                if status == "BUILDING":
                    row["built_at"] = datetime(2026, 1, 8)
                elif status == "VALIDATED":
                    row["built_at"] = row["built_at"] or datetime(2026, 1, 8)
                    row["validated_at"] = datetime(2026, 1, 8)
                elif status == "FROZEN":
                    row["validated_at"] = row["validated_at"] or datetime(2026, 1, 8)
                    row["frozen_at"] = datetime(2026, 1, 8)


class TrackingProgress:
    def __init__(self):
        self.inner = Checkpoint(InMemoryCheckpointStore())
        self.done_scopes = []

    def load(self, *args):
        return self.inner.load(*args)

    def mark_progress(self, *args):
        return self.inner.mark_progress(*args)

    def mark_done(self, run_id, job_name, scope_key):
        self.done_scopes.append(scope_key)
        return self.inner.mark_done(run_id, job_name, scope_key)


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


def test_monitor_rejects_status_without_required_lifecycle_timestamp():
    rows = [_row(index, "VALIDATED") for index in range(1, 5)]
    rows[0]["validated_at"] = None
    report = SnapshotSeriesMonitor(FakeRepository(rows), object()).inspect(
        _selector(), mode=MonitorMode.ACCEPTANCE,
    )
    assert any(issue.code == "lifecycle_evidence_incomplete" for issue in report.issues)
    assert report.blocking


def test_acceptance_reports_mixed_build_code_identity_without_losing_lineage():
    rows = [_row(index, "VALIDATED") for index in range(1, 5)]
    rows[-1]["code_version"] = "different"
    rows[-1]["git_commit"] = "different"
    report = SnapshotSeriesMonitor(FakeRepository(rows), object()).inspect(
        _selector(), mode=MonitorMode.ACCEPTANCE,
    )
    assert any(issue.code == "mixed_build_code_identity" for issue in report.issues)
    issue = next(issue for issue in report.issues if issue.code == "mixed_build_code_identity")
    assert issue.severity == "WARNING"
    assert not report.blocking


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

    progress = TrackingProgress()
    runner = SnapshotSeriesResume(
        SnapshotSeriesMonitor(repository, object()),
        progress,
        build,
        validate,
    )
    report = runner.run(_selector(), batch_size=2)

    assert [kind for kind, _ in calls] == ["validate", "build", "validate"]
    assert calls[0][1] == (_row(1, "BUILDING")["snapshot_id"],)
    assert report.status_counts == {"VALIDATED": 4}
    assert any(":validate-building:" in scope for scope in progress.done_scopes)
    assert any(":build-validate:" in scope for scope in progress.done_scopes)


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
    assert {
        "baseline", "quality_report", "verify_source_manifest",
        "metadata", "code_archive_evidence",
    } <= set(parameters)


def test_inspection_helper_contains_no_lifecycle_write_calls():
    source = inspect.getsource(inspect_snapshots_batch)
    assert "transition_snapshot" not in source
    assert "mark_failed" not in source
    assert "freeze_snapshot" not in source


def test_batch_validation_does_not_mark_failed_on_infrastructure_error(monkeypatch):
    service_calls = []
    monkeypatch.setattr(
        research_snapshot_job,
        "inspect_snapshots_batch",
        lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("database timeout")),
    )
    monkeypatch.setattr(
        research_snapshot_job,
        "_service",
        lambda label: service_calls.append(label),
    )
    with pytest.raises(TimeoutError, match="database timeout"):
        research_snapshot_job.validate_snapshots_batch([_row(1, "BUILDING")["snapshot_id"]])
    assert service_calls == []


def test_batch_validation_marks_only_snapshots_with_deterministic_issues(monkeypatch):
    first = _row(1, "BUILDING")["snapshot_id"]
    second = _row(2, "BUILDING")["snapshot_id"]
    manifests = {
        first: {
            "snapshot_id": first, "cutoff": DATES[0], "baseline": "baseline",
            "schema_version": "research-v2.0", "status": "BUILDING",
        },
        second: {
            "snapshot_id": second, "cutoff": DATES[1], "baseline": "baseline",
            "schema_version": "research-v2.0", "status": "BUILDING",
        },
    }

    class Repository:
        def __init__(self):
            self.failed = []
            self.validated = []

        def manifest(self, snapshot_id, context):
            return manifests[snapshot_id]

        def mark_failed(self, snapshot_id, cutoff, reason, context):
            self.failed.append(snapshot_id)

        def validate_snapshot(self, manifest, baseline, quality_hash, issues, context):
            self.validated.append(str(manifest["snapshot_id"]))

    class Kernel:
        def close(self):
            pass

    repository = Repository()
    monkeypatch.setattr(
        research_snapshot_job,
        "inspect_snapshots_batch",
        lambda *args, **kwargs: {first: [{"lineage": "missing"}]},
    )
    monkeypatch.setattr(
        research_snapshot_job,
        "_service",
        lambda label: (Kernel(), repository, object()),
    )
    with pytest.raises(RuntimeError, match="批量校验失败"):
        research_snapshot_job.validate_snapshots_batch(
            [first, second],
            baseline=("baseline",),
            quality_reports={
                first: ({"failures": {}}, "quality"),
                second: ({"failures": {}}, "quality"),
            },
        )
    assert repository.failed == [first]
    assert repository.validated == [second]


def test_freeze_preserves_the_code_identity_recorded_by_build():
    source = inspect.getsource(ResearchSnapshotRepository.freeze_snapshot)
    assert '"code_version": metadata' not in source
    assert '"git_commit": metadata' not in source
    assert '"git_dirty": metadata' not in source


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


def test_finalize_skips_completed_preflight_after_restart():
    repository = FakeRepository([_row(index, "VALIDATED") for index in range(1, 5)])
    calls = []
    checkpoint = Checkpoint(InMemoryCheckpointStore())
    run_id = f"snapshot-series-finalize-{_selector().digest}"
    checkpoint.mark_done(run_id, "snapshot_series_finalize", f"{_selector().digest}:preflight")

    def inspect(ids):
        calls.append(("inspect", tuple(ids)))
        return {}

    def freeze(ids):
        calls.append(("freeze", tuple(ids)))
        repository.set_statuses(ids, "FROZEN")

    runner = SnapshotSeriesFinalize(
        SnapshotSeriesMonitor(repository, object()), checkpoint, inspect, freeze,
    )
    report = runner.run(_selector(), batch_size=2)

    assert [kind for kind, _ in calls] == [
        "inspect", "freeze", "inspect", "freeze",
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


def test_finalize_rejects_dirty_builds_before_deep_scan_without_archive():
    rows = [_row(index, "VALIDATED") for index in range(1, 5)]
    for row in rows:
        row["git_dirty"] = True
        row["diff_hash"] = "d" * 64
    inspected = []
    runner = SnapshotSeriesFinalize(
        SnapshotSeriesMonitor(FakeRepository(rows), object()),
        Checkpoint(InMemoryCheckpointStore()),
        lambda ids: inspected.extend(ids) or {},
        lambda ids: None,
        code_archive_available=False,
    )
    with pytest.raises(RuntimeError, match="code_archive_path"):
        runner.run(_selector(), batch_size=2)
    assert inspected == []


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
    assert findings[0].severity == "ERROR"


def test_bare_rollback_is_reported_without_becoming_a_false_fatal(tmp_path: Path):
    path = tmp_path / "snapshot.out.log"
    path.write_text("smoke verification complete; ROLLBACK\n", encoding="utf-8")
    repository = FakeRepository([_row(index, "VALIDATED") for index in range(1, 5)])
    report = SnapshotSeriesMonitor(repository, object()).inspect(
        _selector(), mode=MonitorMode.ACCEPTANCE, log_paths=[path],
    )
    assert report.log_findings[0].category == "rollback"
    assert report.log_findings[0].severity == "WARNING"
    assert not report.blocking


@pytest.mark.parametrize("batch_size", [0, 21])
def test_operational_batches_are_strictly_bounded(batch_size):
    with pytest.raises(ValueError):
        bounded_batches(range(5), batch_size)


def test_stale_building_is_warning_and_never_an_automatic_reset():
    row = _row(1, "BUILDING")
    row["built_at"] = date(2020, 1, 1)  # deliberately old naive temporal value
    repository = FakeRepository([row, _row(2, "VALIDATED"), _row(3, "VALIDATED"), _row(4, "VALIDATED")])
    # Use a datetime in the real row shape for the actual check.
    repository.rows[0]["built_at"] = datetime(2020, 1, 1)
    report = SnapshotSeriesMonitor(repository, object()).inspect(
        _selector(), stale_building_after=timedelta(hours=1),
    )
    issue = next(item for item in report.issues if item.code == "stale_building_candidate")
    assert issue.severity == "WARNING"
