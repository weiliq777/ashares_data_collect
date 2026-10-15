from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace

import pytest

from data_collect.jobs.research_snapshot_series import (
    create_snapshot_series,
    normalize_cutoff_dates,
    validate_cutoff_dates_are_open,
)
from data_collect.data_access.repository.snapshot_series_repository import SnapshotSeriesRepository


def test_snapshot_series_dates_are_sorted_and_unique():
    assert normalize_cutoff_dates(["2026-01-06", date(2026, 1, 2)]) == (
        date(2026, 1, 2), date(2026, 1, 6)
    )
    with pytest.raises(ValueError, match="不能重复"):
        normalize_cutoff_dates(["2026-01-02", "2026-01-02"])


def test_snapshot_series_requires_ordered_lifecycle_flags():
    with pytest.raises(ValueError, match="build=True"):
        create_snapshot_series(["2026-01-02"], "baseline", build=False, validate=True)
    with pytest.raises(ValueError, match="validate=True"):
        create_snapshot_series(["2026-01-02"], "baseline", build=True, validate=False, freeze=True)


def test_snapshot_series_has_a_safe_default_size_limit():
    with pytest.raises(ValueError, match="安全上限"):
        create_snapshot_series(
            ["2026-01-02", "2026-01-03"], "baseline", build=False,
            validate=False, max_snapshots=1,
        )


def test_snapshot_series_rejects_explicit_closed_cutoff(monkeypatch):
    monkeypatch.setattr(
        "data_collect.jobs.research_snapshot_series.open_trade_dates",
        lambda start, end, exchange="SSE": (date(2026, 1, 2),),
    )
    with pytest.raises(ValueError, match="开市日"):
        validate_cutoff_dates_are_open(["2026-01-03"])


def test_snapshot_series_detects_partial_build_before_validation():
    calls = []
    repository = SnapshotSeriesRepository.__new__(SnapshotSeriesRepository)

    def fake_query(context, source, sql, params, scope, max_rows):
        calls.append((source, sql, params, max_rows))
        return [{"snapshot_id": "partial-id"}]

    repository.kernel = SimpleNamespace(
        read=SimpleNamespace(_repository_query=fake_query),
    )
    result = repository.incomplete_building_snapshots(
        ["partial-id", "complete-id"], 10, "context"
    )

    assert result == ("partial-id",)
    assert calls[0][0] == "research.snapshot_manifest"
    assert calls[0][2] == (
        ["partial-id", "complete-id"], ["partial-id", "complete-id"], 10
    )
    assert calls[0][3] == 3


def test_snapshot_series_reopen_requires_explicit_reason_and_updates_validated():
    calls = []
    lock_events = []
    repository = SnapshotSeriesRepository.__new__(SnapshotSeriesRepository)

    class LockManager:
        @contextmanager
        def session_exclusive_many(self, target, scope_keys, *, wait=False):
            lock_events.append(("acquired", target, tuple(scope_keys), wait))
            try:
                yield
            finally:
                lock_events.append(("released", target, tuple(scope_keys)))

    def fake_transition(**kwargs):
        calls.append(kwargs)
        lock_events.append(("write",))
        return len(kwargs["rows"])

    repository.kernel = SimpleNamespace(
        write=SimpleNamespace(transition_snapshot_manifests=fake_transition),
        lock_manager=LockManager(),
    )
    with pytest.raises(ValueError, match="reason"):
        repository.reopen_validated_snapshots(["snapshot-id"], " ", "context")

    assert repository.reopen_validated_snapshots(
        ["snapshot-id"], "repair dirty build", "context"
    ) == 1
    assert calls[0]["from_statuses"] == ("VALIDATED",)
    assert calls[0]["to_status"] == "FAILED"
    assert calls[0]["rows"][0]["failure_reason"].startswith("PRE_FREEZE_REPAIR:")
    assert lock_events == [
        ("acquired", "research.snapshot", ("snapshot-id",), True),
        ("write",),
        ("released", "research.snapshot", ("snapshot-id",)),
    ]
