from datetime import date

import pytest

from data_collect.jobs.research_snapshot_series import (
    create_snapshot_series,
    normalize_cutoff_dates,
    validate_cutoff_dates_are_open,
)


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
