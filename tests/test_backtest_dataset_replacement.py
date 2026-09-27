from datetime import date

import pytest

from data_collect.jobs.backtest_dataset_replacement import (
    select_replacement_snapshot_ids,
)


def _source():
    return [
        {"research_date": date(2023, 9, 11), "research_snapshot_id": "old-a"},
        {"research_date": date(2023, 9, 12), "research_snapshot_id": "old-b"},
        {"research_date": date(2023, 9, 13), "research_snapshot_id": "old-c"},
    ]


def test_replacement_mapping_preserves_unrepaired_dates_and_order():
    rows = [{
        "research_date": "2023-09-12",
        "old_snapshot_id": "old-b",
        "new_snapshot_id": "new-b",
    }]
    assert select_replacement_snapshot_ids(_source(), rows) == [
        "old-a", "new-b", "old-c",
    ]


def test_replacement_mapping_rejects_stale_old_identity():
    with pytest.raises(ValueError, match="old snapshot mismatch"):
        select_replacement_snapshot_ids(
            _source(), [{
                "research_date": "2023-09-12",
                "old_snapshot_id": "wrong",
                "new_snapshot_id": "new-b",
            }]
        )


def test_replacement_mapping_rejects_duplicate_dates():
    row = {
        "research_date": "2023-09-12",
        "old_snapshot_id": "old-b",
        "new_snapshot_id": "new-b",
    }
    with pytest.raises(ValueError, match="duplicate"):
        select_replacement_snapshot_ids(_source(), [row, dict(row, new_snapshot_id="new-b2")])
