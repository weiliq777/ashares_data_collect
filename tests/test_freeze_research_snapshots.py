import pytest

from data_collect.jobs.freeze_research_snapshots import _batches


def test_snapshot_freeze_batches_are_bounded_and_ordered():
    assert _batches(range(7), 3) == ((0, 1, 2), (3, 4, 5), (6,))


@pytest.mark.parametrize("batch_size", [0, 101])
def test_snapshot_freeze_batch_size_is_bounded(batch_size):
    with pytest.raises(ValueError):
        _batches((1,), batch_size)
