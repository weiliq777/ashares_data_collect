"""Shared bounds for Research Snapshot lifecycle operations."""
from __future__ import annotations

from collections.abc import Iterable, Iterator


MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE = 20


def snapshot_id_batches(
    snapshot_ids: Iterable[str], batch_size: int = MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE,
) -> Iterator[tuple[str, ...]]:
    """Return stable, de-duplicated ID batches within the lifecycle SQL bound."""
    if isinstance(snapshot_ids, (str, bytes)):
        raise TypeError("snapshot_ids must be an iterable of IDs, not a string")
    if batch_size < 1 or batch_size > MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE:
        raise ValueError(
            "snapshot batch_size must be between 1 and "
            f"{MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE}"
        )
    ids = tuple(dict.fromkeys(str(snapshot_id) for snapshot_id in snapshot_ids))
    if any(not snapshot_id.strip() for snapshot_id in ids):
        raise ValueError("snapshot batches do not accept blank snapshot ids")
    for offset in range(0, len(ids), batch_size):
        yield ids[offset:offset + batch_size]


def require_snapshot_batch(
    snapshot_ids: Iterable[str], operation: str, *, allow_empty: bool = True,
) -> tuple[str, ...]:
    """Validate IDs passed to one atomic lifecycle operation."""
    if isinstance(snapshot_ids, (str, bytes)):
        raise TypeError(f"{operation} requires an iterable of snapshot IDs")
    ids = tuple(str(snapshot_id) for snapshot_id in snapshot_ids)
    if not ids and not allow_empty:
        raise ValueError(f"{operation} requires at least one snapshot id")
    if len(ids) != len(set(ids)):
        raise ValueError(f"{operation} requires unique snapshot ids")
    if any(not snapshot_id.strip() for snapshot_id in ids):
        raise ValueError(f"{operation} does not accept blank snapshot ids")
    if len(ids) > MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE:
        raise ValueError(
            f"{operation} is limited to {MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE} snapshots"
        )
    return ids
