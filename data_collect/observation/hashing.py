"""观察输入/结果的流式确定性哈希。

哈希函数按输入顺序逐行更新摘要，不把整个历史结果集 materialize（物化）到
内存中。调用方必须让 Repository（仓储）以稳定业务键排序后再传入。
"""
from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from data_collect.observation.contracts import canonical_json, canonicalize


def hash_rows(
    rows: Iterable[Mapping[str, Any] | Sequence[Any]],
    *,
    columns: Sequence[str] | None = None,
    algorithm: str = "sha256",
) -> tuple[str, int]:
    """Return ``(digest, row_count)`` using bounded per-row updates.

    Mapping rows use the explicitly supplied column order. Sequence rows are
    hashed as-is. A newline delimiter makes row boundaries unambiguous.
    """
    digest = hashlib.new(algorithm)
    count = 0
    for row in rows:
        if isinstance(row, Mapping):
            if columns is None:
                payload = {str(key): canonicalize(value) for key, value in sorted(row.items())}
            else:
                payload = [canonicalize(row.get(column)) for column in columns]
        else:
            payload = [canonicalize(value) for value in row]
        digest.update(canonical_json(payload).encode("utf-8"))
        digest.update(b"\n")
        count += 1
    return digest.hexdigest(), count


def hash_object(value: Any, *, algorithm: str = "sha256") -> str:
    return hashlib.new(algorithm, canonical_json(value).encode("utf-8")).hexdigest()


__all__ = ["hash_rows", "hash_object"]
