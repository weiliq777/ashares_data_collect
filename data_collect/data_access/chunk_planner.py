"""ChunkPlanner：按【实际输入】切 chunk（§6）。

SqlPlan.estimated_rows 只用于预警/选模式/容量规划，【不是】事务切分的安全依据。
切分依据是实际 row_count + memory_size（rows 的真实长度与字节估算）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable, Iterator, Mapping, Sequence

from data_collect.data_access.errors import TransactionBudgetExceeded


@dataclass(frozen=True)
class ChunkConfig:
    max_rows_per_tx: int = 50_000
    max_bytes_per_tx: int = 67_108_864  # 64MB


class ChunkPlanner:
    """把实际行序列切分成不超过事务硬预算的 chunk。"""

    def __init__(self, config: ChunkConfig | None = None) -> None:
        self.config = config or ChunkConfig()
        if self.config.max_rows_per_tx <= 0 or self.config.max_bytes_per_tx <= 0:
            raise ValueError("max_rows_per_tx / max_bytes_per_tx 必须 > 0")

    @classmethod
    def estimate_row_bytes(cls, value: Any) -> int:
        """Return a conservative payload-size estimate without serializing rows.

        The estimate intentionally includes small structural overheads.  It is
        not a Python heap profiler; it is the transaction payload budget used
        to stop a few very wide rows from bypassing a row-count-only limit.
        """
        if value is None:
            return 1
        if isinstance(value, bool):
            return 1
        if isinstance(value, int):
            return 8
        if isinstance(value, float):
            return 8
        if isinstance(value, (date, datetime)):
            return 16
        if isinstance(value, Decimal):
            return len(str(value).encode("utf-8")) + 8
        if isinstance(value, str):
            return len(value.encode("utf-8")) + 4
        if isinstance(value, (bytes, bytearray, memoryview)):
            return len(value) + 4
        if isinstance(value, Mapping):
            return 16 + sum(
                cls.estimate_row_bytes(key) + cls.estimate_row_bytes(item)
                for key, item in value.items()
            )
        if isinstance(value, (list, tuple, set, frozenset)):
            return 16 + sum(cls.estimate_row_bytes(item) for item in value)
        return len(repr(value).encode("utf-8")) + 16

    def iter_chunks(
        self,
        rows: Iterable[Any],
        bytes_per_row: int = 0,
        max_rows_per_chunk: int | None = None,
    ) -> Iterator[list[Any]]:
        """Yield chunks bounded by both actual row count and estimated bytes."""
        if bytes_per_row < 0:
            raise ValueError("bytes_per_row must be >= 0")
        if max_rows_per_chunk is not None and max_rows_per_chunk <= 0:
            raise ValueError("max_rows_per_chunk must be > 0")
        row_limit = min(
            self.config.max_rows_per_tx,
            max_rows_per_chunk or self.config.max_rows_per_tx,
        )

        current: list[Any] = []
        current_bytes = 0
        for row in rows:
            row_bytes = bytes_per_row or self.estimate_row_bytes(row)
            if row_bytes > self.config.max_bytes_per_tx:
                raise TransactionBudgetExceeded(
                    "single row exceeds max_bytes_per_tx="
                    f"{self.config.max_bytes_per_tx}: estimated {row_bytes} bytes"
                )
            if current and (
                len(current) >= row_limit
                or current_bytes + row_bytes > self.config.max_bytes_per_tx
            ):
                yield current
                current = []
                current_bytes = 0
            current.append(row)
            current_bytes += row_bytes

        if current:
            yield current

    def plan(
        self,
        rows: Sequence[Any],
        bytes_per_row: int = 0,
        max_rows_per_chunk: int | None = None,
    ) -> list[list[Any]]:
        """按实际 row_count（+ 可选 memory_size）切 chunk。

        bytes_per_row <= 0 时仅按行数切；> 0 时同时按行数与字节数切。
        返回 list[list]，保证：
          - 每个 chunk 的行数 <= max_rows_per_tx
          - bytes_per_row > 0 时每个 chunk 的估算字节 <= max_bytes_per_tx
        """
        return list(
            self.iter_chunks(
                rows,
                bytes_per_row=bytes_per_row,
                max_rows_per_chunk=max_rows_per_chunk,
            )
        )
