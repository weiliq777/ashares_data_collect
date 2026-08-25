"""RawRepository：Raw 层领域仓储。"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class RawRepository(BaseRepository):
    def append(
        self,
        context: RunContext,
        dataset: str,
        rows: Sequence[Mapping[str, Any]],
        business_key: Sequence[str],
        payload_hash_column: str = "payload_hash",
    ) -> int:
        return self.kernel.raw.append(
            context=context, dataset=dataset, rows=rows,
            business_key=business_key, payload_hash_column=payload_hash_column,
        )

    def recover_corrupt_window(
        self, context: RunContext, target: str, source: str,
        start_date, end_date, codes: Sequence[str] | None = None,
    ) -> int:
        return self.kernel.raw.recover_from_corrupt_table(
            context=context, target=target, source=source,
            start_date=start_date, end_date=end_date, codes=codes,
            scope=WorkScope(
                date_from=start_date, date_to=end_date,
                codes=tuple(codes) if codes is not None else None,
                partition=f"recovery:{target}",
            ),
        )

    def stock_codes(self, context: RunContext) -> list[str]:
        rows = self.kernel.read.query(
            context=context, source="std.stock_basic_info", columns=["stock_code"],
            limit=100_000, scope=WorkScope(partition="recovery:stock-codes"),
        )
        return [row[0] for row in rows]
