"""Repository adapter for market activity reads.

The expensive SQL lives here, at the Repository seam.  Callers receive
bounded batches of dictionaries and never receive a Connection/Cursor or a
generic SQL executor.
"""
from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Any

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class MarketActivityRepository(BaseRepository):
    """Read the canonical Standard daily bars for one bounded date window."""

    def stream_daily_bars(
        self,
        start: date,
        end: date,
        context: RunContext,
        *,
        batch_rows: int = 10_000,
        limit_rows: int = 250_001,
    ) -> Iterable[list[dict[str, Any]]]:
        if start > end:
            raise ValueError("market activity start must not be after end")
        if (end - start).days > 180:
            raise ValueError("market activity date window must not exceed 180 calendar days")
        if not 1 <= batch_rows <= 10_000:
            raise ValueError("market activity batch_rows must be in 1..10000")
        if not 2 <= limit_rows <= 1_000_001:
            raise ValueError("market activity limit_rows must be in 2..1000001")
        scope = WorkScope(
            date_from=start,
            date_to=end,
            partition="market-activity:daily-kline",
        )
        return self.kernel.read._repository_stream(
            context=context,
            source="std.daily_kline",
            sql="""
                SELECT stock_code, trade_date, close, volume, amount
                  FROM std.daily_kline
                 WHERE trade_date BETWEEN %s AND %s
                 ORDER BY trade_date, stock_code
                 LIMIT %s
            """,
            params=(start, end, limit_rows),
            scope=scope,
            batch_rows=batch_rows,
        )
