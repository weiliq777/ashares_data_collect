"""Typed checkpoints for resumable source-data ingestion."""
from __future__ import annotations

from typing import Any, Iterable

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class HistoryRepository(BaseRepository):
    """Owns durable ingestion checkpoints and source-universe reads."""

    def status(self, dataset: str, key: str, context: RunContext) -> str | None:
        rows = self.kernel.read.query(
            context=context, source="ops.tushare_history_checkpoint", columns=["status"],
            where={"dataset__eq": dataset, "checkpoint_key__eq": key},
            scope=WorkScope(partition=f"checkpoint:{dataset}:{key}"), limit=1,
        )
        return str(rows[0][0]) if rows else None

    def completed_covering_window(
        self, dataset: str, code: str, start: str, end: str, context: RunContext
    ) -> bool:
        rows = self.kernel.read._repository_query(
            context=context, source="ops.tushare_history_checkpoint", sql="""
                SELECT 1
                  FROM ops.tushare_history_checkpoint
                 WHERE dataset=%s AND status='done'
                   AND split_part(checkpoint_key, ':', 1)=%s
                   AND split_part(checkpoint_key, ':', 2)<=%s
                   AND split_part(checkpoint_key, ':', 3)>=%s
                 LIMIT 1
            """, params=(dataset, code, start, end),
            scope=WorkScope(partition=f"checkpoint:{dataset}:{code}"), max_rows=1,
        )
        return bool(rows)

    def save(
        self, dataset: str, key: str, status: str, rows_written: int,
        context: RunContext, error: str | None = None,
    ) -> int:
        row: dict[str, Any] = {
            "dataset": dataset, "checkpoint_key": key, "status": status,
            "rows_written": int(rows_written), "error_message": error,
        }
        return self.kernel.write.upsert(
            context=context, scope=WorkScope(partition=f"checkpoint:{dataset}:{key}"),
            target="ops.tushare_history_checkpoint", rows=[row],
            key=("dataset", "checkpoint_key"),
            update_columns=("status", "rows_written", "error_message"),
        )

    def pending_keys(self, dataset: str, statuses: Iterable[str], context: RunContext) -> list[str]:
        values = tuple(statuses)
        if not values:
            return []
        rows = self.kernel.read._repository_query(
            context=context, source="ops.tushare_history_checkpoint", sql="""
                SELECT checkpoint_key
                  FROM ops.tushare_history_checkpoint
                 WHERE dataset=%s AND status=ANY(%s)
                 ORDER BY checkpoint_key
            """, params=(dataset, list(values)),
            scope=WorkScope(partition=f"checkpoint:{dataset}:pending"), max_rows=100_000,
        )
        return [str(row["checkpoint_key"]) for row in rows]

    def active_stock_codes(self, context: RunContext) -> list[str]:
        rows = self.kernel.read.query(
            context=context, source="std.stock_basic_info", columns=["stock_code"],
            where={"list_status__in": ["L", "D", "P", "G"]},
            scope=WorkScope(partition="active-stock-codes"),
        )
        return sorted(str(row[0]) for row in rows)

    def raw_snapshot_count(self, table: str, trade_date, context: RunContext) -> int:
        if table not in {"tushare_daily_raw", "tushare_daily_basic_raw"}:
            raise ValueError(f"unsupported historical raw table: {table}")
        rows = self.kernel.read._repository_query(
            context=context,
            source=table,
            sql=f"SELECT COUNT(*) AS count FROM {table} WHERE trade_date=%s",
            params=(trade_date,),
            scope=WorkScope(date_from=trade_date, date_to=trade_date),
            max_rows=1,
        )
        return int(rows[0]["count"] or 0) if rows else 0
