"""Typed append/read primitives for legacy collection jobs.

The job layer supplies normalized rows; this Repository owns the table
allow-list and the Kernel write/read policy boundary.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


_TABLES = {
    "block_trade": "trade_date", "daily_kline": "trade_date", "fund_flow_daily": "trade_date",
    "margin_daily": "trade_date", "valuation_daily": "trade_date", "divid_factors": "date",
    "etf_divid_factors": "date", "etf_minute": "日期", "etf_option_daily": "trade_date",
    "a_share_minute": "trade_date", "index_minute": "trade_date",
    "daily_kline_rebuild_validation": "trade_date", "minutedata_tdx": "交易日期",
}


class CollectorRepository(BaseRepository):
    def _target(self, table: str) -> str:
        if table not in _TABLES:
            raise ValueError(f"unsupported collector table: {table}")
        return {
            "daily_kline": "std.daily_kline",
            "valuation_daily": "std.valuation_daily",
        }.get(table, table)

    def append(self, table: str, rows: Sequence[Mapping[str, Any]], context: RunContext, *, partition: str = "collector") -> tuple[int, int]:
        target = self._target(table)
        if not rows:
            return 0, 0
        dates = [row.get(_TABLES[table]) for row in rows if row.get(_TABLES[table]) is not None]
        scope = WorkScope(partition=partition, date_from=min(dates) if dates else None, date_to=max(dates) if dates else None)
        if target == "std.daily_kline":
            materialized = list(rows)
            key = ("stock_code", "trade_date")
            update_columns = tuple(
                column for column in materialized[0].keys() if column not in key
            )
            # After the validated physical migration, std.daily_kline is the
            # sole canonical Standard target.  public.daily_kline is retained
            # as an immutable legacy backup/source and is never dual-written.
            written = self.kernel.write.upsert(
                context=context, scope=scope, target=target,
                rows=materialized, key=key, update_columns=update_columns,
                skip_noop=False,
            )
            return written, len(rows)
        return self.kernel.write.bulk_insert_ignore(context, scope, target, list(rows))

    def dates_with_data(self, table: str, start_date: date, end_date: date, context: RunContext) -> set[date]:
        target = self._target(table)
        column = _TABLES[table]
        rows = self.kernel.read._repository_query(
            context, target,
            f'SELECT DISTINCT "{column}" AS value FROM {target} WHERE "{column}" >= %s AND "{column}" <= %s',
            (start_date, end_date), WorkScope(partition=f"dates:{target}", date_from=start_date, date_to=end_date), 100_000,
        )
        return {row["value"] for row in rows}

    def replace_day(self, table: str, day: date, rows: Sequence[Mapping[str, Any]], context: RunContext) -> tuple[int, int]:
        if table not in {"fund_flow_daily", "etf_option_daily"}:
            raise ValueError(f"replace_day is not allowed for collector table: {table}")
        scope = WorkScope(partition=f"replace:{table}", date_from=day, date_to=day)
        return self.kernel.write.replace_day(context, scope, table, _TABLES[table], day, list(rows))

    def existing_codes(self, table: str, day: date, code_column: str, context: RunContext) -> set[str]:
        if table != "index_minute" or code_column != "index_code":
            raise ValueError("existing_codes is only defined for index_minute")
        rows = self.kernel.read._repository_query(
            context, table, "SELECT DISTINCT index_code FROM index_minute WHERE trade_date=%s",
            (day,), WorkScope(partition="index-minute", date_from=day, date_to=day), 100_000,
        )
        return {str(row["index_code"]) for row in rows}

    def existing_code_dates(self, context: RunContext, start_date: date, end_date: date) -> set[tuple[str, str]]:
        rows = self.kernel.read._repository_query(
            context, "minutedata_tdx", 'SELECT DISTINCT "交易日期" AS trade_date,"代码" AS stock_code FROM minutedata_tdx WHERE "交易日期">=%s AND "交易日期"<=%s',
            (start_date, end_date), WorkScope(partition="minute-dates", date_from=start_date, date_to=end_date), 500_000,
        )
        return {(str(row["trade_date"]), str(row["stock_code"]).strip()) for row in rows}
