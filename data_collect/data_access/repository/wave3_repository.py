"""Typed Repository for the wave3 technical-screening job."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class Wave3Repository(BaseRepository):
    def _scope(self, partition: str, *, date_from: date | None = None, date_to: date | None = None) -> WorkScope:
        return WorkScope(partition=partition, date_from=date_from, date_to=date_to)

    def daily_kline(self, start_date: date, end_date: date, context: RunContext) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(context, "std.daily_kline", """SELECT trade_date,stock_code,open,high,low,close,volume,amount FROM std.daily_kline WHERE trade_date>=%s AND trade_date<=%s ORDER BY stock_code,trade_date""", (start_date, end_date), self._scope("wave3-kline", date_from=start_date, date_to=end_date), 500_000)

    def daily_kline_chunked(
        self, start_date: date, end_date: date, context: RunContext, *, chunk_days: int = 31
    ) -> list[dict[str, Any]]:
        """Read a bounded warm-up window without crossing the repository row cap."""
        if start_date > end_date:
            return []
        if chunk_days <= 0:
            raise ValueError("chunk_days must be positive")
        rows: list[dict[str, Any]] = []
        cursor = start_date
        while cursor <= end_date:
            window_end = min(end_date, cursor + timedelta(days=chunk_days - 1))
            rows.extend(self.daily_kline(cursor, window_end, context))
            cursor = window_end + timedelta(days=1)
        rows.sort(key=lambda row: (str(row["stock_code"]), row["trade_date"]))
        return rows

    def daily_kline_stock_codes(
        self, start_date: date, end_date: date, context: RunContext
    ) -> list[str]:
        """Return the bounded output universe for a technical-feature window."""
        if start_date > end_date:
            return []
        rows = self.kernel.read._repository_query(
            context,
            "std.daily_kline",
            """SELECT DISTINCT stock_code
                 FROM std.daily_kline
                WHERE trade_date >= %s AND trade_date <= %s
                ORDER BY stock_code""",
            (start_date, end_date),
            self._scope("wave3-kline-codes", date_from=start_date, date_to=end_date),
            10_000,
        )
        return [str(row["stock_code"]) for row in rows]

    def daily_kline_for_codes(
        self,
        start_date: date,
        end_date: date,
        stock_codes: Sequence[str],
        context: RunContext,
    ) -> list[dict[str, Any]]:
        """Read a warm-up window for one bounded stock batch."""
        if start_date > end_date:
            return []
        codes = tuple(str(code) for code in stock_codes if code)
        if not codes:
            return []
        return self.kernel.read._repository_query(
            context,
            "std.daily_kline",
            """SELECT trade_date,stock_code,open,high,low,close,volume,amount
                 FROM std.daily_kline
                WHERE trade_date >= %s AND trade_date <= %s
                  AND stock_code = ANY(%s)
                ORDER BY stock_code,trade_date""",
            (start_date, end_date, list(codes)),
            WorkScope(
                date_from=start_date,
                date_to=end_date,
                codes=codes,
                partition="wave3-kline-codes",
            ),
            100_000,
        )

    def price_adjustment_factors(
        self,
        stock_codes: Sequence[str],
        context: RunContext,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        if not stock_codes:
            return []
        if start_date is None or end_date is None:
            raise ValueError("price adjustment factor reads require a bounded date range")
        return self.kernel.read._repository_query(
            context,
            "feature.price_adjustment_factor_asof",
            """SELECT ts_code AS stock_code, trade_date,
                      adj_adjustment_factor AS adjustment_factor
                 FROM feature.price_adjustment_factor_asof
                WHERE ts_code=ANY(%s)
                  AND trade_date >= %s AND trade_date <= %s
                  AND adj_adjustment_factor IS NOT NULL
                ORDER BY ts_code,trade_date""",
            (list(stock_codes), start_date, end_date),
            self._scope("wave3-price-adjustment", date_from=start_date, date_to=end_date),
            # The technical job bounds the query by stock batch and warm-up
            # window.  Keep the repository read ceiling explicit.
            100_000,
        )

    def stock_summary(self, context: RunContext) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(context, "股票全部信息汇总", """SELECT "ts代码" AS stock_code,"上市日期" AS list_date,"总股本" AS total_capital_wan,"股票名称" AS stock_name,"所属行业" AS industry FROM "股票全部信息汇总""", (), WorkScope(partition="wave3-summary"), 100_000)

    def historical_capital(self, context: RunContext) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(context, "fin_capital", "SELECT stock_code,report_date,total_capital FROM fin_capital WHERE total_capital IS NOT NULL ORDER BY stock_code,report_date", (), WorkScope(partition="wave3-capital"), 500_000)

    def technical_features(self, start_date: date, end_date: date, context: RunContext) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context,
            "feature.technical_daily_v1",
            """SELECT d.trade_date,d.stock_code,d.open,d.high,d.low,d.close,d.volume,d.amount,
                      f.wr10 AS wr1,f.wr20 AS wr2,f.rsi9,f.k,f.d,f.j,
                      f.quality_status,f.feature_contract_version
                 FROM std.daily_kline d
                 JOIN feature.technical_daily_v1 f
                   ON f.stock_code=d.stock_code AND f.trade_date=d.trade_date
                WHERE d.trade_date>=%s AND d.trade_date<=%s
                ORDER BY d.stock_code,d.trade_date""",
            (start_date, end_date),
            self._scope("wave3-technical", date_from=start_date, date_to=end_date),
            500_000,
        )

    def upsert_technical_features(
        self, rows: Sequence[dict[str, Any]], start_date: date, end_date: date, context: RunContext
    ) -> int:
        if not rows:
            return 0
        scope = self._scope("technical-daily", date_from=start_date, date_to=end_date)
        return self.kernel.write.upsert(
            context,
            scope,
            "feature.technical_daily_v1",
            list(rows),
            key=("stock_code", "trade_date"),
            update_columns=("wr10", "wr20", "rsi9", "k", "d", "j", "quality_status", "feature_contract_version"),
            skip_noop=True,
        )

    def insert_results(self, rows: Sequence[dict[str, Any]], context: RunContext) -> int:
        if not rows:
            return 0
        scope = self._scope("wave3-results", date_from=min(row["trade_date"] for row in rows), date_to=max(row["trade_date"] for row in rows))
        return self.kernel.write.upsert(context, scope, "wave3_stocks", list(rows), key=("trade_date", "stock_code"), update_columns=tuple(key for key in rows[0] if key not in {"trade_date", "stock_code"}), skip_noop=True)
