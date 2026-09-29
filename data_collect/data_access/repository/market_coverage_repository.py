"""Repository for market coverage gates and their historical evidence."""
from __future__ import annotations

from datetime import timedelta

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class MarketCoverageRepository(BaseRepository):
    def latest_daily_gate(
        self, end_date, lookback_days: int, context: RunContext,
    ) -> dict | None:
        """Find the newest daily-bar gate in a small, explicit date window."""
        days = int(lookback_days)
        if not 1 <= days <= 31:
            raise ValueError("market gate lookback_days must be in 1..31")
        start_date = end_date - timedelta(days=days - 1)
        rows = self.kernel.read._repository_query(
            context=context,
            source="ops.market_data_quality_gate",
            sql="""
                SELECT trade_date, status, actual_rows, expected_rows,
                       batch_id, reason, created_at
                  FROM ops.market_data_quality_gate
                 WHERE dataset='daily' AND trade_date BETWEEN %s AND %s
                 ORDER BY trade_date DESC, created_at DESC, gate_id DESC
                 LIMIT 1
            """,
            params=(start_date, end_date),
            scope=WorkScope(
                date_from=start_date,
                date_to=end_date,
                partition="coverage:latest-daily-gate",
            ),
            max_rows=1,
        )
        return rows[0] if rows else None

    def previous_counts(self, dataset: str, trade_date, baseline_days: int, context: RunContext) -> list[int]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="tushare_market_raw_version",
            sql="""
                SELECT trade_date, COUNT(DISTINCT ts_code) AS stock_count
                  FROM tushare_market_raw_version
                 WHERE dataset=%s AND trade_date < %s
                 GROUP BY trade_date
                 ORDER BY trade_date DESC
                 LIMIT %s
            """,
            params=(dataset, trade_date, int(baseline_days)),
            scope=WorkScope(date_to=trade_date, partition=f"coverage:{dataset}"),
            max_rows=baseline_days,
        )
        return [int(row["stock_count"]) for row in rows]

    def save_gate(self, result, batch_id: str, context: RunContext) -> int:
        row = {
            "dataset": result.dataset,
            "trade_date": result.trade_date,
            "actual_rows": result.actual_rows,
            "expected_rows": result.expected_rows,
            "baseline_days": result.baseline_days,
            "threshold_pct": result.threshold_pct,
            "drop_pct": result.drop_pct,
            "status": result.status,
            "sample_mode": result.sample_mode,
            "batch_id": batch_id,
            "reason": result.reason,
        }
        return self.kernel.write.upsert(
            context=context,
            scope=WorkScope(date_from=result.trade_date, date_to=result.trade_date),
            target="ops.market_data_quality_gate",
            rows=[row],
            key=("dataset", "trade_date", "batch_id"),
            update_columns=tuple(column for column in row if column not in {"dataset", "trade_date", "batch_id"}),
        )

    def latest_failures(self, trade_date, context: RunContext) -> list[tuple]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="ops.market_data_quality_gate",
            sql="""
                SELECT DISTINCT ON (dataset) dataset, status, reason, batch_id, created_at
                  FROM ops.market_data_quality_gate
                 WHERE trade_date=%s
                 ORDER BY dataset, created_at DESC, gate_id DESC
            """,
            params=(trade_date,),
            scope=WorkScope(date_from=trade_date, date_to=trade_date),
        )
        return [
            (row["dataset"], row["status"], row["reason"], row["batch_id"], row["created_at"])
            for row in rows if row["status"] != "PASS"
        ]

    def latest_failures_in_range(self, start, end, context: RunContext) -> list[tuple]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="ops.market_data_quality_gate",
            sql="""
                SELECT dataset, trade_date, status, reason, batch_id, created_at
                  FROM (
                    SELECT DISTINCT ON (dataset, trade_date)
                           dataset, trade_date, status, reason, batch_id, created_at
                      FROM ops.market_data_quality_gate
                     WHERE trade_date BETWEEN %s AND %s
                     ORDER BY dataset, trade_date, created_at DESC, gate_id DESC
                  ) latest
                 WHERE status <> 'PASS'
                 ORDER BY trade_date, dataset
            """,
            params=(start, end),
            scope=WorkScope(date_from=start, date_to=end),
        )
        return [
            (row["dataset"], row["trade_date"], row["status"], row["reason"], row["batch_id"], row["created_at"])
            for row in rows
        ]
