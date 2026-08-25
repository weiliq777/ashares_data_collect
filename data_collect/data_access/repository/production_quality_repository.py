"""Bounded quality checks for one production increment window."""
from __future__ import annotations

from datetime import date
from typing import Any

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class ProductionQualityRepository(BaseRepository):
    _WINDOWS = {
        "pit.universe_daily": "trade_date",
        "pit.security_tradeability_daily": "trade_date",
        "feature.price_daily_v2": "trade_date",
        "feature.valuation_daily_v1": "trade_date",
        "feature.technical_daily_v1": "trade_date",
    }

    def inspect_window(self, start: date, end: date, context: RunContext) -> dict[str, Any]:
        result: dict[str, Any] = {}
        scope = WorkScope(date_from=start, date_to=end, partition="production-quality")
        for table, date_column in self._WINDOWS.items():
            row = self.kernel.read._repository_query(
                context, table,
                f"""SELECT COUNT(*) AS rows,
                    COUNT(*) FILTER (WHERE quality_status='MISSING_SOURCE') AS missing_source,
                    COUNT(*) FILTER (WHERE quality_status NOT IN ('VALID','WARNING','INSUFFICIENT_HISTORY')) AS unexpected_status
                    FROM {table} WHERE {date_column} BETWEEN %s AND %s""",
                params=(start, end), scope=scope, max_rows=2,
            )[0]
            result[table] = {key: int(value or 0) for key, value in row.items()}

        gate_rows = self.kernel.read._repository_query(
            context, "ops.market_data_quality_gate",
            """WITH latest AS (
                    SELECT DISTINCT ON (dataset, trade_date)
                           dataset, trade_date, status
                      FROM ops.market_data_quality_gate
                     WHERE trade_date BETWEEN %s AND %s
                     ORDER BY dataset, trade_date, created_at DESC, gate_id DESC
                )
                SELECT COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status='PASS') AS passed,
                    COUNT(*) FILTER (WHERE status='BLOCKED') AS blocked
                  FROM latest""",
            params=(start, end), scope=scope, max_rows=2,
        )[0]
        result["ops.market_data_quality_gate"] = {
            key: int(value or 0) for key, value in gate_rows.items()
        }
        failures = []
        for table in ("feature.price_daily_v2", "feature.valuation_daily_v1"):
            if result[table]["unexpected_status"]:
                failures.append(f"{table}: unexpected quality status")
        if result["ops.market_data_quality_gate"]["blocked"]:
            failures.append("market_data_quality_gate contains BLOCKED rows")
        result["failures"] = failures
        return result
