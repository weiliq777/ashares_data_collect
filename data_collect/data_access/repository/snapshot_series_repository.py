"""Repository operations used by the multi-day snapshot orchestrator."""
from __future__ import annotations

from datetime import date
from typing import Iterable

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository
from data_collect.data_access.research_contract import is_foundation_schema


class SnapshotSeriesRepository(BaseRepository):
    def select_snapshots(
        self, baseline_tag: str, schema_version: str, start, end, context: RunContext,
    ) -> list[dict]:
        rows = self.kernel.read._repository_query(
            context, "research.snapshot_manifest",
            """SELECT snapshot_id, research_cutoff_date, status
                 FROM research.snapshot_manifest
                WHERE data_baseline_tag=%s AND schema_version=%s
                  AND (%s::date IS NULL OR research_cutoff_date >= %s::date)
                  AND (%s::date IS NULL OR research_cutoff_date <= %s::date)
                ORDER BY research_cutoff_date""",
            params=(baseline_tag, schema_version, start, start, end, end),
            scope=WorkScope(date_from=start, date_to=end, partition="snapshot:freeze-selection"),
            max_rows=100_000,
        )
        return rows

    def open_trade_dates(self, start: date, end: date, exchange: str, context: RunContext) -> tuple[date, ...]:
        rows = self.kernel.read._repository_query(
            context, "std.trading_calendar",
            """SELECT trade_date FROM std.trading_calendar
                WHERE exchange=%s AND is_open IS TRUE AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date""",
            params=(exchange, start, end),
            scope=WorkScope(date_from=start, date_to=end, partition=f"calendar:{exchange}"),
            max_rows=100_000,
        )
        return tuple(row["trade_date"] for row in rows)

    def financial_warmup_count(self, first_date: date, schema_version: str, context: RunContext) -> int:
        if is_foundation_schema(schema_version):
            source = "feature.company_financial_v2"
            sql = """SELECT COUNT(*) AS value FROM feature.company_financial_v2
                      WHERE data_available_timestamp < (%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'
                        AND report_date <= %s"""
            params = (first_date, first_date)
        else:
            source = "pit.financial_asof"
            sql = """SELECT COUNT(*) AS value FROM pit.financial_asof
                      WHERE decision_date=%s AND effective_announce_date IS NOT NULL
                        AND effective_announce_date <= decision_date"""
            params = (first_date,)
        return int(self.kernel.read._repository_query(
            context, source, sql, params=params,
            scope=WorkScope(date_to=first_date, partition="snapshot:financial-warmup"), max_rows=2,
        )[0]["value"] or 0)

    def existing_snapshots(self, dates: Iterable[date], baseline_tag: str, schema_version: str, context: RunContext) -> dict[date, tuple[str, str]]:
        dates = tuple(dates)
        if not dates:
            return {}
        rows = self.kernel.read._repository_query(
            context, "research.snapshot_manifest",
            """SELECT research_cutoff_date, snapshot_id, status
                 FROM research.snapshot_manifest
                WHERE data_baseline_tag=%s AND schema_version=%s
                  AND research_cutoff_date=ANY(%s::date[])""",
            params=(baseline_tag, schema_version, list(dates)),
            scope=WorkScope(date_from=min(dates), date_to=max(dates), partition="snapshot:series"),
            max_rows=len(dates),
        )
        return {row["research_cutoff_date"]: (str(row["snapshot_id"]), row["status"]) for row in rows}

    def reset_building(self, snapshot_ids: Iterable[str], reason: str, context: RunContext) -> int:
        ids = tuple(str(value) for value in snapshot_ids)
        if not ids:
            return 0
        scope = WorkScope(partition="snapshot:series-reset", dimensions={"snapshot_ids": ids})
        return self.kernel.write.fail_building_snapshots(
            context=context, scope=scope, snapshot_ids=ids, reason=reason,
        )
