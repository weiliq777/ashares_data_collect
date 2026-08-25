"""Small, read-only source-audit queries for external-provider reconciliation."""
from __future__ import annotations

from datetime import date
from typing import Iterable

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class SourceAuditRepository(BaseRepository):
    """Read a bounded sample without exposing a database handle to the audit job."""

    _RAW_TARGETS = {
        "daily": "tushare_daily_raw",
        "daily_basic": "tushare_daily_basic_raw",
        "adj_factor": "tushare_adj_factor_raw",
    }

    def sample_codes(
        self,
        trade_date: date,
        sample_size: int,
        seed: str,
        context: RunContext,
        requested_codes: Iterable[str] | None = None,
    ) -> list[str]:
        if sample_size <= 0 or sample_size > 100:
            raise ValueError("sample_size must be between 1 and 100")
        requested = tuple(str(code) for code in (requested_codes or ()) if str(code))
        clauses = ["trade_date=%s"]
        params: list[object] = [trade_date]
        if requested:
            clauses.append("stock_code=ANY(%s)")
            params.append(list(requested))
        # The date predicate is index-friendly; the bounded hash ordering only
        # randomizes the few thousand codes on one day, never the full history.
        params.extend([str(seed), sample_size])
        rows = self.kernel.read._repository_query(
            context=context,
            source="std.daily_kline",
            sql=(
                "SELECT stock_code AS ts_code "
                "FROM std.daily_kline WHERE " + " AND ".join(clauses) + " "
                "ORDER BY md5(stock_code || %s) LIMIT %s"
            ),
            params=tuple(params),
            scope=WorkScope(
                date_from=trade_date, date_to=trade_date,
                codes=requested,
                partition="source-audit-sample",
            ),
            max_rows=sample_size,
        )
        return [str(row[0] if not isinstance(row, dict) else row["ts_code"]) for row in rows]

    def latest_raw_payloads(
        self,
        dataset: str,
        trade_date: date,
        codes: Iterable[str],
        context: RunContext,
    ) -> dict[str, dict]:
        target = self._RAW_TARGETS.get(dataset)
        if target is None:
            raise ValueError(f"unsupported audit dataset: {dataset}")
        codes = tuple(str(code) for code in codes)
        if not codes:
            return {}
        rows = self.kernel.read._repository_query(
            context=context,
            source=target,
            sql=(
                "SELECT DISTINCT ON (ts_code) ts_code, trade_date, payload, fetched_at "
                f"FROM {target} WHERE trade_date=%s AND ts_code=ANY(%s) "
                "ORDER BY ts_code, fetched_at DESC"
            ),
            params=(trade_date, list(codes)),
            scope=WorkScope(
                date_from=trade_date, date_to=trade_date, codes=codes,
                partition=f"source-audit:{dataset}",
            ),
            max_rows=len(codes),
        )
        result: dict[str, dict] = {}
        for row in rows:
            code = str(row["ts_code"])
            payload = row["payload"]
            if isinstance(payload, str):
                import json

                payload = json.loads(payload)
            result[code] = {
                "payload": dict(payload or {}),
                "fetched_at": row.get("fetched_at"),
            }
        return result
