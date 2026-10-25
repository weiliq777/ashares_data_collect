"""SnapshotRepository: typed Research Snapshot access."""
from __future__ import annotations

from typing import Any, Iterable

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class SnapshotRepository(BaseRepository):
    def load_watchlist_members(self, watchlist_id: str, context: RunContext) -> list[dict[str, Any]]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="research.watchlist",
            sql="""
                SELECT w.snapshot_id, w.status, m.research_date, m.ts_code,
                       COALESCE(s.name, m.ts_code) AS display_name
                  FROM research.watchlist w
                  JOIN research.watchlist_member m ON m.watchlist_id=w.watchlist_id
                  JOIN research.stock_snapshot_daily s
                    ON s.snapshot_id=m.snapshot_id
                   AND s.research_date=m.research_date
                   AND s.ts_code=m.ts_code
                 WHERE w.watchlist_id=%s
                 ORDER BY m.rank_no, m.ts_code
            """,
            params=(watchlist_id,),
            scope=WorkScope(partition=f"watchlist:{watchlist_id}"),
            max_rows=100_000,
        )
        if not rows:
            raise ValueError(f"unknown or empty watchlist: {watchlist_id}")
        if rows[0]["status"] != "ACTIVE":
            raise RuntimeError(f"watchlist is not ACTIVE: {watchlist_id}")
        return [
            {
                "snapshot_id": str(row["snapshot_id"]),
                "research_date": row["research_date"].isoformat(),
                "ts_code": row["ts_code"],
                "display_name": row["display_name"],
            }
            for row in rows
        ]

    def load_manifest(self, snapshot_id: str, context: RunContext) -> dict[str, Any]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="research.snapshot_manifest",
            sql="SELECT * FROM research.snapshot_manifest WHERE snapshot_id=%s",
            params=(snapshot_id,), scope=WorkScope(partition=str(snapshot_id)), max_rows=1,
        )
        if not rows:
            raise ValueError(f"unknown Research Snapshot: {snapshot_id}")
        return rows[0]

    def read_frozen_rows(
        self,
        snapshot_id: str,
        context: RunContext,
        limit: int = 100_000,
        *,
        codes: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        selected_codes = tuple(sorted({str(code) for code in (codes or ()) if str(code)}))
        code_filter = " AND ts_code=ANY(%s)" if selected_codes else ""
        params: tuple[Any, ...] = (snapshot_id,)
        if selected_codes:
            params += (list(selected_codes),)
        return self.kernel.read._repository_query(
            context=context,
            source="research.stock_snapshot_daily",
            sql="""
                SELECT * FROM research.stock_snapshot_daily
                 WHERE snapshot_id=%s
            """ + code_filter + """
                 ORDER BY research_date, ts_code
            """,
            params=params,
            scope=WorkScope(partition=str(snapshot_id), codes=selected_codes or None),
            max_rows=limit,
        )

    def publish_rows(self, target: str, rows: Iterable[dict[str, Any]], snapshot_id: str, context: RunContext) -> int:
        return self.kernel.publish.snapshot(
            context=context,
            scope=WorkScope(partition=str(snapshot_id), dimensions={"snapshot_id": str(snapshot_id)}),
            target=target, rows=list(rows),
        )
