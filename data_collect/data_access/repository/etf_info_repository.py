"""Typed Repository for ETF instrument snapshots and change log."""
from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class EtfInfoRepository(BaseRepository):
    FIELDS = ("name", "ext_name", "exchange", "list_date", "total_volume", "price_tick", "fund_type", "short_name", "total_mv", "latest_share")

    def existing(self, codes: Sequence[str], context: RunContext) -> dict[str, dict[str, Any]]:
        if not codes:
            return {}
        rows = self.kernel.read._repository_query(context, "etf_info", "SELECT code,name,ext_name,exchange,list_date,total_volume,price_tick,fund_type,short_name,total_mv,latest_share,updated_at FROM etf_info WHERE code=ANY(%s)", (list(codes),), WorkScope(partition="etf-info"), 100_000)
        result = {}
        for row in rows:
            item = dict(row); code = item.pop("code"); item.pop("updated_at", None); result[code] = item
        return result

    def persist(self, upserts: Sequence[Mapping[str, Any]], changelogs: Sequence[Mapping[str, Any]], context: RunContext) -> tuple[int, int]:
        if not upserts and not changelogs:
            return 0, 0
        scope = WorkScope(partition="etf-info")
        def write():
            if upserts:
                self.kernel.write.upsert(context, scope, "etf_info", list(upserts), key=("code",), update_columns=self.FIELDS, skip_noop=True)
            if changelogs:
                self.kernel.write.bulk_insert_ignore(context, scope, "etf_info_changelog", list(changelogs))
        self.atomic(context, scope, write, estimated_rows=len(upserts) + len(changelogs))
        return len(upserts), len(changelogs)
