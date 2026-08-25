"""Typed Repository for Tushare stock-basic raw/version/standard writes."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class InstrumentRepository(BaseRepository):
    def name_dict(self, context: RunContext) -> list[dict[str, Any]]:
        """Load the bounded A-share name snapshot for news tagging."""
        return self.kernel.read._repository_query(
            context=context,
            source="instrument_info",
            sql=(
                'SELECT "stock_code", "InstrumentName" FROM instrument_info '
                "WHERE \"stock_code\" ~ %s"
            ),
            params=(r"^\d{6}\.(SH|SZ|BJ)$",),
            scope=WorkScope(partition="instrument:name-dict"),
            max_rows=100_000,
        )

    def a_share_codes(self, context: RunContext) -> list[dict[str, Any]]:
        """Load the bounded A-share code snapshot for news collection."""
        return self.kernel.read._repository_query(
            context=context,
            source="instrument_info",
            sql=(
                'SELECT "stock_code" FROM instrument_info '
                "WHERE \"stock_code\" ~ %s"
            ),
            params=(r"^\d{6}\.(SH|SZ|BJ)$",),
            scope=WorkScope(partition="instrument:a-share-codes"),
            max_rows=100_000,
        )

    def persist_stock_basic(self, rows: Sequence[dict[str, Any]], batch_id: str, context: RunContext) -> int:
        if not rows:
            return 0
        raw_versions = []
        raw_current = []
        standard = []
        for row in rows:
            encoded = json.dumps(row, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))
            digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
            code = row["ts_code"]
            raw_versions.append({"ts_code": code, "business_key": str(code), "payload": encoded, "payload_hash": digest, "batch_id": batch_id})
            raw_current.append({"ts_code": code, "payload": encoded})
            standard.append({
                "stock_code": code, "symbol": row.get("symbol"), "name": row.get("name"),
                "area": row.get("area"), "industry": row.get("industry"), "market": row.get("market"),
                "list_date": row.get("list_date"), "delist_date": row.get("delist_date"),
                "list_status": row.get("list_status"), "source": "tushare", "fetched_at": datetime.now(),
            })
        scope = WorkScope(partition="stock-basic")
        def persist():
            self.kernel.write.upsert(context, scope, "tushare_stock_basic_raw_version", raw_versions, key=("business_key", "payload_hash"), update_columns=(), skip_noop=True)
            # The legacy Raw table is first-seen only. Keep every correction
            # in ``tushare_stock_basic_raw_version`` above and never require
            # UPDATE privilege on the append-only compatibility table.
            self.kernel.write.bulk_insert_ignore(
                context, scope, "tushare_stock_basic_raw", raw_current
            )
            self.kernel.write.upsert(context, scope, "std.stock_basic_info", standard, key=("stock_code",), update_columns=tuple(col for col in standard[0] if col != "stock_code"), skip_noop=True)
        self.atomic(context, scope, persist, estimated_rows=len(rows) * 3)
        return len(rows)
