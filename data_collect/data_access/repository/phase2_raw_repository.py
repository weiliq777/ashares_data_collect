"""Typed repository for phase-2 event/index Raw ingestion."""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from collections.abc import Mapping
from numbers import Real
from typing import Iterable

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class Phase2RawRepository(BaseRepository):
    TARGETS = frozenset({
        "tushare_stock_st_raw", "tushare_namechange_raw", "tushare_suspend_d_raw",
        "tushare_stk_limit_raw", "tushare_dividend_raw", "tushare_index_daily_raw",
    })
    KEYS = {
        "tushare_stock_st_raw": ("ts_code", "trade_date", "type", "payload_hash"),
        "tushare_namechange_raw": ("ts_code", "payload_hash"),
        "tushare_suspend_d_raw": ("ts_code", "trade_date", "suspend_type", "payload_hash"),
        "tushare_stk_limit_raw": ("ts_code", "trade_date", "payload_hash"),
        "tushare_dividend_raw": ("ts_code", "payload_hash"),
        "tushare_index_daily_raw": ("ts_code", "trade_date", "payload_hash"),
    }

    @staticmethod
    def _date(value):
        if value in (None, ""):
            return None
        text = str(value).replace("-", "")
        if len(text) == 8 and text.isdigit():
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        return value

    @classmethod
    def _json_safe(cls, value):
        """Normalize API numeric sentinels before strict JSON encoding.

        Tushare responses can contain NaN/Inf for nullable numeric fields.  The
        database payload contract uses JSON null for those values; emitting the
        non-standard ``NaN`` token would either fail strict encoding or create
        a payload that PostgreSQL/other consumers interpret inconsistently.
        Keep this recursive and dependency-free so repository serialization is
        deterministic even when the source frame uses numpy scalar values.
        """
        if isinstance(value, Mapping):
            return {key: cls._json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [cls._json_safe(item) for item in value]
        if isinstance(value, Real) and not math.isfinite(value):
            return None
        return value

    def save_frame(self, target: str, records: Iterable[dict], batch_id: str, context: RunContext) -> int:
        if target not in self.TARGETS:
            raise ValueError(f"unsupported phase2 Raw target: {target}")
        materialized = list(records)
        if not materialized:
            return 0
        rows = []
        for record in materialized:
            clean = {
                key: self._json_safe(value)
                for key, value in record.items()
                if not key.startswith("_")
            }
            encoded = json.dumps(clean, ensure_ascii=False, default=str, allow_nan=False)
            common = {"payload": encoded, "source": "tushare", "batch_id": batch_id,
                      "payload_hash": hashlib.sha256(encoded.encode("utf-8")).hexdigest()}
            if target == "tushare_stock_st_raw":
                row = {"ts_code": clean.get("ts_code"), "trade_date": self._date(clean.get("trade_date")), "type": clean.get("type"), **common}
            elif target == "tushare_namechange_raw":
                row = {"ts_code": clean.get("ts_code"), "start_date": self._date(clean.get("start_date")), "end_date": self._date(clean.get("end_date")), "ann_date": self._date(clean.get("ann_date")), **common}
            elif target == "tushare_suspend_d_raw":
                row = {"ts_code": clean.get("ts_code"), "trade_date": self._date(clean.get("trade_date")), "suspend_type": clean.get("suspend_type"), **common}
            elif target == "tushare_stk_limit_raw":
                row = {"ts_code": clean.get("ts_code"), "trade_date": self._date(clean.get("trade_date")), **common}
            elif target == "tushare_dividend_raw":
                row = {"ts_code": clean.get("ts_code"), "end_date": self._date(clean.get("end_date")), "ann_date": self._date(clean.get("ann_date")), "div_proc": clean.get("div_proc"), "record_date": self._date(clean.get("record_date")), "ex_date": self._date(clean.get("ex_date")), **common}
            else:
                row = {"ts_code": clean.get("ts_code"), "trade_date": self._date(clean.get("trade_date")), **common}
            rows.append(row)
        return self.kernel.raw.append_payload_rows(
            context=context, target=target, rows=rows,
            business_key=self.KEYS[target],
            scope=WorkScope(partition=f"phase2-raw:{target}"),
        )

    def trade_dates(self, start, end, context: RunContext, lookback_days: int | None = None) -> list[str]:
        rows = self.kernel.read._repository_query(
            context, "tushare_trade_cal_raw",
            """SELECT cal_date FROM tushare_trade_cal_raw
                WHERE exchange='SSE' AND cal_date BETWEEN %s AND %s
                  AND (payload->>'is_open')::int=1 ORDER BY cal_date""",
            params=(self._date(start), self._date(end)),
            scope=WorkScope(date_from=self._date(start), date_to=self._date(end), partition="phase2:trade-calendar"),
            max_rows=100_000,
        )
        values = [row["cal_date"].strftime("%Y%m%d") for row in rows]
        return values[-int(lookback_days):] if lookback_days else values

    def stock_codes(self, context: RunContext, limit: int | None = None) -> list[str]:
        rows = self.kernel.read.query(
            context, "std.stock_basic_info", ["stock_code"],
            where={"list_status__in": ["L", "D", "P", "G"]},
            scope=WorkScope(partition="phase2:stock-codes"), limit=100_000,
        )
        codes = [str(row[0]) for row in rows]
        return codes[:int(limit)] if limit else codes

    def baseline_cutoff(self, context: RunContext):
        rows = self.kernel.read._repository_query(
            context, "ops.tushare_data_release",
            """SELECT baseline_cutoff_date FROM ops.tushare_data_release
                WHERE status IN ('validated', 'superseded')
                  AND baseline_domain='RESEARCH'
                ORDER BY CASE WHEN status='validated' THEN 0 ELSE 1 END,
                         baseline_cutoff_date DESC,
                         validated_at DESC NULLS LAST, release_id DESC LIMIT 1""",
            scope=WorkScope(partition="phase2:baseline"), max_rows=2,
        )
        if not rows:
            raise RuntimeError("no non-revoked RESEARCH data release watermark")
        return rows[0]["baseline_cutoff_date"]
