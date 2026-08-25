"""MarketRepository：行情领域仓储（指数日线）。

迁移自 tushare_phase2_standard.rebuild_index_daily（去掉 TRUNCATE，纯 upsert）。
"""
from __future__ import annotations

import hashlib
import json
from datetime import date

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.repository.base import BaseRepository

_INDEX_DAILY_REBUILD_SQL = """
INSERT INTO std.index_daily_tushare
    (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg,
     volume, amount, source)
SELECT ts_code, trade_date,
       NULLIF(payload->>'open', '')::double precision,
       NULLIF(payload->>'high', '')::double precision,
       NULLIF(payload->>'low', '')::double precision,
       NULLIF(payload->>'close', '')::double precision,
       NULLIF(payload->>'pre_close', '')::double precision,
       NULLIF(payload->>'change', '')::double precision,
       NULLIF(payload->>'pct_chg', '')::double precision,
       NULLIF(payload->>'vol', '')::double precision * 100,
       NULLIF(payload->>'amount', '')::double precision * 1000,
       source
FROM tushare_index_daily_raw
ON CONFLICT (ts_code, trade_date, source) DO UPDATE SET
  open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
  pre_close=EXCLUDED.pre_close, change=EXCLUDED.change, pct_chg=EXCLUDED.pct_chg,
  volume=EXCLUDED.volume, amount=EXCLUDED.amount, fetched_at=CURRENT_TIMESTAMP
"""


class MarketRepository(BaseRepository):
    @staticmethod
    def _date_value(value):
        if value in (None, ""):
            return None
        if isinstance(value, date):
            return value
        if hasattr(value, "date"):
            return value.date()
        value = str(value).replace("-", "")
        if len(value) == 8 and value.isdigit():
            return date(int(value[:4]), int(value[4:6]), int(value[6:8]))
        return value

    def append_market_versions(
        self, dataset: str, rows: list[dict], batch_id: str, context: RunContext,
    ) -> tuple[int, int]:
        """Append one market API response to immutable Raw and version layers."""
        if dataset not in {"daily", "daily_basic", "adj_factor", "trade_cal"}:
            raise ValueError(f"unsupported market dataset: {dataset}")
        if not rows:
            return 0, 0
        raw_target = {
            "daily": "tushare_daily_raw",
            "daily_basic": "tushare_daily_basic_raw",
            "adj_factor": "tushare_adj_factor_raw",
            "trade_cal": "tushare_trade_cal_raw",
        }[dataset]
        key_columns = ("exchange", "cal_date") if dataset == "trade_cal" else ("ts_code", "trade_date")
        raw_rows = []
        version_rows = []
        for source in rows:
            clean = {key: value for key, value in source.items() if not key.startswith("_")}
            payload = json.dumps(clean, ensure_ascii=False, default=str, allow_nan=False)
            digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
            values = {key: self._date_value(clean.get(key)) if key in {"trade_date", "cal_date"} else clean.get(key) for key in key_columns}
            raw_rows.append({**values, "payload": payload, "source": "tushare"})
            business_key = ":".join(str(clean.get(key) or "") for key in key_columns)
            version_rows.append({
                "dataset": dataset, "business_key": business_key,
                "ts_code": None if dataset == "trade_cal" else clean.get("ts_code"),
                "trade_date": None if dataset == "trade_cal" else self._date_value(clean.get("trade_date")),
                "exchange": clean.get("exchange") if dataset == "trade_cal" else None,
                "cal_date": self._date_value(clean.get("cal_date")) if dataset == "trade_cal" else None,
                "payload": payload, "source": "tushare", "batch_id": batch_id,
                "payload_hash": digest,
            })
        scope = WorkScope(
            date_from=min((row.get("trade_date") or row.get("cal_date") for row in raw_rows if row.get("trade_date") or row.get("cal_date")), default=None),
            date_to=max((row.get("trade_date") or row.get("cal_date") for row in raw_rows if row.get("trade_date") or row.get("cal_date")), default=None),
            partition=f"market-raw:{dataset}",
        )
        raw_inserted = self.kernel.raw.append_payload_rows(
            context=context, target=raw_target, rows=raw_rows,
            business_key=key_columns, scope=scope,
        )
        versions = self.kernel.raw.append(
            context=context, dataset="tushare_market_raw_version",
            rows=version_rows, business_key=("dataset", "business_key"),
        )
        return versions, raw_inserted

    def latest_market_versions(self, dataset: str, trade_date, context: RunContext) -> list[dict]:
        if dataset not in {"daily", "daily_basic", "adj_factor"}:
            raise ValueError(f"unsupported market version dataset: {dataset}")
        return self.kernel.read._repository_query(
            context=context, source="tushare_market_raw_version", sql="""
                SELECT DISTINCT ON (ts_code, trade_date) payload
                  FROM tushare_market_raw_version
                 WHERE dataset=%s AND trade_date=%s
                 ORDER BY ts_code, trade_date, fetched_at DESC, record_id DESC
            """, params=(dataset, trade_date),
            scope=WorkScope(date_from=trade_date, date_to=trade_date, partition=f"market-version:{dataset}"),
            max_rows=100_000,
        )

    def replace_daily_partition(self, target: str, trade_date, rows: list[dict], context: RunContext) -> int:
        if target not in {"daily_kline", "valuation_daily", "adj_factor_daily", "std.trading_calendar"}:
            raise ValueError(f"unsupported daily target: {target}")
        if not rows:
            return 0
        if target == "daily_kline":
            scope = WorkScope(
                date_from=trade_date,
                date_to=trade_date,
                partition="daily-kline",
            )
            return self.kernel.rebuild.replace_daily_partition_rows(
                context=context,
                scope=scope,
                rows=rows,
            )
        columns = list(rows[0])
        if any(list(row) != columns for row in rows):
            raise ValueError("daily rows have inconsistent columns")
        values_sql = ", ".join("(" + ", ".join(["%s"] * len(columns)) + ")" for _ in rows)
        canonical_target = {"adj_factor_daily": "std.adj_factor_daily", "valuation_daily": "std.valuation_daily"}.get(target, target)
        sql = f"DELETE FROM {canonical_target} WHERE trade_date=%s; INSERT INTO {canonical_target} ({', '.join(columns)}) VALUES {values_sql} ON CONFLICT DO NOTHING"
        params = [trade_date]
        params.extend(value for row in rows for value in (row[column] for column in columns))
        plan = SqlPlan(Operation.REBUILD, canonical_target, WorkScope(date_from=trade_date, date_to=trade_date), expected_mode="SET_BASED", allow_delete=True)
        return self.kernel.rebuild.run_scoped_merge(sql, plan, context, params=tuple(params))

    def standard_count(self, target: str, trade_date, context: RunContext) -> int:
        if target not in {"daily_kline", "valuation_daily", "adj_factor_daily"}:
            raise ValueError(f"unsupported daily target: {target}")
        canonical_target = {"adj_factor_daily": "std.adj_factor_daily", "valuation_daily": "std.valuation_daily"}.get(target, target)
        rows = self.kernel.read._repository_query(
            context=context, source=canonical_target,
            sql=f"SELECT COUNT(*) AS count FROM {canonical_target} WHERE trade_date=%s",
            params=(trade_date,), scope=WorkScope(date_from=trade_date, date_to=trade_date), max_rows=1,
        )
        return int(rows[0]["count"] or 0) if rows else 0

    def upsert_calendar(self, rows: list[dict], context: RunContext) -> int:
        if not rows:
            return 0
        scope = WorkScope(partition="trading-calendar")

        return self.kernel.write.upsert(
            context=context, scope=scope, target="std.trading_calendar",
            rows=rows, key=("exchange", "trade_date", "source"),
            update_columns=("is_open", "pretrade_date", "fetched_at"),
        )

    def existing_codes(self, source: str, code_column: str, trade_date, context: RunContext) -> set[str]:
        if not code_column.isidentifier():
            raise ValueError("code_column invalid")
        rows = self.kernel.read.query(
            context=context, source=source, columns=[code_column],
            where={"trade_date__eq": trade_date},
            scope=WorkScope(date_from=trade_date, date_to=trade_date),
        )
        return {str(row[0]) for row in rows}

    def existing_keys(
        self,
        source: str,
        date_column: str,
        code_column: str,
        date_from,
        date_to,
        context: RunContext,
    ) -> set[tuple[str, str]]:
        """Return the bounded date/code keys already present in a market table."""
        if not date_column.isidentifier() or not code_column.isidentifier():
            raise ValueError("market key columns invalid")
        rows = self.kernel.read.query(
            context=context,
            source=source,
            columns=[date_column, code_column],
            where={f"{date_column}__gte": date_from, f"{date_column}__lte": date_to},
            scope=WorkScope(date_from=date_from, date_to=date_to),
        )
        return {(str(row[0]), str(row[1])) for row in rows}

    def existing_dates(self, source: str, date_column: str, date_from, date_to, context: RunContext) -> set:
        if not date_column.isidentifier():
            raise ValueError("date_column invalid")
        rows = self.kernel.read.query(
            context=context,
            source=source,
            columns=[date_column],
            where={f"{date_column}__gte": date_from, f"{date_column}__lte": date_to},
            scope=WorkScope(date_from=date_from, date_to=date_to),
        )
        return {row[0] for row in rows}

    def missing_open_dates(self, raw_table: str, date_from, date_to, context: RunContext) -> list[str]:
        """Return open trading dates absent from one bounded raw market table."""
        if raw_table not in {"tushare_daily_raw", "tushare_daily_basic_raw"}:
            raise ValueError(f"unsupported market raw table: {raw_table}")
        rows = self.kernel.read._repository_query(
            context=context,
            source="tushare_trade_cal_raw",
            sql=(
                "SELECT c.cal_date FROM tushare_trade_cal_raw c "
                "WHERE c.exchange=%s AND (c.payload->>'is_open')::int=1 "
                "AND c.cal_date BETWEEN %s AND %s "
                f"AND NOT EXISTS (SELECT 1 FROM {raw_table} r WHERE r.trade_date=c.cal_date) "
                "ORDER BY c.cal_date"
            ),
            params=("SSE", date_from, date_to),
            scope=WorkScope(date_from=date_from, date_to=date_to),
            max_rows=10000,
        )
        return [row[0].strftime("%Y%m%d") if hasattr(row[0], "strftime") else str(row[0]).replace("-", "") for row in rows]

    def raw_count(self, raw_table: str, trade_date, context: RunContext) -> int:
        """Return a single-day row count for an allow-listed raw market table."""
        if raw_table not in {"tushare_daily_raw", "tushare_daily_basic_raw"}:
            raise ValueError(f"unsupported market raw table: {raw_table}")
        rows = self.kernel.read._repository_query(
            context=context,
            source=raw_table,
            sql=f"SELECT COUNT(*) AS count FROM {raw_table} WHERE trade_date=%s",
            params=(trade_date,),
            scope=WorkScope(date_from=trade_date, date_to=trade_date),
            max_rows=1,
        )
        return int(rows[0]["count"] or 0) if rows else 0

    def upsert_daily_bars(
        self,
        target: str,
        rows: list[dict],
        code_column: str,
        context: RunContext,
        date_from,
        date_to,
    ) -> int:
        """Persist one bounded daily-bar batch through the write gateway."""
        if target not in {"daily_kline", "etf_daily_kline", "index_daily"}:
            raise ValueError(f"unsupported market target: {target}")
        if not rows:
            return 0
        scope = WorkScope(date_from=date_from, date_to=date_to)
        columns = tuple(rows[0])
        key = (code_column, "trade_date")
        update_columns = tuple(column for column in columns if column not in key)
        if target == "daily_kline":
            return self.kernel.write.upsert(
                context=context, scope=scope, target="std.daily_kline", rows=rows,
                key=key, update_columns=update_columns,
            )
        return self.kernel.write.upsert(context=context, scope=scope, target=target, rows=rows, key=key, update_columns=update_columns)

    def upsert_etf_nav(
        self,
        rows: list[dict],
        context: RunContext,
        date_from,
        date_to,
        update_columns: tuple[str, ...],
    ) -> int:
        if not rows:
            return 0
        return self.kernel.write.upsert(
            context=context,
            scope=WorkScope(date_from=date_from, date_to=date_to),
            target="etf_nav",
            rows=rows,
            key=("code", "trade_date"),
            update_columns=update_columns,
        )

    def replace_limit_day(self, target: str, trade_date, rows: list[dict], context: RunContext) -> int:
        """Replace one limit-pool day atomically, including stale-row removal."""
        if target not in {"limit_pool", "limit_up_reason"}:
            raise ValueError(f"unsupported limit target: {target}")
        if not rows:
            return 0
        columns = list(rows[0])
        if any(not column.isidentifier() for column in columns):
            raise ValueError("limit-pool row contains an invalid column")
        if any(list(row) != columns for row in rows):
            raise ValueError("limit-pool rows have inconsistent columns")
        target_sql = target
        column_sql = ", ".join(columns)
        value_sql = ", ".join("(" + ", ".join(["%s"] * len(columns)) + ")" for _ in rows)
        sql = (
            f"DELETE FROM {target_sql} WHERE trade_date=%s; "
            f"INSERT INTO {target_sql} ({column_sql}) VALUES {value_sql} ON CONFLICT DO NOTHING"
        )
        params = [trade_date]
        for row in rows:
            params.extend(row[column] for column in columns)
        scope = WorkScope(date_from=trade_date, date_to=trade_date)
        plan = SqlPlan(
            operation=Operation.REBUILD,
            target=target,
            scope=scope,
            expected_mode="SET_BASED",
            estimated_rows=len(rows),
            allow_delete=True,
        )
        return self.kernel.rebuild.run_scoped_merge(
            sql=sql, plan=plan, context=context, params=tuple(params)
        )

    def rebuild_index_daily(self, context: RunContext) -> int:
        """重建指数日线（纯 upsert，无 TRUNCATE）。"""
        plan = SqlPlan(
            operation=Operation.REBUILD,
            target="std.index_daily_tushare",
            scope=WorkScope(),
            expected_mode="SET_BASED",
            estimated_rows=100_000,
            allow_insert_select=True,
        )
        return self.kernel.rebuild.run_insert_select(
            sql=_INDEX_DAILY_REBUILD_SQL, plan=plan, context=context
        )

    def missing_adj_factor_codes(self, start, end, context: RunContext) -> list[tuple[str, int]]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="std.daily_kline",
            sql="""
                WITH expected AS (
                    SELECT stock_code, COUNT(*) AS expected
                      FROM std.daily_kline
                     WHERE trade_date BETWEEN %s AND %s
                     GROUP BY stock_code
                ), actual AS (
                    SELECT ts_code AS stock_code, COUNT(*) AS actual
                      FROM tushare_adj_factor_raw
                     WHERE trade_date BETWEEN %s AND %s
                     GROUP BY ts_code
                )
                SELECT e.stock_code, e.expected - COALESCE(a.actual, 0) AS missing
                  FROM expected e
                  LEFT JOIN actual a USING (stock_code)
                 WHERE COALESCE(a.actual, 0) < e.expected
                 ORDER BY missing DESC, e.stock_code
            """,
            params=(start, end, start, end),
            scope=WorkScope(date_from=start, date_to=end),
            max_rows=100_000,
        )
        return [(str(row["stock_code"]), int(row["missing"])) for row in rows]

    def validated_baseline_cutoff(self, context: RunContext):
        rows = self.kernel.read._repository_query(
            context=context,
            source="ops.tushare_data_release",
            sql="""
                SELECT baseline_cutoff_date
                  FROM ops.tushare_data_release
                 WHERE status='validated'
                 ORDER BY validated_at DESC NULLS LAST, release_id DESC
                 LIMIT 1
            """,
            scope=WorkScope(partition="latest-validated-release"),
            max_rows=1,
        )
        if not rows:
            raise RuntimeError("no validated historical baseline")
        return rows[0]["baseline_cutoff_date"]
