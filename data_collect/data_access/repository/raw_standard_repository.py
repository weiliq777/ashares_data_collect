"""Typed Raw-to-Standard projection repository.

All SQL in this module is projection-specific. Jobs only normalize payloads and
submit bounded rows; they never receive a database handle.
"""
from __future__ import annotations

import json
import time
from datetime import date
from typing import Iterator

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class RawStandardRepository(BaseRepository):
    _RAW_TABLES = frozenset({
        "tushare_daily_raw", "tushare_daily_basic_raw", "tushare_adj_factor_raw",
        "tushare_trade_cal_raw", "tushare_fina_indicator_raw",
    })
    _STANDARD_TARGETS = frozenset({
        "std.daily_kline", "std.valuation_daily", "std.adj_factor_daily",
        "std.trading_calendar", "std.stock_financial_indicator",
    })

    def iter_payload_rows(
        self,
        table: str,
        context: RunContext,
        chunk_size: int = 5000,
        *,
        version_dataset: str | None = None,
        key_kind: str = "security_date",
        stock_codes: list[str] | None = None,
        start_date=None,
        end_date=None,
    ) -> Iterator[list[dict]]:
        if table not in self._RAW_TABLES:
            raise ValueError(f"unsupported Raw table: {table}")
        if chunk_size <= 0 or chunk_size > 10_000:
            raise ValueError("chunk_size must be in 1..10000")
        if version_dataset is None:
            if table != "tushare_fina_indicator_raw":
                raise ValueError(f"{table} requires a version dataset")
            last_key = None
            while True:
                filters, params = [], []
                if stock_codes:
                    filters.append("ts_code=ANY(%s)")
                    params.append(stock_codes)
                if start_date is not None:
                    filters.append("end_date >= %s")
                    params.append(start_date)
                if end_date is not None:
                    filters.append("end_date <= %s")
                    params.append(end_date)
                if last_key is not None:
                    filters.append("(ts_code, ann_date, end_date) > (%s, %s, %s)")
                    params.extend(last_key)
                where = " AND ".join(filters) if filters else "TRUE"
                rows = self.kernel.read._repository_query(
                    context=context,
                    source=table,
                    sql=f"""
                        SELECT ts_code, ann_date, end_date, payload, fetched_at
                          FROM {table}
                         WHERE {where}
                         ORDER BY ts_code, ann_date, end_date
                         LIMIT %s
                    """,
                    params=(*params, chunk_size),
                    scope=WorkScope(partition=f"raw-standard:{table}"),
                    max_rows=chunk_size,
                )
                if not rows:
                    return
                yield [self._payload_row(row["payload"], row.get("fetched_at")) for row in rows]
                last_key = (rows[-1]["ts_code"], rows[-1]["ann_date"], rows[-1]["end_date"])
            return

        if version_dataset not in {"daily", "daily_basic", "adj_factor", "trade_cal"}:
            raise ValueError(f"unsupported market version dataset: {version_dataset}")
        date_column = "cal_date" if key_kind == "calendar" else "trade_date"
        version_filters = ["v.dataset=%s"]
        version_params: list = [version_dataset]
        legacy_filters: list[str] = []
        legacy_params: list = []
        if stock_codes and key_kind != "calendar":
            version_filters.append("v.ts_code=ANY(%s)")
            version_params.append(stock_codes)
            legacy_filters.append("legacy.ts_code=ANY(%s)")
            legacy_params.append(stock_codes)
        if start_date is not None:
            version_filters.append(f"v.{date_column} >= %s")
            version_params.append(start_date)
            legacy_filters.append(f"legacy.{date_column} >= %s")
            legacy_params.append(start_date)
        if end_date is not None:
            version_filters.append(f"v.{date_column} <= %s")
            version_params.append(end_date)
            legacy_filters.append(f"legacy.{date_column} <= %s")
            legacy_params.append(end_date)
        legacy_where = " AND ".join(legacy_filters) if legacy_filters else "TRUE"
        dates_rows = self.kernel.read._repository_query(
            context=context,
            source="tushare_market_raw_version",
            sql=f"""
                SELECT DISTINCT {date_column}
                  FROM (
                      SELECT v.{date_column}
                        FROM tushare_market_raw_version v
                       WHERE {" AND ".join(version_filters)}
                      UNION
                      SELECT legacy.{date_column}
                        FROM {table} legacy
                       WHERE {legacy_where}
                  ) dates
                 WHERE {date_column} IS NOT NULL
                 ORDER BY {date_column}
            """,
            params=(*version_params, *legacy_params),
            scope=WorkScope(partition=f"raw-standard-dates:{version_dataset}"),
            max_rows=100_000,
        )
        for date_row in dates_rows:
            current_date = date_row[date_column]
            if key_kind == "calendar":
                sql = f"""
                    WITH latest AS (
                        SELECT DISTINCT ON (exchange, cal_date) exchange, cal_date, payload, fetched_at
                          FROM tushare_market_raw_version
                         WHERE dataset=%s AND cal_date=%s
                         ORDER BY exchange, cal_date, fetched_at DESC, record_id DESC
                    )
                    SELECT payload, fetched_at FROM latest
                    UNION ALL
                    SELECT legacy.payload, legacy.fetched_at
                      FROM {table} legacy
                     WHERE legacy.cal_date=%s
                       AND NOT EXISTS (
                           SELECT 1 FROM tushare_market_raw_version version
                            WHERE version.dataset=%s
                              AND version.exchange=legacy.exchange
                              AND version.cal_date=legacy.cal_date
                       )
                """
                params = (version_dataset, current_date, current_date, version_dataset)
            else:
                code_clause = " AND v.ts_code=ANY(%s)" if stock_codes else ""
                legacy_code_clause = " AND legacy.ts_code=ANY(%s)" if stock_codes else ""
                sql = f"""
                    WITH candidates AS (
                        SELECT v.ts_code,v.trade_date,v.payload,v.fetched_at,1 AS source_priority,v.record_id
                          FROM tushare_market_raw_version v
                         WHERE v.dataset=%s AND v.trade_date=%s{code_clause}
                        UNION ALL
                        SELECT legacy.ts_code,legacy.trade_date,legacy.payload,legacy.fetched_at,0 AS source_priority,0::bigint
                          FROM {table} legacy
                         WHERE legacy.trade_date=%s{legacy_code_clause}
                    ), latest AS (
                        SELECT DISTINCT ON (ts_code,trade_date) ts_code,trade_date,payload,fetched_at
                          FROM candidates
                         ORDER BY ts_code,trade_date,source_priority DESC,fetched_at DESC,record_id DESC
                    )
                    SELECT payload,fetched_at FROM latest ORDER BY ts_code,trade_date
                """
                params = [version_dataset, current_date]
                if stock_codes:
                    params.append(stock_codes)
                params.append(current_date)
                if stock_codes:
                    params.append(stock_codes)
                params = tuple(params)
            rows = self.kernel.read._repository_query(
                context=context, source="tushare_market_raw_version", sql=sql,
                params=params, scope=WorkScope(date_from=current_date, date_to=current_date, partition=f"raw-standard:{version_dataset}"),
                max_rows=100_000,
            )
            for offset in range(0, len(rows), chunk_size):
                yield [self._payload_row(row["payload"], row.get("fetched_at")) for row in rows[offset:offset + chunk_size]]

    @staticmethod
    def _payload_row(value, fetched_at):
        row = value.copy() if isinstance(value, dict) else json.loads(value)
        row["_source_observed_at"] = fetched_at
        return row

    def upsert_standard_rows(
        self, target: str, rows: list[dict], key: tuple[str, ...], context: RunContext,
        page_size: int = 5000,
    ) -> int:
        if target not in self._STANDARD_TARGETS:
            raise ValueError(f"unsupported Standard target: {target}")
        if not rows:
            return 0
        columns = tuple(rows[0])
        if any(tuple(row) != columns for row in rows):
            raise ValueError("standard rows have inconsistent columns")
        update_columns = tuple(column for column in columns if column not in key)
        if target == "std.trading_calendar":
            scope = WorkScope(partition="standard:trading_calendar")
            return self.kernel.write.upsert(
                context=context, scope=scope, target="std.trading_calendar",
                rows=rows, key=key, update_columns=update_columns, skip_noop=True,
            )
        if target == "std.daily_kline":
            scope = WorkScope(partition="standard:daily_kline")
            return self.kernel.write.upsert(
                context=context, scope=scope, target="std.daily_kline", rows=rows,
                key=key, update_columns=update_columns, skip_noop=True,
            )
        if target == "std.adj_factor_daily":
            return self.kernel.write.upsert(
                context=context, scope=WorkScope(partition="standard:adj_factor_daily"),
            target=target, rows=rows, key=key,
                update_columns=update_columns, skip_noop=True,
            )
        return self.kernel.write.upsert(
            context=context, scope=WorkScope(partition=f"standard:{target}"),
            target=target, rows=rows, key=key, update_columns=update_columns,
            skip_noop=True,
        )

    def rebuild_valuation_set_based(
        self, start_date: date, end_date: date, context: RunContext,
        batch_trade_dates: int = 1, throttle_seconds: float = 0.1,
    ) -> tuple[int, int]:
        """Project daily_basic in bounded date batches inside PostgreSQL."""
        if batch_trade_dates <= 0:
            raise ValueError("batch_trade_dates must be positive")
        if throttle_seconds < 0:
            raise ValueError("throttle_seconds must not be negative")
        dates = self.kernel.read._repository_query(
            context=context,
            source="tushare_market_raw_version",
            sql="""
                SELECT DISTINCT trade_date
                  FROM (
                      SELECT trade_date FROM tushare_market_raw_version
                       WHERE dataset='daily_basic' AND trade_date BETWEEN %s AND %s
                      UNION
                      SELECT trade_date FROM tushare_daily_basic_raw
                       WHERE trade_date BETWEEN %s AND %s
                  ) dates
                 WHERE trade_date IS NOT NULL
                 ORDER BY trade_date
            """,
            params=(start_date, end_date, start_date, end_date),
            scope=WorkScope(date_from=start_date, date_to=end_date, partition="valuation-dates"),
            max_rows=100_000,
        )
        numeric = ",".join(
            f"NULLIF(NULLIF(BTRIM(payload->>'{field}'),''),'NaN')::double precision AS {field}"
            for field in ("turnover_rate", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm", "total_mv", "circ_mv")
        )
        changed_columns = (
            "turnover_rate", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "total_mv", "circ_mv",
            "fetched_at", "pe_ttm_vendor", "ps_ttm_vendor", "pb_vendor", "dv_ttm", "dv_ttm_vendor",
            "dv_ttm_source", "source_observed_at", "data_available_timestamp",
        )
        updates = ",".join(f"{column}=EXCLUDED.{column}" for column in changed_columns)
        changed = " OR ".join(f"valuation_daily.{column} IS DISTINCT FROM EXCLUDED.{column}" for column in changed_columns)
        statement = f"""
            WITH candidates AS (
                SELECT ts_code,trade_date,payload,fetched_at,1 AS source_priority,record_id
                  FROM tushare_market_raw_version
                 WHERE dataset='daily_basic' AND trade_date=ANY(%s)
                UNION ALL
                SELECT ts_code,trade_date,payload,fetched_at,0 AS source_priority,0::bigint
                  FROM tushare_daily_basic_raw
                 WHERE trade_date=ANY(%s)
            ), latest AS (
                SELECT DISTINCT ON (ts_code,trade_date) ts_code,trade_date,payload,fetched_at
                  FROM candidates
                 ORDER BY ts_code,trade_date,source_priority DESC,fetched_at DESC,record_id DESC
            ), normalized AS (
                SELECT ts_code,trade_date,{numeric},fetched_at FROM latest
            ), upserted AS (
                INSERT INTO std.valuation_daily
                    (stock_code,trade_date,turnover_rate,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,total_mv,circ_mv,
                     source,fetched_at,pe_ttm_vendor,ps_ttm_vendor,pb_vendor,dv_ttm,dv_ttm_vendor,
                     dv_ttm_source,source_observed_at,data_available_timestamp)
                SELECT ts_code,trade_date,turnover_rate,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,total_mv,circ_mv,
                       'tushare',fetched_at AT TIME ZONE 'UTC',pe_ttm,ps_ttm,pb,dv_ttm,dv_ttm,
                       'TUSHARE_DAILY_BASIC',fetched_at,
                       (trade_date::timestamp + INTERVAL '17 hours') AT TIME ZONE 'Asia/Shanghai'
                  FROM normalized
                ON CONFLICT (stock_code,trade_date,source) DO UPDATE SET {updates}
                WHERE {changed}
                RETURNING 1
            )
            SELECT (SELECT COUNT(*) FROM normalized), (SELECT COUNT(*) FROM upserted)
        """
        attempted = affected = 0
        trade_dates = [row["trade_date"] for row in dates]
        for offset in range(0, len(trade_dates), batch_trade_dates):
            current = trade_dates[offset:offset + batch_trade_dates]
            plan = SqlPlan(
                operation=Operation.REBUILD, target="std.valuation_daily",
                scope=WorkScope(date_from=current[0], date_to=current[-1], partition="valuation-daily"),
                expected_mode="SET_BASED", estimated_rows=0, allow_insert_select=True,
            )
            current_attempted, current_affected = self.kernel.rebuild.run_scoped_merge_counted(
                statement, plan, context, params=(current, current),
            )
            attempted += current_attempted
            affected += current_affected
            if throttle_seconds:
                time.sleep(throttle_seconds)
        return attempted, affected
