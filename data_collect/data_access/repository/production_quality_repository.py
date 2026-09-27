"""Bounded quality checks for one production increment window."""
from __future__ import annotations

from datetime import date
from typing import Any

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class ProductionQualityRepository(BaseRepository):
    _EXPECTED_GATE_DATASETS = ("daily", "daily_basic", "adj_factor")
    _WINDOWS = {
        "pit.universe_daily": "trade_date",
        "pit.security_tradeability_daily": "trade_date",
        "feature.price_daily_v2": "trade_date",
        "feature.valuation_daily_v1": "trade_date",
        "feature.technical_daily_v1": "trade_date",
    }

    def inspect_window(
        self,
        start: date,
        end: date,
        context: RunContext,
        expected_dates: tuple[date, ...] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        scope = WorkScope(date_from=start, date_to=end, partition="production-quality")
        expected_date_values = {
            value.isoformat() if hasattr(value, "isoformat") else str(value)
            for value in (expected_dates or ())
        }
        expected_date_objects = tuple(
            value if isinstance(value, date) else date.fromisoformat(str(value))
            for value in sorted(expected_date_values)
        )
        if len(expected_date_objects) > 31:
            raise ValueError("production quality code coverage is limited to 31 trade dates")
        if any(value < start or value > end for value in expected_date_objects):
            raise ValueError("production quality expected dates must be inside the inspection window")
        for table, date_column in self._WINDOWS.items():
            unexpected_status_predicate = {
                # A listed security without a price is an expected no-price
                # observation, not a usable market row.  Keep its
                # MISSING_SOURCE count in the report, but only fail the gate
                # when the row is part of the usable output population.
                "pit.universe_daily": "(quality_status IS NULL OR quality_status NOT IN ('VALID','WARNING','INSUFFICIENT_HISTORY')) AND has_price_history IS TRUE",
                "pit.security_tradeability_daily": "(quality_status IS NULL OR quality_status NOT IN ('VALID','WARNING','INSUFFICIENT_HISTORY')) AND is_tradeable IS TRUE",
            }.get(
                table,
                "quality_status IS NULL OR quality_status NOT IN ('VALID','WARNING','INSUFFICIENT_HISTORY')",
            )
            row = self.kernel.read._repository_query(
                context, table,
                f"""SELECT COUNT(*) AS rows,
                    COUNT(DISTINCT {date_column}) AS observed_dates,
                    ARRAY_AGG(DISTINCT {date_column} ORDER BY {date_column}) AS observed_date_values,
                    COUNT(*) FILTER (WHERE quality_status='MISSING_SOURCE') AS missing_source,
                    COUNT(*) FILTER (WHERE {unexpected_status_predicate}) AS unexpected_status
                    FROM {table} WHERE {date_column} BETWEEN %s AND %s""",
                params=(start, end), scope=scope, max_rows=2,
            )[0]
            observed_values = [
                value.isoformat() if hasattr(value, "isoformat") else str(value)
                for value in (row.get("observed_date_values") or ())
            ]
            result[table] = {
                "rows": int(row.get("rows") or 0),
                "observed_dates": int(row.get("observed_dates") or 0),
                "observed_date_values": observed_values,
                "missing_source": int(row.get("missing_source") or 0),
                "unexpected_status": int(row.get("unexpected_status") or 0),
            }

        gate_rows = self.kernel.read._repository_query(
            context, "ops.market_data_quality_gate",
            """WITH latest AS (
                    SELECT DISTINCT ON (dataset, trade_date)
                           dataset, trade_date, status
                      FROM ops.market_data_quality_gate
                     WHERE trade_date BETWEEN %s AND %s
                     ORDER BY dataset, trade_date, created_at DESC, gate_id DESC
                )
                SELECT dataset, trade_date, status
                  FROM latest
                 ORDER BY trade_date, dataset""",
            params=(start, end), scope=scope, max_rows=10_000,
        )
        expected_gate_keys = {
            (dataset, value)
            for dataset in self._EXPECTED_GATE_DATASETS
            for value in expected_date_values
        }
        actual_gate_keys = {
            (str(row["dataset"]),
             row["trade_date"].isoformat()
             if hasattr(row["trade_date"], "isoformat")
             else str(row["trade_date"]))
            for row in gate_rows
        }
        missing_gate_keys = sorted(expected_gate_keys - actual_gate_keys)
        non_pass_gate_keys = sorted(
            (str(row["dataset"]),
             row["trade_date"].isoformat()
             if hasattr(row["trade_date"], "isoformat")
             else str(row["trade_date"]))
            for row in gate_rows
            if str(row["status"]) != "PASS"
        )
        result["ops.market_data_quality_gate"] = {
            "total": len(gate_rows),
            "passed": sum(1 for row in gate_rows if str(row["status"]) == "PASS"),
            "blocked": sum(1 for row in gate_rows if str(row["status"]) == "BLOCKED"),
            "expected_total": len(expected_gate_keys),
            "missing_keys": [f"{dataset}:{trade_date}" for dataset, trade_date in missing_gate_keys],
            "non_pass_keys": [f"{dataset}:{trade_date}" for dataset, trade_date in non_pass_gate_keys],
        }
        failures = []
        if not expected_date_values:
            failures.append("production quality: expected trade-date set is empty")
        for table in self._WINDOWS:
            table_result = result[table]
            if table_result["rows"] == 0:
                failures.append(f"{table}: no rows in production window")
            observed_values = set(table_result["observed_date_values"])
            missing_values = sorted(expected_date_values - observed_values)
            extra_values = sorted(observed_values - expected_date_values)
            if missing_values or extra_values:
                failures.append(
                    f"{table}: date set mismatch missing={missing_values[:10]} extra={extra_values[:10]}"
                )
        for table in self._WINDOWS:
            if result[table]["unexpected_status"]:
                failures.append(f"{table}: unexpected quality status")

        coverage_sql = """WITH expected_core AS (
                    SELECT trade_date, ts_code
                      FROM std.security_status_daily
                     WHERE trade_date = ANY(%s::date[])
                ), expected_price AS (
                    -- Price/technical producers consume the Standard daily
                    -- kline population.  Retain the Universe population as
                    -- a second source so missing candidate rows are still
                    -- detected when a producer's source scopes diverge.
                    SELECT DISTINCT trade_date, stock_code AS ts_code
                      FROM std.daily_kline
                     WHERE trade_date = ANY(%s::date[])
                    UNION
                    SELECT DISTINCT trade_date, ts_code
                      FROM pit.universe_daily
                     WHERE trade_date = ANY(%s::date[])
                       AND has_price_history IS TRUE
                ), expected_valuation AS (
                    SELECT DISTINCT trade_date, stock_code AS ts_code
                      FROM std.valuation_daily
                     WHERE source='tushare'
                       AND trade_date = ANY(%s::date[])
                ), expected AS (
                    SELECT 'pit.universe_daily' AS table_name, trade_date, ts_code
                      FROM expected_core
                    UNION ALL
                    SELECT 'pit.security_tradeability_daily', trade_date, ts_code
                      FROM expected_core
                    UNION ALL
                    SELECT 'feature.price_daily_v2', trade_date, ts_code
                      FROM expected_price
                    UNION ALL
                    SELECT 'feature.valuation_daily_v1', trade_date, ts_code
                      FROM expected_valuation
                    UNION ALL
                    SELECT 'feature.technical_daily_v1', trade_date, ts_code
                      FROM expected_price
                ), actual AS (
                    SELECT 'pit.universe_daily' AS table_name, trade_date, ts_code
                      FROM pit.universe_daily
                     WHERE trade_date = ANY(%s::date[])
                    UNION ALL
                    SELECT 'pit.security_tradeability_daily', trade_date, ts_code
                      FROM pit.security_tradeability_daily
                     WHERE trade_date = ANY(%s::date[])
                    UNION ALL
                    SELECT 'feature.price_daily_v2', trade_date, ts_code
                      FROM feature.price_daily_v2
                     WHERE trade_date = ANY(%s::date[])
                    UNION ALL
                    SELECT 'feature.valuation_daily_v1', trade_date, ts_code
                      FROM feature.valuation_daily_v1
                     WHERE trade_date = ANY(%s::date[])
                    UNION ALL
                    SELECT 'feature.technical_daily_v1', trade_date, stock_code AS ts_code
                      FROM feature.technical_daily_v1
                     WHERE trade_date = ANY(%s::date[])
                ), joined AS (
                    SELECT COALESCE(e.table_name, a.table_name) AS table_name,
                           COALESCE(e.trade_date, a.trade_date) AS trade_date,
                           e.ts_code AS expected_code,
                           a.ts_code AS actual_code
                      FROM expected e
                      FULL OUTER JOIN actual a
                        ON a.table_name=e.table_name
                       AND a.trade_date=e.trade_date
                       AND a.ts_code=e.ts_code
                )
                SELECT table_name, trade_date,
                       COUNT(expected_code) AS expected_rows,
                       COUNT(actual_code) AS actual_rows,
                       COUNT(*) FILTER (
                           WHERE expected_code IS NOT NULL AND actual_code IS NULL
                       ) AS missing_rows,
                       COUNT(*) FILTER (
                           WHERE expected_code IS NULL AND actual_code IS NOT NULL
                       ) AS extra_rows,
                       MD5(COALESCE(
                           STRING_AGG(expected_code, ',' ORDER BY expected_code)
                               FILTER (WHERE expected_code IS NOT NULL), ''
                       )) AS expected_fingerprint,
                       MD5(COALESCE(
                           STRING_AGG(actual_code, ',' ORDER BY actual_code)
                               FILTER (WHERE actual_code IS NOT NULL), ''
                       )) AS actual_fingerprint
                  FROM joined
                 GROUP BY table_name, trade_date
                 ORDER BY table_name, trade_date"""
        coverage_rows = []
        for trade_date in expected_date_objects:
            day_scope = WorkScope(
                date_from=trade_date,
                date_to=trade_date,
                partition="production-quality-code-coverage",
            )
            day_values = [trade_date]
            coverage_rows.extend(self.kernel.read._repository_query(
                context,
                "pit.universe_daily",
                coverage_sql,
                params=(day_values,) * 9,
                scope=day_scope,
                max_rows=len(self._WINDOWS),
            ))
        coverage = {
            f"{row['table_name']}:{row['trade_date'].isoformat() if hasattr(row['trade_date'], 'isoformat') else row['trade_date']}": {
                key: int(row[key] or 0)
                for key in ("expected_rows", "actual_rows", "missing_rows", "extra_rows")
            } | {
                "expected_fingerprint": row.get("expected_fingerprint"),
                "actual_fingerprint": row.get("actual_fingerprint"),
            }
            for row in coverage_rows
        }
        result["code_coverage"] = coverage
        for table in self._WINDOWS:
            for trade_date in expected_date_values:
                key = f"{table}:{trade_date}"
                item = coverage.get(key)
                if item is None:
                    failures.append(f"{table}:{trade_date}: missing code coverage result")
                    continue
                if item["missing_rows"] or item["extra_rows"]:
                    failures.append(
                        f"{key}: code coverage mismatch "
                        f"missing={item['missing_rows']} extra={item['extra_rows']}"
                    )
                if item["expected_fingerprint"] != item["actual_fingerprint"]:
                    failures.append(
                        f"{key}: code coverage fingerprint mismatch "
                        f"expected={item['expected_fingerprint']} "
                        f"actual={item['actual_fingerprint']}"
                    )
        gate = result["ops.market_data_quality_gate"]
        if gate["missing_keys"]:
            failures.append(
                "market_data_quality_gate: missing expected keys "
                + ", ".join(gate["missing_keys"][:10])
            )
        if gate["non_pass_keys"]:
            failures.append(
                "market_data_quality_gate: non-PASS keys "
                + ", ".join(gate["non_pass_keys"][:10])
            )
        result["failures"] = failures
        return result
