"""QualityGateway：质量检查记录与门禁（§4 quality / 架构方案 §6 第 5 条）。

六类检查（Completeness/Freshness/Uniqueness/Validity/Consistency/PIT Safety），
带 severity: INFO/WARN/BLOCK；仅 BLOCK=0 才允许 FROZEN。
"""
from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any, Mapping

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.errors import DataAccessError
from data_collect.data_access.query_guard import validate_qualified_name


class QualityViolation(DataAccessError):
    """存在 severity=BLOCK 的质量问题。"""

    def __init__(self, blocks: list[str]) -> None:
        self.blocks = blocks
        super().__init__("质量门禁未通过（BLOCK）: " + "; ".join(blocks))


class QualityGateway:
    def __init__(self, kernel) -> None:
        self._kernel = kernel

    def record(
        self,
        context: RunContext,
        scope: WorkScope,
        checks: Mapping[str, Mapping[str, Any]],
        target: str | None = None,
    ) -> dict[str, Any]:
        """记录一次质量检查结果。checks: {check_name: {severity, detail}}。"""
        if not isinstance(context, RunContext):
            raise TypeError("quality.record 必须携带 RunContext")
        result = {
            "run_id": context.run_id,
            "job_name": context.job_name,
            "scope": {
                "date_from": scope.date_from.isoformat() if scope.date_from else None,
                "date_to": scope.date_to.isoformat() if scope.date_to else None,
            },
            "checks": {name: dict(meta) for name, meta in checks.items()},
        }
        blocks = [
            name for name, meta in checks.items() if str(meta.get("severity", "")).upper() == "BLOCK"
        ]
        result["block_count"] = len(blocks)
        canonical = json.dumps(result, sort_keys=True, ensure_ascii=False, default=str)
        result["report_hash"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        if target is not None and "ops.data_quality_result" in self._kernel.policy_registry:
            target = validate_qualified_name(target)
            quality_scope = WorkScope(
                date_from=scope.date_from,
                date_to=scope.date_to,
                codes=scope.codes,
                partition=f"quality:{target}",
                dimensions=dict(scope.dimensions),
            )
            quality_id = str(uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{context.run_id}:{target}:{result['report_hash']}",
            ))
            self._kernel.write.bulk_insert_ignore(
                context,
                quality_scope,
                "ops.data_quality_result",
                [{
                    "quality_result_id": quality_id,
                    "run_id": context.run_id,
                    "job_name": context.job_name,
                    "target": target,
                    "scope": json.dumps(result["scope"], sort_keys=True),
                    "checks": json.dumps(result["checks"], sort_keys=True, default=str),
                    "block_count": result["block_count"],
                    "report_hash": result["report_hash"],
                }],
            )
        return result

    def validate(
        self,
        context: RunContext,
        scope: WorkScope,
        staging: str,
        target: str,
    ) -> None:
        """发布前质量门禁：存在 BLOCK 则抛 QualityViolation，阻止 FROZEN/publish。"""
        # Quality scans can involve several aggregate queries. Keep them
        # inside one governed read transaction so statement_timeout and the
        # cumulative TransactionManager deadline apply to the whole gate.
        checks = self._kernel._run_chunk(
            lambda: self._gather_checks(context, scope, staging, target)
        )
        result = self.record(context, scope, checks, target=target)
        if result["block_count"] > 0:
            raise QualityViolation(
                [name for name, m in checks.items() if str(m.get("severity", "")).upper() == "BLOCK"]
            )

    def _gather_checks(
        self, context: RunContext, scope: WorkScope, staging: str, target: str
    ) -> Mapping[str, Mapping[str, Any]]:
        """收集六类检查（生产实现接入真实覆盖/新鲜度/唯一性/合法性/一致性/PIT 安全）。"""
        # 默认：无 BLOCK，生产由子类/注入覆盖
        staging = validate_qualified_name(staging)
        target = validate_qualified_name(target)
        conn = self._kernel._db_connection()
        policy = self._kernel.policy_registry.require(target)
        duplicate_count = 0
        null_key_count = 0
        if policy.primary_key:
            key_sql = ", ".join(policy.primary_key)
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM (SELECT {key_sql}, COUNT(*) AS n "
                    f"FROM {staging} GROUP BY {key_sql} HAVING COUNT(*) > 1) duplicates"
                )
                row = cur.fetchone()
            duplicate_count = int(row[0]) if row else 0

        date_column = next(
            (
                column for column in policy.partition_key
                if column in {"trade_date", "research_date", "asof_date", "cal_date"}
            ),
            None,
        )
        # Combine the row-count, null-key, scope, freshness and PIT scans into
        # one bounded aggregate over staging.  The duplicate-group check is
        # intentionally kept separate because it requires GROUP BY.  The
        # materialized JSON projection makes the generic PIT check evaluate
        # to_jsonb once per staged row instead of once per referenced field.
        aggregate_columns = ["COUNT(*) AS row_count"]
        if policy.primary_key:
            null_predicate = " OR ".join(
                f"s.{column} IS NULL" for column in policy.primary_key
            )
            aggregate_columns.append(
                f"COUNT(*) FILTER (WHERE {null_predicate}) AS null_key_count"
            )
        else:
            aggregate_columns.append("0 AS null_key_count")

        aggregate_params: list[Any] = []
        if date_column:
            outside_clauses = []
            if scope.date_from is not None:
                outside_clauses.append(f"s.{date_column} < %s")
                aggregate_params.append(scope.date_from)
            if scope.date_to is not None:
                outside_clauses.append(f"s.{date_column} > %s")
                aggregate_params.append(scope.date_to)
            outside_expression = (
                " OR ".join(outside_clauses) if outside_clauses else "FALSE"
            )
            aggregate_columns.extend([
                f"COUNT(*) FILTER (WHERE {outside_expression}) AS outside_scope_count",
                f"MAX(s.{date_column}) AS max_observed",
            ])
        else:
            aggregate_columns.extend([
                "0 AS outside_scope_count",
                "NULL AS max_observed",
            ])

        availability = (
            "COALESCE(s.quality_row_json->>'version_available_date', "
            "s.quality_row_json->>'source_available_at', "
            "s.quality_row_json->>'financial_version_available_date', "
            "s.quality_row_json->>'effective_announce_date')"
        )
        asof = (
            "COALESCE(s.quality_row_json->>'asof_date', "
            "s.quality_row_json->>'research_date', "
            "s.quality_row_json->>'trade_date')"
        )
        aggregate_columns.append(
            f"COUNT(*) FILTER (WHERE {availability} IS NOT NULL "
            f"AND {asof} IS NOT NULL AND {availability}::timestamp > {asof}::timestamp) "
            "AS pit_violation_count"
        )
        selected_columns = list(policy.primary_key)
        if date_column:
            selected_columns.append(date_column)
        selected_columns = list(dict.fromkeys(selected_columns))
        select_list = ", ".join(f"t.{column}" for column in selected_columns)
        if select_list:
            select_list += ", "
        aggregate_sql = (
            "WITH staged AS MATERIALIZED ("
            f"SELECT {select_list}to_jsonb(t) AS quality_row_json FROM {staging} t"
            ") SELECT " + ", ".join(aggregate_columns) + " FROM staged s"
        )
        with conn.cursor() as cur:
            cur.execute(aggregate_sql, tuple(aggregate_params))
            row = cur.fetchone()
        row = row or (0, 0, 0, None, 0)
        row_count = int(row[0] or 0)
        null_key_count = int(row[1] or 0)
        outside_scope_count = int(row[2] or 0)
        max_observed = row[3]
        pit_violation_count = int(row[4] or 0)

        completeness_blocks = row_count == 0 or null_key_count > 0
        freshness_block = bool(
            scope.date_to is not None
            and date_column is not None
            and max_observed is not None
            and max_observed < scope.date_to
        )
        return {
            "completeness": {
                "severity": "BLOCK" if completeness_blocks else "INFO",
                "rows": row_count,
                "null_primary_keys": null_key_count,
            },
            "freshness": {
                "severity": "BLOCK" if freshness_block else "INFO",
                "max_observed": max_observed,
                "expected_through": scope.date_to,
            },
            "uniqueness": {
                "severity": "BLOCK" if duplicate_count else "INFO",
                "duplicate_groups": duplicate_count,
            },
            "validity": {
                "severity": "BLOCK" if outside_scope_count else "INFO",
                "outside_scope_rows": outside_scope_count,
            },
            "consistency": {
                "severity": "BLOCK" if duplicate_count else "INFO",
                "detail": "primary-key and scope consistency",
            },
            "pit_safety": {
                "severity": "BLOCK" if pit_violation_count else "INFO",
                "availability_after_asof_rows": pit_violation_count,
            },
        }
