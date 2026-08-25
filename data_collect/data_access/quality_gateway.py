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
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {staging}")
            row = cur.fetchone()
        row_count = int(row[0]) if row else 0
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
            null_predicate = " OR ".join(
                f"{column} IS NULL" for column in policy.primary_key
            )
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM {staging} WHERE {null_predicate}"
                )
                row = cur.fetchone()
            null_key_count = int(row[0]) if row else 0

        date_column = next(
            (
                column for column in policy.partition_key
                if column in {"trade_date", "research_date", "asof_date", "cal_date"}
            ),
            None,
        )
        outside_scope_count = 0
        max_observed = None
        if date_column:
            clauses = []
            params = []
            if scope.date_from is not None:
                clauses.append(f"{date_column} < %s")
                params.append(scope.date_from)
            if scope.date_to is not None:
                clauses.append(f"{date_column} > %s")
                params.append(scope.date_to)
            if clauses:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {staging} WHERE " + " OR ".join(clauses),
                        tuple(params),
                    )
                    row = cur.fetchone()
                outside_scope_count = int(row[0]) if row else 0
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX({date_column}) FROM {staging}")
                row = cur.fetchone()
            max_observed = row[0] if row else None

        completeness_blocks = row_count == 0 or null_key_count > 0
        freshness_block = bool(
            scope.date_to is not None
            and date_column is not None
            and max_observed is not None
            and max_observed < scope.date_to
        )
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT COUNT(*) FROM {staging} t
                     WHERE COALESCE(
                               to_jsonb(t)->>'version_available_date',
                               to_jsonb(t)->>'source_available_at',
                               to_jsonb(t)->>'financial_version_available_date',
                               to_jsonb(t)->>'effective_announce_date'
                           ) IS NOT NULL
                       AND COALESCE(
                               to_jsonb(t)->>'asof_date',
                               to_jsonb(t)->>'research_date',
                               to_jsonb(t)->>'trade_date'
                           ) IS NOT NULL
                       AND COALESCE(
                               to_jsonb(t)->>'version_available_date',
                               to_jsonb(t)->>'source_available_at',
                               to_jsonb(t)->>'financial_version_available_date',
                               to_jsonb(t)->>'effective_announce_date'
                           )::timestamp
                           > COALESCE(
                               to_jsonb(t)->>'asof_date',
                               to_jsonb(t)->>'research_date',
                               to_jsonb(t)->>'trade_date'
                           )::timestamp"""
            )
            row = cur.fetchone()
        pit_violation_count = int(row[0]) if row else 0
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
