"""QueryGuard：SQL 执行前的强制检查（§5.2）。

检查依据 = SqlPlan + WorkScope + DatasetPolicy + 数据库角色。
SQL 字符串静态检查只是辅助，不是主要防线。
"""
from __future__ import annotations

from data_collect.data_access.context import Operation, SqlPlan
from data_collect.data_access.dataset_policy import (
    DatasetPolicy,
    DatasetPolicyRegistry,
    ScanPolicy,
    WriteMode,
)
from data_collect.data_access.errors import GuardViolation, PolicyViolation


# 写操作 → 需要的 DatasetPolicy.write_mode
_OPERATION_TO_WRITE_MODE = {
    Operation.APPEND: WriteMode.APPEND_VERSIONED,
    Operation.UPSERT: WriteMode.UPSERT,
    Operation.BULK_INSERT: WriteMode.BULK_INSERT,
    Operation.REBUILD: WriteMode.REBUILD,
    Operation.PUBLISH: WriteMode.PUBLISH_ONLY,
}


class QueryGuard:
    """执行前守卫。任何违反契约的操作在这里被确定性拒绝。"""

    def __init__(self, registry: DatasetPolicyRegistry, *, read_only: bool = False) -> None:
        self._registry = registry
        self._read_only = read_only

    def check(self, plan: SqlPlan) -> None:
        """按 §5.2 规则表逐条检查；违反抛 PolicyViolation / GuardViolation。"""
        if not isinstance(plan, SqlPlan):
            raise TypeError("QueryGuard requires SqlPlan")
        policy = self._registry.require(plan.target)
        op = plan.operation

        if self._read_only and op is not Operation.READ:
            raise GuardViolation(
                f"read-only audit kernel rejects database operation: {op.value}"
            )

        # Schema defaults are useful for read compatibility during migration,
        # but they must never authorize an unregistered write target. Every
        # writable table therefore needs a concrete DatasetPolicy entry.
        if op in _OPERATION_TO_WRITE_MODE and not self._registry.is_explicit(plan.target):
            raise PolicyViolation(
                f"写入目标必须有表级 DatasetPolicy，拒绝 schema 默认策略: {plan.target}"
            )

        self._check_write_mode(plan, policy)
        self._check_frozen(plan, policy)
        self._check_delete_scope(plan)
        self._check_delete_policy(plan, policy)
        self._check_read_scan_policy(plan, policy)
        self._check_require_scope(plan, policy)

    # ---- 各规则 ----

    def _check_write_mode(self, plan: SqlPlan, policy: DatasetPolicy) -> None:
        """规则 1：违反 DatasetPolicy 的写（如 upsert 目标是 append-only 的 Raw）。"""
        if plan.operation in _OPERATION_TO_WRITE_MODE:
            required = _OPERATION_TO_WRITE_MODE[plan.operation]
            if required not in policy.write_modes:
                raise PolicyViolation(
                    f"DatasetPolicy 拒绝: {plan.target} 不允许 {required.value}，"
                    f"仅允许 {sorted(m.value for m in policy.write_modes)}"
                )
        # 额外：即使写模式匹配，若 policy 显式禁止 update/delete，也要拦
        if plan.operation == Operation.UPSERT and not policy.update_allowed:
            raise PolicyViolation(f"DatasetPolicy 禁止 UPDATE: {plan.target}")

    def _check_frozen(self, plan: SqlPlan, policy: DatasetPolicy) -> None:
        """规则 3：禁止 UPDATE/DELETE FROZEN 表（immutable_after_freeze 且 frozen）。"""
        if policy.effectively_read_only and plan.operation in _OPERATION_TO_WRITE_MODE:
            raise GuardViolation(
                f"FROZEN 表绝对只读，禁止 {plan.operation.value}: {plan.target}"
            )
        if policy.effectively_read_only and plan.allow_delete:
            raise GuardViolation(f"FROZEN 表禁止 DELETE: {plan.target}")

    def _check_delete_scope(self, plan: SqlPlan) -> None:
        """规则 2：禁止无范围 DELETE（allow_delete 但 scope 无任何范围约束）。"""
        if plan.allow_delete and not plan.scope.is_scoped():
            raise GuardViolation(
                f"禁止无范围 DELETE: {plan.target}（allow_delete 必须携带 scope）"
            )

    def _check_delete_policy(self, plan: SqlPlan, policy: DatasetPolicy) -> None:
        if plan.allow_delete and not policy.delete_allowed:
            raise PolicyViolation(f"DatasetPolicy 禁止 DELETE: {plan.target}")

    def _check_read_scan_policy(self, plan: SqlPlan, policy: DatasetPolicy) -> None:
        """规则 6/7：大表禁止无 Scope 扫描；小维度表允许全表读。"""
        if plan.operation == Operation.READ:
            if policy.scan_policy == ScanPolicy.SCOPE_REQUIRED and not plan.scope.is_scoped():
                raise GuardViolation(
                    f"scan_policy=SCOPE_REQUIRED 的表必须带 Scope/分区/日期范围: {plan.target}"
                )
            # FULL_SCAN_ALLOWED → 放行，不检查

    def _check_require_scope(self, plan: SqlPlan, policy: DatasetPolicy) -> None:
        """规则补充：policy.require_scope 为真且 scope 无范围 → 拒绝。"""
        if plan.operation in _OPERATION_TO_WRITE_MODE and policy.require_scope:
            if not plan.scope.is_scoped():
                raise GuardViolation(f"DatasetPolicy 要求 scope: {plan.target}")
        if (
            plan.operation in _OPERATION_TO_WRITE_MODE
            and policy.require_snapshot_id
            and not plan.scope.dimensions.get("snapshot_id")
        ):
            raise GuardViolation(f"DatasetPolicy 要求 snapshot_id: {plan.target}")


def validate_table_name(name: str) -> str:
    """校验表名/列名，防止 SQL 注入（迁移自 utils/db.py）。

    只允许合法标识符（字母/数字/下划线），且不能是纯数字。
    """
    if not isinstance(name, str) or not name.isidentifier():
        raise ValueError(f"非法标识符: {name!r}")
    return name


def validate_qualified_name(name: str) -> str:
    """校验 'schema.table' 形式的限定名。"""
    parts = name.split(".")
    if len(parts) > 2:
        raise ValueError(f"非法限定名: {name!r}")
    return ".".join(validate_table_name(p) for p in parts)
