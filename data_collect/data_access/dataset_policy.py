"""DatasetPolicy / DatasetPolicyRegistry（§5.1）。

Kernel 靠它知道每个 Dataset 能干什么，而不是靠调用者自觉选对 Gateway。
未注册的 Dataset 一律 fail-closed（拒绝）。
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum

from data_collect.data_access.errors import PolicyViolation


class ScanPolicy(Enum):
    """读扫描策略（§5.2）：大表必须带 Scope，小维度表允许全表读。"""

    SCOPE_REQUIRED = "SCOPE_REQUIRED"
    FULL_SCAN_ALLOWED = "FULL_SCAN_ALLOWED"


class WriteMode(Enum):
    """写模式（对应 DatasetPolicy.write_modes 白名单）。"""

    APPEND_VERSIONED = "APPEND_VERSIONED"  # Raw：只追加，新版本 INSERT，历史不覆盖
    UPSERT = "UPSERT"                       # Standard/PIT/Feature：增量幂等更新
    REBUILD = "REBUILD"                     # 大范围重建（staging → publish）
    BULK_INSERT = "BULK_INSERT"             # 纯插入（staging / 新版本）
    PUBLISH_ONLY = "PUBLISH_ONLY"           # 只能通过 publish 流程写入，禁止直写


@dataclass(frozen=True)
class DatasetPolicy:
    """一个 Dataset（表/视图）的可写/可读/可扫描约束。"""

    name: str
    write_modes: frozenset[WriteMode] = frozenset()
    primary_key: tuple[str, ...] = ()
    partition_key: tuple[str, ...] = ()
    require_scope: bool = False                       # 写入是否强制带 scope
    scan_policy: ScanPolicy = ScanPolicy.SCOPE_REQUIRED
    frozen: bool = False                              # 是否已冻结
    immutable_after_freeze: bool = False              # 冻结后绝对只读
    require_snapshot_id: bool = False                 # 写入是否强制 snapshot_id
    update_allowed: bool = True
    delete_allowed: bool = True
    direct_read_allowed: bool = True
    max_read_rows: int = 100_000                       # query() 的硬上限；超限必须 stream()

    def __post_init__(self) -> None:
        if not str(self.name).strip():
            raise ValueError("DatasetPolicy.name 不能为空")
        if self.primary_key and not isinstance(self.primary_key, tuple):
            object.__setattr__(self, "primary_key", tuple(self.primary_key))
        if self.partition_key and not isinstance(self.partition_key, tuple):
            object.__setattr__(self, "partition_key", tuple(self.partition_key))
        if self.write_modes and not isinstance(self.write_modes, frozenset):
            object.__setattr__(self, "write_modes", frozenset(self.write_modes))
        for column in (*self.primary_key, *self.partition_key):
            if not isinstance(column, str) or not column.isidentifier():
                raise ValueError(f"DatasetPolicy 非法列名: {column!r}")
        if self.max_read_rows <= 0:
            raise ValueError("DatasetPolicy.max_read_rows 必须 > 0")

    def allows_write(self, mode: WriteMode) -> bool:
        return mode in self.write_modes

    @property
    def effectively_read_only(self) -> bool:
        """冻结后只读的表（immutable_after_freeze 且 frozen）。"""
        return self.immutable_after_freeze and self.frozen


class DatasetPolicyRegistry:
    """DatasetPolicy 注册表。未注册即 fail-closed。"""

    def __init__(self) -> None:
        self._policies: dict[str, DatasetPolicy] = {}

    def register(self, policy: DatasetPolicy) -> None:
        if policy.name in self._policies:
            raise ValueError(f"DatasetPolicy 重复注册: {policy.name}")
        self._policies[policy.name] = policy

    def register_many(self, policies: list[DatasetPolicy]) -> None:
        for policy in policies:
            self.register(policy)

    def get(self, name: str) -> DatasetPolicy | None:
        """精确匹配优先；否则按 schema 级通配（如注册 "raw.*" 匹配 "raw.tushare_daily"）。"""
        if name in self._policies:
            return self._policies[name]
        for policy_name, policy in self._policies.items():
            if policy_name.endswith(".*") and name.startswith(policy_name[:-1]):
                return policy
        return None

    def is_explicit(self, name: str) -> bool:
        """Whether a dataset has a concrete table-level policy entry."""
        return name in self._policies and not name.endswith(".*")

    def require(self, name: str) -> DatasetPolicy:
        """取 policy；未注册则抛 PolicyViolation（fail-closed）。"""
        policy = self.get(name)
        if policy is None:
            raise PolicyViolation(f"未注册的 Dataset，默认拒绝（fail-closed）: {name}")
        return policy

    def names(self) -> list[str]:
        return sorted(self._policies)

    def freeze(self, name: str) -> DatasetPolicy:
        """将 DatasetPolicy 原子切换为 FROZEN；不存在或已冻结都 fail-closed。"""
        policy = self.require(name)
        if not policy.immutable_after_freeze:
            raise PolicyViolation(f"Dataset 不允许冻结: {name}")
        frozen = replace(policy, frozen=True)
        self._policies[name] = frozen
        return frozen

    def __contains__(self, name: str) -> bool:
        return name in self._policies
