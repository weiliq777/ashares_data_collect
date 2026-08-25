"""核心数据对象：WorkScope / RunContext / SqlPlan / Operation / RebuildMode。

严格对应《data_access_kernel.md》§3。这些对象是"操作意图"的载体：
Repository 调 Kernel 时只传意图（读什么、写什么、哪个范围、哪种操作），
不传 Connection/Cursor/裸 SQL 执行权。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any, Mapping


class Operation(Enum):
    """数据库行为类型（对应 §4 的 6 种接口）。"""

    APPEND = "APPEND"              # raw.append()
    UPSERT = "UPSERT"              # write.upsert()
    BULK_INSERT = "BULK_INSERT"    # write.bulk_insert()
    REBUILD = "REBUILD"            # rebuild.publish() / run_insert_select() ...
    READ = "READ"                  # read.query() / read.stream()
    PUBLISH = "PUBLISH"            # publish.snapshot() / dataset()


class RebuildMode(Enum):
    """大范围重建的三种发布粒度（对应 §4.6）。"""

    TABLE_REPLACE = "TABLE_REPLACE"          # 整表重建：staging 建好 → swap 整表
    PARTITION_REPLACE = "PARTITION_REPLACE"  # 整分区重建：新 partition → validate → attach/swap
    SCOPED_MERGE = "SCOPED_MERGE"            # 小范围增量：staging → validate → scoped upsert/merge


@dataclass(frozen=True)
class WorkScope:
    """任务处理范围（§3.1）。所有大规模任务统一用它描述范围。

    date_from / date_to / codes / partition 是常用维度；
    dimensions 是可扩展维度（财务报告期/行业/指数成分/snapshot_id 等），
    未来新增数据域不再改 WorkScope 结构。
    """

    date_from: date | None = None
    date_to: date | None = None
    codes: tuple[str, ...] | None = None
    partition: str | None = None
    dimensions: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.codes is not None and not isinstance(self.codes, tuple):
            # 冻结为 tuple，保证不可变与可哈希
            object.__setattr__(self, "codes", tuple(self.codes))
        if (
            self.date_from is not None
            and self.date_to is not None
            and self.date_from > self.date_to
        ):
            raise ValueError(f"WorkScope date_from {self.date_from} > date_to {self.date_to}")

    def has_time_range(self) -> bool:
        return self.date_from is not None or self.date_to is not None

    def has_partition(self) -> bool:
        return self.partition is not None

    def is_scoped(self) -> bool:
        """是否带任意一种范围约束（时间/分区/股票）。"""
        return self.has_time_range() or self.has_partition() or bool(self.codes)


@dataclass(frozen=True)
class RunContext:
    """谁干的（§3.2）。所有数据库操作强制携带，无则拒绝执行。"""

    run_id: str
    job_name: str
    code_version: str
    config_hash: str
    pipeline_version: str

    def __post_init__(self) -> None:
        for attr in ("run_id", "job_name", "code_version", "config_hash", "pipeline_version"):
            value = getattr(self, attr)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"RunContext.{attr} 不能为空")


@dataclass(frozen=True)
class SqlPlan:
    """操作意图描述（§3.3），QueryGuard 不依赖脆弱的 SQL 字符串分析。"""

    operation: Operation
    target: str
    scope: WorkScope
    expected_mode: str = "ROW_BASED"   # "SET_BASED" / "ROW_BASED"
    estimated_rows: int = 0            # 仅预警/选模式/容量规划，非事务切分依据
    allow_insert_select: bool = False  # 是否允许 INSERT...SELECT
    allow_delete: bool = False         # 是否允许 DELETE（大范围重建默认 False）

    def __post_init__(self) -> None:
        if not isinstance(self.operation, Operation):
            raise TypeError("SqlPlan.operation 必须是 Operation")
        if not str(self.target).strip():
            raise ValueError("SqlPlan.target 不能为空")
        if self.expected_mode not in {"SET_BASED", "ROW_BASED"}:
            raise ValueError(
                f"SqlPlan.expected_mode 只能是 SET_BASED/ROW_BASED: {self.expected_mode}"
            )
        if self.estimated_rows < 0:
            raise ValueError("SqlPlan.estimated_rows 不能为负")
        if not isinstance(self.scope, WorkScope):
            raise TypeError("SqlPlan.scope 必须是 WorkScope")
