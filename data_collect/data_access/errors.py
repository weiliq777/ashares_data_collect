"""Data Access Framework 统一异常体系。

异常层次用于 RetryPolicy 分类与 QueryGuard/TransactionManager 的行为判定：
任何违反契约（Policy / Guard / 事务预算）的错误都应是 *确定性的业务错误*，
绝不落入"可重试"分支。
"""
from __future__ import annotations


class DataAccessError(Exception):
    """Data Access Framework 所有异常的基类。"""


class PolicyViolation(DataAccessError):
    """违反 DatasetPolicy：对不允许的操作（如 upsert 目标是 append-only 的 Raw 表）。"""


class GuardViolation(DataAccessError):
    """QueryGuard 拒绝：无范围 DELETE、FROZEN 表修改、无 Scope 大表扫描等。"""


class TransactionBudgetExceeded(DataAccessError):
    """事务硬预算被触发（单事务累计时间超过 deadline），事务已回滚。"""


class RetryNotAllowed(DataAccessError):
    """该错误不可重试（语法/约束/策略/质量/业务数据错误）。"""


class CheckpointError(DataAccessError):
    """Checkpoint 状态不一致（断点续传失败）。"""


class LockNotAcquired(DataAccessError):
    """未取得单并发排他锁（同类任务正在运行）。"""
