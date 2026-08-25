"""RetryPolicy：重试与错误分类（§6.1）。

可重试 = 幂等、瞬时性错误；禁重试 = 确定性错误（重试无意义或有害）。
"""
from __future__ import annotations

import time
from typing import Callable, Type, TypeVar

from data_collect.data_access.errors import (
    DataAccessError,
    GuardViolation,
    PolicyViolation,
    TransactionBudgetExceeded,
)

T = TypeVar("T")

# psycopg2 异常类型（可选导入，保证无 DB 环境也能 import 本模块）
try:  # pragma: no cover - 取决于环境是否安装 psycopg2
    from psycopg2 import errors as pg_errors
except ImportError:  # pragma: no cover
    pg_errors = None


class RetryPolicy:
    """重试分类器。默认 max_attempts 次，可配置。"""

    # 可重试的瞬时性 PostgreSQL 错误（§6.1）
    RETRYABLE_PG_EXCEPTIONS: tuple[Type[Exception], ...] = (
        (pg_errors.OperationalError,      # 连接瞬断
         pg_errors.DeadlockDetected,      # deadlock (40P01)
         pg_errors.SerializationFailure,  # serialization failure (40001)
         pg_errors.LockNotAvailable,      # lock timeout (55P03))
         ) if pg_errors is not None else ()
    )

    # 明确禁重试的错误（确定性，重试只会放大锁等待/WAL）
    NON_RETRYABLE_EXCEPTIONS: tuple[Type[Exception], ...] = (
        PolicyViolation,
        GuardViolation,
        TransactionBudgetExceeded,
    )

    def __init__(self, max_attempts: int = 3, backoff_seconds: float = 0.05) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts 必须 >= 1")
        self.max_attempts = max_attempts
        self.backoff_seconds = max(0.0, backoff_seconds)

    def run(self, operation: Callable[[], T], rollback: Callable[[], None] | None = None) -> T:
        """运行一个已具备幂等语义的 Gateway chunk。

        RetryPolicy 只负责瞬时错误；业务/策略/质量错误绝不重试。每次重试前
        先回滚当前事务，避免在 aborted transaction 上继续发送 SQL。
        """
        last: BaseException | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                return operation()
            except Exception as exc:
                last = exc
                if not self.should_retry(exc) or attempt >= self.max_attempts:
                    raise
                if rollback is not None:
                    rollback()
                if self.backoff_seconds:
                    time.sleep(self.backoff_seconds * attempt)
        assert last is not None
        raise last

    def should_retry(self, exc: BaseException) -> bool:
        """该错误是否可自动重试。"""
        # 框架自己的确定性错误（策略/守卫/事务预算）→ 禁重试
        if isinstance(exc, self.NON_RETRYABLE_EXCEPTIONS):
            return False
        if isinstance(exc, DataAccessError):
            # 其余框架错误（CheckpointError/LockNotAcquired 等）默认禁重试
            return False
        # PostgreSQL 瞬时性错误 → 可重试
        if isinstance(exc, self.RETRYABLE_PG_EXCEPTIONS):
            return True
        # 语法错误(SyntaxError)/约束冲突(IntegrityError)/数据错误(DataError) 等 → 禁重试
        return False

    def classify(self, exc: BaseException) -> str:
        """返回错误分类标签（供日志/监控）。"""
        if self.should_retry(exc):
            return "RETRYABLE"
        if isinstance(exc, (PolicyViolation, GuardViolation)):
            return "POLICY_GUARD"
        if isinstance(exc, TransactionBudgetExceeded):
            return "TX_BUDGET"
        return "NON_RETRYABLE"
