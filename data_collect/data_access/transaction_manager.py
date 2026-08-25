"""TransactionManager：事务硬预算（§6）。

rows/bytes 由 ChunkPlanner 执行前控制；时间超时只能中止（rollback），不能"执行中 commit"。
业务代码禁止 commit/rollback —— 本类只被 Gateway 内部使用，绝不对业务层暴露 connection。
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

from data_collect.data_access.errors import TransactionBudgetExceeded


@dataclass(frozen=True)
class TransactionConfig:
    max_rows_per_tx: int = 50_000
    max_bytes_per_tx: int = 67_108_864
    transaction_deadline: float = 30.0  # 单事务累计时间上限（秒），超时只能中止


class TransactionManager:
    """统一事务入口。

    连接由内部持有。这里不提供 public ``connection`` 属性；Gateway 通过
    Kernel 的 private bridge 使用它，业务层和 Repository 都拿不到 Connection。
    """

    def __init__(
        self,
        config: TransactionConfig | None = None,
        connection_factory: Callable[[], object] | None = None,
    ) -> None:
        self.config = config or TransactionConfig()
        if (
            self.config.max_rows_per_tx <= 0
            or self.config.max_bytes_per_tx <= 0
            or self.config.transaction_deadline <= 0
        ):
            raise ValueError("transaction budgets must all be > 0")
        self._connection_factory = connection_factory
        self._conn: object | None = None
        self._tx_started_at: float | None = None
        self._atomic_depth = 0

    def _default_connect(self) -> object:
        """Kernel 私有连接入口（取代旧 get_connection 的职责）。"""
        from data_collect.data_access.schema import connect_with_safety_limits

        return connect_with_safety_limits()

    def _connection(self) -> object:
        """返回内部连接。

        这是 framework-private API。调用者只能是 Gateway/Kernel 内部代码；
        不使用 public property 是为了避免 ``kernel.transaction_manager.connection``
        变成新的绕过入口。
        """
        if self._conn is not None and bool(getattr(self._conn, "closed", False)):
            self._conn = None
        if self._conn is None:
            factory = self._connection_factory or self._default_connect
            self._conn = factory()
        return self._conn

    def begin_chunk(self) -> None:
        """标记一个 chunk 事务开始。"""
        conn = self._connection()
        module = type(conn).__module__
        if module.startswith(("psycopg2", "psycopg")) or getattr(
            conn, "supports_statement_deadline", False
        ):
            deadline_ms = max(1, int(self.config.transaction_deadline * 1000))
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = %s", (deadline_ms,))
        self._tx_started_at = time.monotonic()

    @property
    def in_atomic(self) -> bool:
        """Whether a Repository-owned short multi-write transaction is active."""
        return self._atomic_depth > 0

    def begin_atomic(self) -> None:
        """Start a Repository workflow transaction without exposing the connection."""
        if self._atomic_depth:
            raise RuntimeError("nested atomic workflows are not supported")
        self._atomic_depth = 1
        self.begin_chunk()

    def commit_chunk(self) -> None:
        """提交当前 chunk；若累计时间超 deadline，回滚并抛 TransactionBudgetExceeded。"""
        if (
            self._tx_started_at is not None
            and (time.monotonic() - self._tx_started_at) > self.config.transaction_deadline
        ):
            self.rollback()
            raise TransactionBudgetExceeded(
                f"单事务累计时间超过 deadline {self.config.transaction_deadline}s，已回滚"
            )
        self._connection().commit()
        self._tx_started_at = None
        self._atomic_depth = 0

    def rollback(self) -> None:
        """回滚当前事务（用于失败路径）。"""
        conn = getattr(self, "_conn", None)
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                # A broken connection cannot retain a usable transaction.
                # Discard it without masking the original database exception;
                # an idempotent retry will obtain a fresh connection.
                try:
                    conn.close()
                except Exception:
                    pass
                self._conn = None
            finally:
                self._tx_started_at = None
                self._atomic_depth = 0

    def close(self) -> None:
        """关闭连接。"""
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
                self._tx_started_at = None
                self._atomic_depth = 0

    def __enter__(self) -> "TransactionManager":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is not None:
                self.rollback()
        finally:
            self.close()
