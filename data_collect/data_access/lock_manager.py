"""LockManager：单并发排他锁（§2）。

同表同区间禁止并发写。底层用 PostgreSQL advisory lock；
key 由 scope/target 派生，保证同一范围只有一个任务在写。
"""
from __future__ import annotations

import hashlib
from contextlib import contextmanager
from collections.abc import Iterable

from data_collect.data_access.transaction_manager import TransactionManager


class LockManager:
    """会话级 advisory lock。事务级可用 pg_advisory_xact_lock 变体。"""

    def __init__(self, transaction_manager: TransactionManager) -> None:
        self._tm = transaction_manager

    @staticmethod
    def lock_key(target: str, scope_key: str) -> int:
        """把 target + scope 派生成稳定的 32 位整数 advisory key。"""
        digest = hashlib.sha256(f"{target}:{scope_key}".encode("utf-8")).digest()
        # 取前 8 字节解释为有符号 64 位整数（advisory lock 支持 bigint）
        return int.from_bytes(digest[:8], "big", signed=True)

    def acquire_exclusive(self, target: str, scope_key: str) -> None:
        """阻塞式获取排他锁；另一任务持有时会等待。

        使用事务级 advisory lock（pg_advisory_xact_lock）：随事务提交/回滚自动释放，
        避免会话泄漏锁。
        """
        key = self.lock_key(target, scope_key)
        conn = self._tm._connection()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (key,))

    def try_acquire_exclusive(self, target: str, scope_key: str) -> bool:
        """非阻塞尝试获取排他锁；失败立即返回 False（不等待）。"""
        from data_collect.data_access.errors import LockNotAcquired

        key = self.lock_key(target, scope_key)
        conn = self._tm._connection()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (key,))
            row = cur.fetchone()
        acquired = bool(row and row[0])
        if not acquired:
            raise LockNotAcquired(f"同类任务正在运行，未取得排他锁: {target}/{scope_key}")
        return True

    @contextmanager
    def session_exclusive(self, target: str, scope_key: str):
        """Hold a session-scoped lock across a multi-step rebuild."""
        from data_collect.data_access.errors import LockNotAcquired

        if self._tm.in_atomic:
            raise RuntimeError(
                "session_exclusive cannot run inside an atomic workflow"
            )

        key = self.lock_key(target, scope_key)
        conn = self._tm._connection()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
            row = cur.fetchone()
        if not row or not row[0]:
            raise LockNotAcquired(f"session lock unavailable: {target}/{scope_key}")
        conn.commit()
        try:
            yield
        finally:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (key,))
                conn.commit()
            except Exception:
                self._tm.rollback()

    @contextmanager
    def session_exclusive_many(self, target: str, scope_keys: Iterable[str]):
        """Hold several deterministic session locks during one bounded batch."""
        from data_collect.data_access.errors import LockNotAcquired

        if self._tm.in_atomic:
            raise RuntimeError(
                "session_exclusive_many cannot run inside an atomic workflow"
            )
        normalized = tuple(sorted({str(value) for value in scope_keys if str(value).strip()}))
        if not normalized:
            raise ValueError("session_exclusive_many requires at least one scope key")

        conn = self._tm._connection()
        acquired: list[int] = []
        try:
            with conn.cursor() as cur:
                for scope_key in normalized:
                    key = self.lock_key(target, scope_key)
                    cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
                    row = cur.fetchone()
                    if not row or not row[0]:
                        raise LockNotAcquired(
                            f"同类任务正在运行，未取得排他锁: {target}/{scope_key}"
                        )
                    acquired.append(key)
            conn.commit()
            yield
        finally:
            if not acquired:
                return
            try:
                with conn.cursor() as cur:
                    for key in reversed(acquired):
                        cur.execute("SELECT pg_advisory_unlock(%s)", (key,))
                conn.commit()
            except Exception:
                self._tm.rollback()
