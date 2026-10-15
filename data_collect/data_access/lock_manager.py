"""LockManager：单并发排他锁（§2）。

同表同区间禁止并发写。底层用 PostgreSQL advisory lock；
key 由 scope/target 派生，保证同一范围只有一个任务在写。
"""
from __future__ import annotations

import hashlib
import logging
import time
from contextlib import contextmanager
from collections.abc import Iterable

from data_collect.data_access.transaction_manager import TransactionManager

logger = logging.getLogger(__name__)


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

    def _discard_connection_after_unlock_failure(self, target: str) -> None:
        """Close the session that may still own an advisory lock."""
        logger.warning(
            "failed to release session advisory lock for %s; closing its connection",
            target,
            exc_info=True,
        )
        try:
            self._tm.close()
        except Exception:
            # Keep cleanup failures visible without masking an exception from
            # the protected operation or leaving its original traceback lost.
            logger.exception(
                "failed to close connection after advisory unlock failure for %s",
                target,
            )

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
    def session_exclusive(
        self, target: str, scope_key: str, *, wait: bool = False,
    ):
        """Hold a session lock across a multi-step operation.

        By default, fail immediately when another session owns the lock. Use
        ``wait=True`` for lifecycle operations that must serialize in order.
        """
        from data_collect.data_access.errors import LockNotAcquired

        if self._tm.in_atomic:
            raise RuntimeError(
                "session_exclusive cannot run inside an atomic workflow"
            )

        key = self.lock_key(target, scope_key)
        conn = self._tm._connection()
        if wait:
            # Poll with try-lock rather than a blocking SQL lock call. The
            # connection factory applies a short lock_timeout to every new
            # session; polling preserves that safety budget while allowing a
            # lifecycle operation to wait for a long validation/build.
            delay = 0.1
            acquired = False
            while not acquired:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
                    row = cur.fetchone()
                acquired = bool(row and row[0])
                if not acquired:
                    # End the failed try-lock transaction before sleeping.
                    conn.commit()
                    time.sleep(delay)
                    delay = min(delay * 2, 1.0)
        else:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
                row = cur.fetchone()
            acquired = bool(row and row[0])
        if not acquired:
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
                self._discard_connection_after_unlock_failure(target)

    @contextmanager
    def session_exclusive_many(
        self, target: str, scope_keys: Iterable[str], *, wait: bool = False,
    ):
        """Hold several deterministic session locks in stable key order."""
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
            for scope_key in normalized:
                key = self.lock_key(target, scope_key)
                delay = 0.1
                while True:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
                        row = cur.fetchone()
                    if row and row[0]:
                        acquired.append(key)
                        break
                    if not wait:
                        raise LockNotAcquired(
                            f"同类任务正在运行，未取得排他锁: {target}/{scope_key}"
                        )
                    conn.commit()
                    time.sleep(delay)
                    delay = min(delay * 2, 1.0)
            conn.commit()
            yield
        finally:
            if acquired:
                try:
                    with conn.cursor() as cur:
                        for key in reversed(acquired):
                            cur.execute("SELECT pg_advisory_unlock(%s)", (key,))
                    conn.commit()
                except Exception:
                    self._discard_connection_after_unlock_failure(target)
            else:
                # A failed first acquisition must leave the shared kernel
                # connection out of any open/aborted transaction.
                self._tm.rollback()
