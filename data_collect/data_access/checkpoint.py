"""Checkpoint：断点续传（§2）。

大任务按 chunk 推进时持久化进度；失败后从断点续跑而非从头。
存储可注入：默认内存实现（测试用），生产用 PostgreSQL 实现（ops.foundation_rebuild_checkpoint）。
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Protocol


@dataclass(frozen=True)
class CheckpointState:
    run_id: str
    job_name: str
    scope_key: str
    cursor: int            # 进度游标（如已处理的 chunk 序号 / 行数）
    done: bool = False


class CheckpointStore(Protocol):
    def load(self, run_id: str, job_name: str, scope_key: str) -> CheckpointState | None: ...
    def save(self, state: CheckpointState) -> None: ...
    def clear(self, run_id: str, job_name: str, scope_key: str) -> None: ...


class InMemoryCheckpointStore:
    """进程内 checkpoint 存储（测试与无 DB 场景）。"""

    def __init__(self) -> None:
        self._data: dict[tuple[str, str, str], CheckpointState] = {}

    def _key(self, run_id: str, job_name: str, scope_key: str) -> tuple[str, str, str]:
        return (run_id, job_name, scope_key)

    def load(self, run_id: str, job_name: str, scope_key: str) -> CheckpointState | None:
        return self._data.get(self._key(run_id, job_name, scope_key))

    def save(self, state: CheckpointState) -> None:
        self._data[self._key(state.run_id, state.job_name, state.scope_key)] = state

    def clear(self, run_id: str, job_name: str, scope_key: str) -> None:
        self._data.pop(self._key(run_id, job_name, scope_key), None)


class PostgresCheckpointStore:
    """持久化 checkpoint store。

    每次读写使用独立短事务，避免 checkpoint 被业务 chunk 回滚；表由
    ``sql/047_data_access_framework.sql`` 创建。连接工厂是 framework-private
    注入点，业务层不会接触 Connection/Cursor。
    """

    def __init__(self, connection_factory=None) -> None:
        self._connection_factory = connection_factory

    def _connect(self):
        if self._connection_factory is not None:
            return self._connection_factory()
        from data_collect.data_access.schema import connect_with_safety_limits
        return connect_with_safety_limits()

    def load(self, run_id: str, job_name: str, scope_key: str) -> CheckpointState | None:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT cursor_position, done
                       FROM ops.data_access_checkpoint
                      WHERE run_id=%s AND job_name=%s AND scope_key=%s""",
                    (run_id, job_name, scope_key),
                )
                row = cur.fetchone()
            return (
                CheckpointState(run_id, job_name, scope_key, int(row[0]), bool(row[1]))
                if row else None
            )
        finally:
            conn.close()

    def save(self, state: CheckpointState) -> None:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO ops.data_access_checkpoint
                         (run_id, job_name, scope_key, cursor_position, done)
                       VALUES (%s,%s,%s,%s,%s)
                       ON CONFLICT (run_id, job_name, scope_key) DO UPDATE SET
                         cursor_position=GREATEST(
                           data_access_checkpoint.cursor_position,
                           EXCLUDED.cursor_position
                         ),
                         done=(data_access_checkpoint.done OR EXCLUDED.done),
                         updated_at=CURRENT_TIMESTAMP""",
                    (state.run_id, state.job_name, state.scope_key, state.cursor, state.done),
                )
            conn.commit()
        finally:
            conn.close()

    def clear(self, run_id: str, job_name: str, scope_key: str) -> None:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM ops.data_access_checkpoint WHERE run_id=%s AND job_name=%s AND scope_key=%s",
                    (run_id, job_name, scope_key),
                )
            conn.commit()
        finally:
            conn.close()


class Checkpoint:
    """断点续传协调器。"""

    def __init__(self, store: CheckpointStore | None = None) -> None:
        self._store = store or InMemoryCheckpointStore()

    def load(self, run_id: str, job_name: str, scope_key: str) -> CheckpointState | None:
        return self._store.load(run_id, job_name, scope_key)

    def save(self, state: CheckpointState) -> None:
        self._store.save(state)

    def mark_progress(self, run_id: str, job_name: str, scope_key: str, cursor: int) -> None:
        prev = self.load(run_id, job_name, scope_key)
        if prev is not None and prev.done:
            return
        if prev is not None and cursor < prev.cursor:
            raise ValueError(
                f"checkpoint cursor cannot move backwards: {cursor} < {prev.cursor}"
            )
        self._store.save(
            CheckpointState(run_id=run_id, job_name=job_name, scope_key=scope_key, cursor=cursor)
        )

    def mark_done(self, run_id: str, job_name: str, scope_key: str) -> None:
        prev = self.load(run_id, job_name, scope_key)
        if prev is not None and prev.done:
            return
        cursor = prev.cursor if prev is not None else 0
        self._store.save(
            CheckpointState(run_id=run_id, job_name=job_name, scope_key=scope_key,
                            cursor=cursor, done=True)
        )

    def clear(self, run_id: str, job_name: str, scope_key: str) -> None:
        self._store.clear(run_id, job_name, scope_key)
