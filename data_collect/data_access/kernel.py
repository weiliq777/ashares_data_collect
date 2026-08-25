"""DataAccessKernel：组合根（§4）。

Kernel 是唯一数据库入口，【不向业务层公开 execute()】，也不暴露 Connection/Cursor。
业务层只能通过 raw/read/write/rebuild/publish/quality 六个 Gateway 访问数据库。
"""
from __future__ import annotations

import hashlib
import functools
import json
from datetime import datetime, timezone
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Callable, TypeVar

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.checkpoint import Checkpoint, CheckpointStore, InMemoryCheckpointStore, PostgresCheckpointStore
from data_collect.data_access.chunk_planner import ChunkConfig, ChunkPlanner
from data_collect.data_access.errors import TransactionBudgetExceeded
from data_collect.data_access.dataset_policy import DatasetPolicyRegistry
from data_collect.data_access.lock_manager import LockManager
from data_collect.data_access.publish_gateway import PublishGateway
from data_collect.data_access.quality_gateway import QualityGateway
from data_collect.data_access.query_guard import QueryGuard
from data_collect.data_access.raw_gateway import RawAppendGateway
from data_collect.data_access.read_gateway import ReadGateway
from data_collect.data_access.rebuild_gateway import RebuildGateway
from data_collect.data_access.retry_policy import RetryPolicy
from data_collect.data_access.transaction_manager import TransactionConfig, TransactionManager
from data_collect.data_access.write_gateway import WriteGateway

T = TypeVar("T")


class DataAccessKernel:
    """统一数据库访问内核。所有数据库行为必须经此进入。"""

    def __init__(
        self,
        policy_registry: DatasetPolicyRegistry | None = None,
        transaction_config: TransactionConfig | None = None,
        chunk_config: ChunkConfig | None = None,
        connection_factory: Callable[[], object] | None = None,
        checkpoint_store: CheckpointStore | None = None,
    ) -> None:
        # A supplied connection factory is the explicit fake/test seam. A
        # custom checkpoint store does not make the database connection fake;
        # production kernels must still record the Run Manifest in that case.
        production_kernel = connection_factory is None
        self.policy_registry = policy_registry or DatasetPolicyRegistry()
        self.query_guard = QueryGuard(self.policy_registry)
        transaction_config = transaction_config or TransactionConfig()
        requested_chunk = chunk_config or ChunkConfig()
        effective_chunk = ChunkConfig(
            max_rows_per_tx=min(
                requested_chunk.max_rows_per_tx,
                transaction_config.max_rows_per_tx,
            ),
            max_bytes_per_tx=min(
                requested_chunk.max_bytes_per_tx,
                transaction_config.max_bytes_per_tx,
            ),
        )
        self.chunk_planner = ChunkPlanner(effective_chunk)
        self.transaction_manager = TransactionManager(
            transaction_config, connection_factory
        )
        self.retry_policy = RetryPolicy()
        self.lock_manager = LockManager(self.transaction_manager)
        if checkpoint_store is None:
            # Tests/fake connections stay isolated; production defaults to a
            # durable store so process restart can resume committed chunks.
            checkpoint_store = (
                InMemoryCheckpointStore() if connection_factory is not None else PostgresCheckpointStore()
            )
        self.checkpoint = Checkpoint(checkpoint_store)
        self._deferred_checkpoint_actions: list[tuple[str, tuple]] | None = None
        self._atomic_reserved_rows = 0
        self._atomic_reserved_bytes = 0
        # The real application uses the framework connection factory. Tests
        # inject a fake connection and should not need the production control
        # table. The flag is deliberately private; business code cannot turn
        # off manifest recording for a real Kernel.
        self._manifest_enabled = production_kernel
        self._manifest_started: set[str] = set()
        self._manifest_failures: dict[str, str] = {}
        self._manifest_rows_read: dict[str, int] = {}
        self._manifest_rows_written: dict[str, int] = {}

        # 六个 Gateway（业务层唯一入口）
        self.raw: RawAppendGateway = RawAppendGateway(self)
        self.read: ReadGateway = ReadGateway(self)
        self.write: WriteGateway = WriteGateway(self)
        self.rebuild: RebuildGateway = RebuildGateway(self)
        self.publish: PublishGateway = PublishGateway(self)
        self.quality: QualityGateway = QualityGateway(self)
        self._install_manifest_hooks()

    def close(self) -> None:
        self._finalize_run_manifests()
        self.transaction_manager.close()

    def _install_manifest_hooks(self) -> None:
        """Instrument typed Gateway entry points without adding a seventh Gateway.

        Repositories and Jobs keep their normal APIs. The wrapper only extracts
        the already-required RunContext/WorkScope and delegates all database
        work to the Kernel's private control-plane writer.
        """
        if not self._manifest_enabled:
            return
        gateway_methods = {
            "raw": None,
            "read": {"_repository_query", "_repository_stream"},
            "write": None,
            "rebuild": None,
            "publish": None,
            "quality": None,
        }
        for gateway_name, private_methods in gateway_methods.items():
            gateway = getattr(self, gateway_name)
            for method_name in dir(gateway):
                if method_name.startswith("__"):
                    continue
                if method_name.startswith("_") and (
                    private_methods is None or method_name not in private_methods
                ):
                    continue
                original = getattr(gateway, method_name, None)
                if not callable(original) or getattr(original, "_manifest_hook", False):
                    continue

                @functools.wraps(original)
                def governed(*args, _original=original, _gateway=gateway_name, **kwargs):
                    context = self._find_argument(args, kwargs, RunContext)
                    scope = self._find_argument(args, kwargs, WorkScope)
                    if context is not None:
                        if scope is None:
                            scope = WorkScope(partition=f"{_gateway}:operation")
                        self._ensure_run_manifest(context, scope)
                    try:
                        result = _original(*args, **kwargs)
                    except Exception as exc:
                        if context is not None:
                            self._manifest_failures[context.run_id] = str(exc)[:4000]
                        raise
                    if context is not None:
                        if _gateway == "read" and isinstance(result, Iterator):
                            return self._track_manifest_iterator(
                                context.run_id, _gateway, result
                            )
                        self._record_manifest_counts(context.run_id, _gateway, result)
                    return result

                governed._manifest_hook = True
                setattr(gateway, method_name, governed)

    @staticmethod
    def _find_argument(args, kwargs, expected_type):
        for value in (*args, *kwargs.values()):
            if isinstance(value, expected_type):
                return value
        return None

    @staticmethod
    def _scope_json(scope: WorkScope) -> str:
        return json.dumps(
            {
                "date_from": scope.date_from.isoformat() if scope.date_from else None,
                "date_to": scope.date_to.isoformat() if scope.date_to else None,
                "codes": list(scope.codes or ()),
                "partition": scope.partition,
                "dimensions": dict(scope.dimensions),
            },
            sort_keys=True,
            default=str,
        )

    def _ensure_run_manifest(self, context: RunContext, scope: WorkScope) -> None:
        if not self._manifest_enabled or context.run_id in self._manifest_started:
            return
        started_at = datetime.now(timezone.utc)

        def insert_manifest() -> None:
            conn = self._db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO ops.run_manifest
                         (run_id, job_name, pipeline_version, code_commit,
                          config_hash, scope, start_time, status)
                       VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,'RUNNING')
                       ON CONFLICT (run_id) DO UPDATE SET
                         job_name=EXCLUDED.job_name,
                         pipeline_version=EXCLUDED.pipeline_version,
                         code_commit=EXCLUDED.code_commit,
                         config_hash=EXCLUDED.config_hash,
                         scope=EXCLUDED.scope,
                         start_time=LEAST(ops.run_manifest.start_time,
                                         EXCLUDED.start_time),
                         status=CASE WHEN ops.run_manifest.status='SUCCEEDED'
                                     THEN ops.run_manifest.status
                                     ELSE 'RUNNING' END,
                         end_time=CASE WHEN ops.run_manifest.status='SUCCEEDED'
                                       THEN ops.run_manifest.end_time ELSE NULL END,
                         error=NULL,
                         updated_at=CURRENT_TIMESTAMP""",
                    (
                        context.run_id, context.job_name, context.pipeline_version,
                        context.code_version, context.config_hash,
                        self._scope_json(scope), started_at,
                    ),
                )

        # This is a control-plane, idempotent write. It still uses the same
        # transaction/deadline/retry machinery as ordinary Gateway work.
        self._run_chunk(insert_manifest)
        self._manifest_started.add(context.run_id)

    def _record_manifest_counts(self, run_id: str, gateway: str, result: Any) -> None:
        """Accumulate bounded return-count telemetry for the run row."""
        if isinstance(result, bool):
            return
        count = None
        if isinstance(result, int):
            count = max(0, result)
        elif isinstance(result, tuple) and result and all(
            isinstance(value, int) for value in result
        ):
            count = max(0, result[-1])
        elif isinstance(result, list):
            count = len(result)
        if count is None:
            return
        if gateway == "read":
            self._manifest_rows_read[run_id] = self._manifest_rows_read.get(run_id, 0) + count
        elif gateway in {"raw", "write", "rebuild", "publish"}:
            self._manifest_rows_written[run_id] = self._manifest_rows_written.get(run_id, 0) + count

    def _track_manifest_iterator(
        self, run_id: str, gateway: str, iterator: Iterator[Any]
    ) -> Iterator[Any]:
        """Record streamed batches and preserve failures raised on iteration.

        ``read.stream`` and the Repository streaming bridge are generator
        functions: their SQL executes only after the caller starts iterating.
        Keeping this wrapper at the Kernel boundary makes the Run Manifest
        describe the actual stream rather than merely the generator creation.
        """
        try:
            for item in iterator:
                self._record_manifest_counts(run_id, gateway, item)
                yield item
        except BaseException as exc:
            self._manifest_failures[run_id] = str(exc)[:4000]
            raise

    def _finalize_run_manifests(self) -> None:
        if not self._manifest_enabled:
            return
        for run_id in tuple(self._manifest_started):
            failure = self._manifest_failures.get(run_id)
            status = "FAILED" if failure else "SUCCEEDED"
            rows_read = self._manifest_rows_read.get(run_id, 0)
            rows_written = self._manifest_rows_written.get(run_id, 0)

            def finish(
                run_id=run_id, status=status, failure=failure,
                rows_read=rows_read, rows_written=rows_written,
            ) -> None:
                conn = self._db_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        """UPDATE ops.run_manifest
                              SET status=%s, end_time=CURRENT_TIMESTAMP,
                                  error=%s, rows_read=%s, rows_written=%s,
                                  updated_at=CURRENT_TIMESTAMP
                            WHERE run_id=%s AND status='RUNNING'""",
                        (status, failure, rows_read, rows_written, run_id),
                    )

            try:
                self._run_chunk(finish, idempotent=False)
            except Exception:
                # Do not hide the original Job exception during shutdown. The
                # next run can still inspect RUNNING and recover/mark FAILED.
                pass

    def _db_connection(self) -> object:
        """Framework-private connection bridge used only by Gateway code."""
        return self.transaction_manager._connection()

    def _run_chunk(self, operation, *, idempotent: bool = True):
        """Framework-private transaction + retry boundary for one idempotent chunk."""
        if self.transaction_manager.in_atomic:
            # A Repository-owned atomic workflow has its own retry boundary.
            # Joining here prevents a child Gateway call from committing only
            # part of a multi-table business operation.
            return operation()

        def attempt():
            self.transaction_manager.begin_chunk()
            try:
                result = operation()
                self.transaction_manager.commit_chunk()
                return result
            except Exception:
                self.transaction_manager.rollback()
                raise

        if not idempotent:
            return attempt()
        return self.retry_policy.run(attempt, rollback=self.transaction_manager.rollback)

    def _checkpoint_progress(
        self, run_id: str, job_name: str, scope_key: str, cursor: int
    ) -> None:
        self._checkpoint_action("progress", run_id, job_name, scope_key, cursor)

    def _checkpoint_done(self, run_id: str, job_name: str, scope_key: str) -> None:
        self._checkpoint_action("done", run_id, job_name, scope_key)

    def _checkpoint_action(self, action: str, *args) -> None:
        """Persist only committed progress.

        Child gateway calls inside ``run_atomic`` have not committed yet, so
        their checkpoint actions are deferred until the outer commit succeeds.
        """
        if self.transaction_manager.in_atomic:
            if self._deferred_checkpoint_actions is None:
                raise RuntimeError("atomic checkpoint buffer is not initialized")
            self._deferred_checkpoint_actions.append((action, args))
            return
        if action == "progress":
            self.checkpoint.mark_progress(*args)
        elif action == "done":
            self.checkpoint.mark_done(*args)
        else:  # pragma: no cover - framework-private invariant
            raise ValueError(f"unknown checkpoint action: {action}")

    def _flush_checkpoint_actions(self, actions: tuple[tuple[str, tuple], ...]) -> None:
        for action, args in actions:
            if action == "progress":
                self.checkpoint.mark_progress(*args)
            else:
                self.checkpoint.mark_done(*args)

    @staticmethod
    def _checkpoint_scope_key(prefix: str, target: str, scope: WorkScope | None = None) -> str:
        if scope is None:
            return f"{prefix}:{target}"
        dimensions = tuple(
            sorted((str(key), repr(value)) for key, value in scope.dimensions.items())
        )
        identity = repr(
            (
                scope.date_from,
                scope.date_to,
                scope.codes,
                scope.partition,
                dimensions,
            )
        )
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
        return f"{prefix}:{target}:{digest}"

    def _checkpoint_batch_scope_key(
        self,
        prefix: str,
        target: str,
        scope: WorkScope,
        items: Sequence[Any],
    ) -> str:
        """Bind progress to one logical input batch and its chunking contract.

        A scope can legitimately be written more than once in a single run.
        Scope-only checkpoints would mark the first call done and silently skip
        later, different rows.  Chunk limits are part of the identity too,
        because a numeric chunk cursor is meaningful only for that plan.
        """
        digest = hashlib.sha256()

        def add(value: Any) -> None:
            payload = f"{type(value).__qualname__}:{value!r}".encode(
                "utf-8", errors="backslashreplace"
            )
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)

        for item in items:
            if isinstance(item, Mapping):
                digest.update(b"mapping")
                for key in sorted(item, key=lambda value: str(value)):
                    add(key)
                    add(item[key])
            else:
                add(item)
        config = self.chunk_planner.config
        add(config.max_rows_per_tx)
        add(config.max_bytes_per_tx)
        base = self._checkpoint_scope_key(prefix, target, scope)
        return f"{base}:batch:{digest.hexdigest()[:24]}"

    def _checkpoint_cursor(
        self, context: RunContext, scope_key: str
    ) -> tuple[int, bool]:
        state = self.checkpoint.load(
            context.run_id, context.job_name, scope_key
        )
        return (state.cursor, state.done) if state is not None else (0, False)

    def _reserve_atomic_chunk(self, rows) -> None:
        """Prevent child Gateway chunks from accumulating into one huge tx."""
        if not self.transaction_manager.in_atomic:
            return
        row_count = len(rows)
        byte_count = sum(self.chunk_planner.estimate_row_bytes(row) for row in rows)
        next_rows = self._atomic_reserved_rows + row_count
        next_bytes = self._atomic_reserved_bytes + byte_count
        config = self.transaction_manager.config
        if next_rows > config.max_rows_per_tx or next_bytes > config.max_bytes_per_tx:
            raise TransactionBudgetExceeded(
                "run_atomic child writes exceed one transaction budget: "
                f"rows={next_rows}/{config.max_rows_per_tx}, "
                f"bytes={next_bytes}/{config.max_bytes_per_tx}"
            )
        self._atomic_reserved_rows = next_rows
        self._atomic_reserved_bytes = next_bytes

    def run_atomic(
        self,
        context: RunContext,
        scope: WorkScope,
        operation: Callable[[], T],
        *,
        estimated_rows: int = 0,
        idempotent: bool = False,
    ) -> T:
        """Run one short Repository workflow as one governed transaction.

        This is a workflow boundary, not a SQL escape hatch: ``operation``
        may call typed Repository/Gateway methods but cannot receive a
        connection or cursor. Large row workloads must remain in normal
        Gateway chunks so the transaction budget is not bypassed.
        """
        if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
            raise TypeError("run_atomic requires RunContext and WorkScope")
        if not callable(operation):
            raise TypeError("run_atomic requires a callable workflow")
        if estimated_rows < 0 or estimated_rows > self.transaction_manager.config.max_rows_per_tx:
            raise ValueError("run_atomic is limited to one bounded short workflow")
        self._ensure_run_manifest(context, scope)

        def attempt() -> tuple[T, tuple[tuple[str, tuple], ...]]:
            self._deferred_checkpoint_actions = []
            self._atomic_reserved_rows = 0
            self._atomic_reserved_bytes = 0
            self.transaction_manager.begin_atomic()
            try:
                result = operation()
                actions = tuple(self._deferred_checkpoint_actions)
                self.transaction_manager.commit_chunk()
                return result, actions
            except Exception:
                self._manifest_failures[context.run_id] = "atomic workflow failed"
                self.transaction_manager.rollback()
                raise
            finally:
                self._deferred_checkpoint_actions = None
                self._atomic_reserved_rows = 0
                self._atomic_reserved_bytes = 0

        if idempotent:
            result, actions = self.retry_policy.run(
                attempt, rollback=self.transaction_manager.rollback
            )
        else:
            result, actions = attempt()
        # Checkpoint persistence is intentionally outside the database retry
        # boundary: retrying after the business commit could replay the whole
        # atomic workflow.  A missing checkpoint is safe and idempotent to
        # replay; a checkpoint for rolled-back data is not.
        self._flush_checkpoint_actions(actions)
        return result

    def __enter__(self) -> "DataAccessKernel":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
