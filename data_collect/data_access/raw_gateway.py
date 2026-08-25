"""RawAppendGateway：Raw 不可变追加（§4.1）。

版本判断必须批量 SET-based，禁止逐条 SELECT（§4.1 的 > 注）。
"""
from __future__ import annotations

import hashlib
from typing import Any, Mapping, Sequence
from uuid import uuid4

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.query_guard import validate_qualified_name, validate_table_name
from data_collect.data_access.bulk_loader import (
    copy_or_execute_values,
    create_projection_staging,
)


class RawAppendGateway:
    """Raw 只追加：同 payload_hash ignore，异 hash INSERT 新版本。"""

    # Append-only Raw tables carry multi-million-row unique indexes.  Keep
    # their conflict probes short enough for the 30-second transaction fuse;
    # the global ChunkPlanner budget remains the hard upper bound.
    _INDEXED_RAW_CHUNK_ROWS = 1_000

    def __init__(self, kernel) -> None:
        self._kernel = kernel

    def append(
        self,
        context: RunContext,
        dataset: str,
        rows: Sequence[Mapping[str, Any]],
        business_key: Sequence[str],
        payload_hash_column: str = "payload_hash",
    ) -> int:
        if not isinstance(context, RunContext):
            raise TypeError("append 必须携带 RunContext")
        if not rows:
            return 0
        business_key = tuple(business_key)
        if not business_key:
            raise ValueError("business_key 不能为空")

        target = validate_qualified_name(dataset)
        columns = self._columns(rows)
        business_key = tuple(validate_table_name(col) for col in business_key)
        payload_hash_column = validate_table_name(payload_hash_column)
        for col in (*business_key, payload_hash_column):
            if col not in columns:
                raise ValueError(f"business_key/payload_hash_column 不在数据列中: {col}")

        batch_hash = self._batch_identity(rows, (*business_key, payload_hash_column))
        append_scope = WorkScope(
            partition=f"raw:{target}",
            dimensions={"batch_hash": batch_hash},
        )
        plan = SqlPlan(
            operation=Operation.APPEND,
            target=target,
            scope=append_scope,
            expected_mode="ROW_BASED",
            estimated_rows=len(rows),
            allow_insert_select=True,
        )
        self._kernel.query_guard.check(plan)

        chunks = self._kernel.chunk_planner.plan(
            list(rows), max_rows_per_chunk=self._INDEXED_RAW_CHUNK_ROWS
        )
        scope_key = self._kernel._checkpoint_batch_scope_key(
            "append", target, append_scope, rows
        )
        resume_after, done = self._kernel._checkpoint_cursor(context, scope_key)
        if done:
            return 0
        written = 0
        for idx, chunk in enumerate(chunks, start=1):
            if idx <= resume_after:
                continue
            self._kernel._reserve_atomic_chunk(chunk)
            n = self._kernel._run_chunk(
                lambda: self._append_chunk_set_based(
                    target, columns, chunk, business_key, payload_hash_column
                )
            )
            written += n
            self._kernel._checkpoint_progress(context.run_id, context.job_name, scope_key, idx)
        self._kernel._checkpoint_done(context.run_id, context.job_name, scope_key)
        return written

    def append_payload_rows(
        self,
        context: RunContext,
        target: str,
        rows: Sequence[Mapping[str, Any]],
        business_key: Sequence[str],
        scope: WorkScope,
    ) -> int:
        """Append legacy payload rows without exposing a generic SQL executor."""
        if target not in {
            "tushare_fina_indicator_raw", "tushare_income_raw",
            "tushare_balancesheet_raw", "tushare_cashflow_raw",
            "tushare_daily_raw", "tushare_daily_basic_raw",
            "tushare_adj_factor_raw", "tushare_trade_cal_raw",
            "tushare_stock_st_raw", "tushare_namechange_raw",
            "tushare_suspend_d_raw", "tushare_stk_limit_raw",
            "tushare_dividend_raw", "tushare_index_daily_raw",
        }:
            raise ValueError(f"unsupported payload raw target: {target}")
        if not rows:
            return 0
        columns = self._columns(rows)
        key = tuple(business_key)
        if not key or any(column not in columns for column in key):
            raise ValueError("payload raw business key is invalid")
        target = validate_qualified_name(target)
        plan = SqlPlan(
            operation=Operation.APPEND, target=target, scope=scope,
            expected_mode="ROW_BASED", estimated_rows=len(rows),
        )
        self._kernel.query_guard.check(plan)
        written = 0
        scope_key = self._kernel._checkpoint_batch_scope_key(
            "append-payload", target, scope, rows
        )
        resume_after, done = self._kernel._checkpoint_cursor(context, scope_key)
        if done:
            return 0
        for index, chunk in enumerate(
            self._kernel.chunk_planner.plan(
                list(rows), max_rows_per_chunk=self._INDEXED_RAW_CHUNK_ROWS
            ),
            start=1,
        ):
            if index <= resume_after:
                continue
            self._kernel._reserve_atomic_chunk(chunk)
            written += self._kernel._run_chunk(
                lambda: self._append_payload_chunk(target, columns, chunk)
            )
            self._kernel._checkpoint_progress(context.run_id, context.job_name, scope_key, index)
        self._kernel._checkpoint_done(context.run_id, context.job_name, scope_key)
        return written

    def recover_from_corrupt_table(
        self,
        context: RunContext,
        target: str,
        source: str,
        start_date,
        end_date,
        codes: Sequence[str] | None = None,
        scope: WorkScope | None = None,
    ) -> int:
        """Copy readable rows from an allowlisted quarantine table into Raw.

        This is deliberately a recovery-specific operation, not a public SQL
        runner: both relations and the five-column projection are fixed by the
        recovery contract, and the operation remains append-only.
        """
        targets = {"tushare_daily_raw", "tushare_daily_basic_raw"}
        sources = {
            '"public"."tushare_daily_raw_corrupt_20260811"',
            '"public"."tushare_daily_basic_raw_corrupt_20260811"',
        }
        if target not in targets or source not in sources:
            raise ValueError("unsupported corrupt Raw recovery relation")
        if start_date > end_date:
            raise ValueError("recovery date range is invalid")
        target_name = validate_qualified_name(target)
        scope = scope or WorkScope(date_from=start_date, date_to=end_date, partition=f"recovery:{target}")
        plan = SqlPlan(
            operation=Operation.APPEND, target=target_name, scope=scope,
            expected_mode="SET_BASED", estimated_rows=0, allow_insert_select=True,
        )
        self._kernel.query_guard.check(plan)
        sql = f"""
            INSERT INTO {target_name} (ts_code, trade_date, payload, source, fetched_at)
            SELECT ts_code, trade_date, payload, source, fetched_at
              FROM {source}
             WHERE trade_date BETWEEN %s AND %s
               {"AND ts_code = ANY(%s)" if codes is not None else ""}
            ON CONFLICT DO NOTHING
        """
        params = [start_date, end_date, *([codes] if codes is not None else [])]
        def run_chunk() -> int:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return int(cur.rowcount or 0)
        return self._kernel._run_chunk(run_chunk)

    # ---- 内部实现 ----

    @staticmethod
    def _columns(rows: Sequence[Mapping[str, Any]]) -> list[str]:
        first = rows[0]
        cols = [validate_table_name(column) for column in first.keys()]
        for row in rows:
            if list(row.keys()) != cols:
                raise ValueError("append 行之间列不一致")
        return cols

    @staticmethod
    def _batch_identity(
        rows: Sequence[Mapping[str, Any]], identity_columns: Sequence[str]
    ) -> str:
        """Fingerprint one logical Raw batch for independent checkpoints."""
        digest = hashlib.sha256()
        for row in rows:
            for column in identity_columns:
                value = repr(row[column]).encode("utf-8")
                digest.update(len(value).to_bytes(8, "big"))
                digest.update(value)
        return digest.hexdigest()

    def _append_chunk_set_based(
        self,
        target: str,
        columns: list[str],
        chunk: Sequence[Mapping[str, Any]],
        business_key: tuple[str, ...],
        payload_hash_column: str,
    ) -> int:
        """批量写入 staging，再【一次】 SET-based 版本判断，绝不逐条 SELECT。"""
        conn = self._kernel._db_connection()
        staging = f"_staging_raw_append_{uuid4().hex[:12]}"
        col_list = ", ".join(columns)
        bkey_eq = " AND ".join(f"d.{c} = s.{c}" for c in business_key)

        with conn.cursor() as cur:
            create_projection_staging(cur, staging, target, columns)
            # COPY into staging, then one set-based version judgement.
            copy_or_execute_values(
                cur, staging, columns,
                [[row[c] for c in columns] for row in chunk],
            )
            # 一次 SET-based 版本判断：相同 hash 忽略，异 hash 插入新版本
            cur.execute(
                f"""
                INSERT INTO {target} ({col_list})
                SELECT {", ".join(f"s.{c}" for c in columns)}
                  FROM {staging} s
                 WHERE NOT EXISTS (
                       SELECT 1 FROM {target} d
                        WHERE {bkey_eq}
                          AND d.{payload_hash_column} = s.{payload_hash_column}
                 )
                """
            )
            return int(cur.rowcount or 0)

    def _append_payload_chunk(self, target: str, columns: list[str], chunk) -> int:
        col_list = ", ".join(columns)
        staging = f"_staging_raw_payload_{uuid4().hex[:12]}"
        conn = self._kernel._db_connection()
        with conn.cursor() as cur:
            create_projection_staging(cur, staging, target, columns)
            copy_or_execute_values(
                cur, staging, columns,
                [[row[column] for column in columns] for row in chunk],
            )
            cur.execute(
                f"INSERT INTO {target} ({col_list}) "
                f"SELECT {col_list} FROM {staging} ON CONFLICT DO NOTHING"
            )
            return int(cur.rowcount or 0)
