"""ReadGateway：受控读取（§4.4 / §4.5）。

调用方不能传任意 SQL；where 用 dict 表达（列 + lookup），SQL 由框架生成。
大结果集用 stream()（server-side cursor），全系统禁止 fetchall() 读超大表。
"""
from __future__ import annotations

import re
import uuid
from typing import Any, Iterable, Mapping, Sequence

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.dataset_policy import ScanPolicy
from data_collect.data_access.query_guard import validate_qualified_name

_LOOKUP_OPS = {
    "": "=",
    "eq": "=",
    "ne": "<>",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
    "in": "IN",
}


class ReadGateway:
    def __init__(self, kernel) -> None:
        self._kernel = kernel

    def query(
        self,
        context: RunContext,
        source: str,
        columns: Sequence[str],
        where: Mapping[str, Any] | None = None,
        limit: int | None = None,
        scope: WorkScope | None = None,
    ) -> list[tuple]:
        if not isinstance(context, RunContext):
            raise TypeError("read.query 必须携带 RunContext")
        target = validate_qualified_name(source)
        columns = tuple(columns)
        if not columns:
            raise ValueError("columns 不能为空")
        for col in columns:
            if not col.isidentifier():
                raise ValueError(f"非法列名: {col!r}")
        scope = scope or self._derive_scope(where or {}, source)
        policy = self._kernel.policy_registry.require(target)
        self._check_public_read(target, policy)
        if limit is None:
            limit = policy.max_read_rows
        elif limit > policy.max_read_rows:
            raise ValueError(
                f"read.query 超过 {target} 的 max_read_rows={policy.max_read_rows}，请改用 stream()"
            )
        plan = SqlPlan(operation=Operation.READ, target=target, scope=scope)
        self._kernel.query_guard.check(plan)
        if policy.scan_policy == ScanPolicy.SCOPE_REQUIRED and not (where or {}):
            raise ValueError("scope-required read.query must include bounded where predicates")

        sql, params = self._build_select(target, columns, where or {}, limit)
        def read_once() -> list[tuple]:
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())

        return self._kernel._run_chunk(read_once)

    def stream(
        self,
        context: RunContext,
        source: str,
        columns: Sequence[str],
        where: Mapping[str, Any] | None = None,
        batch_rows: int = 50_000,
        scope: WorkScope | None = None,
    ) -> Iterable[list[tuple]]:
        """大结果集流式读取：server-side cursor，每批 batch_rows 行。"""
        if not isinstance(context, RunContext):
            raise TypeError("read.stream 必须携带 RunContext")
        if batch_rows <= 0:
            raise ValueError("batch_rows 必须 > 0")
        target = validate_qualified_name(source)
        columns = tuple(columns)
        if not columns:
            raise ValueError("columns 不能为空")
        for col in columns:
            if not col.isidentifier():
                raise ValueError(f"非法列名: {col!r}")
        if batch_rows > self._kernel.chunk_planner.config.max_rows_per_tx:
            raise ValueError("batch_rows exceeds the governed row budget")
        scope = scope or self._derive_scope(where or {}, source)
        policy = self._kernel.policy_registry.require(target)
        self._check_public_read(target, policy)
        plan = SqlPlan(operation=Operation.READ, target=target, scope=scope)
        self._kernel.query_guard.check(plan)
        if policy.scan_policy == ScanPolicy.SCOPE_REQUIRED and not (where or {}):
            raise ValueError("scope-required read.stream must include bounded where predicates")

        sql, params = self._build_select(target, columns, where or {}, None)
        def stream_batches():
            manage_transaction = not self._kernel.transaction_manager.in_atomic
            if manage_transaction:
                self._kernel.transaction_manager.begin_chunk()
            try:
                conn = self._kernel._db_connection()
                with conn.cursor(name=f"read_stream_{uuid.uuid4().hex}") as cur:
                    cur.itersize = batch_rows
                    cur.execute(sql, params)
                    while True:
                        rows = cur.fetchmany(batch_rows)
                        if not rows:
                            break
                        yield list(rows)
                if manage_transaction:
                    self._kernel.transaction_manager.commit_chunk()
            except BaseException:
                if manage_transaction:
                    self._kernel.transaction_manager.rollback()
                raise

        yield from stream_batches()

    def scalar(
        self,
        context: RunContext,
        source: str,
        aggregate: str,
        where: Mapping[str, Any] | None = None,
        scope: WorkScope | None = None,
    ):
        """受控聚合/标量读取（仅 Repository 层使用，业务层用 query/stream）。

        aggregate 形如 'MIN(cal_date), MAX(cal_date)'；仍受 QueryGuard scan_policy 约束。
        只读，绝不支持写——不是万能 execute()。
        """
        if not isinstance(context, RunContext):
            raise TypeError("read.scalar 必须携带 RunContext")
        if not isinstance(aggregate, str) or not aggregate.strip():
            raise ValueError("aggregate 不能为空")
        if not re.fullmatch(
            r"\s*(?:MIN|MAX|COUNT|SUM|AVG)\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*\)"
            r"(?:\s*,\s*(?:MIN|MAX|COUNT|SUM|AVG)\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*\))*\s*",
            aggregate,
            re.I,
        ):
            raise ValueError("scalar 只允许白名单聚合函数和标识符")
        target = validate_qualified_name(source)
        policy = self._kernel.policy_registry.require(target)
        self._check_public_read(target, policy)
        scope = scope or self._derive_scope(where or {}, source)
        plan = SqlPlan(operation=Operation.READ, target=target, scope=scope)
        self._kernel.query_guard.check(plan)
        if policy.scan_policy == ScanPolicy.SCOPE_REQUIRED and not (where or {}):
            raise ValueError("scope-required read.scalar must include bounded where predicates")

        sql, params = self._build_select(target, [aggregate], where or {}, None)
        def read_once():
            conn = self._kernel._db_connection()
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()

        return self._kernel._run_chunk(read_once)

    def _repository_query(
        self,
        context: RunContext,
        source: str,
        sql: str,
        params: Sequence[Any] = (),
        scope: WorkScope | None = None,
        max_rows: int = 100_000,
    ) -> list[dict[str, Any]]:
        """Repository-only bounded SELECT bridge.

        Public callers must use ``query``/``stream``. Repositories may use this
        private bridge for approved domain SQL that cannot be represented by a
        simple ``where`` mapping; it remains read-only and always goes through
        QueryGuard, a scope and a row ceiling.
        """
        if not isinstance(context, RunContext):
            raise TypeError("repository query 必须携带 RunContext")
        target = validate_qualified_name(source)
        normalized = sql.strip().rstrip(";").strip()
        if ";" in normalized or not re.match(r"^(SELECT|WITH)\b", normalized, re.I):
            raise ValueError("Repository read SQL 只能是单条 SELECT/WITH")
        if re.search(r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE)\b", normalized, re.I):
            raise ValueError("Repository read SQL 含写入或 DDL")
        # A logical unqualified policy name (for example ``daily_kline``) is
        # retained only for terminal migration evidence. Production
        # consumers must use the canonical qualified Standard relation
        # ``std.daily_kline``; this branch cannot broaden access.
        if "." in target:
            parts = target.split(".")
            relation_candidates = (
                target,
                f'{parts[0]}."{parts[1]}"',
                f'"{parts[0]}".{parts[1]}',
                f'"{parts[0]}"."{parts[1]}"',
            )
            relation = "|".join(
                re.escape(candidate)
                for candidate in relation_candidates
            )
        else:
            relation_candidates = (
                rf"(?:public\.)?{re.escape(target)}",
                rf"(?:public\.)?{re.escape(chr(34) + target + chr(34))}",
            )
            relation = "|".join(relation_candidates)
        relation_pattern = re.compile(
            rf"\b(?:FROM|JOIN)\s+(?:{relation})(?=\s|$)", re.I
        )
        if not relation_pattern.search(normalized):
            raise ValueError(f"Repository read SQL 未包含 source={source}")
        if max_rows <= 0:
            raise ValueError("max_rows 必须 > 0")
        policy = self._kernel.policy_registry.require(target)
        if max_rows > policy.max_read_rows:
            raise ValueError(
                f"Repository read max_rows exceeds policy ceiling={policy.max_read_rows}"
            )
        scope = scope or WorkScope()
        plan = SqlPlan(operation=Operation.READ, target=target, scope=scope)
        self._kernel.query_guard.check(plan)
        def read_once() -> list[dict[str, Any]]:
            conn = self._kernel._db_connection()
            with conn.cursor(name=f"repository_query_{uuid.uuid4().hex}") as cur:
                cur.itersize = min(max_rows + 1, 10_000)
                cur.execute(normalized, tuple(params))
                rows = cur.fetchmany(max_rows + 1)
                if len(rows) > max_rows:
                    raise ValueError("Repository read 超过 max_rows，请拆分 scope 或使用专用 stream")
                columns = [item.name for item in cur.description]
                return [dict(zip(columns, row, strict=True)) for row in rows]

        return self._kernel._run_chunk(read_once)

    def _repository_stream(
        self,
        context: RunContext,
        source: str,
        sql: str,
        params: Sequence[Any] = (),
        scope: WorkScope | None = None,
        batch_rows: int = 10_000,
    ) -> Iterable[list[dict[str, Any]]]:
        """Bounded streaming bridge for Repository content hashing/scans.

        This is deliberately private: domain code still cannot submit SQL or
        obtain a cursor.  Repositories use it when a complete result set must
        be consumed without turning one transaction into an unbounded fetch.
        """
        if not isinstance(context, RunContext):
            raise TypeError("repository stream requires RunContext")
        if batch_rows <= 0:
            raise ValueError("batch_rows must be > 0")
        target = validate_qualified_name(source)
        normalized = sql.strip().rstrip(";").strip()
        if ";" in normalized or not re.match(r"^(SELECT|WITH)\b", normalized, re.I):
            raise ValueError("Repository read SQL must be one SELECT/WITH")
        if re.search(r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE)\b", normalized, re.I):
            raise ValueError("Repository read SQL contains write or DDL")
        relation_pattern = re.compile(
            rf"\b(?:FROM|JOIN)\s+{re.escape(target)}\b", re.I
        )
        if not relation_pattern.search(normalized):
            raise ValueError(f"Repository read SQL does not contain source={source}")
        if batch_rows > self._kernel.chunk_planner.config.max_rows_per_tx:
            raise ValueError("Repository stream batch exceeds governed row budget")
        scope = scope or WorkScope()
        plan = SqlPlan(operation=Operation.READ, target=target, scope=scope)
        self._kernel.query_guard.check(plan)
        def stream_batches():
            manage_transaction = not self._kernel.transaction_manager.in_atomic
            if manage_transaction:
                self._kernel.transaction_manager.begin_chunk()
            try:
                conn = self._kernel._db_connection()
                # A holdable server-side cursor lets the Gateway commit each
                # fetched batch while retaining the result set on PostgreSQL.
                # Without ``withhold=True`` a large but row-bounded stream
                # still forms one long transaction and can hit the hard
                # transaction deadline before the final commit.
                with conn.cursor(
                    name=f"repository_stream_{uuid.uuid4().hex}",
                    withhold=manage_transaction,
                ) as cur:
                    cur.itersize = batch_rows
                    cur.execute(normalized, tuple(params))
                    # psycopg2 named cursors may populate ``description``
                    # only after the first FETCH. Preserve that first batch
                    # before deriving the mapping instead of assuming the
                    # metadata is available immediately after execute().
                    rows = cur.fetchmany(batch_rows)
                    if rows and not cur.description:
                        raise RuntimeError("Repository stream returned rows without column metadata")
                    columns = [item.name for item in (cur.description or ())]
                    if manage_transaction:
                        self._kernel.transaction_manager.commit_chunk()
                    while rows:
                        yield [dict(zip(columns, row, strict=True)) for row in rows]
                        if manage_transaction:
                            self._kernel.transaction_manager.begin_chunk()
                        rows = cur.fetchmany(batch_rows)
                        if manage_transaction:
                            self._kernel.transaction_manager.commit_chunk()
            except BaseException:
                if manage_transaction:
                    self._kernel.transaction_manager.rollback()
                raise

        yield from stream_batches()

    # ---- 内部 ----

    @staticmethod
    def _check_public_read(target, policy) -> None:
        """Keep mutable Feature consumption behind Repository contracts."""
        if not policy.direct_read_allowed:
            raise ValueError(
                f"direct ReadGateway access is forbidden for mutable dataset {target}; "
                "use a Repository-backed mart or frozen contract"
            )

    @staticmethod
    def _derive_scope(where: Mapping[str, Any], source: str) -> WorkScope:
        """从 where 推导 WorkScope（日期字段 → date_from/date_to）。"""
        from datetime import date as _date

        date_from: _date | None = None
        date_to: _date | None = None
        partition: str | None = None
        codes: tuple[str, ...] | None = None
        for key, value in where.items():
            if not isinstance(key, str):
                raise ValueError(f"非法 where 列名: {key!r}")
            if "__" in key:
                col, _, op = key.rpartition("__")
            else:
                col, op = key, ""
            if op in ("", "eq"):
                if col in ("partition", "trade_date_partition"):
                    partition = str(value)
                elif col in {"trade_date", "date", "cal_date", "decision_date", "asof_date", "research_date", "valuation_date"}:
                    d = value if isinstance(value, _date) else None
                    if d is not None:
                        date_from = date_to = d
                elif col in {"ts_code", "stock_code", "code"} and value is not None:
                    codes = (str(value),)
            elif op in ("gte", "gt"):
                d = value if isinstance(value, _date) else None
                if d is not None:
                    date_from = d
            elif op in ("lte", "lt"):
                d = value if isinstance(value, _date) else None
                if d is not None:
                    date_to = d
            elif op == "in" and col == "ts_code":
                if isinstance(value, (list, tuple)):
                    codes = tuple(value)
        return WorkScope(date_from=date_from, date_to=date_to, partition=partition, codes=codes)

    @staticmethod
    def _build_select(
        target: str, columns: Sequence[str], where: Mapping[str, Any], limit: int | None
    ) -> tuple[str, tuple]:
        col_sql = ", ".join(columns)
        sql = f"SELECT {col_sql} FROM {target}"
        params: list[Any] = []
        if where:
            clauses = []
            for key, value in where.items():
                if not isinstance(key, str):
                    raise ValueError(f"非法 where 列名: {key!r}")
                if "__" in key:
                    col, _, op = key.rpartition("__")
                else:
                    col, op = key, ""
                if not col.isidentifier():
                    raise ValueError(f"非法 where 列名: {col!r}")
                if op not in _LOOKUP_OPS:
                    raise ValueError(f"不支持的 lookup: {key!r}")
                if op == "in":
                    if not isinstance(value, (list, tuple)):
                        raise ValueError(f"__in 需要 list/tuple: {key!r}")
                    placeholders = ", ".join(["%s"] * len(value))
                    clauses.append(f"{col} IN ({placeholders})")
                    params.extend(value)
                else:
                    clauses.append(f"{col} {_LOOKUP_OPS[op]} %s")
                    params.append(value)
            sql += " WHERE " + " AND ".join(clauses)
        if limit is not None:
            if limit <= 0:
                raise ValueError("limit 必须 > 0")
            sql += f" LIMIT {int(limit)}"
        return sql, tuple(params)
