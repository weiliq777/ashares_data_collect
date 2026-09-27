"""Fake PostgreSQL connection/cursor（单元测试用，不连真实 DB）。

记录所有执行过的 SQL（sql + params + kind），用于行为验证：
- N+1 检测（版本判断是否只发一次 SET-based SQL）
- chunk 切分（executemany 被调用几次、每批多少行）
- commit/rollback 次数（事务边界）
"""
from __future__ import annotations

import re
from types import SimpleNamespace


class FakeCursor:
    def __init__(self, conn: "FakeConnection", name=None, withhold=False):
        self.conn = conn
        self.name = name
        self.withhold = withhold
        self.rowcount = 0
        self.itersize = None
        self._fetch_rows = list(conn.stream_rows) if name is not None else []
        self.description = []

    def execute(self, sql, params=None):
        if sql.startswith("SET LOCAL statement_timeout"):
            self.conn.internal_executed.append(
                {"sql": sql, "params": params, "kind": "execute"}
            )
            return
        self.conn.executed.append({"sql": sql, "params": params, "kind": "execute"})
        staged_rowcount = self.rowcount
        self.rowcount = 0
        if sql.lstrip().upper().startswith("EXPLAIN ("):
            self._fetch_rows = [([{"Plan": {"Node Type": "Index Scan"}}],)]
        elif "pg_try_advisory_lock" in sql or "pg_advisory_unlock" in sql:
            self._fetch_rows = [(True,)]
        elif sql.lstrip().upper().startswith("SELECT TO_REGCLASS"):
            relation = params[0]
            self._fetch_rows = [(relation if relation in self.conn.temp_relations else None,)]
        elif sql.lstrip().upper().startswith("CREATE TEMP TABLE"):
            match = re.search(r"CREATE\s+TEMP\s+TABLE\s+([^\s(]+)", sql, re.IGNORECASE)
            if match:
                self.conn.temp_relations.add(match.group(1))
        elif sql.lstrip().upper().startswith("DROP TABLE"):
            match = re.search(
                r"DROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+([^\s;]+)", sql, re.IGNORECASE
            )
            if match:
                self.conn.temp_relations.discard(match.group(1))
        elif "pit_violation_count" in sql:
            self._fetch_rows = [(1, 0, 0, None, 0)]
            self.description = [SimpleNamespace(name="row_count")]
        elif sql.lstrip().upper().startswith("SELECT COUNT(*)"):
            self._fetch_rows = [(0 if "duplicates" in sql or " WHERE " in sql else 1,)]
            self.description = [SimpleNamespace(name="row_count")]
        elif sql.lstrip().upper().startswith("INSERT INTO") and " FROM _staging_" in sql:
            # PostgreSQL reports rows affected by the final INSERT ... SELECT,
            # not by the preceding COPY.  Simulate that contract for gateway tests.
            self.rowcount = staged_rowcount
        elif (
            sql.lstrip().upper().startswith("WITH")
            and "INSERT INTO research.snapshot_lineage_bundle" in sql
            and "FROM research.snapshot_lineage" in sql
            and params
            and isinstance(params[-1], (list, tuple))
        ):
            # The legacy lineage migration validates the final bundle count
            # before Kernel commits.  Mirror PostgreSQL's final INSERT count.
            self.rowcount = len(params[-1])

    def executemany(self, sql, params_list):
        params_list = list(params_list)
        self.conn.executed.append(
            {"sql": sql, "params": params_list, "kind": "executemany", "rowcount": len(params_list)}
        )
        self.rowcount = len(params_list)

    def fetchall(self):
        return list(self._fetch_rows)

    def fetchone(self):
        return self._fetch_rows[0] if self._fetch_rows else None

    def fetchmany(self, size):
        self.conn.fetchmany_calls.append(size)
        rows = list(self._fetch_rows[:size])
        del self._fetch_rows[:size]
        return rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeConnection:
    def __init__(self, stream_rows=None):
        self.executed: list[dict] = []
        self.internal_executed: list[dict] = []
        self.cursor_names: list[str | None] = []
        self.stream_rows = list(stream_rows or [])
        self.fetchmany_calls: list[int] = []
        self.temp_relations: set[str] = set()
        self.supports_statement_deadline = True
        self.committed = 0
        self.rolled_back = 0
        self.closed = False

    def cursor(self, name=None, withhold=False):
        self.cursor_names.append(name)
        return FakeCursor(self, name, withhold)

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1

    def close(self):
        self.closed = True
