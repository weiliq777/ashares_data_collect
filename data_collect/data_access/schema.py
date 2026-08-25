"""连接工厂与表结构读取（迁移自 utils/db.py 的 get_connection/get_table_schema）。

connect_with_safety_limits 是 Kernel 内部私有的连接入口（旧 get_connection 的职责收敛到这里），
业务层 / Repository 层都不得调用——只有 TransactionManager 使用它。
"""
from __future__ import annotations

import importlib
import os
from typing import List, Tuple


def require_psycopg2():
    """惰性导入 psycopg2（Kernel 内部使用）。"""
    try:
        return importlib.import_module("psycopg2")
    except Exception as exc:  # pragma: no cover
        raise ImportError("缺少 psycopg2-binary，请先安装：pip install psycopg2-binary") from exc


def connect_with_safety_limits():
    """创建带安全超时的 PostgreSQL 连接（Kernel 私有入口）。

    应用 statement_timeout / lock_timeout / idle_in_transaction_session_timeout，
    使任何新连接都不会无限跑单条 SQL、也不会滞留空闲事务。
    """
    psycopg2 = require_psycopg2()
    from data_collect.config import get_db_config

    config = get_db_config()
    config.setdefault("connect_timeout", 10)
    config.setdefault("application_name", "ashares_data_collect")
    conn = psycopg2.connect(**config)
    statement_timeout_ms = int(os.getenv("ASHARES_DB_STATEMENT_TIMEOUT_MS", "30000"))
    lock_timeout_ms = int(os.getenv("ASHARES_DB_LOCK_TIMEOUT_MS", "5000"))
    idle_timeout_ms = int(os.getenv("ASHARES_DB_IDLE_IN_TX_TIMEOUT_MS", "60000"))
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = %s", (statement_timeout_ms,))
        cur.execute("SET lock_timeout = %s", (lock_timeout_ms,))
        cur.execute("SET idle_in_transaction_session_timeout = %s", (idle_timeout_ms,))
    conn.commit()
    return conn


def split_qualified_name(name: str) -> tuple[str, str]:
    """把 'schema.table' 拆成 (schema, table)；无 schema 时默认 public。"""
    if "." in name:
        schema, table = name.split(".", 1)
        return schema, table
    return "public", name


_schema_cache: dict = {}


def get_table_schema(conn, table_name: str) -> List[Tuple[str, str, int | None]]:
    """读取表字段及类型，按定义顺序返回（支持 schema.table）。"""
    schema, table = split_qualified_name(table_name)
    sql = """
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (schema, table))
        rows = cursor.fetchall()
    return [(row[0], row[1], row[2]) for row in rows]


def get_table_columns(conn, table_name: str) -> List[str]:
    """读取表字段名，按定义顺序返回。"""
    return [name for name, _, _ in get_table_schema(conn, table_name)]


def _get_cached_schema(table_name: str) -> List[Tuple[str, str, int | None]]:
    """进程内缓存表 schema（同一子进程内不变，避免 backfill 循环重复查询）。"""
    from data_collect.data_access.transaction_manager import TransactionManager

    if table_name not in _schema_cache:
        tm = TransactionManager()
        try:
            _schema_cache[table_name] = get_table_schema(tm._connection(), table_name)
        finally:
            tm.close()
    return _schema_cache[table_name]
