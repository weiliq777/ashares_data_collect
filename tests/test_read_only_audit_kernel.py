from __future__ import annotations

from datetime import date

import pytest

from data_collect.data_access import DataAccessKernel, GuardViolation, schema
from data_collect.data_access.context import Operation, SqlPlan, WorkScope
from data_collect.data_access.dataset_policy import (
    DatasetPolicy,
    DatasetPolicyRegistry,
    ScanPolicy,
    WriteMode,
)


class _Cursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, params=None):
        self.connection.executed.append((sql, params))

    def fetchone(self):
        return self.connection.identity


class _Connection:
    def __init__(self, identity):
        self.identity = identity
        self.session_options = None
        self.closed = False
        self.commits = 0
        self.executed = []

    def set_session(self, **kwargs):
        self.session_options = kwargs

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


def _registry():
    registry = DatasetPolicyRegistry()
    registry.register(DatasetPolicy(
        name="audit.source",
        write_modes=frozenset({WriteMode.UPSERT}),
        scan_policy=ScanPolicy.SCOPE_REQUIRED,
    ))
    return registry


def test_read_only_audit_connection_verifies_app_role_and_read_only_transaction(monkeypatch):
    connection = _Connection(("ashares", "ashares_app", "on", False))

    class Psycopg:
        @staticmethod
        def connect(**_config):
            return connection

    monkeypatch.setattr(schema, "require_psycopg2", lambda: Psycopg)
    monkeypatch.setattr(
        "data_collect.config.get_db_config",
        lambda: {"database": "ashares", "user": "ashares_app"},
    )

    result = schema.connect_read_only_audit_with_safety_limits()

    assert result is connection
    assert connection.session_options == {"readonly": True, "autocommit": False}
    assert connection.closed is False
    assert connection.commits == 1
    assert [sql for sql, _ in connection.executed[:3]] == [
        "SET statement_timeout = %s",
        "SET lock_timeout = %s",
        "SET idle_in_transaction_session_timeout = %s",
    ]


def test_read_only_audit_connection_closes_and_rejects_superuser(monkeypatch):
    connection = _Connection(("ashares", "ashares_app", "on", True))

    class Psycopg:
        @staticmethod
        def connect(**_config):
            return connection

    monkeypatch.setattr(schema, "require_psycopg2", lambda: Psycopg)
    monkeypatch.setattr(
        "data_collect.config.get_db_config",
        lambda: {"database": "ashares", "user": "ashares_app"},
    )

    with pytest.raises(RuntimeError, match="database identity rejected"):
        schema.connect_read_only_audit_with_safety_limits()

    assert connection.closed is True


def test_read_only_audit_connection_rejects_non_production_config_before_connect(monkeypatch):
    connect_called = False

    class Psycopg:
        @staticmethod
        def connect(**_config):
            nonlocal connect_called
            connect_called = True
            raise AssertionError("must fail before opening a connection")

    monkeypatch.setattr(schema, "require_psycopg2", lambda: Psycopg)
    monkeypatch.setattr(
        "data_collect.config.get_db_config",
        lambda: {"database": "ashares_test", "user": "ashares_app"},
    )

    with pytest.raises(RuntimeError, match="database=ashares"):
        schema.connect_read_only_audit_with_safety_limits()

    assert connect_called is False


def test_read_only_audit_kernel_rejects_all_non_read_operations():
    kernel = DataAccessKernel(policy_registry=_registry(), read_only_audit=True)
    try:
        assert kernel._manifest_enabled is False
        with pytest.raises(GuardViolation, match="read-only audit kernel"):
            kernel.query_guard.check(SqlPlan(
                operation=Operation.UPSERT,
                target="audit.source",
                scope=WorkScope(date_from=date(2026, 1, 1)),
            ))

        kernel.query_guard.check(SqlPlan(
            operation=Operation.READ,
            target="audit.source",
            scope=WorkScope(date_from=date(2026, 1, 1)),
        ))
    finally:
        kernel.close()
