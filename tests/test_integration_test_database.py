"""Smoke checks against the isolated Docker PostgreSQL test database.

Run only with ``tools/integration_test_db.ps1 -Action test``. The fixture
requires the fail-closed connection profile. The frozen-trigger check creates
temporary rows inside a transaction and always rolls the transaction back, so
the suite leaves no persistent business rows or schema changes.
"""
from __future__ import annotations

import hashlib
import os
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest

from data_collect.data_access import DataAccessKernel
from data_collect.jobs.tushare_release import _quality_gate

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session", autouse=True)
def require_isolated_integration_profile():
    assert os.getenv("ASHARES_ENV") == "integration_test", (
        "Run these checks through tools/integration_test_db.ps1 -Action test; "
        "refusing to run database checks without the isolated profile."
    )
    assert os.getenv("ASHARES_TEST_ENVIRONMENT_ID")
    assert os.getenv("DATA_COLLECT_CONFIG")


def test_kernel_connection_is_bound_to_the_test_database():
    expected_id = os.environ["ASHARES_TEST_ENVIRONMENT_ID"]
    kernel = DataAccessKernel()
    try:
        connection = kernel._db_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT current_database(), current_user,
                          identity.environment_id::text,
                          identity.environment_kind,
                          identity.migration_chain_hash,
                          role.rolsuper, role.rolcreatedb, role.rolcreaterole
                     FROM integration_test.environment_identity AS identity
                     JOIN pg_roles AS role ON role.rolname=current_user
                    WHERE identity.singleton=TRUE"""
            )
            row = cursor.fetchone()

        assert row is not None
        database, user, environment_id, kind, migration_hash, superuser, createdb, createrole = row
        assert database == "ashares_test"
        assert user == "ashares_app"
        assert environment_id == expected_id
        assert kind == "TEST_FIXTURE"
        assert len(migration_hash.strip()) == 64
        assert (superuser, createdb, createrole) == (False, False, False)
    finally:
        kernel.close()


def test_application_role_cannot_connect_to_migration_compatibility_database():
    psycopg2 = pytest.importorskip("psycopg2")
    config_path = os.environ["DATA_COLLECT_CONFIG"]
    import yaml

    with open(config_path, encoding="utf-8") as handle:
        config = yaml.safe_load(handle)["database"]
    config = {**config, "database": "ashares", "connect_timeout": 2}

    with pytest.raises(psycopg2.OperationalError):
        psycopg2.connect(**config)


def test_expected_research_constraints_are_installed():
    kernel = DataAccessKernel()
    try:
        connection = kernel._db_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT EXISTS (
                           SELECT 1 FROM pg_trigger t
                           JOIN pg_class c ON c.oid=t.tgrelid
                           JOIN pg_namespace n ON n.oid=c.relnamespace
                          WHERE n.nspname='research'
                            AND c.relname='stock_snapshot_daily'
                            AND NOT t.tgisinternal
                            AND t.tgname LIKE '%snapshot%'
                       ),
                       to_regclass('backtest.dataset_manifest') IS NOT NULL,
                       to_regclass('ops.tushare_data_release') IS NOT NULL,
                       to_regclass('pit.universe_daily') IS NOT NULL"""
            )
            frozen_guards, dataset_manifest, release_ledger, pit_universe = cursor.fetchone()
        assert frozen_guards
        assert dataset_manifest and release_ledger and pit_universe
    finally:
        kernel.close()


def test_database_migration_ledger_matches_checked_in_sql():
    migration_dir = Path(__file__).resolve().parents[1] / "sql"
    expected = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in migration_dir.glob("*.sql")
    }
    kernel = DataAccessKernel()
    try:
        connection = kernel._db_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT migration_name, btrim(migration_sha256)
                     FROM integration_test.schema_migration"""
            )
            actual = dict(cursor.fetchall())
        assert actual == expected
    finally:
        kernel.close()


def test_application_role_cannot_create_objects_in_public_schema():
    kernel = DataAccessKernel()
    try:
        connection = kernel._db_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT has_schema_privilege(current_user, 'public', 'CREATE')")
            can_create = cursor.fetchone()[0]
        assert can_create is False
    finally:
        kernel.close()


def test_empty_test_fixture_cannot_be_published_as_a_research_release():
    report = _quality_gate(date(2023, 10, 20), include_foundation_v2=True)

    assert report["failures"]
    assert report["foundation_v2_validated_run_count"] == 0
    assert report["failures"]["foundation_v2:validated_production_run"] == 0


def test_database_rejects_mutating_a_frozen_snapshot():
    psycopg2 = pytest.importorskip("psycopg2")
    kernel = DataAccessKernel()
    connection = kernel._db_connection()
    release_tag = f"it-fixture-{uuid4().hex}"
    snapshot_id = str(uuid4())
    quality_hash = "a" * 64
    input_hash = "b" * 64
    try:
        connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO ops.tushare_data_release
                       (release_tag, baseline_cutoff_date, baseline_domain,
                        schema_version, notes)
                   VALUES (%s, %s, 'RESEARCH', 'feature-contract-v2.0',
                           'TEST_FIXTURE trigger probe')""",
                (release_tag, date(2023, 10, 20)),
            )
            cursor.execute(
                """INSERT INTO research.snapshot_manifest
                       (snapshot_id, research_cutoff_date, data_baseline_tag,
                        schema_version, code_version, quality_report_hash,
                        input_manifest_hash, status, notes)
                   VALUES (%s, %s, %s, 'research-v2.1', 'integration-test',
                           %s, %s, 'DRAFT', 'TEST_FIXTURE trigger probe')""",
                (snapshot_id, date(2023, 10, 20), release_tag, quality_hash, input_hash),
            )
            for next_status in ("BUILDING", "VALIDATED", "FROZEN"):
                cursor.execute(
                    "UPDATE research.snapshot_manifest SET status=%s WHERE snapshot_id=%s",
                    (next_status, snapshot_id),
                )
            with pytest.raises(
                psycopg2.errors.RaiseException,
                match="FROZEN research snapshot is immutable",
            ):
                cursor.execute(
                    "UPDATE research.snapshot_manifest SET notes='tampered' WHERE snapshot_id=%s",
                    (snapshot_id,),
                )
    finally:
        connection.rollback()
        kernel.close()
