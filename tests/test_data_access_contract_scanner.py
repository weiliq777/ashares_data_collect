from pathlib import Path

from tools.check_data_access_contract import _business_py_files, _scan_file


def _write(tmp_path: Path, source: str) -> Path:
    path = tmp_path / "business.py"
    path.write_text(source, encoding="utf-8")
    return path


def test_scanner_accepts_typed_repository_calls(tmp_path: Path):
    path = _write(
        tmp_path,
        """
from data_collect.data_access.repository import MarketRepository

def load(repo: MarketRepository):
    return repo.existing_dates()
""",
    )

    assert _scan_file(path) == []


def test_scanner_rejects_legacy_import_and_connection_parameter(tmp_path: Path):
    path = _write(
        tmp_path,
        """
from data_collect.utils.db import get_connection

def load(connection):
    return get_connection()
""",
    )

    rules = {hit[1] for hit in _scan_file(path)}
    assert "LEGACY_DB_IMPORT" in rules
    assert "GET_CONNECTION" in rules
    assert "DB_HANDLE_PARAMETER" in rules


def test_scanner_rejects_importing_legacy_job_database_helpers(tmp_path: Path):
    path = _write(
        tmp_path,
        """
from data_collect.jobs.tushare_financial_raw_init import _save_raw
""",
    )

    assert "LEGACY_JOB_DB_HELPER" in {hit[1] for hit in _scan_file(path)}


def test_scanner_rejects_driver_alias_raw_execution_and_pandas_sql(tmp_path: Path):
    path = _write(
        tmp_path,
        """
import psycopg2 as db
import pandas as pd

def load(cur):
    cur.execute("SELECT 1")
    cur.rollback()
    return pd.read_sql_query("SELECT 1", db)
""",
    )

    rules = {hit[1] for hit in _scan_file(path)}
    assert "DB_DRIVER" in rules
    assert "RAW_EXECUTE" in rules
    assert "MANUAL_COMMIT" in rules
    assert "PANDAS_SQL" in rules
    assert "DB_HANDLE_PARAMETER" in rules
    assert "RAW_SQL_READ" in rules


def test_business_file_scanner_covers_new_business_directories(tmp_path: Path):
    business_file = tmp_path / "data_collect" / "new_business" / "loader.py"
    business_file.parent.mkdir(parents=True)
    business_file.write_text("def load():\n    return []\n", encoding="utf-8")

    assert business_file in _business_py_files(tmp_path)


def test_scanner_rejects_private_bridges_dynamic_driver_and_execute_alias(tmp_path: Path):
    path = _write(
        tmp_path,
        """
import importlib

def bypass(kernel, cur):
    importlib.import_module('psycopg2')
    execute = cur.execute
    getattr(cur, 'commit')()
    return kernel.read._repository_query(None, 'x', 'SELECT 1')
""",
    )
    rules = {hit[1] for hit in _scan_file(path)}
    assert {"DB_DRIVER", "DYNAMIC_DB_ACCESS", "PRIVATE_KERNEL_BRIDGE"} <= rules


def test_scanner_rejects_business_job_reaching_through_repository_kernel(tmp_path: Path):
    path = _write(
        tmp_path,
        """
def transition(repository, context):
    return repository.kernel.write.transition_dataset_manifest(context, None, 'x', (), 'BUILDING', {})
""",
    )

    assert "BUSINESS_KERNEL_GATEWAY" in {hit[1] for hit in _scan_file(path)}
