from pathlib import Path
from datetime import date

import pandas as pd

from data_collect.jobs import tushare_financial_vip_incremental as vip


def test_recent_report_periods_never_selects_an_open_future_quarter():
    assert vip._recent_report_periods(date(2026, 8, 14), 3) == [
        "20260630", "20260331", "20251231"
    ]


def test_indicator_vip_keeps_only_current_update_flag_and_has_safe_identity(monkeypatch):
    anomalies = []
    monkeypatch.setattr(
        vip,
        "_record_anomaly",
        lambda *args, **kwargs: anomalies.append((args, kwargs)),
    )
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ", "000002.SZ"],
            "ann_date": ["20250823", "20250823", "20250823"],
            "end_date": ["20250630", "20250630", "20250630"],
            "update_flag": ["0", "1", "1"],
            "roe": [1.0, 2.0, 3.0],
        }
    )

    records = vip._validate_and_prepare(
        frame, "fina_indicator_vip", "fina_indicator", "20250630", "batch-1"
    )

    assert [row["ts_code"] for row in records] == ["000001.SZ", "000002.SZ"]
    assert [row["roe"] for row in records] == [2.0, 3.0]
    assert anomalies == []


def test_indicator_vip_quarantines_ambiguous_current_rows(monkeypatch):
    anomalies = []
    monkeypatch.setattr(
        vip,
        "_record_anomaly",
        lambda *args, **kwargs: anomalies.append((args, kwargs)),
    )
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "ann_date": ["20250823", "20250823"],
            "end_date": ["20250630", "20250630"],
            "update_flag": ["1", "1"],
            "roe": [1.0, 2.0],
        }
    )

    records = vip._validate_and_prepare(
        frame, "fina_indicator_vip", "fina_indicator", "20250630", "batch-2"
    )

    assert records == []
    assert any(args[4] == "INDICATOR_IDENTITY_AMBIGUOUS" for args, _ in anomalies)


def test_report_vip_requires_identity_columns_and_current_flag(monkeypatch):
    anomalies = []
    monkeypatch.setattr(
        vip,
        "_record_anomaly",
        lambda *args, **kwargs: anomalies.append((args, kwargs)),
    )
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "ann_date": ["20250823", "20250823"],
            "f_ann_date": ["20250823", "20250823"],
            "end_date": ["20250630", "20250630"],
            "report_type": [1, 1],
            "comp_type": [1, 1],
            "end_type": [2, 2],
            "update_flag": [0, 1],
            "n_income": [10.0, 10.0],
        }
    )

    records = vip._validate_and_prepare(
        frame, "income_vip", "income", "20250630", "batch-3"
    )

    assert len(records) == 1
    assert records[0]["update_flag"] == 1
    assert anomalies == []


def test_report_vip_missing_identity_is_quarantined(monkeypatch):
    anomalies = []
    monkeypatch.setattr(
        vip,
        "_record_anomaly",
        lambda *args, **kwargs: anomalies.append((args, kwargs)),
    )
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "ann_date": ["20250823"],
            "end_date": ["20250630"],
            "update_flag": [1],
            "n_income": [10.0],
        }
    )

    records = vip._validate_and_prepare(
        frame, "income_vip", "income", "20250630", "batch-4"
    )

    assert records == []
    assert any(args[4] == "MISSING_IDENTITY_COLUMNS" for args, _ in anomalies)


def test_financial_vip_migration_has_bounded_cutover_and_atomic_queue_contract():
    sql = Path("sql/046_financial_vip_incremental.sql").read_text(encoding="utf-8")
    source = Path(
        "data_collect/jobs/tushare_financial_vip_incremental.py"
    ).read_text(encoding="utf-8")
    repository = Path(
        "data_collect/data_access/repository/financial_vip_repository.py"
    ).read_text(encoding="utf-8")
    gateway = Path(
        "data_collect/data_access/write_gateway.py"
    ).read_text(encoding="utf-8")

    assert "financial_change_queue" in sql
    assert "financial_ingest_anomaly" in sql
    assert "financial_version_cutover_watermark" in sql
    assert "legacy-cutover-046" in sql
    assert "tushare_income_raw" not in sql.split("CREATE TABLE IF NOT EXISTS financial_change_queue", 1)[1].split("CREATE TABLE IF NOT EXISTS financial_ingest_anomaly", 1)[0]
    assert "_write_versions_and_queue" in source
    assert "ON CONFLICT DO NOTHING" in gateway
    assert "_db_connection" not in repository
    assert "financial_change_queue" in repository
    assert "limit=PAGE_SIZE, offset=offset" in source
    assert "r.record_id > %s" in repository
    assert "def _legacy_pending_codes" in source
    assert "set(queue_codes) | set(legacy_codes)" in source


def test_financial_vip_page_lookup_has_nonblocking_batch_hash_indexes():
    sql = Path("sql/060_financial_vip_page_lookup_indexes.sql").read_text(encoding="utf-8")
    assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS" in sql
    assert "(batch_id, dataset, payload_hash)" in sql
    assert "(batch_id, payload_hash)" in sql
