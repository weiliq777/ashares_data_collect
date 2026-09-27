from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from data_collect.jobs import a_share_instrument_tushare as job


def test_stock_basic_response_is_fetched_once_and_shared_with_both_repositories(monkeypatch):
    source = pd.DataFrame([{
        "ts_code": "600000.SH",
        "symbol": "600000",
        "name": "浦发银行",
        "area": "上海",
        "industry": "银行",
        "market": "主板",
        "list_date": "19991110",
        "delist_date": None,
        "list_status": "L",
    }])
    calls = []
    saved = {}

    monkeypatch.setattr(job, "get_pro", lambda: SimpleNamespace(stock_basic=object()))

    def fetch(*args, **kwargs):
        calls.append((args, kwargs))
        return source

    monkeypatch.setattr(job, "call_api", fetch)
    monkeypatch.setattr(job, "build_default_registry", lambda: object())
    monkeypatch.setattr(job, "DataAccessKernel", lambda **kwargs: SimpleNamespace(close=lambda: None))

    class FakeInstrumentRepository:
        def __init__(self, _kernel):
            pass

        def persist_stock_basic(self, rows, batch_id, context):
            saved["stock_basic_rows"] = rows
            saved["batch_id"] = batch_id
            saved["context"] = context
            return len(rows)

    class FakeIndustryRepository:
        def __init__(self, _kernel):
            pass

        def persist_daily_capture(self, rows, **kwargs):
            saved["industry_rows"] = rows
            saved["industry_kwargs"] = kwargs
            return SimpleNamespace(
                status="CAPTURED",
                trade_date="2026-09-25",
                inserted_rows=1,
                unknown_industry_rows=0,
                reason=None,
            )

    monkeypatch.setattr(job, "InstrumentRepository", FakeInstrumentRepository)
    monkeypatch.setattr(job, "IndustryMembershipRepository", FakeIndustryRepository)

    result = job.run("20260925")

    assert len(calls) == 1
    assert saved["stock_basic_rows"] is saved["industry_rows"]
    assert saved["industry_kwargs"]["run_date"] == "20260925"
    assert saved["industry_kwargs"]["context"] is saved["context"]
    assert "行业每日观测=CAPTURED" in result


def test_empty_stock_basic_response_fails_instead_of_reporting_success(monkeypatch):
    monkeypatch.setattr(job, "get_pro", lambda: SimpleNamespace(stock_basic=object()))
    monkeypatch.setattr(job, "call_api", lambda *args, **kwargs: pd.DataFrame())

    with pytest.raises(RuntimeError, match="空响应"):
        job.run("20260925")
