from types import SimpleNamespace

import pandas as pd

import data_collect.jobs.tushare_history_init as history


def test_market_history_retries_by_code_chunks_at_api_limit(monkeypatch):
    calls = []
    first = pd.DataFrame({"ts_code": [f"X{i:04d}" for i in range(6000)], "trade_date": ["20260803"] * 6000})

    def fake_call(api, **kwargs):
        calls.append(kwargs)
        if "ts_code" not in kwargs:
            return first
        codes = kwargs["ts_code"].split(",")
        return pd.DataFrame({"ts_code": codes, "trade_date": [kwargs["trade_date"]] * len(codes)})

    monkeypatch.setattr(history, "call_api", fake_call)
    pro = SimpleNamespace(daily=object(), daily_basic=object())
    codes = [f"X{i:04d}" for i in range(2001)]

    result = history._request_history_market_frame(pro, "daily", "20260803", codes)

    assert len(calls) == 4  # initial full-day request + 3 code chunks
    assert all("ts_code" in call for call in calls[1:])
    assert len(result) == 2001
    assert result["ts_code"].is_unique


def test_market_history_does_not_split_below_api_limit(monkeypatch):
    calls = []
    frame = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260803"]})

    def fake_call(api, **kwargs):
        calls.append(kwargs)
        return frame

    monkeypatch.setattr(history, "call_api", fake_call)
    pro = SimpleNamespace(daily=object(), daily_basic=object())

    result = history._request_history_market_frame(pro, "valuation", "20260803", ["000001.SZ"])

    assert len(calls) == 1
    assert result.equals(frame)
