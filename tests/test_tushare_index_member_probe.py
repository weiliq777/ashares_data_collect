from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest


TOOL_PATH = Path(__file__).resolve().parents[1] / "tools" / "probe_tushare_index_member_all.py"
SPEC = importlib.util.spec_from_file_location("probe_tushare_index_member_all", TOOL_PATH)
assert SPEC and SPEC.loader
probe_tool = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe_tool)


def test_summarize_response_bounds_rows_and_reports_effective_dates() -> None:
    frame = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "l3_code": "850531.SI", "in_date": "20180101", "out_date": None},
            {"ts_code": "000001.SZ", "l3_code": "850531.SI", "in_date": "20200101", "out_date": "20221231"},
        ]
    )

    report = probe_tool.summarize_response(frame, sample_size=1)

    assert report["row_count"] == 2
    assert len(report["rows"]) == 1
    assert report["date_ranges"]["in_date"]["min"] == "2018-01-01"
    assert report["date_ranges"]["in_date"]["max"] == "2020-01-01"
    assert report["date_ranges"]["out_date"]["non_null"] == 1
    assert report["distinct_industry_codes"] == ["850531.SI"]


def test_probe_uses_latest_and_historical_modes_without_database(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, str]] = []

    class FakePro:
        def index_member_all(self, **kwargs: str) -> pd.DataFrame:
            calls.append(kwargs)
            return pd.DataFrame([kwargs | {"in_date": "20200101", "out_date": None}])

    monkeypatch.setattr(probe_tool, "get_pro", lambda: FakePro())
    monkeypatch.setattr(
        probe_tool,
        "call_api",
        lambda fn, **kwargs: fn(**kwargs),
    )

    report = probe_tool.probe("000001.SZ", sample_size=2)

    assert [call["is_new"] for call in calls] == ["Y", "N"]
    assert report["database_access"] is False
    assert report["modes"]["is_new=Y"]["row_count"] == 1


def test_probe_can_query_one_industry_code(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, str]] = []

    class FakePro:
        def index_member_all(self, **kwargs: str) -> pd.DataFrame:
            calls.append(kwargs)
            return pd.DataFrame([kwargs])

    monkeypatch.setattr(probe_tool, "get_pro", lambda: FakePro())
    monkeypatch.setattr(probe_tool, "call_api", lambda fn, **kwargs: fn(**kwargs))

    report = probe_tool.probe(l3_code="857831.SI", sample_size=1)

    assert report["query"] == {"l3_code": "857831.SI"}
    assert [call["l3_code"] for call in calls] == ["857831.SI", "857831.SI"]


def test_probe_rejects_two_query_keys() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        probe_tool.probe("000001.SZ", l3_code="857831.SI")


@pytest.mark.parametrize("sample_size", [0, 51])
def test_probe_rejects_unbounded_sample_size(sample_size: int) -> None:
    with pytest.raises(ValueError, match="sample_size"):
        probe_tool.probe("000001.SZ", sample_size=sample_size)
