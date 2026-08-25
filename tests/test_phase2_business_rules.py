from datetime import date
from pathlib import Path

from data_collect.jobs.phase2_business_rules import (
    derive_lifecycle_interval,
    dividend_business_key,
    enters_shareholder_return,
)
from data_collect.jobs.tushare_financial_incremental import _indicator_windows, _year_windows


def test_lifecycle_open_interval_closes_before_next_name():
    assert derive_lifecycle_interval(
        date(2020, 1, 1), None, date(2021, 2, 1), date(2019, 1, 1), None
    ) == (date(2020, 1, 1), date(2021, 1, 31))


def test_lifecycle_clamps_to_delist():
    assert derive_lifecycle_interval(
        date(2020, 1, 1), date(2025, 1, 1), None, None, date(2023, 1, 1)
    ) == (date(2020, 1, 1), date(2023, 1, 1))


def test_dividend_business_key_keeps_plan_and_implementation_separate():
    plan = dividend_business_key("000001.SZ", "20241231", "20250101", "预案", None, None, None)
    implementation = dividend_business_key("000001.SZ", "20241231", "20250101", "实施", "20250301", "20250302", "20250220")
    assert plan != implementation


def test_dividend_business_key_is_stable_when_event_dates_are_corrected():
    original = dividend_business_key(
        "000001.SZ", "20241231", "20250101", "实施",
        "20250301", "20250302", "20250220",
    )
    corrected = dividend_business_key(
        "000001.SZ", "20241231", "20250105", "实施",
        "20250304", "20250305", "20250225",
    )
    assert original == corrected


def test_only_implemented_ex_date_before_decision_enters_return():
    decision = date(2025, 3, 10)
    assert enters_shareholder_return("实施", date(2025, 3, 2), decision)
    assert not enters_shareholder_return("预案", date(2025, 3, 2), decision)
    assert not enters_shareholder_return("实施", date(2025, 3, 11), decision)


def test_indicator_windows_are_split_by_report_year():
    assert list(_year_windows("20240101", "20250331")) == [
        ("20240101", "20241231"), ("20250101", "20250331")
    ]


def test_indicator_windows_use_quarters_for_api_limit():
    assert list(_indicator_windows("20240101", "20241001")) == [
        ("20240101", "20240331"), ("20240401", "20240630"),
        ("20240701", "20240930"), ("20241001", "20241001")
    ]


def test_financial_asof_rebuild_is_batched_and_does_not_truncate_production():
    source = Path(
        "data_collect/data_access/repository/pit_repository.py"
    ).read_text(encoding="utf-8")
    function = source.split("def rebuild_financial_asof", 1)[1].split(
        "_FINANCIAL_ASOF_INSERT_SQL", 1
    )[0]
    assert "batch_days" in function
    assert "run_table_replace" in function
    assert 'INSERT INTO pit."{staging}"' in source
    assert "TRUNCATE TABLE pit.financial_asof" not in source
    assert "DROP TABLE pit." not in source
