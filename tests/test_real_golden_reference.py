from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

import pytest

from data_collect.alpha_research.labels import build_forward_labels
from tests.integration_fixtures.real_golden_reference import independent_forward_labels


def _bar(day: date, code: str, close: float, *, suspended: bool = False) -> dict:
    opening = close * 0.99
    return {
        "trade_date": day,
        "ts_code": code,
        "raw_open": opening,
        "raw_high": close * 1.02,
        "raw_low": opening * 0.98,
        "raw_close": close,
        "volume": 1000.0,
        "amount": close * 1000.0,
        "is_suspended": suspended,
        "quality_status": "VALID",
        "source_payload_hash": f"hash-{code}-{day.isoformat()}",
    }


def _labels_for_system(observations, calendar, bars, actions, delist_dates):
    bars_by_day = defaultdict(dict)
    for row in bars:
        bars_by_day[row["trade_date"]][row["ts_code"]] = row
    actions_by_day = defaultdict(lambda: defaultdict(list))
    for row in actions:
        actions_by_day[row["ex_date"]][row["ts_code"]].append(row)
    return [
        row.as_dict()
        for row in build_forward_labels(
            observations,
            calendar=calendar,
            bars=bars_by_day,
            actions=actions_by_day,
            delist_dates=delist_dates,
            horizons=(20, 40, 60),
        )
    ]


def test_independent_reference_matches_label_builder_for_returns_and_failures():
    calendar = [date(2024, 1, 2) + timedelta(days=offset) for offset in range(61)]
    signal_date = calendar[0]
    codes = ("000001.SZ", "600000.SH", "000666.SZ")
    observations = [
        {"research_date": signal_date, "ts_code": code} for code in codes
    ]
    bars = []
    for index, code in enumerate(codes):
        last_offset = 10 if code == "000666.SZ" else 60
        for offset, day in enumerate(calendar[:last_offset + 1]):
            close = 10.0 + index + offset * 0.02
            bars.append(_bar(
                day, code, close,
                suspended=code == "600000.SH" and offset == 11,
            ))
    actions = [{
        "ts_code": "000001.SZ",
        "ex_date": calendar[20],
        "implementation_status": "\u5b9e\u65bd",
        "stock_div": None,
        "stock_bonus_rate": 1.0,
        "stock_conversion_rate": 0.0,
        "cash_div": 0.1,
        "source_record_key": "000001.SZ:20231231",
        "source_record_version": 1,
    }]
    delist_dates = {"000666.SZ": calendar[50]}

    reference = independent_forward_labels(
        observations,
        calendar=calendar,
        bars=bars,
        actions=actions,
        delist_dates=delist_dates,
        horizons=(20, 40, 60),
    )
    system = _labels_for_system(
        observations, calendar, bars, actions, delist_dates,
    )

    assert len(reference) == len(system) == 9
    for expected, actual in zip(reference, system, strict=True):
        for key in (
            "signal_date", "ts_code", "horizon", "target_date", "status",
            "reason", "entry_reference_status", "relative_pool_complete_count",
            "relative_pool_total_count",
        ):
                assert expected[key] == actual[key], (key, expected, actual)
        for key in (
            "prediction_total_return", "entry_reference_total_return",
            "relative_total_return", "relative_pool_mean_return",
        ):
            if expected[key] is None:
                assert actual[key] is None
            else:
                assert actual[key] == pytest.approx(expected[key], abs=1e-12)

    by_identity = {(row["ts_code"], row["horizon"]): row for row in reference}
    assert by_identity[("600000.SH", 20)]["reason"] == "SUSPENDED_PATH"
    assert by_identity[("000666.SZ", 60)]["reason"] == "DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE"
    assert by_identity[("000001.SZ", 20)]["status"] == "COMPLETE"
