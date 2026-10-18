from datetime import date, timedelta

from data_collect.alpha_research.labels import build_forward_labels


def _bar(day, close, *, open_price=None, quality="VALID", suspended=False):
    return {
        "trade_date": day,
        "ts_code": "000001.SZ",
        "raw_open": close if open_price is None else open_price,
        "raw_close": close,
        "quality_status": quality,
        "is_suspended": suspended,
    }


def _calendar(count=5):
    start = date(2025, 1, 2)
    return tuple(start + timedelta(days=index) for index in range(count))


def test_signal_and_next_open_reference_returns_use_distinct_anchors():
    calendar = _calendar(3)
    bars = {
        calendar[0]: {"000001.SZ": _bar(calendar[0], 10)},
        calendar[1]: {"000001.SZ": _bar(calendar[1], 13, open_price=12)},
        calendar[2]: {"000001.SZ": _bar(calendar[2], 14, open_price=13)},
    }
    labels = build_forward_labels(
        [{"research_date": calendar[0], "ts_code": "000001.SZ"}],
        calendar=calendar, bars=bars, actions={}, horizons=(2,),
    )
    label = labels[0]
    assert label.status == "COMPLETE"
    assert abs(label.prediction_total_return - 0.4) < 1e-12
    assert abs(label.entry_reference_total_return - (14 / 12 - 1)) < 1e-12
    assert label.execution_status == "NOT_SIMULATED_BACKTEST_REQUIRED"


def test_execution_warning_for_missing_limits_keeps_valid_price_return_label():
    calendar = _calendar(3)
    limit_warning_bar = {
        **_bar(calendar[1], 11, open_price=10.5, quality="WARNING"),
        "raw_high": 11.5,
        "raw_low": 10.0,
        "volume": 1000.0,
        "amount": 11000.0,
        "source_payload_hash": "daily-price-payload",
    }
    bars = {
        calendar[0]: {"000001.SZ": _bar(calendar[0], 10)},
        calendar[1]: {"000001.SZ": limit_warning_bar},
        calendar[2]: {"000001.SZ": _bar(calendar[2], 12, open_price=11.5)},
    }

    label = build_forward_labels(
        [{"research_date": calendar[0], "ts_code": "000001.SZ"}],
        calendar=calendar,
        bars=bars,
        actions={},
        horizons=(2,),
    )[0]

    assert label.status == "COMPLETE"
    assert abs(label.prediction_total_return - 0.2) < 1e-12
    assert abs(label.entry_reference_total_return - (12 / 10.5 - 1)) < 1e-12


def test_execution_warning_without_complete_price_evidence_stays_incomplete():
    calendar = _calendar(3)
    warning_bar = {
        **_bar(calendar[1], 11, quality="WARNING"),
        "raw_high": 11.5,
        "raw_low": 10.5,
        "volume": 1000.0,
        "amount": 11000.0,
        "source_payload_hash": None,
    }
    bars = {
        calendar[0]: {"000001.SZ": _bar(calendar[0], 10)},
        calendar[1]: {"000001.SZ": warning_bar},
        calendar[2]: {"000001.SZ": _bar(calendar[2], 12)},
    }

    label = build_forward_labels(
        [{"research_date": calendar[0], "ts_code": "000001.SZ"}],
        calendar=calendar,
        bars=bars,
        actions={},
        horizons=(2,),
    )[0]

    assert label.status == "INCOMPLETE"
    assert label.reason == "INVALID_PATH_BAR_QUALITY"


def test_total_return_matches_frozen_price_feature_company_action_formula():
    calendar = _calendar(3)
    bars = {
        calendar[0]: {"000001.SZ": _bar(calendar[0], 10)},
        calendar[1]: {"000001.SZ": _bar(calendar[1], 9, open_price=8)},
        calendar[2]: {"000001.SZ": _bar(calendar[2], 10, open_price=9)},
    }
    actions = {
        calendar[1]: {
            "000001.SZ": [{
                "implementation_status": "实施",
                "stock_div": 0.1,
                "cash_div": 0.1,
                "source_record_key": "action-1",
                "source_record_version": 1,
            }]
        }
    }
    label = build_forward_labels(
        [{"research_date": calendar[0], "ts_code": "000001.SZ"}],
        calendar=calendar, bars=bars, actions=actions, horizons=(2,),
    )[0]
    expected_prediction = ((9 * 1.1 + 0.1) / 10) * (10 / 9) - 1
    assert label.status == "COMPLETE"
    assert abs(label.prediction_total_return - expected_prediction) < 1e-12
    # The T+1 open buyer does not receive an ex-date distribution.
    assert abs(label.entry_reference_total_return - (10 / 8 - 1)) < 1e-12


def test_missing_bar_and_suspension_are_retained_as_incomplete_labels():
    calendar = _calendar(3)
    signal = {"research_date": calendar[0], "ts_code": "000001.SZ"}
    bars = {
        calendar[0]: {"000001.SZ": _bar(calendar[0], 10)},
        calendar[2]: {"000001.SZ": _bar(calendar[2], 12)},
    }
    missing = build_forward_labels(
        [signal], calendar=calendar, bars=bars, actions={}, horizons=(2,),
    )[0]
    assert missing.status == "INCOMPLETE"
    assert missing.reason == "MISSING_PATH_BAR"
    assert missing.prediction_total_return is None

    bars[calendar[1]] = {"000001.SZ": _bar(calendar[1], 11, suspended=True)}
    suspended = build_forward_labels(
        [signal], calendar=calendar, bars=bars, actions={}, horizons=(2,),
    )[0]
    assert suspended.status == "INCOMPLETE"
    assert suspended.reason == "SUSPENDED_PATH"


def test_delisting_inside_horizon_is_retained_as_unresolved_delisting():
    calendar = _calendar(3)
    bars = {
        calendar[0]: {"000001.SZ": _bar(calendar[0], 10)},
        calendar[1]: {"000001.SZ": _bar(calendar[1], 11)},
    }
    labels = build_forward_labels(
        [{"research_date": calendar[0], "ts_code": "000001.SZ"}],
        calendar=calendar,
        bars=bars,
        actions={},
        horizons=(2,),
        delist_dates={"000001.SZ": calendar[1]},
    )

    label = labels[0]
    assert label.status == "INCOMPLETE"
    assert label.reason == "DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE"
    assert label.prediction_total_return is None
    assert label.entry_reference_total_return is None
    assert label.entry_reference_status == "DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE"


def test_delisting_after_target_does_not_invalidate_a_mature_label():
    calendar = _calendar(3)
    bars = {
        calendar[0]: {"000001.SZ": _bar(calendar[0], 10)},
        calendar[1]: {"000001.SZ": _bar(calendar[1], 11)},
    }
    label = build_forward_labels(
        [{"research_date": calendar[0], "ts_code": "000001.SZ"}],
        calendar=calendar,
        bars=bars,
        actions={},
        horizons=(1,),
        delist_dates={"000001.SZ": calendar[2]},
    )[0]

    assert label.status == "COMPLETE"
    assert label.reason is None
    assert abs(label.prediction_total_return - 0.1) < 1e-12


def test_signal_after_delisting_is_not_treated_as_a_valid_prediction():
    calendar = _calendar(3)
    bars = {day: {"000001.SZ": _bar(day, 10 + index)} for index, day in enumerate(calendar)}
    label = build_forward_labels(
        [{"research_date": calendar[1], "ts_code": "000001.SZ"}],
        calendar=calendar,
        bars=bars,
        actions={},
        horizons=(1,),
        delist_dates={"000001.SZ": calendar[0]},
    )[0]

    assert label.status == "INCOMPLETE"
    assert label.reason == "ALREADY_DELISTED_AT_SIGNAL"


def test_invalid_action_and_unknown_suspension_status_are_not_assumed_valid():
    calendar = _calendar(3)
    bars = {
        calendar[0]: {"000001.SZ": _bar(calendar[0], 10)},
        calendar[1]: {"000001.SZ": _bar(calendar[1], 11)},
        calendar[2]: {"000001.SZ": _bar(calendar[2], 12)},
    }
    actions = {calendar[1]: {"000001.SZ": [{
        "implementation_status": "实施",
        "cash_div": float("nan"),
        "source_record_key": "bad-action",
        "source_record_version": 1,
    }]}}
    invalid_action = build_forward_labels(
        [{"research_date": calendar[0], "ts_code": "000001.SZ"}],
        calendar=calendar, bars=bars, actions=actions, horizons=(2,),
    )[0]
    assert invalid_action.reason == "INVALID_CORPORATE_ACTION"

    bars[calendar[1]]["000001.SZ"]["is_suspended"] = None
    unknown_status = build_forward_labels(
        [{"research_date": calendar[0], "ts_code": "000001.SZ"}],
        calendar=calendar, bars=bars, actions={}, horizons=(2,),
    )[0]
    assert unknown_status.reason == "SUSPENSION_STATUS_UNKNOWN"


def test_immature_window_is_not_filled_with_zero_and_relative_sample_is_reported():
    calendar = _calendar(3)
    bars = {
        calendar[0]: {"000001.SZ": _bar(calendar[0], 10), "000002.SZ": _bar(calendar[0], 20)},
        calendar[1]: {"000001.SZ": _bar(calendar[1], 11)},
        calendar[2]: {"000001.SZ": _bar(calendar[2], 12)},
    }
    labels = build_forward_labels(
        [
            {"research_date": calendar[0], "ts_code": "000001.SZ"},
            {"research_date": calendar[0], "ts_code": "000002.SZ"},
        ],
        calendar=calendar, bars=bars, actions={}, horizons=(2, 3),
    )
    complete = next(row for row in labels if row.ts_code == "000001.SZ" and row.horizon == 2)
    incomplete = next(row for row in labels if row.ts_code == "000002.SZ" and row.horizon == 2)
    immature = next(row for row in labels if row.ts_code == "000001.SZ" and row.horizon == 3)
    assert complete.relative_total_return == 0.0
    assert complete.relative_pool_complete_count == 1
    assert complete.relative_pool_total_count == 2
    assert incomplete.status == "INCOMPLETE"
    assert immature.status == "IMMATURE"
    assert immature.prediction_total_return is None
