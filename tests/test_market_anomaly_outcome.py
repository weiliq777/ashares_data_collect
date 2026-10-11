from datetime import date, timedelta

import pytest

from data_collect.observation.outcome_metrics import OutcomePolicy, compute_outcome


T = date(2026, 9, 1)
_sessions = []
_candidate = T
while len(_sessions) <= 120:
    if _candidate.weekday() < 5:
        _sessions.append(_candidate)
    _candidate += timedelta(days=1)
CALENDAR = tuple(_sessions)


def _event():
    return {
        "observation_id": "11111111-1111-4111-8111-111111111111",
        "snapshot_id": "22222222-2222-4222-8222-222222222222",
        "observation_date": T,
        "ts_code": "000001.SZ",
        "methodology_hash": "a" * 64,
        "data_baseline_id": "market-activity-20260901",
        "input_manifest_hash": "b" * 64,
        "observation_hash": "c" * 64,
        "anchor_adj_close_base": 100.0,
        "anchor_total_return_price": 100.0,
        "anchor_benchmark_close": 200.0,
        "anchor_quality_status": "VALID",
        "anchor_source_asof_date": T,
        "anchor_price_coordinate_contract_version": "market-price-coordinate-v1",
        "anchor_source_hash": "d" * 64,
    }


def _stock_rows(prices, totals=None, code="000001.SZ"):
    totals = totals or prices
    return [
        {
            "ts_code": code,
            "trade_date": CALENDAR[index],
            "adj_close_base": price,
            "total_return_price": totals[index],
            "quality_status": "VALID",
            "source_asof_date": CALENDAR[index],
            "price_coordinate_contract_version": "market-price-coordinate-v1",
            "calculated_at": "2026-09-01T16:00:00+08:00",
        }
        for index, price in enumerate(prices)
    ]


def _benchmark_rows(closes):
    return [
        {"ts_code": "000985.CSI", "trade_date": CALENDAR[i], "close": close}
        for i, close in enumerate(closes)
    ]


def test_outcome_uses_exact_trading_session_and_benchmark_excess():
    result = compute_outcome(
        _event(),
        calendar_dates=CALENDAR,
        stock_rows=_stock_rows([100, 95, 110, 105, 115, 120]),
        benchmark_rows=_benchmark_rows([200, 201, 202, 203, 204, 210]),
        through_date=CALENDAR[5],
    )

    assert result["maturity_status"] == "PENDING_MATURITY"
    assert result["future_price_return_5d"] == pytest.approx(0.20)
    assert result["future_total_return_5d"] == pytest.approx(0.20)
    assert result["excess_return_5d"] == pytest.approx(0.15)
    assert result["max_drawdown_5d"] == pytest.approx(-0.05)
    assert result["max_favorable_excursion_5d"] == pytest.approx(0.20)
    assert result["max_adverse_excursion_5d"] == pytest.approx(-0.05)
    assert result["horizon_status"]["5"]["target_date"] == CALENDAR[5].isoformat()
    assert result["horizon_status"]["10"]["status"] == "PENDING"
    assert result["next_evaluation_date"] == CALENDAR[10]


def test_missing_exact_session_is_not_shifted_to_a_later_price_row():
    rows = _stock_rows([100, 101, 102, 103, 104, 105, 106])
    rows = [row for row in rows if row["trade_date"] != CALENDAR[5]]
    result = compute_outcome(
        _event(),
        calendar_dates=CALENDAR,
        stock_rows=rows,
        benchmark_rows=_benchmark_rows([200, 201, 202, 203, 204, 205]),
        through_date=CALENDAR[5],
    )

    assert result["future_price_return_5d"] is None
    assert result["horizon_status"]["5"]["status"] == "MISSING_SOURCE"
    assert "MISSING_STOCK_PRICE_AT_TARGET_5D" in result["missing_reason_codes"]
    assert result["maturity_status"] == "PENDING_MATURITY"


def test_incomplete_path_keeps_endpoint_return_but_withholds_path_metrics():
    rows = _stock_rows([100, 101, None, 103, 104, 110])
    result = compute_outcome(
        _event(),
        calendar_dates=CALENDAR,
        stock_rows=rows,
        benchmark_rows=_benchmark_rows([200, 201, 202, 203, 204, 210]),
        through_date=CALENDAR[5],
    )

    assert result["future_price_return_5d"] == pytest.approx(0.10)
    assert result["max_drawdown_5d"] is None
    assert result["max_favorable_excursion_5d"] is None
    assert result["volatility_5d"] is None
    assert result["horizon_status"]["5"]["status"] == "MISSING_SOURCE"


def test_anchor_must_be_reconstructable_and_valid_at_observation_date():
    event = {**_event(), "anchor_source_asof_date": date(2026, 8, 31)}
    result = compute_outcome(
        event,
        calendar_dates=CALENDAR,
        stock_rows=[],
        benchmark_rows=[],
        through_date=T,
    )

    assert result["maturity_status"] == "INVALID_INPUT"
    assert "ANCHOR_NOT_RECONSTRUCTABLE" in result["missing_reason_codes"]


def test_snapshot_and_observation_identity_must_match():
    event = {**_event(), "observation_baseline_id": "different-baseline"}
    result = compute_outcome(
        event,
        calendar_dates=CALENDAR,
        stock_rows=[],
        benchmark_rows=[],
        through_date=T,
    )

    assert result["maturity_status"] == "INVALID_INPUT"
    assert "OBSERVATION_MANIFEST_INCOMPLETE" in result["missing_reason_codes"]


def test_duplicate_or_cross_security_price_input_is_rejected():
    rows = _stock_rows([100, 101])
    with pytest.raises(ValueError, match="duplicate price date"):
        compute_outcome(
            _event(),
            calendar_dates=CALENDAR,
            stock_rows=rows + [rows[-1]],
            benchmark_rows=[],
            through_date=CALENDAR[1],
        )

    with pytest.raises(ValueError, match="security code"):
        compute_outcome(
            _event(),
            calendar_dates=CALENDAR,
            stock_rows=_stock_rows([100], code="600000.SH"),
            benchmark_rows=[],
            through_date=CALENDAR[0],
        )


def test_same_inputs_produce_same_hash_and_scenario_fields_are_absent():
    arguments = dict(
        calendar_dates=CALENDAR,
        stock_rows=_stock_rows([100, 101, 102, 103, 104, 105]),
        benchmark_rows=_benchmark_rows([200, 201, 202, 203, 204, 205]),
        through_date=CALENDAR[5],
    )
    first = compute_outcome(_event(), **arguments)
    second = compute_outcome(_event(), **arguments)

    assert first["input_manifest_hash"] == second["input_manifest_hash"]
    assert first["outcome_hash"] == second["outcome_hash"]
    assert first["outcome_revision"] == 1
    assert "entry_price" not in first
    assert "hit_target_before_stop" not in first


def test_trading_calendar_must_be_unique_ordered_and_include_through_date():
    with pytest.raises(ValueError, match="strictly increasing"):
        compute_outcome(
            _event(),
            calendar_dates=(T, CALENDAR[2], CALENDAR[1]),
            stock_rows=[],
            benchmark_rows=[],
            through_date=CALENDAR[1],
        )

    with pytest.raises(ValueError, match="through_date must be an open session"):
        compute_outcome(
            _event(),
            calendar_dates=CALENDAR,
            stock_rows=[],
            benchmark_rows=[],
            through_date=date(2026, 9, 20),
        )


def test_policy_hash_binds_benchmark_and_horizon_semantics():
    default = OutcomePolicy()
    changed = OutcomePolicy(benchmark_code="399300.SZ")
    assert default.methodology_hash != changed.methodology_hash
