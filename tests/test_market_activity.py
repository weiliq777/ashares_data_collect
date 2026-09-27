from datetime import date, datetime, timedelta

import pytest

from data_collect.data_access.context import RunContext
from data_collect.market_activity import (
    MarketActivityMonitor,
    MarketActivityRequest,
)
from data_collect.data_access.repository.market_activity_repository import (
    MarketActivityRepository,
)
from data_collect.market_activity.metrics import build_daily_report


TARGET = date(2026, 9, 22)


def _context() -> RunContext:
    return RunContext(
        run_id="market-activity-test",
        job_name="market_activity_monitor",
        code_version="test",
        config_hash="test",
        pipeline_version="data-access-v1",
    )


def _batches(days: int = 20):
    rows = []
    for offset in range(days, 0, -1):
        day = TARGET - timedelta(days=offset)
        rows.extend([
            {"stock_code": "000001.SZ", "trade_date": day, "close": 10, "volume": 100, "amount": 1000},
            {"stock_code": "000002.SZ", "trade_date": day, "close": 20, "volume": 200, "amount": 2000},
        ])
    rows.extend([
        {"stock_code": "000001.SZ", "trade_date": TARGET, "close": 11, "volume": 300, "amount": 4000},
        {"stock_code": "000002.SZ", "trade_date": TARGET, "close": 19, "volume": 100, "amount": 1000},
    ])
    return [rows]


def test_daily_report_ranks_and_calculates_market_concentration():
    report = build_daily_report(
        _batches(),
        MarketActivityRequest(trade_date=TARGET, top_n=1),
    )

    assert report.quality_status == "VALID"
    assert report.total_amount == 5000
    assert report.total_volume == 400
    assert report.top_n_amount == 4000
    assert report.top_n_amount_share == pytest.approx(0.8)
    first = next(row for row in report.rows if row.ts_code == "000001.SZ")
    assert first.amount_rank == 1
    assert first.volume_rank == 1
    assert first.amount_ratio_previous == pytest.approx(4.0)
    assert first.volume_ratio_previous == pytest.approx(3.0)


def test_report_deterministically_breaks_equal_amount_ties_by_code():
    rows = [
        {"stock_code": "000002.SZ", "trade_date": TARGET, "volume": 100, "amount": 1000},
        {"stock_code": "000001.SZ", "trade_date": TARGET, "volume": 100, "amount": 1000},
    ]
    report = build_daily_report(
        [rows],
        MarketActivityRequest(trade_date=TARGET, top_n=1),
    )
    assert report.top_n_amount == 1000
    assert next(row for row in report.rows if row.ts_code == "000001.SZ").amount_rank == 1


def test_insufficient_history_is_explicit_and_not_imputed_to_zero():
    rows = [{"stock_code": "000001.SZ", "trade_date": TARGET, "volume": 100, "amount": 1000}]
    report = build_daily_report(
        [rows],
        MarketActivityRequest(trade_date=TARGET, lookback_trading_days=20),
    )
    row = report.rows[0]
    assert report.quality_status == "PARTIAL_HISTORY"
    assert report.insufficient_history_stock_count == 1
    assert "INSUFFICIENT_LOOKBACK" in row.reason_codes
    assert row.amount_ratio_previous is None
    assert row.volume_ratio_previous is None


def test_request_rejects_datetime_to_keep_the_date_contract_unambiguous():
    with pytest.raises(TypeError, match="datetime.date"):
        MarketActivityRequest(trade_date=datetime(2026, 9, 22)).validate()


def test_future_rows_and_duplicates_are_rejected():
    future = [{"stock_code": "000001.SZ", "trade_date": TARGET + timedelta(days=1), "volume": 1, "amount": 1}]
    with pytest.raises(ValueError, match="future"):
        build_daily_report([future], MarketActivityRequest(trade_date=TARGET))

    duplicate = [
        {"stock_code": "000001.SZ", "trade_date": TARGET, "volume": 1, "amount": 1},
        {"stock_code": "000001.SZ", "trade_date": TARGET, "volume": 1, "amount": 1},
    ]
    with pytest.raises(ValueError, match="duplicate"):
        build_daily_report([duplicate], MarketActivityRequest(trade_date=TARGET))


def test_invalid_target_bar_is_reported_and_valid_rows_are_kept():
    rows = [
        {"stock_code": "000001.SZ", "trade_date": TARGET, "volume": None, "amount": 1000},
        {"stock_code": "000002.SZ", "trade_date": TARGET, "volume": 100, "amount": 1000},
    ]
    report = build_daily_report([rows], MarketActivityRequest(trade_date=TARGET))
    assert report.quality_status == "PARTIAL"
    assert report.invalid_target_row_count == 1
    assert len(report.rows) == 1


def test_monitor_uses_one_bounded_stream_and_no_write(monkeypatch):
    calls = []

    class FakeRead:
        def _repository_stream(self, **kwargs):
            calls.append(kwargs)
            return iter(_batches())

    class FakeKernel:
        read = FakeRead()

    monitor = MarketActivityMonitor(MarketActivityRepository(FakeKernel()))
    result = monitor.daily(MarketActivityRequest(trade_date=TARGET), _context())

    assert result.total_amount == 5000
    assert len(calls) == 1
    assert calls[0]["source"] == "std.daily_kline"
    assert "BETWEEN %s AND %s" in calls[0]["sql"]
    assert "LIMIT %s" in calls[0]["sql"]
    assert calls[0]["params"] == (TARGET - timedelta(days=45), TARGET, 250_001)
    assert calls[0]["batch_rows"] == 10_000
