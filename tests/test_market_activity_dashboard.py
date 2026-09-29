from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from data_collect.data_access.context import RunContext
from data_collect.data_access.repository.market_activity_dashboard_repository import (
    MarketActivityDashboardRepository,
)


def _context():
    return RunContext(
        run_id="market-dashboard-test",
        job_name="market_activity_dashboard",
        code_version="test",
        config_hash="test",
        pipeline_version="test",
    )


class _ReadProbe:
    def __init__(self, responses=()):
        self.responses = list(responses)
        self.calls = []

    def _repository_query(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0)


def test_dashboard_date_catalog_is_bounded_to_recent_year_and_frozen_rows():
    probe = _ReadProbe([[{"trade_date": date(2026, 9, 24)}]])
    repository = MarketActivityDashboardRepository(SimpleNamespace(read=probe))

    result = repository.available_dates(date(2026, 9, 29), _context())

    assert result == [date(2026, 9, 24)]
    call = probe.calls[0]
    assert "trade_date BETWEEN %s AND %s" in call["sql"]
    assert "s.status='FROZEN'" in call["sql"]
    assert "LIMIT %s" in call["sql"]
    assert call["params"][0:2] == (date(2025, 9, 29), date(2026, 9, 29))
    assert call["params"][3] == 260
    assert call["max_rows"] == 260
    assert call["scope"].date_from == date(2025, 9, 29)
    assert call["scope"].date_to == date(2026, 9, 29)


def test_dashboard_ranking_page_uses_snapshot_key_and_hard_row_cap():
    probe = _ReadProbe([
        [{
            "activity_snapshot_id": "activity-snapshot",
            "trade_date": date(2026, 9, 24),
            "data_baseline_id": "market-activity-20260924",
            "quality_status": "PARTIAL_HISTORY",
            "total_volume": 10,
            "total_amount": 20,
            "top_n": 20,
            "top_n_volume": 2,
            "top_n_amount": 5,
            "top_n_volume_share": 0.2,
            "top_n_amount_share": 0.25,
            "input_row_count": 100,
            "target_row_count": 95,
            "invalid_target_row_count": 0,
            "invalid_history_row_count": 0,
            "insufficient_history_stock_count": 5,
            "observation_snapshot_id": None,
        }],
        [{"ts_code": f"000{i:03}.SZ", "amount": 10-i, "amount_rank": i+1}
         for i in range(101)],
    ])
    repository = MarketActivityDashboardRepository(SimpleNamespace(read=probe))

    result = repository.frozen_day(date(2026, 9, 24), _context(), view="amount")

    assert result["truncated"] is True
    assert len(result["rows"]) == 100
    assert result["summary"]["data_baseline_id"] == "market-activity-20260924"
    query = probe.calls[1]
    assert "activity_snapshot_id=%s AND trade_date=%s" in query["sql"]
    assert "ORDER BY amount_rank, ts_code" in query["sql"]
    assert query["params"] == ("activity-snapshot", date(2026, 9, 24), 101)
    assert query["max_rows"] == 101


def test_anomaly_view_returns_tag_evidence_for_frozen_observations():
    probe = _ReadProbe([
        [{
            "activity_snapshot_id": "activity-snapshot",
            "trade_date": date(2026, 9, 24),
            "data_baseline_id": "market-activity-20260924",
            "quality_status": "VALID",
            "observation_snapshot_id": "observation-snapshot",
            "total_amount": 1,
            "total_volume": 2,
            "top_n": 20,
            "top_n_amount_share": 0.1,
            "top_n_volume_share": 0.2,
            "target_row_count": 10,
            "input_row_count": 10,
            "invalid_target_row_count": 0,
            "invalid_history_row_count": 0,
            "insufficient_history_stock_count": 0,
        }],
        [{"observation_id": "00000000-0000-0000-0000-000000000001",
          "ts_code": "600000.SH", "activity_amount_ratio_previous": 4.2,
          "activity_volume_ratio_previous": 3.1}],
        [{"observation_id": "00000000-0000-0000-0000-000000000001",
          "tag_code": "SUDDEN_ACTIVITY",
          "tag_payload": {"actual_value": 3.1, "threshold": 2.0,
                          "source_metric": "volume_ratio_previous"}}],
    ])
    repository = MarketActivityDashboardRepository(SimpleNamespace(read=probe))

    result = repository.frozen_day(date(2026, 9, 24), _context(), view="anomaly")

    assert result["observation_available"] is True
    assert result["rows"][0]["tags"][0]["code"] == "SUDDEN_ACTIVITY"
    assert result["rows"][0]["tags"][0]["evidence"]["threshold"] == 2.0
    assert "anomaly_flag IS TRUE" in probe.calls[1]["sql"]
    assert "ANY(%s::uuid[])" in probe.calls[2]["sql"]
    assert probe.calls[2]["params"][1:3] == (
        "observation-snapshot", date(2026, 9, 24),
    )


@pytest.mark.parametrize("view", ["all", "raw_sql", ""])
def test_dashboard_rejects_unknown_view_before_database_access(view):
    probe = _ReadProbe()
    repository = MarketActivityDashboardRepository(SimpleNamespace(read=probe))

    with pytest.raises(ValueError, match="unsupported market activity view"):
        repository.frozen_day(date(2026, 9, 24), _context(), view=view)

    assert probe.calls == []


def test_dashboard_rejects_future_date_without_database_access():
    probe = _ReadProbe()
    repository = MarketActivityDashboardRepository(SimpleNamespace(read=probe))

    with pytest.raises(ValueError, match="future"):
        repository.frozen_day(date.today() + timedelta(days=1), _context())

    assert probe.calls == []


def test_dashboard_static_page_has_no_external_script_or_chart_dependency():
    from pathlib import Path

    page = Path("data_collect/web/static/market_activity_dashboard.html").read_text(encoding="utf-8")
    assert "/api/market-activity/day" in page
    assert "data-view=\"anomaly\"" in page
    assert "https://" not in page
    assert "net funds" in page.lower() or "净资金流入" in page
