from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from data_collect.data_access.context import RunContext
from data_collect.data_access.repository.market_activity_dashboard_repository import (
    MarketActivityDashboardRepository,
)
from data_collect.observation.market_activity_rules import MarketActivityObservationPolicy


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


def test_stock_detail_is_snapshot_bound_bounded_and_keeps_retro_data_separate():
    observation_id = "00000000-0000-0000-0000-000000000101"
    probe = _ReadProbe([
        [{
            "activity_snapshot_id": "00000000-0000-0000-0000-000000000201",
            "activity_status": "FROZEN",
            "methodology_hash": "m" * 64,
            "data_baseline_id": "market-activity-20260924",
            "input_manifest_hash": "i" * 64,
            "release_status": "validated",
            "baseline_cutoff_date": date(2026, 9, 24),
            "trade_date": date(2026, 9, 24),
            "activity_quality_status": "VALID",
            "stock_quality_status": "VALID",
            "ts_code": "600000.SH",
            "close": 10.5,
            "volume": 1000,
            "amount": 10500,
            "amount_ratio_previous": 2.5,
            "volume_ratio_previous": 3.0,
            "history_observation_count": 20,
            "observation_snapshot_id": "00000000-0000-0000-0000-000000000301",
            "observation_status": "FROZEN",
            "observation_id": observation_id,
            "primary_state": "SUDDEN_ACTIVITY",
        }],
        [{"tag_code": "SUDDEN_ACTIVITY", "tag_payload": {"actual_value": 3.0, "threshold": 2.0}}],
        [{"ts_code": "600000.SH", "name": "浦发银行", "name_quality_status": "VALID"}],
        [{"trade_date": date(2026, 9, 23), "close": 10.4, "volume": 900, "amount": 9360}],
        [{"ts_code": "600000.SH", "trade_date": date(2026, 9, 24),
          "open": 10.4, "high": 10.8, "low": 10.2, "close": 10.5,
          "volume": 1000, "amount": 10500}],
        [{"ts_code": "600000.SH", "trade_date": date(2026, 9, 24),
          "return_1d": 0.01, "adjustment_mode": "point_in_time_total_return",
          "source_asof_date": date(2026, 9, 24), "quality_status": "VALID"}],
        [{"trade_date": date(2026, 9, 25), "close": 10.6, "volume": 1200, "amount": 12720}],
        [{"is_st": False, "is_suspended": False, "is_tradeable": True, "quality_status": "VALID"}],
        [{"pe_ttm": 5.0, "data_available_timestamp": "2026-09-24T14:50:00+08:00", "quality_status": "VALID"}],
        [{"report_date": date(2026, 6, 30), "roe_ttm": 0.1, "quality_status": "VALID"}],
        [],
    ])
    repository = MarketActivityDashboardRepository(SimpleNamespace(read=probe))

    result = repository.stock_detail(
        "00000000-0000-0000-0000-000000000201",
        date(2026, 9, 24),
        "600000.SH",
        _context(),
    )

    assert result["identity"]["name"] == "浦发银行"
    assert result["observation_context"]["activity"]["volume_ratio_previous"] == 3.0
    assert result["observation_context"]["observation_snapshot_id"] == "00000000-0000-0000-0000-000000000301"
    assert result["observation_context"]["tags"][0]["code"] == "SUDDEN_ACTIVITY"
    assert result["price_activity_history"]["rows"][0]["trade_date"] == date(2026, 9, 23)
    assert result["price_activity_history"]["source_status"] == "PARTIAL_HISTORY"
    assert result["price_behavior"]["bar"]["open"] == 10.4
    assert result["price_behavior"]["return_1d"] == 0.01
    assert result["price_behavior"]["amplitude_pct_raw_price"] == pytest.approx(0.6 / 10.4 * 100)
    assert result["price_behavior"]["close_position"] == pytest.approx(0.5)
    ratio_history = result["activity_ratio_history"]
    assert ratio_history["source_semantics"].startswith("CURRENT_STANDARD_CONTEXT")
    assert len(ratio_history["rows"]) == 2
    assert ratio_history["rows"][-1]["calculation_status"] == "INSUFFICIENT_HISTORY"
    assert ratio_history["frozen_target_comparison"]["status"] == "NOT_COMPARABLE_INSUFFICIENT_HISTORY"
    assert result["subsequent_path"]["classification"] == "RETROSPECTIVE_ONLY"
    assert result["subsequent_path"]["rows"][0]["trade_date"] == date(2026, 9, 25)
    assert result["subsequent_path"]["source_status"] == "AVAILABLE"
    assert result["security_context"]["industry_observation"]["quality_status"] == "UNKNOWN"
    assert "feature.company_financial_v2" in result["provenance"]["standard_context"]["source"]
    assert "pit.industry_membership_daily" in result["provenance"]["standard_context"]["source"]
    assert len(probe.calls) == 11
    assert probe.calls[0]["params"] == (
        MarketActivityObservationPolicy().methodology_hash,
        "00000000-0000-0000-0000-000000000201",
        date(2026, 9, 24),
        "600000.SH",
    )
    assert all(call["max_rows"] <= 101 for call in probe.calls)
    history_call = probe.calls[3]
    assert "stock_code=%s AND trade_date>=%s AND trade_date<%s" in history_call["sql"]
    assert history_call["scope"].codes == ("600000.SH",)
    assert history_call["max_rows"] == 60
    current_bar_call = probe.calls[4]
    assert current_bar_call["params"] == ("600000.SH", date(2026, 9, 24))
    assert "stock_code=%s AND trade_date=%s" in current_bar_call["sql"]
    assert current_bar_call["scope"].codes == ("600000.SH",)
    assert current_bar_call["max_rows"] == 1
    return_feature_call = probe.calls[5]
    assert return_feature_call["params"] == ("600000.SH", date(2026, 9, 24))
    assert "trade_date=%s" in return_feature_call["sql"]
    assert return_feature_call["scope"].codes == ("600000.SH",)
    assert return_feature_call["max_rows"] == 1
    assert probe.calls[9]["params"][-1].hour == 15


@pytest.mark.parametrize("snapshot_id,code", [
    ("not-a-uuid", "600000.SH"),
    ("00000000-0000-0000-0000-000000000201", "600000.SH; DROP TABLE x"),
])
def test_stock_detail_rejects_invalid_identity_before_database_access(snapshot_id, code):
    probe = _ReadProbe()
    repository = MarketActivityDashboardRepository(SimpleNamespace(read=probe))

    with pytest.raises(ValueError):
        repository.stock_detail(snapshot_id, date(2026, 9, 24), code, _context())

    assert probe.calls == []


def test_stock_detail_missing_frozen_snapshot_stops_before_context_queries():
    probe = _ReadProbe([[]])
    repository = MarketActivityDashboardRepository(SimpleNamespace(read=probe))

    with pytest.raises(LookupError, match="not found"):
        repository.stock_detail(
            "00000000-0000-0000-0000-000000000201",
            date(2026, 9, 24), "600000.SH", _context(),
        )

    assert len(probe.calls) == 1


def test_dashboard_static_page_has_no_external_script_or_chart_dependency():
    from pathlib import Path

    page = Path("data_collect/web/static/market_activity_dashboard.html").read_text(encoding="utf-8")
    assert "/api/market-activity/day" in page
    assert "data-view=\"anomaly\"" in page
    assert "https://" not in page
    assert "net funds" in page.lower() or "净资金流入" in page
    assert "/api/market-activity/stock-detail" in page
    assert "data-ts-code" in page
    assert "观察日之后走势 · 事后信息" in page
    assert "最近10个个股有效交易日量额倍数趋势" in page
    assert "当前标准层上下文" in page


def test_stock_detail_explain_surface_covers_each_fixed_read_shape_without_analyze():
    class ExplainProbe:
        def __init__(self):
            self.calls = []

        def _repository_explain(self, *args):
            self.calls.append(args)
            return {"Plan": {"Node Type": "Index Scan"}}

    probe = ExplainProbe()
    repository = MarketActivityDashboardRepository(SimpleNamespace(read=probe))

    plans = repository.explain_stock_detail_paths(
        "00000000-0000-0000-0000-000000000201",
        date(2026, 9, 24), "600000.SH", _context(),
    )

    assert set(plans) == {
        "frozen_observation", "identity", "prior_price_history",
        "current_day_bar", "return_feature",
        "subsequent_price_path", "tradeability", "valuation",
        "financial_pit", "industry_observation", "anomaly_tags",
    }
    assert len(probe.calls) == 11
    for _, _, sql, _, scope in probe.calls:
        assert "ANALYZE" not in sql.upper()
        assert scope.codes == ("600000.SH",)
        assert scope.date_from is not None and scope.date_to is not None
    history_sql = probe.calls[2][2]
    assert "stock_code=%s AND trade_date>=%s AND trade_date<%s" in history_sql
    assert "LIMIT %s" in history_sql
