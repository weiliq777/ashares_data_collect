"""Manual, read-only live probe for one Market Activity stock detail.

Run explicitly with:
    python -m pytest tests/manual_market_activity_detail_probe.py -s

This file is intentionally not collected by the normal ``test_*.py`` suite.
It exercises one recent frozen observation and never writes database rows.
"""
from __future__ import annotations

from datetime import date
import json
import math

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_activity_dashboard_repository import (
    MarketActivityDashboardRepository,
)
from data_collect.data_access.repository.market_activity_detail_probe_repository import (
    MarketActivityDetailProbeRepository,
)


_PROBE_CONTEXT = RunContext(
    run_id="manual-market-activity-detail-probe",
    job_name="manual_market_activity_detail_probe",
    code_version="uncommitted-probe",
    config_hash="read-only-probe",
    pipeline_version="data-access-v1",
)
_DUMMY_ACTIVITY_ID = "00000000-0000-0000-0000-000000000101"
_DUMMY_OBSERVATION_ID = "00000000-0000-0000-0000-000000000102"
_LARGE_RELATION_TOKENS = (
    "daily_kline",
    "stock_activity_daily",
    "stock_anomaly_daily",
    "price_daily_v2",
    "company_financial",
    "security_lifecycle",
)


def _plan_nodes(value):
    if isinstance(value, dict):
        if "Node Type" in value:
            yield value
        for nested in value.values():
            yield from _plan_nodes(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _plan_nodes(nested)


def _assert_no_large_sequential_scan(name: str, plan: dict) -> None:
    bad = []
    for node in _plan_nodes(plan):
        relation = str(node.get("Relation Name", "")).lower()
        node_type = str(node.get("Node Type", ""))
        if (
            node_type == "Seq Scan"
            and any(token in relation for token in _LARGE_RELATION_TOKENS)
        ):
            bad.append({
                "relation": relation,
                "plan_rows": node.get("Plan Rows"),
                "plan_width": node.get("Plan Width"),
            })
    assert not bad, f"{name} has a sequential scan on a large relation: {bad}"


class TestOneLiveMarketActivityDetail:
    """Explicitly invoked integration probe; never part of routine CI collection."""

    def test_latest_frozen_anomaly_has_bounded_detail_inputs(self):
        kernel = DataAccessKernel(policy_registry=build_default_registry())
        try:
            dashboard = MarketActivityDashboardRepository(kernel)
            probe = MarketActivityDetailProbeRepository(kernel)

            # Explain the existing date/ranking query shapes before selecting a sample.
            discovery_plans = dashboard.explain_read_paths(
                date.today(),
                _PROBE_CONTEXT,
                activity_snapshot_id=_DUMMY_ACTIVITY_ID,
                observation_snapshot_id=_DUMMY_OBSERVATION_ID,
            )
            for name, plan in discovery_plans.items():
                _assert_no_large_sequential_scan(f"discovery.{name}", plan)

            dates = dashboard.available_dates(date.today(), _PROBE_CONTEXT, days=60)
            assert dates, "no frozen Market Activity date was found in the bounded 60-day window"
            trade_date = dates[0]
            day = dashboard.frozen_day(trade_date, _PROBE_CONTEXT, view="anomaly")
            rows = day["rows"]
            if not rows:
                day = dashboard.frozen_day(trade_date, _PROBE_CONTEXT, view="amount")
                rows = day["rows"]
            assert rows, f"no stock row exists in the latest frozen activity snapshot for {trade_date}"

            sample = rows[0]
            ts_code = sample["ts_code"]
            activity_snapshot_id = str(day["summary"]["activity_snapshot_id"])

            # Explain every existing detail query before reading the selected stock.
            detail_plans = dashboard.explain_stock_detail_paths(
                activity_snapshot_id, trade_date, ts_code, _PROBE_CONTEXT,
            )
            for name, plan in detail_plans.items():
                _assert_no_large_sequential_scan(f"detail.{name}", plan)

            detail = dashboard.stock_detail(
                activity_snapshot_id, trade_date, ts_code, _PROBE_CONTEXT,
            )
            activity = detail["observation_context"]["activity"]
            methodology_hash = detail["observation_context"]["methodology_hash"]

            calendar_plan = probe.explain_recent_trading_dates(trade_date, _PROBE_CONTEXT)
            _assert_no_large_sequential_scan("probe.recent_trading_dates", calendar_plan)
            recent_dates = probe.recent_trading_dates(trade_date, _PROBE_CONTEXT)
            assert trade_date in recent_dates, "the selected observation date is missing from its own frozen series"
            trend_plan = probe.explain_recent_ratio_series(
                ts_code, recent_dates, methodology_hash, _PROBE_CONTEXT,
            )
            _assert_no_large_sequential_scan("probe.recent_ratio_series", trend_plan)
            ratio_rows = probe.recent_ratio_series(
                ts_code, recent_dates, methodology_hash, _PROBE_CONTEXT,
            )

            history_rows = detail["price_activity_history"]["rows"]
            standard_ratio_history = detail["activity_ratio_history"]
            standard_ratio_points = standard_ratio_history["rows"]
            price_behavior = detail["price_behavior"]
            report = {
                "sample": {"trade_date": trade_date, "ts_code": ts_code},
                "frozen_activity": {
                    key: activity.get(key) for key in (
                        "amount", "volume", "amount_share", "volume_share",
                        "amount_rank", "volume_rank", "amount_ratio_previous",
                        "volume_ratio_previous", "history_observation_count",
                        "calculation_status",
                    )
                },
                "quality_status": {
                    "activity_snapshot": detail["observation_context"]["quality_status"],
                    "stock_row_inherited_snapshot_quality": detail["observation_context"]["stock_quality_status"],
                    "valuation": detail["security_context"]["valuation"].get("quality_status"),
                    "industry": detail["security_context"]["industry_observation"].get("quality_status"),
                },
                "target_day_price": price_behavior,
                "price_source": "repository response from std.daily_kline + feature.price_daily_v2; current context, not frozen OHLC",
                "ratio_history": {
                    "frozen_capture": {
                        "source_semantics": "FROZEN_MARKET_ACTIVITY_SNAPSHOTS",
                        "requested_dates": len(recent_dates),
                        "returned_rows": len(ratio_rows),
                        "missing_trading_dates": sorted(set(recent_dates) - {row["trade_date"] for row in ratio_rows}),
                        "status": (
                            "COMPLETE_10"
                            if len(recent_dates) == 10
                            and {row["trade_date"] for row in ratio_rows} == set(recent_dates)
                            else "PARTIAL_CAPTURE"
                        ),
                        "rows": ratio_rows,
                    },
                    "current_standard_context": {
                        "source_semantics": standard_ratio_history["source_semantics"],
                        "requested_dates": len(recent_dates),
                        "returned_points": standard_ratio_history["returned_rows"],
                        "valid_rows": standard_ratio_history["valid_rows"],
                        "status": standard_ratio_history["source_status"],
                        "rows": standard_ratio_points,
                    },
                    "target_reconciliation": standard_ratio_history["frozen_target_comparison"],
                    # Top-level aliases retained for the original manual probe output.
                    "requested_dates": len(recent_dates),
                    "returned_rows": len(ratio_rows),
                    "missing_trading_dates": sorted(set(recent_dates) - {row["trade_date"] for row in ratio_rows}),
                    "status": (
                        "COMPLETE_10"
                        if len(recent_dates) == 10
                        and {row["trade_date"] for row in ratio_rows} == set(recent_dates)
                        else "PARTIAL_CAPTURE"
                    ),
                    "rows": ratio_rows,
                },
                "detail_context": {
                    "identity_quality": detail.get("identity", {}).get("name_quality_status"),
                    "is_st_asof": detail.get("security_context", {}).get("tradeability", {}).get("is_st"),
                    "valuation_quality": detail.get("security_context", {}).get("valuation", {}).get("quality_status"),
                    "industry_quality": detail.get("security_context", {}).get("industry_observation", {}).get("quality_status"),
                    "prior_price_rows": detail["price_activity_history"]["returned_rows"],
                    "subsequent_rows_retrospective_only": detail["subsequent_path"]["returned_rows"],
                    "baseline": detail["observation_context"]["data_baseline_id"],
                },
                "plan_summaries": {
                    "detail_reads": {
                        name: probe.plan_summary(plan)
                        for name, plan in detail_plans.items()
                    },
                    "recent_trading_dates": probe.plan_summary(calendar_plan),
                    "recent_ratio_series": probe.plan_summary(trend_plan),
                },
            }
            print(json.dumps(report, ensure_ascii=False, default=str, indent=2))

            assert price_behavior["bar"] is not None, "the frozen activity row has no matching standard daily bar"
            assert price_behavior["bar"]["trade_date"] == trade_date
            for field in ("open", "high", "low", "close"):
                assert price_behavior["bar"][field] is not None, f"target-day OHLC field {field} is missing"
            assert price_behavior["bar"]["low"] <= price_behavior["bar"]["close"] <= price_behavior["bar"]["high"]
            assert math.isclose(
                float(price_behavior["bar"]["close"]), float(activity["close"]), rel_tol=1e-9,
            ), "standard close differs from the frozen Market Activity close"
            target_ratio_row = next(row for row in ratio_rows if row["trade_date"] == trade_date)
            assert math.isclose(
                float(target_ratio_row["amount_ratio_previous"]),
                float(activity["amount_ratio_previous"]), rel_tol=1e-9,
            )
            assert math.isclose(
                float(target_ratio_row["volume_ratio_previous"]),
                float(activity["volume_ratio_previous"]), rel_tol=1e-9,
            )
            assert len(ratio_rows) <= 10
            assert len(standard_ratio_points) <= 10
            assert standard_ratio_history["source_semantics"].startswith("CURRENT_STANDARD_CONTEXT")
            assert standard_ratio_history["frozen_target_comparison"]["status"] in {
                "MATCHING_VALUES", "DIFFERS_BY_SOURCE_OR_REVISION",
                "NOT_COMPARABLE_INSUFFICIENT_HISTORY", "NOT_COMPARABLE_INVALID_BAR",
                "NOT_COMPARABLE_NO_STOCK_BAR",
            }
            assert [row["trade_date"] for row in ratio_rows] == sorted(
                {row["trade_date"] for row in ratio_rows}
            )
            assert detail["subsequent_path"]["classification"] == "RETROSPECTIVE_ONLY"
        finally:
            kernel.close()
