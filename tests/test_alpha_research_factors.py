import math
from datetime import date, datetime
from zoneinfo import ZoneInfo

from data_collect.alpha_research.factors import factor_value


def _financial_row(**overrides):
    row = {
        "research_date": date(2025, 8, 18),
        "financial_feature_contract_version": "feature-contract-v2.0",
        "financial_data_available_timestamp": datetime(
            2025, 8, 18, 14, 0, tzinfo=ZoneInfo("Asia/Shanghai"),
        ),
        "financial_vintage_quality": "SOURCE_ANNOUNCEMENT_ASSUMED",
        "financial_component_lineage": {"income:key:1": {"payload_hash": "a" * 64}},
        "roe_ttm": 0.15,
        "roe_quality_status": "VALID",
        "ttm_complete": False,
        "revenue_growth_yoy": 0.3,
        "profit_growth_yoy": 0.2,
        "growth_quality_status": "VALID",
    }
    row.update(overrides)
    return row


def test_factor_quality_gates_and_directional_inputs_are_explicit():
    row = {
        "pe_pct_5y": 0.2,
        "is_pe_meaningful": True,
        "has_full_pe_5y_history": True,
        "pe_ttm": 12,
        "roe_ttm": 0.15,
        "ttm_complete": True,
        "roe_quality_status": "VALID",
        "debt_to_assets": 0.35,
        "revenue_growth_yoy": 0.3,
        "profit_growth_yoy": 0.2,
        "growth_quality_status": "VALID",
        "return_20d": 0.08,
        "return_60d": 0.12,
        "price_feature_version": "price_daily_v2",
        "price_adjustment_mode": "point_in_time_total_return",
        "adj_close_base": 110,
        "ma60": 100,
        "volume_ratio_20d": 1.5,
    }
    assert factor_value("pe_percentile_5y", row).value == 0.2
    assert factor_value("roe_ttm", _financial_row()).value == 0.15
    assert factor_value("revenue_growth_yoy", _financial_row()).value == 0.3
    assert abs(factor_value("close_vs_ma60", row).value - 0.1) < 1e-12
    assert factor_value("return_20d", row).value == 0.08
    assert factor_value("volume_ratio_20d", row).status == "VALID"

    row["has_full_pe_5y_history"] = False
    assert factor_value("pe_percentile_5y", row).reason == "PE_HISTORY_INCOMPLETE"
    assert factor_value("roe_ttm", _financial_row()).status == "VALID"
    assert factor_value("roe_ttm", {"roe_ttm": 0.1}).status == "UNAVAILABLE"


def test_growth_factors_use_their_own_value_when_the_shared_status_is_downgraded():
    row = _financial_row(
        growth_quality_status="INSUFFICIENT_HISTORY",
        profit_growth_yoy=None,
    )

    revenue = factor_value("revenue_growth_yoy", row)
    profit = factor_value("profit_growth_yoy", row)

    assert revenue.status == "VALID"
    assert revenue.value == 0.3
    assert revenue.reason == "SIBLING_GROWTH_FACTOR_INSUFFICIENT"
    assert profit.status == "MISSING"
    assert profit.reason == "GROWTH_VALUE_MISSING_OR_NONFINITE"


def test_financial_factors_fail_closed_on_unverified_pit_evidence():
    row = _financial_row()
    assert factor_value(
        "revenue_growth_yoy",
        {**row, "growth_quality_status": None},
    ).reason == "GROWTH_QUALITY_STATUS_NOT_VALID"
    assert factor_value(
        "revenue_growth_yoy",
        {**row, "financial_feature_contract_version": "unknown"},
    ).reason == "FINANCIAL_FEATURE_VERSION_NOT_SUPPORTED"
    assert factor_value(
        "revenue_growth_yoy",
        {**row, "financial_data_available_timestamp": datetime(
            2025, 8, 18, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai"),
        )},
    ).reason == "FINANCIAL_DATA_NOT_AVAILABLE_BY_SIGNAL_CLOSE"
    assert factor_value(
        "roe_ttm",
        {**row, "financial_vintage_quality": "WAREHOUSE_OBSERVED_ONLY"},
    ).reason == "FINANCIAL_PIT_EVIDENCE_NOT_SOURCE_ANNOUNCEMENT"
    assert factor_value(
        "roe_ttm",
        {**row, "financial_component_lineage": {}},
    ).reason == "FINANCIAL_COMPONENT_LINEAGE_MISSING"


def test_roe_uses_its_own_quality_status_without_global_ttm_gate():
    value = factor_value(
        "roe_ttm",
        _financial_row(ttm_complete=False, roe_quality_status="VALID"),
    )
    assert value.status == "VALID"
    assert value.value == 0.15

    invalid = factor_value(
        "roe_ttm",
        _financial_row(roe_quality_status="NEGATIVE_OR_ZERO_EQUITY"),
    )
    assert invalid.status == "INVALID"
    assert invalid.reason == "ROE_QUALITY_STATUS_NOT_VALID"


def test_momentum_requires_the_frozen_point_in_time_total_return_coordinate():
    row = {
        "return_20d": 0.08,
        "return_60d": 0.12,
        "price_feature_version": "price_daily_v2",
        "price_adjustment_mode": "point_in_time_total_return",
    }
    assert factor_value("return_20d", row).value == 0.08
    assert factor_value("return_60d", row).value == 0.12

    wrong_coordinate = factor_value(
        "return_20d", {**row, "price_adjustment_mode": "adj_close_base"},
    )
    assert wrong_coordinate.status == "INVALID"
    assert wrong_coordinate.reason == "PRICE_COORDINATE_NOT_SUPPORTED"
    assert math.isclose(factor_value(
        "close_vs_ma60", {
            "adj_close_base": 110,
            "ma60": 100,
            "price_feature_version": "price_daily_v2",
            "price_adjustment_mode": "point_in_time_total_return",
        },
    ).value, 0.1)
    assert factor_value(
        "close_vs_ma60", {
            "adj_close_base": 110,
            "ma60": 100,
            "price_feature_version": "price_daily_v1",
            "price_adjustment_mode": "point_in_time_total_return",
        },
    ).reason == "PRICE_COORDINATE_NOT_SUPPORTED"


def test_invalid_valuation_and_nonfinite_values_are_not_imputed():
    assert factor_value("pe_ttm", {"pe_ttm": -4, "is_pe_meaningful": True}).status == "INVALID"
    assert factor_value("debt_to_assets", {"debt_to_assets": float("nan")}).value is None
    assert factor_value("pe_percentile_5y", {
        "pe_pct_5y": 1.1,
        "is_pe_meaningful": True,
        "has_full_pe_5y_history": True,
    }).reason == "PE_PERCENTILE_OUT_OF_RANGE"
