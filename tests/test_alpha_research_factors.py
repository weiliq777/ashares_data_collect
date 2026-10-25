import math
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from data_collect.alpha_research.contracts import SOURCE_QUALITY_EVIDENCE_VERSION
from data_collect.alpha_research.factors import factor_value


def _financial_row(**overrides):
    row = {
        "research_date": date(2025, 8, 18),
        "financial_feature_contract_version": "feature-contract-v2.0",
        "financial_data_available_timestamp": datetime(
            2025, 8, 18, 14, 0, tzinfo=ZoneInfo("Asia/Shanghai"),
        ),
        "financial_vintage_quality": "SOURCE_ANNOUNCEMENT_ASSUMED",
        "financial_component_lineage": {
            "__quality_evidence__": {"version": SOURCE_QUALITY_EVIDENCE_VERSION},
            "income:key:1": {
                "payload_hash": "a" * 64,
                "source_quality_status": "VALID",
                "used_by": ["roe_ttm", "revenue_growth_yoy", "profit_growth_yoy"],
            },
        },
        "roe_ttm": 0.15,
        "roe_quality_status": "VALID",
        "ttm_complete": False,
        "revenue_growth_yoy": 0.3,
        "profit_growth_yoy": 0.2,
        "growth_quality_status": "VALID",
    }
    row.update(overrides)
    return row


def _debt_to_assets_row(**overrides):
    row = {
        "research_date": date(2025, 8, 18),
        "debt_to_assets": 8.619,
        "debt_to_assets_quality_status": "VALID",
        "debt_to_assets_quality_reason": None,
        "debt_to_assets_data_available_timestamp": datetime(
            2025, 8, 18, 14, 0, tzinfo=ZoneInfo("Asia/Shanghai"),
        ),
        "debt_to_assets_source_observed_at": datetime(2025, 8, 17, 12, 0),
        "debt_to_assets_vintage_quality": "SOURCE_ANNOUNCEMENT_ASSUMED",
        "debt_to_assets_source_quality_status": "VALID",
        "debt_to_assets_source_record_key": "balance:000001.SZ:2025-06-30",
        "debt_to_assets_source_record_version": 42,
        "debt_to_assets_source_payload_hash": "a" * 64,
        "debt_to_assets_feature_contract_version": "debt-to-assets-v1",
        "debt_to_assets_report_date": date(2025, 6, 30),
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
        **_debt_to_assets_row(),
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
    assert factor_value("debt_to_assets", row).value == 8.619
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


def test_financial_factors_fail_closed_when_source_quality_evidence_is_absent():
    row = _financial_row(
        financial_component_lineage={"income:key:1": {"payload_hash": "a" * 64}},
    )

    value = factor_value("revenue_growth_yoy", row)

    assert value.status == "INVALID"
    assert value.reason == "FINANCIAL_SOURCE_QUALITY_EVIDENCE_MISSING"


def test_financial_factor_checks_quality_only_for_its_declared_source_components():
    row = _financial_row(
        financial_component_lineage={
            "__quality_evidence__": {"version": SOURCE_QUALITY_EVIDENCE_VERSION},
            "income:key:1": {
                "payload_hash": "a" * 64,
                "source_quality_status": "VALID",
                "used_by": ["revenue_growth_yoy", "profit_growth_yoy"],
            },
            "cashflow:key:1": {
                "payload_hash": "b" * 64,
                "source_quality_status": "WARNING",
                "used_by": ["roe_ttm"],
            },
        },
    )

    growth = factor_value("revenue_growth_yoy", row)
    roe = factor_value("roe_ttm", row)

    assert growth.status == "VALID"
    assert growth.value == 0.3
    assert roe.status == "INVALID"
    assert roe.reason == "FINANCIAL_SOURCE_COMPONENT_QUALITY_NOT_VALID"


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


def test_debt_to_assets_uses_only_balance_source_quality_and_pit_evidence():
    value = factor_value("debt_to_assets", _debt_to_assets_row())
    assert value.status == "VALID"
    assert value.value == 8.619
    assert value.reason == "PIT_SOURCE_ANNOUNCEMENT_ASSUMED"

    warning = factor_value(
        "debt_to_assets",
        _debt_to_assets_row(
            debt_to_assets_quality_status="WARNING",
            debt_to_assets_quality_reason="SOURCE_QUALITY_NOT_VALID",
            debt_to_assets_source_quality_status="WARNING",
        ),
    )
    assert warning.status == "INVALID"
    assert warning.reason == "SOURCE_QUALITY_NOT_VALID"

    late = factor_value(
        "debt_to_assets",
        _debt_to_assets_row(
            debt_to_assets_data_available_timestamp=datetime(
                2025, 8, 18, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai"),
            ),
        ),
    )
    assert late.reason == "DEBT_TO_ASSETS_NOT_AVAILABLE_BY_SIGNAL_CLOSE"

    missing_identity = factor_value(
        "debt_to_assets", _debt_to_assets_row(debt_to_assets_source_record_key=None),
    )
    assert missing_identity.reason == "DEBT_TO_ASSETS_SOURCE_IDENTITY_INCOMPLETE"

    wrong_contract = factor_value(
        "debt_to_assets",
        _debt_to_assets_row(debt_to_assets_feature_contract_version="feature-contract-v2.0"),
    )
    assert wrong_contract.reason == "DEBT_TO_ASSETS_FEATURE_VERSION_NOT_SUPPORTED"

    warehouse_late = factor_value(
        "debt_to_assets",
        _debt_to_assets_row(
            debt_to_assets_vintage_quality="WAREHOUSE_OBSERVED_ONLY",
            debt_to_assets_source_observed_at=datetime(
                2025, 8, 18, 15, 1, tzinfo=ZoneInfo("Asia/Shanghai"),
            ),
        ),
    )
    assert warehouse_late.reason == "DEBT_TO_ASSETS_SOURCE_NOT_OBSERVED_BY_SIGNAL_CLOSE"

    future_report = factor_value(
        "debt_to_assets",
        _debt_to_assets_row(debt_to_assets_report_date=date(2025, 8, 19)),
    )
    assert future_report.reason == "DEBT_TO_ASSETS_REPORT_DATE_AFTER_SIGNAL"


@pytest.mark.parametrize(
    ("source_record_version", "source_payload_hash", "expected_reason"),
    (
        (None, "a" * 64, "DEBT_TO_ASSETS_SOURCE_IDENTITY_INCOMPLETE"),
        (1, "z" * 64, "DEBT_TO_ASSETS_SOURCE_IDENTITY_INVALID"),
    ),
)
def test_debt_to_assets_rejects_incomplete_or_invalid_source_identity(
    source_record_version, source_payload_hash, expected_reason,
):
    value = factor_value(
        "debt_to_assets",
        _debt_to_assets_row(
            debt_to_assets_source_record_version=source_record_version,
            debt_to_assets_source_payload_hash=source_payload_hash,
        ),
    )

    assert value.status == "INVALID"
    assert value.reason == expected_reason


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
