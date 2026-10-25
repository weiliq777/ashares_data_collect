"""Pure PIT factor extraction with explicit missing-data reasons."""
from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from data_collect.alpha_research.contracts import (
    DEBT_TO_ASSETS_FEATURE_CONTRACT_VERSION,
    FACTOR_BY_NAME,
    SOURCE_QUALITY_EVIDENCE_VERSION,
    FactorSpec,
)


@dataclass(frozen=True, slots=True)
class FactorValue:
    factor_name: str
    value: float | None
    status: str
    reason: str | None = None


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _truth(value: Any) -> bool:
    return value is True or str(value).strip().lower() in {"true", "t", "1", "yes"}


def _quality_is_valid(value: Any) -> bool:
    return str(value or "").strip().upper() in {"VALID", "PASS"}


def _financial_pit_failure(row: Mapping[str, Any]) -> str | None:
    if row.get("financial_feature_contract_version") != "feature-contract-v2.0":
        return "FINANCIAL_FEATURE_VERSION_NOT_SUPPORTED"

    vintage = str(row.get("financial_vintage_quality") or "").strip().upper()
    if vintage != "SOURCE_ANNOUNCEMENT_ASSUMED":
        return "FINANCIAL_PIT_EVIDENCE_NOT_SOURCE_ANNOUNCEMENT"

    lineage = row.get("financial_component_lineage")
    if isinstance(lineage, str):
        try:
            lineage = json.loads(lineage)
        except (TypeError, ValueError, json.JSONDecodeError):
            lineage = None
    if not isinstance(lineage, Mapping) or not lineage:
        return "FINANCIAL_COMPONENT_LINEAGE_MISSING"
    evidence = lineage.get("__quality_evidence__")
    if (
        not isinstance(evidence, Mapping)
        or evidence.get("version") != SOURCE_QUALITY_EVIDENCE_VERSION
    ):
        return "FINANCIAL_SOURCE_QUALITY_EVIDENCE_MISSING"

    signal_day = row.get("research_date")
    if isinstance(signal_day, datetime):
        signal_day = signal_day.date()
    elif not isinstance(signal_day, date):
        try:
            signal_day = date.fromisoformat(str(signal_day)[:10])
        except (TypeError, ValueError):
            return "SIGNAL_DATE_MISSING_OR_INVALID"

    available_at = row.get("financial_data_available_timestamp")
    if isinstance(available_at, str):
        try:
            available_at = datetime.fromisoformat(available_at)
        except ValueError:
            return "FINANCIAL_AVAILABILITY_TIMESTAMP_INVALID"
    if not isinstance(available_at, datetime):
        return "FINANCIAL_AVAILABILITY_TIMESTAMP_MISSING"
    if available_at.tzinfo is None or available_at.utcoffset() is None:
        return "FINANCIAL_AVAILABILITY_TIMEZONE_MISSING"

    shanghai = ZoneInfo("Asia/Shanghai")
    signal_close = datetime.combine(signal_day, time(15, 0), tzinfo=shanghai)
    if available_at.astimezone(shanghai) >= signal_close:
        return "FINANCIAL_DATA_NOT_AVAILABLE_BY_SIGNAL_CLOSE"
    return None


def _financial_source_quality_failure(
    row: Mapping[str, Any], factor_name: str,
) -> str | None:
    lineage = row.get("financial_component_lineage")
    if isinstance(lineage, str):
        try:
            lineage = json.loads(lineage)
        except (TypeError, ValueError, json.JSONDecodeError):
            return "FINANCIAL_COMPONENT_LINEAGE_MISSING"
    if not isinstance(lineage, Mapping):
        return "FINANCIAL_COMPONENT_LINEAGE_MISSING"
    required = [
        component for key, component in lineage.items()
        if key != "__quality_evidence__"
        and isinstance(component, Mapping)
        and factor_name in (component.get("used_by") or ())
    ]
    if not required:
        return "FINANCIAL_SOURCE_COMPONENT_LINEAGE_MISSING"
    if any(
        str(component.get("source_quality_status") or "").strip().upper()
        not in {"VALID", "PASS"}
        for component in required
    ):
        return "FINANCIAL_SOURCE_COMPONENT_QUALITY_NOT_VALID"
    return None


def _financial_factor_value(spec: FactorSpec, row: Mapping[str, Any]) -> FactorValue:
    pit_failure = _financial_pit_failure(row)
    if pit_failure is not None:
        return FactorValue(spec.name, None, "INVALID", pit_failure)
    source_quality_failure = _financial_source_quality_failure(row, spec.name)
    if source_quality_failure is not None:
        return FactorValue(spec.name, None, "INVALID", source_quality_failure)

    field = spec.name
    value = _finite(row.get(field))
    if spec.name == "roe_ttm":
        if not _quality_is_valid(row.get("roe_quality_status")):
            return FactorValue(spec.name, None, "INVALID", "ROE_QUALITY_STATUS_NOT_VALID")
        if value is None:
            return FactorValue(spec.name, None, "MISSING", "ROE_VALUE_MISSING_OR_NONFINITE")
        return FactorValue(spec.name, value, "VALID")

    growth_status = str(row.get("growth_quality_status") or "").strip().upper()
    if growth_status not in {"VALID", "INSUFFICIENT_HISTORY"}:
        return FactorValue(
            spec.name, None, "INVALID", "GROWTH_QUALITY_STATUS_NOT_VALID",
        )
    if value is None:
        return FactorValue(
            spec.name, None, "MISSING", "GROWTH_VALUE_MISSING_OR_NONFINITE",
        )
    reason = (
        "SIBLING_GROWTH_FACTOR_INSUFFICIENT"
        if growth_status == "INSUFFICIENT_HISTORY" else None
    )
    return FactorValue(spec.name, value, "VALID", reason)


def _debt_to_assets_value(spec: FactorSpec, row: Mapping[str, Any]) -> FactorValue:
    version = row.get("debt_to_assets_feature_contract_version")
    if version != DEBT_TO_ASSETS_FEATURE_CONTRACT_VERSION:
        return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_FEATURE_VERSION_NOT_SUPPORTED")

    status = str(row.get("debt_to_assets_quality_status") or "").strip().upper()
    if status not in {"VALID", "PASS"}:
        reason = str(row.get("debt_to_assets_quality_reason") or "").strip()
        return FactorValue(
            spec.name, None, "INVALID",
            reason or "DEBT_TO_ASSETS_QUALITY_STATUS_NOT_VALID",
        )
    if str(row.get("debt_to_assets_source_quality_status") or "").strip().upper() not in {"VALID", "PASS"}:
        return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_SOURCE_QUALITY_NOT_VALID")

    source_key = str(row.get("debt_to_assets_source_record_key") or "").strip()
    source_version = row.get("debt_to_assets_source_record_version")
    source_hash = str(row.get("debt_to_assets_source_payload_hash") or "").strip().lower()
    if not source_key or source_version is None or len(source_hash) != 64:
        return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_SOURCE_IDENTITY_INCOMPLETE")
    try:
        int(source_version)
        int(source_hash, 16)
    except (TypeError, ValueError, OverflowError):
        return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_SOURCE_IDENTITY_INVALID")

    signal_day = row.get("research_date")
    if isinstance(signal_day, datetime):
        signal_day = signal_day.date()
    elif not isinstance(signal_day, date):
        try:
            signal_day = date.fromisoformat(str(signal_day)[:10])
        except (TypeError, ValueError):
            return FactorValue(spec.name, None, "INVALID", "SIGNAL_DATE_MISSING_OR_INVALID")

    available_at = row.get("debt_to_assets_data_available_timestamp")
    if isinstance(available_at, str):
        try:
            available_at = datetime.fromisoformat(available_at)
        except ValueError:
            return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_AVAILABILITY_TIMESTAMP_INVALID")
    if not isinstance(available_at, datetime):
        return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_AVAILABILITY_TIMESTAMP_MISSING")
    if available_at.tzinfo is None or available_at.utcoffset() is None:
        return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_AVAILABILITY_TIMEZONE_MISSING")
    signal_close = datetime.combine(
        signal_day, time(15, 0), tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    if available_at.astimezone(ZoneInfo("Asia/Shanghai")) >= signal_close:
        return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_NOT_AVAILABLE_BY_SIGNAL_CLOSE")

    report_day = row.get("debt_to_assets_report_date")
    if isinstance(report_day, datetime):
        report_day = report_day.date()
    elif not isinstance(report_day, date):
        try:
            report_day = date.fromisoformat(str(report_day)[:10])
        except (TypeError, ValueError):
            return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_REPORT_DATE_MISSING")
    if report_day > signal_day:
        return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_REPORT_DATE_AFTER_SIGNAL")

    vintage = str(row.get("debt_to_assets_vintage_quality") or "").strip().upper()
    if vintage == "SOURCE_ANNOUNCEMENT_ASSUMED":
        evidence_reason = "PIT_SOURCE_ANNOUNCEMENT_ASSUMED"
    elif vintage == "WAREHOUSE_OBSERVED_ONLY":
        observed_at = row.get("debt_to_assets_source_observed_at")
        if isinstance(observed_at, str):
            try:
                observed_at = datetime.fromisoformat(observed_at)
            except ValueError:
                return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_SOURCE_OBSERVED_TIMESTAMP_INVALID")
        if not isinstance(observed_at, datetime) or observed_at.tzinfo is None:
            return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_SOURCE_OBSERVED_TIMESTAMP_MISSING")
        if observed_at.astimezone(ZoneInfo("Asia/Shanghai")) >= signal_close:
            return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_SOURCE_NOT_OBSERVED_BY_SIGNAL_CLOSE")
        evidence_reason = "PIT_WAREHOUSE_OBSERVED"
    else:
        return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_PIT_EVIDENCE_UNSUPPORTED")

    value = _finite(row.get("debt_to_assets"))
    if value is None:
        return FactorValue(spec.name, None, "MISSING", "DEBT_TO_ASSETS_VALUE_MISSING_OR_NONFINITE")
    if value < 0:
        return FactorValue(spec.name, None, "INVALID", "DEBT_TO_ASSETS_MUST_BE_NONNEGATIVE")
    return FactorValue(spec.name, value, "VALID", evidence_reason)


def _price_coordinate_failure(row: Mapping[str, Any]) -> str | None:
    if (
        row.get("price_feature_version") != "price_daily_v2"
        or row.get("price_adjustment_mode") != "point_in_time_total_return"
    ):
        return "PRICE_COORDINATE_NOT_SUPPORTED"
    return None


def _raw_value(spec: FactorSpec, row: Mapping[str, Any]) -> FactorValue:
    missing = [field for field in spec.input_fields if field not in row]
    if missing:
        return FactorValue(spec.name, None, "UNAVAILABLE", "FIELD_NOT_IN_INPUT:" + ",".join(missing))

    if spec.name == "pe_percentile_5y":
        if not _truth(row.get("is_pe_meaningful")):
            return FactorValue(spec.name, None, "MISSING", "PE_NOT_MEANINGFUL")
        if not _truth(row.get("has_full_pe_5y_history")):
            return FactorValue(spec.name, None, "MISSING", "PE_HISTORY_INCOMPLETE")
        value = _finite(row.get("pe_pct_5y"))
        if value is None:
            return FactorValue(spec.name, None, "MISSING", "PE_PERCENTILE_MISSING_OR_NONFINITE")
        if not 0.0 <= value <= 1.0:
            return FactorValue(spec.name, None, "INVALID", "PE_PERCENTILE_OUT_OF_RANGE")
        return FactorValue(spec.name, value, "VALID")

    if spec.name == "pe_ttm":
        if not _truth(row.get("is_pe_meaningful")):
            return FactorValue(spec.name, None, "MISSING", "PE_NOT_MEANINGFUL")
        value = _finite(row.get("pe_ttm"))
        if value is None or value <= 0:
            return FactorValue(spec.name, None, "INVALID", "PE_TTM_MUST_BE_POSITIVE_FINITE")
        return FactorValue(spec.name, value, "VALID")

    if spec.name in {"roe_ttm", "revenue_growth_yoy", "profit_growth_yoy"}:
        return _financial_factor_value(spec, row)
    if spec.name == "debt_to_assets":
        return _debt_to_assets_value(spec, row)

    if (
        spec.name in {"return_20d", "return_60d", "close_vs_ma60"}
        and _price_coordinate_failure(row) is not None
    ):
        return FactorValue(
            spec.name, None, "INVALID", "PRICE_COORDINATE_NOT_SUPPORTED",
        )

    field = "adj_close_base" if spec.name == "close_vs_ma60" else spec.name
    if spec.name == "close_vs_ma60":
        price = _finite(row.get("adj_close_base"))
        average = _finite(row.get("ma60"))
        if price is None or average is None:
            return FactorValue(spec.name, None, "MISSING", "PRICE_OR_MA60_MISSING_OR_NONFINITE")
        if price <= 0 or average <= 0:
            return FactorValue(spec.name, None, "INVALID", "PRICE_AND_MA60_MUST_BE_POSITIVE")
        return FactorValue(spec.name, price / average - 1.0, "VALID")

    value = _finite(row.get(field))
    if value is None:
        return FactorValue(spec.name, None, "MISSING", "VALUE_MISSING_OR_NONFINITE")
    if spec.name in {"debt_to_assets", "volume_ratio_20d"} and value < 0:
        return FactorValue(spec.name, None, "INVALID", "VALUE_MUST_BE_NONNEGATIVE")
    return FactorValue(spec.name, value, "VALID")


def factor_value(factor_name: str, row: Mapping[str, Any]) -> FactorValue:
    """Extract one versioned factor value from a point-in-time snapshot row."""
    try:
        spec = FACTOR_BY_NAME[factor_name]
    except KeyError as exc:
        raise ValueError(f"unsupported alpha factor: {factor_name}") from exc
    return _raw_value(spec, row)


def factor_values(row: Mapping[str, Any]) -> dict[str, FactorValue]:
    return {name: factor_value(name, row) for name in FACTOR_BY_NAME}


__all__ = ["FactorValue", "factor_value", "factor_values"]
