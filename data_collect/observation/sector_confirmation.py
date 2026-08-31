"""行业确认增强层（纯计算，不参与个股主状态）。"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

from data_collect.observation.contracts import ObservationPolicy


def _as_bool(value: Any) -> bool:
    return bool(value) if value is not None else False


def _sector_key(row: Mapping[str, Any]) -> tuple[str, int, str] | None:
    system, level, code = row.get("industry_system"), row.get("industry_level"), row.get("industry_code")
    if not system or level is None or not code:
        return None
    try:
        level = int(level)
    except (TypeError, ValueError):
        return None
    return str(system), level, str(code)


def _data_status(rows: list[Mapping[str, Any]], policy: ObservationPolicy) -> str:
    if not rows:
        return "MISSING"
    statuses = {str(row.get("sector_data_status") or "").upper() for row in rows}
    if "INVALID" in statuses:
        return "INVALID"
    if "CONFIRMED" in statuses:
        return "CONFIRMED"
    if "DAILY_CAPTURED" in statuses:
        return "DAILY_CAPTURED"
    return "INVALID"


def aggregate_sector_rows(
    rows: Iterable[Mapping[str, Any]], policy: ObservationPolicy | None = None,
) -> list[dict[str, Any]]:
    """Aggregate anomaly breadth using the supplied PIT universe denominator.

    The caller must provide one row for each eligible stock in the observation
    date's universe. Rows without a valid industry mapping are not silently
    assigned to a sector and are excluded from sector aggregates.
    """
    policy = policy or ObservationPolicy()
    groups: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        key = _sector_key(row)
        if key is not None and _as_bool(row.get("eligible_by_market")):
            groups[(row.get("observation_date"), *key)].append(row)
    output: list[dict[str, Any]] = []
    for (observation_date, system, level, code), members in sorted(groups.items(), key=lambda item: tuple(str(x) for x in item[0])):
        data_status = _data_status(members, policy)
        eligible_count = len(members)
        anomaly_members = [row for row in members if row.get("anomaly_flag") is True]
        new_members = [row for row in anomaly_members if row.get("anomaly_streak") == 1]
        persistent_members = [row for row in anomaly_members if (row.get("anomaly_streak") or 0) >= 2]
        previous_anomaly_count = sum(row.get("previous_anomaly_flag") is True for row in members)
        exited_count = sum(
            row.get("previous_anomaly_flag") is True and row.get("anomaly_flag") is False
            for row in members
        )
        anomaly_count = len(anomaly_members)
        breadth = anomaly_count / eligible_count if eligible_count else None
        new_ratio = len(new_members) / eligible_count if eligible_count else None
        persistence = len(persistent_members) / anomaly_count if anomaly_count else None
        quality = "VALID" if data_status == "CONFIRMED" and eligible_count >= policy.min_universe_size else "WARNING"
        output.append({
            "observation_date": observation_date,
            "industry_system": system,
            "industry_level": level,
            "industry_code": code,
            "industry_name": next((row.get("industry_name") for row in members if row.get("industry_name")), None),
            "eligible_stock_count": eligible_count,
            "anomaly_stock_count": anomaly_count,
            "new_anomaly_count": len(new_members),
            "persistent_anomaly_count": len(persistent_members),
            "previous_anomaly_count": previous_anomaly_count,
            "exited_anomaly_count": exited_count,
            "sector_anomaly_breadth": breadth,
            "sector_new_anomaly_ratio": new_ratio,
            "sector_persistence": persistence,
            "sector_data_status": data_status,
            "quality_status": quality,
        })
    return output


def confirm_stock_sector(
    metrics: Mapping[str, Any], sector_row: Mapping[str, Any] | None,
    policy: ObservationPolicy | None = None,
) -> dict[str, Any]:
    """Return sector evidence without changing any stock metric/state."""
    policy = policy or ObservationPolicy()
    if not sector_row:
        return {
            "sector_data_status": "MISSING",
            "sector_confirmation_status": "UNKNOWN",
            "sector_confirmation_score": None,
            "sector_reason_codes": ("MISSING_INDUSTRY_MAPPING",),
        }
    status = str(sector_row.get("sector_data_status") or "INVALID").upper()
    if status != "CONFIRMED":
        reason = {
            "DAILY_CAPTURED": "PIT_SEMANTICS_NOT_CONFIRMED",
            "MISSING": "MISSING_INDUSTRY_MAPPING",
            "INVALID": "INVALID_INDUSTRY_INPUT",
        }.get(status, "INVALID_INDUSTRY_INPUT")
        return {
            "sector_data_status": status if status in {"DAILY_CAPTURED", "MISSING", "INVALID"} else "INVALID",
            "sector_confirmation_status": "UNKNOWN",
            "sector_confirmation_score": None,
            "sector_reason_codes": (reason,),
        }
    breadth = sector_row.get("sector_anomaly_breadth")
    new_ratio = sector_row.get("sector_new_anomaly_ratio")
    persistence = sector_row.get("sector_persistence")
    try:
        breadth = float(breadth) if breadth is not None else None
        new_ratio = float(new_ratio) if new_ratio is not None else None
        persistence = float(persistence) if persistence is not None else None
    except (TypeError, ValueError):
        breadth = new_ratio = persistence = None
    if sector_row.get("eligible_stock_count", 0) < policy.min_universe_size or breadth is None or new_ratio is None:
        return {
            "sector_data_status": "CONFIRMED",
            "sector_confirmation_status": "UNKNOWN",
            "sector_confirmation_score": None,
            "sector_reason_codes": ("INSUFFICIENT_SECTOR_DENOMINATOR",),
        }
    confirmed = breadth >= 0.20 and new_ratio >= 0.05
    score = max(0.0, min(100.0, breadth * 300.0 + (new_ratio or 0.0) * 300.0 + (persistence or 0.0) * 40.0))
    return {
        "sector_data_status": "CONFIRMED",
        "sector_confirmation_status": "CONFIRMED" if confirmed else "NOT_CONFIRMED",
        "sector_confirmation_score": score,
        "sector_reason_codes": ("SECTOR_BREADTH_THRESHOLD", "SECTOR_NEW_ANOMALY_THRESHOLD") if confirmed else ("SECTOR_CONFIRMATION_NOT_MET",),
    }


__all__ = ["aggregate_sector_rows", "confirm_stock_sector"]
