"""Pure, cost-bounded impact planning for captured release-delta candidates.

This module performs no database access and never validates or publishes a
release.  It only decides whether an existing domain contract can describe a
bounded recheck.  Unsupported or incomplete evidence always requires a full
audit.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any, Mapping


_MARKET_ACTIVITY_LOOKBACK_DAYS = 45
_MARKET_ACTIVITY_MAX_SOURCE_ROWS = 250_000
_MAX_PACKAGE_EVENTS = 1_000
_SUPPORTED_MARKET_ACTIVITY_TARGET = "std.daily_kline"
_SUPPORTED_MARKET_ACTIVITY_OPERATIONS = frozenset({"UPSERT", "REBUILD", "BULK_INSERT"})


class DeltaImpactDisposition(str, Enum):
    """Safe next action for a captured candidate; never implies publication."""

    BOUNDED_RECHECK = "BOUNDED_RECHECK"
    NO_CUTOFF_IMPACT = "NO_CUTOFF_IMPACT"
    FULL_AUDIT_REQUIRED = "FULL_AUDIT_REQUIRED"


@dataclass(frozen=True, slots=True)
class DeltaImpactPlan:
    baseline_domain: str
    disposition: DeltaImpactDisposition
    reasons: tuple[str, ...]
    relevant_event_count: int
    recheck_date: date | None = None
    source_start: date | None = None
    source_end: date | None = None
    max_source_rows: int | None = None
    publishable: bool = False


def resolve_delta_impact(
    candidate: Mapping[str, Any], *, cutoff: date,
) -> DeltaImpactPlan:
    """Plan one bounded impact check without querying facts or changing state.

    Current bounded support is intentionally narrow: one Market Activity
    (市场成交观察) output at ``cutoff``, whose only direct input is
    ``std.daily_kline`` (标准日线行情表).  Its calculation reads a fixed
    45-calendar-day lookback and has a 250,000-row application budget.  The
    Research (研究) domain and all unrecognized/ambiguous event shapes require
    the existing full audit.

    A returned plan is never publishable: CAPTURED candidates still need a
    separately implemented and shadow-verified quality gate.
    """
    if isinstance(cutoff, datetime) or not isinstance(cutoff, date):
        raise TypeError("cutoff must be datetime.date")
    if str(candidate.get("package_status", "")).upper() != "CAPTURED":
        raise ValueError("impact planning requires a CAPTURED candidate")

    domain = str(candidate.get("baseline_domain", "")).strip().upper()
    raw_events = candidate.get("events")
    if not isinstance(raw_events, (list, tuple)):
        raise ValueError("candidate events must be a bounded sequence")
    try:
        event_count = int(candidate.get("event_count"))
    except (TypeError, ValueError) as exc:
        raise ValueError("candidate event_count must be an integer") from exc
    if event_count != len(raw_events):
        raise ValueError(
            f"candidate event_count mismatch: declared={event_count} actual={len(raw_events)}"
        )
    if not 1 <= event_count <= _MAX_PACKAGE_EVENTS:
        raise ValueError(f"candidate event_count must be in 1..{_MAX_PACKAGE_EVENTS}")

    if domain != "MARKET_ACTIVITY":
        return _full_audit(domain, "no bounded impact resolver for this release domain")

    reasons: list[str] = []
    source_start = cutoff - timedelta(days=_MARKET_ACTIVITY_LOOKBACK_DAYS)
    relevant_count = 0
    for index, event in enumerate(raw_events):
        if not isinstance(event, Mapping):
            reasons.append(f"event[{index}] is not an object")
            continue
        if event.get("delta_eligible") is not True:
            reasons.append(f"event[{index}] is not delta-eligible")
        if str(event.get("producer_run_status", "")).upper() != "SUCCEEDED":
            reasons.append(f"event[{index}] producer Run is not SUCCEEDED")
        target = str(event.get("target", "")).strip().lower()
        operation = str(event.get("operation", "")).strip().upper()
        if target != _SUPPORTED_MARKET_ACTIVITY_TARGET:
            reasons.append(f"event[{index}] target {target!r} is outside the bounded dependency contract")
            continue
        if operation not in _SUPPORTED_MARKET_ACTIVITY_OPERATIONS:
            reasons.append(f"event[{index}] operation {operation!r} is not supported for bounded recheck")
            continue
        scope = event.get("scope")
        if not isinstance(scope, Mapping):
            reasons.append(f"event[{index}] has no structured impact scope")
            continue
        try:
            changed_from = _parse_date(scope.get("date_from"))
            changed_to = _parse_date(scope.get("date_to"))
        except (TypeError, ValueError):
            reasons.append(f"event[{index}] has an invalid date scope")
            continue
        if changed_from is None or changed_to is None or changed_from > changed_to:
            reasons.append(f"event[{index}] does not prove a bounded date interval")
            continue
        if changed_to > cutoff:
            reasons.append(f"event[{index}] extends beyond the release cutoff")
            continue
        if changed_to >= source_start and changed_from <= cutoff:
            relevant_count += 1

    if reasons:
        return DeltaImpactPlan(
            baseline_domain=domain,
            disposition=DeltaImpactDisposition.FULL_AUDIT_REQUIRED,
            reasons=tuple(reasons),
            relevant_event_count=relevant_count,
        )
    if relevant_count == 0:
        return DeltaImpactPlan(
            baseline_domain=domain,
            disposition=DeltaImpactDisposition.NO_CUTOFF_IMPACT,
            reasons=("no std.daily_kline change intersects the cutoff's 45-day input window",),
            relevant_event_count=0,
        )
    return DeltaImpactPlan(
        baseline_domain=domain,
        disposition=DeltaImpactDisposition.BOUNDED_RECHECK,
        reasons=("recompute only the cutoff Market Activity report from its fixed input window",),
        relevant_event_count=relevant_count,
        recheck_date=cutoff,
        source_start=source_start,
        source_end=cutoff,
        max_source_rows=_MARKET_ACTIVITY_MAX_SOURCE_ROWS,
    )


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    raise TypeError("scope date must be an ISO date or date object")


def _full_audit(domain: str, reason: str) -> DeltaImpactPlan:
    return DeltaImpactPlan(
        baseline_domain=domain,
        disposition=DeltaImpactDisposition.FULL_AUDIT_REQUIRED,
        reasons=(reason,),
        relevant_event_count=0,
    )
