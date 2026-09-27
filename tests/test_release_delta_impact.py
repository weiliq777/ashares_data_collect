from datetime import date

import pytest

from data_collect.data_access.release_delta_impact import (
    DeltaImpactDisposition,
    resolve_delta_impact,
)


def _candidate(domain="MARKET_ACTIVITY", events=None):
    events = events or []
    return {
        "baseline_domain": domain,
        "event_count": len(events),
        "package_status": "CAPTURED",
        "events": events,
    }


def _event(target="std.daily_kline", start="2026-09-24", end="2026-09-24"):
    return {
        "target": target,
        "operation": "UPSERT",
        "delta_eligible": True,
        "producer_run_status": "SUCCEEDED",
        "scope": {"date_from": start, "date_to": end, "codes": ["600000.SH"]},
    }


def test_market_activity_direct_input_produces_one_bounded_cutoff_recheck():
    plan = resolve_delta_impact(
        _candidate(events=[_event()]), cutoff=date(2026, 9, 25)
    )

    assert plan.disposition is DeltaImpactDisposition.BOUNDED_RECHECK
    assert plan.recheck_date == date(2026, 9, 25)
    assert plan.source_start == date(2026, 8, 11)
    assert plan.source_end == date(2026, 9, 25)
    assert plan.max_source_rows == 250_000
    assert plan.publishable is False


def test_market_activity_event_outside_cutoff_lookback_does_not_trigger_scan():
    plan = resolve_delta_impact(
        _candidate(events=[_event(start="2026-08-10", end="2026-08-10")]),
        cutoff=date(2026, 9, 25),
    )

    assert plan.disposition is DeltaImpactDisposition.NO_CUTOFF_IMPACT
    assert plan.source_start is None
    assert plan.source_end is None


@pytest.mark.parametrize(
    "candidate,cutoff",
    [
        (_candidate("RESEARCH", [_event()]), date(2026, 9, 25)),
        (_candidate(events=[_event(target="feature.price_daily_v2")]), date(2026, 9, 25)),
        (_candidate(events=[_event(target="public.tushare_daily_raw")]), date(2026, 9, 25)),
        (_candidate(events=[{**_event(), "producer_run_status": "FAILED"}]), date(2026, 9, 25)),
        (_candidate(events=[{**_event(), "delta_eligible": False}]), date(2026, 9, 25)),
    ],
)
def test_unproven_or_other_domain_impact_requires_full_audit(candidate, cutoff):
    plan = resolve_delta_impact(candidate, cutoff=cutoff)

    assert plan.disposition is DeltaImpactDisposition.FULL_AUDIT_REQUIRED
    assert plan.publishable is False
    assert plan.source_start is None


def test_candidate_membership_mismatch_is_rejected():
    candidate = _candidate(events=[_event()])
    candidate["event_count"] = 2

    with pytest.raises(ValueError, match="event_count"):
        resolve_delta_impact(candidate, cutoff=date(2026, 9, 25))


def test_multiple_events_reuse_one_fixed_source_window():
    candidate = _candidate(events=[
        _event(start="2026-08-11", end="2026-08-11"),
        _event(start="2026-09-25", end="2026-09-25"),
    ])

    plan = resolve_delta_impact(candidate, cutoff=date(2026, 9, 25))

    assert plan.disposition is DeltaImpactDisposition.BOUNDED_RECHECK
    assert plan.source_start == date(2026, 8, 11)
    assert plan.source_end == date(2026, 9, 25)
