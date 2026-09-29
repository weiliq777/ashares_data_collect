"""Publish and freeze the latest qualified daily Market Activity observations.

This bounded post-quality job creates (or reuses) one MARKET_ACTIVITY release
for the latest daily market-quality gate, then builds only that trading date.
It never invokes the full Research release audit or rebuilds upstream data.
"""
from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_coverage_repository import (
    MarketCoverageRepository,
)
from data_collect.data_access.repository.release_repository import ReleaseRepository
from data_collect.jobs.market_activity_observation_daily import run as build_daily_observation


JOB_NAME = "market_activity_production_daily"
RELEASE_SCHEMA_VERSION = "market-activity-v1"
GATE_LOOKBACK_DAYS = 31


def _parse_date(value: date | str | None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    normalized = str(value).strip().replace("-", "")
    return datetime.strptime(normalized, "%Y%m%d").date()


def _release_tag(trade_date: date) -> str:
    return f"market-activity-{trade_date:%Y%m%d}"


def _assert_release_identity(release: dict, tag: str, cutoff: date) -> None:
    actual_cutoff = release.get("baseline_cutoff_date")
    if isinstance(actual_cutoff, datetime):
        actual_cutoff = actual_cutoff.date()
    elif not isinstance(actual_cutoff, date):
        actual_cutoff = date.fromisoformat(str(actual_cutoff)[:10])
    expected = {
        "release_tag": tag,
        "baseline_domain": ReleaseRepository.MARKET_ACTIVITY_DOMAIN,
        "baseline_cutoff_date": cutoff,
        "schema_version": RELEASE_SCHEMA_VERSION,
    }
    actual = {
        "release_tag": str(release.get("release_tag") or "").strip(),
        "baseline_domain": str(release.get("baseline_domain") or "").strip(),
        "baseline_cutoff_date": actual_cutoff,
        "schema_version": str(release.get("schema_version") or "").strip(),
    }
    if actual != expected:
        raise RuntimeError(
            f"market activity release identity mismatch for {tag}: "
            f"actual={actual!r}, expected={expected!r}"
        )


def _ensure_validated_release(
    repository: ReleaseRepository,
    context: RunContext,
    tag: str,
    cutoff: date,
) -> dict:
    current = repository.current_validated_release(
        ReleaseRepository.MARKET_ACTIVITY_DOMAIN, context,
    )
    if current is not None:
        current_cutoff = current.get("baseline_cutoff_date")
        if isinstance(current_cutoff, datetime):
            current_cutoff = current_cutoff.date()
        elif not isinstance(current_cutoff, date):
            current_cutoff = date.fromisoformat(str(current_cutoff)[:10])
        if current_cutoff > cutoff or (
            current_cutoff == cutoff and current.get("release_tag") != tag
        ):
            raise RuntimeError(
                "refusing to move the current Market Activity release backward "
                "or replace a same-date release: "
                f"current={current.get('release_tag')}@{current_cutoff}, "
                f"requested={tag}@{cutoff}"
            )

    release = repository.get_release(tag, context)
    if release is None:
        repository.create(
            tag,
            cutoff,
            "Daily Market Activity release; derived only after production quality passed.",
            RELEASE_SCHEMA_VERSION,
            context,
        )
        release = repository.get_release(tag, context)
        if release is None:
            raise RuntimeError(f"release creation did not persist: {tag}")

    _assert_release_identity(release, tag, cutoff)
    status = str(release.get("status") or "").strip().lower()
    if status == "validated":
        return {"tag": tag, "status": "validated", "reused": True}
    if status != "draft":
        raise RuntimeError(
            f"cannot reuse non-current market activity release {tag}: status={status!r}"
        )

    result = repository.validate(tag, "", context)
    changed = repository.commit_validation(result, context)
    if changed != 1:
        raise RuntimeError(f"market activity release validation was not committed: {tag}")
    validated = repository.get_release(tag, context)
    if validated is None:
        raise RuntimeError(f"validated release disappeared: {tag}")
    _assert_release_identity(validated, tag, cutoff)
    if str(validated.get("status") or "").strip().lower() != "validated":
        raise RuntimeError(f"market activity release did not reach VALIDATED: {tag}")
    return {"tag": tag, "status": "validated", "reused": False}


def run(
    run_date: date | str | None = None,
    *,
    execution_id: str | None = None,
    lookback_days: int = GATE_LOOKBACK_DAYS,
) -> dict:
    """Freeze one date's activity and observations after a PASS market gate."""
    upper_bound = _parse_date(run_date)
    execution_id = str(execution_id or f"market-activity-release-{uuid4().hex}").strip()
    if not execution_id:
        raise ValueError("execution_id must not be blank")

    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        context = RunContext(
            run_id=execution_id,
            job_name=JOB_NAME,
            code_version="market-activity-v1",
            config_hash="market-activity-v1",
            pipeline_version="data-access-v1",
        )
        gate = MarketCoverageRepository(kernel).latest_daily_gate(
            upper_bound, lookback_days, context,
        )
        if gate is None:
            raise RuntimeError(
                "no daily market quality gate found in bounded lookback: "
                f"end={upper_bound.isoformat()}, days={lookback_days}"
            )
        trade_date = gate["trade_date"]
        if isinstance(trade_date, datetime):
            trade_date = trade_date.date()
        elif not isinstance(trade_date, date):
            trade_date = date.fromisoformat(str(trade_date)[:10])
        status = str(gate.get("status") or "").strip().upper()
        if status != "PASS":
            raise RuntimeError(
                "latest daily market quality gate is not PASS: "
                f"trade_date={trade_date.isoformat()}, status={status or 'UNKNOWN'}, "
                f"reason={gate.get('reason')!r}"
            )

        tag = _release_tag(trade_date)
        repository = ReleaseRepository(kernel)
        with kernel.lock_manager.session_exclusive("tushare-release", "market-activity"):
            release = _ensure_validated_release(repository, context, tag, trade_date)
    finally:
        kernel.close()

    # The builder's default run identities are deterministic by date and
    # methodology, so rerunning a frozen date is idempotent. The scheduler's
    # execution_id is reserved for release orchestration/retry attribution.
    activity = build_daily_observation(
        trade_date,
        tag,
    )
    return {
        "trade_date": trade_date.isoformat(),
        "release": release,
        "activity_observation": activity,
    }
