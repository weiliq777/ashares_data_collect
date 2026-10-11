"""Advance bounded, strategy-neutral outcomes for frozen daily anomalies."""
from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_anomaly_outcome_repository import (
    MarketAnomalyOutcomeRepository,
)
from data_collect.data_access.repository.market_coverage_repository import (
    MarketCoverageRepository,
)
from data_collect.observation.outcome_evaluator import OutcomeEvaluator
from data_collect.observation.outcome_metrics import OutcomePolicy


JOB_NAME = "market_anomaly_outcome_daily"
MAX_GATE_LOOKBACK_DAYS = 31


def _parse_date(value: date | str | None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value).strip().replace("-", ""), "%Y%m%d").date()


def run(
    run_date: date | str | None = None,
    *,
    execution_id: str | None = None,
    lookback_days: int = MAX_GATE_LOOKBACK_DAYS,
    dry_run: bool = False,
) -> dict:
    """Advance only through the latest PASS daily gate in a bounded window.

    This job is a downstream consumer.  It never rebuilds upstream datasets;
    a missing or non-PASS gate fails closed.  Production DAG activation must
    wait until migration 113 and read-query EXPLAIN plans have been approved.
    """
    if not 1 <= int(lookback_days) <= MAX_GATE_LOOKBACK_DAYS:
        raise ValueError("Outcome gate lookback_days must be in 1..31")
    lookback_days = int(lookback_days)
    upper_bound = _parse_date(run_date)
    if dry_run:
        return {
            "status": "dry_run",
            "upper_bound": upper_bound.isoformat(),
            "lookback_days": lookback_days,
            "max_events_per_batch": 100,
            "max_batches_per_run": 5,
        }

    execution_id = str(execution_id or f"market-anomaly-outcome-{uuid4().hex}").strip()
    if not execution_id:
        raise ValueError("execution_id must not be blank")
    policy = OutcomePolicy()
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=execution_id,
        job_name=JOB_NAME,
        code_version=policy.contract_version,
        config_hash=policy.methodology_hash,
        pipeline_version="data-access-v1",
    )
    try:
        gate = MarketCoverageRepository(kernel).latest_daily_gate(
            upper_bound, lookback_days, context,
        )
        if gate is None:
            raise RuntimeError(
                "no daily market quality gate found in bounded Outcome window: "
                f"end={upper_bound.isoformat()}, days={lookback_days}"
            )
        gate_status = str(gate.get("status") or "").strip().upper()
        gate_date = gate.get("trade_date")
        through_date = (
            gate_date.date() if isinstance(gate_date, datetime)
            else gate_date if isinstance(gate_date, date)
            else date.fromisoformat(str(gate_date)[:10])
        )
        if gate_status != "PASS":
            raise RuntimeError(
                "latest daily market quality gate is not PASS: "
                f"trade_date={through_date.isoformat()}, status={gate_status or 'UNKNOWN'}, "
                f"reason={gate.get('reason')!r}"
            )
        report = OutcomeEvaluator(MarketAnomalyOutcomeRepository(kernel), policy).run(
            through_date=through_date,
            context=context,
        )
        result = report.as_dict()
        result["gate_status"] = gate_status
        result["gate_trade_date"] = through_date.isoformat()
        return result
    finally:
        kernel.close()


__all__ = ["JOB_NAME", "MAX_GATE_LOOKBACK_DAYS", "run"]
