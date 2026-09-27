"""Run the complete daily Market Activity -> Observation chain.

This entry point intentionally operates only on the new observation domain:

    std.daily_kline -> Market Activity (FROZEN) -> Observation (FROZEN)

It never rebuilds Raw, Standard, PIT, Feature, Research Snapshot or Backtest
data.  The selected validated baseline is a hard cutoff; a date after that
release is rejected before any write is attempted.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
from typing import Sequence

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_activity_observation_repository import (
    MarketActivityObservationRepository,
)
from data_collect.data_access.repository.market_activity_storage_repository import (
    MarketActivityStorageRepository,
)
from data_collect.market_activity import MarketActivityBuilder, MarketActivityRequest
from data_collect.observation.market_activity_persistence import (
    MarketActivityObservationBuilder,
)
from data_collect.observation.market_activity_rules import (
    MarketActivityObservationPolicy,
)


JOB_NAME = "market_activity_observation_daily"


def run(
    trade_date: date,
    data_baseline_id: str,
    *,
    top_n: int = 20,
    lookback_trading_days: int = 20,
    calendar_lookback_days: int = 45,
    ratio_threshold: float = 2.0,
    high_amount_share_threshold: float = 0.005,
    min_history_observation_count: int = 20,
    run_id: str | None = None,
) -> dict[str, object]:
    """Build and freeze both daily layers with stable stage run identities."""
    request = MarketActivityRequest(
        trade_date=trade_date,
        top_n=top_n,
        lookback_trading_days=lookback_trading_days,
        calendar_lookback_days=calendar_lookback_days,
        data_baseline_id=data_baseline_id,
    )
    policy = MarketActivityObservationPolicy(
        ratio_threshold=ratio_threshold,
        high_amount_share_threshold=high_amount_share_threshold,
        min_history_observation_count=min_history_observation_count,
    )

    # With no explicit execution id, these match the standalone jobs.  That
    # makes migration to the one-click entry point idempotent for an already
    # frozen date.  A scheduler-provided id is split by stage so each stored
    # snapshot still has one unambiguous calculation_run_id.
    activity_run_id = run_id or (
        f"market-activity-build-{trade_date.isoformat()}-"
        f"{request.methodology_hash[:16]}-{str(data_baseline_id).strip()}"
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        activity_context = RunContext(
            run_id=f"{activity_run_id}:activity" if run_id else activity_run_id,
            job_name=JOB_NAME,
            code_version="market-activity-v1",
            config_hash=request.methodology_hash,
            pipeline_version="data-access-v1",
        )
        activity_result = MarketActivityBuilder(
            MarketActivityStorageRepository(kernel)
        ).build(request, activity_context, freeze=True)

        activity_snapshot_id = activity_result.activity_snapshot_id
        observation_run_id = run_id or (
            f"market-activity-observation-{activity_snapshot_id}-"
            f"{policy.methodology_hash[:16]}"
        )
        observation_context = RunContext(
            run_id=f"{observation_run_id}:observation" if run_id else observation_run_id,
            job_name=JOB_NAME,
            code_version="market-activity-observation-v1",
            config_hash=policy.methodology_hash,
            pipeline_version="data-access-v1",
        )
        observation_result = MarketActivityObservationBuilder(
            MarketActivityObservationRepository(kernel)
        ).build(
            activity_snapshot_id,
            policy,
            observation_context,
            freeze=True,
        )
        return {
            "trade_date": trade_date.isoformat(),
            "data_baseline_id": str(data_baseline_id),
            "activity": {
                "snapshot_id": activity_snapshot_id,
                "status": activity_result.status,
                "skipped": activity_result.skipped,
                "validation": activity_result.validation,
            },
            "observation": {
                "snapshot_id": observation_result.snapshot_id,
                "status": observation_result.status,
                "skipped": observation_result.skipped,
                "validation": observation_result.validation,
                "report": (
                    observation_result.report.quality_summary()
                    if observation_result.report else None
                ),
            },
        }
    finally:
        kernel.close()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Build and freeze one Market Activity -> Observation date"
    )
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--data-baseline-id", required=True)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--lookback-trading-days", type=int, default=20)
    parser.add_argument("--calendar-lookback-days", type=int, default=45)
    parser.add_argument("--ratio-threshold", type=float, default=2.0)
    parser.add_argument("--high-amount-share-threshold", type=float, default=0.005)
    parser.add_argument("--min-history-observation-count", type=int, default=20)
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)
    print(json.dumps(
        run(
            date.fromisoformat(args.trade_date),
            args.data_baseline_id,
            top_n=args.top_n,
            lookback_trading_days=args.lookback_trading_days,
            calendar_lookback_days=args.calendar_lookback_days,
            ratio_threshold=args.ratio_threshold,
            high_amount_share_threshold=args.high_amount_share_threshold,
            min_history_observation_count=args.min_history_observation_count,
            run_id=args.run_id,
        ),
        ensure_ascii=False,
        indent=2,
        default=str,
    ))


if __name__ == "__main__":
    main()
