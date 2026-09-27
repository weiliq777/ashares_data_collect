"""Build Observation V1 from one FROZEN Market Activity snapshot.

Only the new Observation tables are written.  Raw, Standard, PIT, Feature
and Market Activity facts are never rebuilt by this job.
"""
from __future__ import annotations

import argparse
import json
from typing import Sequence
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_activity_observation_repository import (
    MarketActivityObservationRepository,
)
from data_collect.observation.market_activity_persistence import (
    MarketActivityObservationBuilder,
)
from data_collect.observation.market_activity_rules import (
    MarketActivityObservationPolicy,
)


JOB_NAME = "market_activity_observation"


def run(
    activity_snapshot_id: str,
    *,
    ratio_threshold: float = 2.0,
    high_amount_share_threshold: float = 0.005,
    min_history_observation_count: int = 20,
    run_id: str | None = None,
    freeze: bool = False,
) -> dict:
    policy = MarketActivityObservationPolicy(
        ratio_threshold=ratio_threshold,
        high_amount_share_threshold=high_amount_share_threshold,
        min_history_observation_count=min_history_observation_count,
    )
    stable_run_id = run_id or (
        f"market-activity-observation-{activity_snapshot_id}-"
        f"{policy.methodology_hash[:16]}"
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=stable_run_id,
        job_name=JOB_NAME,
        code_version="market-activity-observation-v1",
        config_hash=policy.methodology_hash,
        pipeline_version="data-access-v1",
    )
    try:
        result = MarketActivityObservationBuilder(
            MarketActivityObservationRepository(kernel)
        ).build(activity_snapshot_id, policy, context, freeze=freeze)
        return {
            "snapshot_id": result.snapshot_id,
            "status": result.status,
            "skipped": result.skipped,
            "validation": result.validation,
            "report": result.report.quality_summary() if result.report else None,
        }
    finally:
        kernel.close()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Build explainable Observation V1 from frozen Market Activity facts"
    )
    parser.add_argument("--activity-snapshot-id", required=True)
    parser.add_argument("--ratio-threshold", type=float, default=2.0)
    parser.add_argument("--high-amount-share-threshold", type=float, default=0.005)
    parser.add_argument("--min-history-observation-count", type=int, default=20)
    parser.add_argument("--run-id")
    parser.add_argument("--freeze", action="store_true")
    args = parser.parse_args(argv)
    print(json.dumps(
        run(
            args.activity_snapshot_id,
            ratio_threshold=args.ratio_threshold,
            high_amount_share_threshold=args.high_amount_share_threshold,
            min_history_observation_count=args.min_history_observation_count,
            run_id=args.run_id,
            freeze=args.freeze,
        ),
        ensure_ascii=False,
        indent=2,
        default=str,
    ))


if __name__ == "__main__":
    main()
