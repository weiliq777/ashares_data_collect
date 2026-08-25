"""Run one configured data pipeline as a standalone process.

This small entry point keeps Windows process spawning safe for the pipeline's
per-task subprocess isolation. It is intentionally a scheduler helper, not a
database access path.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_collect.pipeline import run_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one configured data pipeline")
    parser.add_argument("--pipeline", required=True)
    parser.add_argument("--run-date")
    parser.add_argument("--only-task")
    parser.add_argument("--no-dag", action="store_true")
    parser.add_argument("--lookback-days", type=int)
    parser.add_argument("--dataset", choices=("all", "daily", "daily_basic", "adj_factor", "trade_cal"))
    parser.add_argument("--dry-run", action="store_true", help="run task logic without database writes")
    args = parser.parse_args()

    base_kwargs = {}
    if args.lookback_days is not None:
        base_kwargs["lookback_days"] = args.lookback_days
    if args.dataset is not None:
        base_kwargs["dataset"] = args.dataset
    if args.dry_run:
        base_kwargs["dry_run"] = True
    results = run_pipeline(
        pipeline_name=args.pipeline,
        run_date=args.run_date,
        only_task=args.only_task,
        show_dag=not args.no_dag,
        **base_kwargs,
    )
    return 0 if all(result.success for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
