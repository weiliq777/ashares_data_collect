"""Create, build, validate, and freeze a historical Backtest Dataset."""
from __future__ import annotations

import argparse
from datetime import date

from data_collect.jobs.backtest_dataset import (
    build_dataset,
    create_dataset,
    freeze_dataset,
    validate_dataset,
)
from data_collect.utils.db import get_connection


def main() -> None:
    parser = argparse.ArgumentParser(description="执行历史 Backtest Dataset 交接流程")
    parser.add_argument("--baseline-tag", required=True)
    parser.add_argument("--schema-version", required=True)
    parser.add_argument("--expected-snapshots", type=int, required=True)
    parser.add_argument("--warmup-start", required=True)
    parser.add_argument("--evaluation-start", required=True)
    parser.add_argument("--evaluation-end", required=True)
    parser.add_argument("--out-of-sample-start")
    parser.add_argument("--dataset-name", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--calendar-rule-version", default="tushare-sse-v1")
    parser.add_argument("--market-rule-version", default="cn-a-share-v2")
    parser.add_argument("--fee-rule-version", default="research-fee-assumption-v3")
    parser.add_argument("--benchmark-code", default="000300.SH")
    parser.add_argument("--code-archive-path")
    parser.add_argument("--dataset-id", help="续跑已有的 DRAFT/FAILED Dataset")
    args = parser.parse_args()

    if args.dataset_id:
        dataset_id = args.dataset_id
    else:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """SELECT snapshot_id
                     FROM research.snapshot_manifest
                    WHERE data_baseline_tag=%s
                      AND schema_version=%s
                      AND status='FROZEN'
                    ORDER BY research_cutoff_date""",
                (args.baseline_tag, args.schema_version),
            )
            snapshot_ids = [str(row[0]) for row in cur.fetchall()]
        if len(snapshot_ids) != args.expected_snapshots:
            raise RuntimeError(
                f"expected {args.expected_snapshots} frozen snapshots, got {len(snapshot_ids)}"
            )

        dataset_id = create_dataset(
            snapshot_ids=snapshot_ids,
            dataset_name=args.dataset_name,
            dataset_version=args.dataset_version,
            baseline_tag=args.baseline_tag,
            warmup_start=date.fromisoformat(args.warmup_start),
            evaluation_start=date.fromisoformat(args.evaluation_start),
            evaluation_end=date.fromisoformat(args.evaluation_end),
            out_of_sample_start=(
                date.fromisoformat(args.out_of_sample_start)
                if args.out_of_sample_start else None
            ),
            calendar_rule_version=args.calendar_rule_version,
            market_rule_version=args.market_rule_version,
            fee_rule_version=args.fee_rule_version,
            benchmark_code=args.benchmark_code,
        )
    print(f"DATASET_ID={dataset_id}", flush=True)
    print(f"BUILD={build_dataset(dataset_id)}", flush=True)
    print(f"VALIDATE={validate_dataset(dataset_id)}", flush=True)
    freeze_dataset(dataset_id, code_archive_path=args.code_archive_path)
    print(f"FROZEN_DATASET_ID={dataset_id}", flush=True)


if __name__ == "__main__":
    main()
