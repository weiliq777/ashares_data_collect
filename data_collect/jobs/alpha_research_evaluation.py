"""Resume Alpha statistics from a verified local sample artifact."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from data_collect.alpha_research.evaluation_artifacts import evaluate_sample_artifact


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="从已验证的 Alpha Label Sample Artifact 恢复横截面统计与报告"
    )
    parser.add_argument("--sample-manifest", required=True, type=Path)
    parser.add_argument("--bootstrap-repetitions", type=int)
    parser.add_argument("--min-bootstrap-dates", type=int)
    parser.add_argument("--seed", type=int)
    parser.add_argument(
        "--assumed-delist-return", type=float, action="append",
        help="仅用于退市敏感性分析，可重复指定；默认 -1、-0.5、0",
    )
    args = parser.parse_args(argv)
    result = evaluate_sample_artifact(
        args.sample_manifest,
        bootstrap_repetitions=args.bootstrap_repetitions,
        min_bootstrap_dates=args.min_bootstrap_dates,
        seed=args.seed,
        assumed_delist_returns=(
            tuple(args.assumed_delist_return)
            if args.assumed_delist_return is not None else (-1.0, -0.5, 0.0)
        ),
    )
    print(json.dumps({key: value for key, value in result.items() if key != "statistics"}, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
