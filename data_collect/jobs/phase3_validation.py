"""阶段三只读验证入口。"""
from __future__ import annotations

import argparse
import json

from data_collect.backtest.validation import verify_pit_cutoff_consistency


def main() -> None:
    parser = argparse.ArgumentParser(description="阶段三 PIT 截面只读校验")
    parser.add_argument("--target-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--snapshot-id")
    args = parser.parse_args()
    report = verify_pit_cutoff_consistency(args.target_date, args.snapshot_id)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
