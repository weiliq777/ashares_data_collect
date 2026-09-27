"""Run one bounded technical_daily_v1 repair under the warehouse write lock."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Executing a file under ``tools/`` does not put the repository root on
# sys.path; make the operational entry point independent of the caller's
# current working directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_collect.jobs.technical_daily import build
from data_collect.jobs.tushare_job_lock import warehouse_write_job


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("start_date", help="inclusive ISO date, YYYY-MM-DD")
    parser.add_argument("end_date", help="inclusive ISO date, YYYY-MM-DD")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    result = warehouse_write_job(build)(args.start_date, args.end_date, run_id=args.run_id)
    print(result, flush=True)


if __name__ == "__main__":
    main()
