"""Backfill pre-evaluation company_financial_v2 rows with bounded transactions."""
from __future__ import annotations

import argparse
from datetime import date

from data_collect.jobs.tushare_foundation_features import rebuild_company_financial_v2
from data_collect.jobs.tushare_foundation_features import DEFAULT_FEATURE_BATCH_STOCKS
from data_collect.jobs.tushare_job_lock import warehouse_write_job


DEFAULT_WARMUP_START = date(2016, 8, 1)
DEFAULT_OUTPUT_END = date(2021, 8, 1)


@warehouse_write_job
def run(
    *,
    requested_codes: list[str] | None = None,
    batch_stocks: int = DEFAULT_FEATURE_BATCH_STOCKS,
) -> dict[str, int]:
    """Materialize historical feature rows needed at the first research date."""
    return rebuild_company_financial_v2(
        None,
        DEFAULT_WARMUP_START,
        DEFAULT_WARMUP_START,
        DEFAULT_OUTPUT_END,
        requested_codes=requested_codes,
        target_table="feature.company_financial_v2",
        batch_stocks=batch_stocks,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code", action="append", dest="codes")
    parser.add_argument("--batch-stocks", type=int, default=DEFAULT_FEATURE_BATCH_STOCKS)
    args = parser.parse_args()
    print(
        run(requested_codes=args.codes, batch_stocks=args.batch_stocks),
        flush=True,
    )


if __name__ == "__main__":
    main()
