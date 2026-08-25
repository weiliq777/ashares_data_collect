"""补齐财务 warm-up，并顺序重建财务 Standard/PIT/Feature。"""
from __future__ import annotations

import argparse
from datetime import date

from data_collect.jobs.tushare_financial_raw_init import run as run_raw
from data_collect.jobs.tushare_fina_indicator_version import _run_unlocked as run_indicator_standard
from data_collect.jobs.tushare_financial_raw_to_standard import _run_unlocked as run_statement_standard
from data_collect.jobs.tushare_foundation_features import rebuild_company_financial_v2
from data_collect.jobs.tushare_foundation_pipeline import latest_completed_trade_date
from data_collect.jobs.tushare_job_lock import warehouse_write_job


DATASETS = ("fina_indicator", "income", "balancesheet", "cashflow")


@warehouse_write_job
def run(start_date: str = "20160801", end_date: str = "20210801",
        batch_size: int = 25, batch_pause: float = 5.0) -> dict[str, object]:
    result: dict[str, object] = {"raw": {}}
    for dataset in DATASETS:
        result["raw"][dataset] = run_raw(
            dataset,
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
            batch_pause=batch_pause,
        )

    # Raw/version tables are complete before any derived table is rebuilt.
    result["fina_indicator_standard"] = run_indicator_standard(
        backfill=False, convert=True
    )
    result["statement_standard"] = run_statement_standard("all")
    result["company_financial_v2"] = rebuild_company_financial_v2(
        None,
        date(2016, 8, 1),
        date(2016, 8, 1),
        latest_completed_trade_date(),
        target_table="feature.company_financial_v2",
        batch_stocks=5,
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="20160801")
    parser.add_argument("--end-date", default="20210801")
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--batch-pause", type=float, default=5.0)
    args = parser.parse_args()
    print(run(args.start_date, args.end_date, args.batch_size, args.batch_pause), flush=True)


if __name__ == "__main__":
    main()
