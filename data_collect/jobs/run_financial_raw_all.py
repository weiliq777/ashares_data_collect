"""按顺序初始化四张财务 Raw 表，避免同时请求造成 API/数据库压力。"""
from __future__ import annotations

import argparse

from data_collect.jobs.tushare_financial_raw_init import run


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="20160801")
    parser.add_argument("--end-date", default="20210801")
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--batch-pause", type=float, default=5.0)
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["fina_indicator", "income", "balancesheet", "cashflow"],
        choices=["fina_indicator", "income", "balancesheet", "cashflow"],
    )
    args = parser.parse_args()
    for dataset in args.datasets:
        print(
            run(
                dataset,
                start_date=args.start_date,
                end_date=args.end_date,
                batch_size=args.batch_size,
                batch_pause=args.batch_pause,
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
