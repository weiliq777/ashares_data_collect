"""Repair missing trade-date snapshots for Tushare daily market Raw tables.

This job deliberately does not use the historical-init checkpoint as its
source of truth.  A date is eligible when the local Raw table has no row for
that trade date.  The job only appends Raw snapshots and records a separate
repair checkpoint; Standard/PIT/Feature rebuilds are a later, explicit step.
"""
from __future__ import annotations

import argparse
import logging
import time
import uuid
from datetime import date
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd

import data_collect.jobs.tushare_daily_incremental as market_shared
from data_collect.jobs.tushare_market_incremental_by_date import _request_market_frame
from data_collect.providers.tushare_client import get_pro
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_repository import MarketRepository


DATASETS = {
    "daily": {
        "raw_table": "tushare_daily_raw",
        "api_dataset": "daily",
    },
    "daily_basic": {
        "raw_table": "tushare_daily_basic_raw",
        "api_dataset": "daily_basic",
    },
}

logger = logging.getLogger("tushare_market_gap_repair")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    log_dir = Path(__file__).resolve().parents[2] / "logs"
    log_dir.mkdir(exist_ok=True)
    handler = RotatingFileHandler(
        log_dir / "tushare_market_gap_repair.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())


def _missing_dates(dataset: str, start_date: str, end_date: str) -> list[str]:
    """Return open SSE dates absent from the selected Raw table."""
    raw_table = DATASETS[dataset]["raw_table"]
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"market-gap-{dataset}", job_name="tushare_market_gap_repair",
        code_version="unknown", config_hash="unknown", pipeline_version="v1",
    )
    try:
        return MarketRepository(kernel).missing_open_dates(
            raw_table, pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date(), context
        )
    finally:
        kernel.close()


def _raw_count(dataset: str, day: str) -> int:
    raw_table = DATASETS[dataset]["raw_table"]
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"market-gap-count-{dataset}", job_name="tushare_market_gap_repair",
        code_version="unknown", config_hash="unknown", pipeline_version="v1",
    )
    try:
        return MarketRepository(kernel).raw_count(raw_table, pd.to_datetime(day).date(), context)
    finally:
        kernel.close()


def _repair_dataset(
    pro,
    dataset: str,
    start_date: str,
    end_date: str,
    pause: float,
    batch_size: int,
    dry_run: bool,
) -> tuple[int, int, int]:
    days = _missing_dates(dataset, start_date, end_date)
    batch_id = f"gap-repair-{dataset}-{uuid.uuid4().hex}"
    logger.info(
        "gap repair started dataset=%s start=%s end=%s missing_dates=%s batch_id=%s",
        dataset,
        start_date,
        end_date,
        len(days),
        batch_id,
    )
    repaired = 0
    api_rows_total = 0
    failures = 0
    checkpoint_dataset = f"{dataset}_gap_repair"

    for index, day in enumerate(days, 1):
        try:
            frame = _request_market_frame(pro, dataset, day)
            api_rows = 0 if frame is None else len(frame)
            api_rows_total += api_rows
            if frame is None or frame.empty:
                failures += 1
                message = "Tushare returned no rows; date remains unresolved"
                market_shared._checkpoint(checkpoint_dataset, day, "empty", 0, message)
                logger.error("dataset=%s date=%s api_rows=0", dataset, day)
                continue

            if dry_run:
                logger.info("dry-run dataset=%s date=%s api_rows=%s", dataset, day, api_rows)
                continue

            # This writes the immutable market version and the first-seen
            # snapshot. Existing Raw records are never updated or deleted.
            market_shared._version_and_snapshot(dataset, frame, batch_id)
            stored_rows = _raw_count(dataset, day)
            if stored_rows <= 0:
                raise RuntimeError(
                    f"API returned {api_rows} rows but {DATASETS[dataset]['raw_table']} "
                    f"still has no rows for {day}"
                )
            repaired += 1
            market_shared._checkpoint(checkpoint_dataset, day, "done", stored_rows, None)
            logger.info(
                "dataset=%s date=%s api_rows=%s raw_rows=%s progress=%s/%s",
                dataset,
                day,
                api_rows,
                stored_rows,
                index,
                len(days),
            )
        except Exception as exc:  # noqa: BLE001
            failures += 1
            logger.exception("dataset=%s date=%s repair failed", dataset, day)
            market_shared._checkpoint(checkpoint_dataset, day, "failed", 0, str(exc)[:500])
        if index % max(int(batch_size), 1) == 0:
            time.sleep(float(pause))

    logger.info(
        "gap repair finished dataset=%s missing_at_start=%s repaired=%s api_rows=%s failures=%s",
        dataset,
        len(days),
        repaired,
        api_rows_total,
        failures,
    )
    return len(days), repaired, failures


def run(
    start_date: str = "20210802",
    end_date: str | None = None,
    dataset: str = "all",
    pause: float = 2.0,
    batch_size: int = 20,
    dry_run: bool = False,
) -> dict[str, tuple[int, int, int]]:
    end = end_date or date.today().strftime("%Y%m%d")
    if dataset not in {"all", *DATASETS}:
        raise ValueError(f"unsupported dataset: {dataset}")
    selected = list(DATASETS) if dataset == "all" else [dataset]
    pro = get_pro()
    return {
        name: _repair_dataset(pro, name, start_date, end, pause, batch_size, dry_run)
        for name in selected
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair missing Tushare market Raw dates")
    parser.add_argument("--start-date", default="20210802")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--dataset", choices=["all", *DATASETS], default="all")
    parser.add_argument("--pause", type=float, default=2.0)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(run(args.start_date, args.end_date, args.dataset, args.pause, args.batch_size, args.dry_run))


if __name__ == "__main__":
    main()
