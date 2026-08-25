"""Recover readable rows from the preserved damaged daily Raw table.

The damaged relation is never modified.  Each date window is committed
independently; a read error is narrowed by date and then by stock code so a
single damaged page cannot abort the complete recovery.
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.raw_repository import RawRepository


SOURCE = '"public"."tushare_daily_raw_corrupt_20260811"'
TARGET = "tushare_daily_raw"


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"recover-daily-raw-{label}-{uuid4().hex}", job_name="recover_daily_raw",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, RawRepository(kernel), context


def _copy_window(start: date, end: date, codes: list[str] | None = None) -> int:
    kernel, repository, context = _service("copy")
    try:
        return repository.recover_corrupt_window(context, TARGET, SOURCE, start, end, codes)
    finally:
        kernel.close()


def _codes() -> list[str]:
    kernel, repository, context = _service("codes")
    try:
        return repository.stock_codes(context)
    finally:
        kernel.close()


def _recover_range(start: date, end: date, failures: list[str]) -> int:
    try:
        inserted = _copy_window(start, end)
        print(f"recovered {start}..{end} rows={inserted}", flush=True)
        return inserted
    except Exception as exc:  # noqa: BLE001
        if start < end:
            midpoint = start + (end - start) // 2
            print(f"split {start}..{end}: {exc}", flush=True)
            return (
                _recover_range(start, midpoint, failures)
                + _recover_range(midpoint + timedelta(days=1), end, failures)
            )

        total = 0
        codes = _codes()
        for offset in range(0, len(codes), 100):
            batch = codes[offset : offset + 100]
            try:
                total += _copy_window(start, end, batch)
            except Exception as batch_exc:  # noqa: BLE001
                if len(batch) == 1:
                    failures.append(f"{start.isoformat()}:{batch[0]}: {batch_exc}")
                else:
                    for code in batch:
                        try:
                            total += _copy_window(start, end, [code])
                        except Exception as one_exc:  # noqa: BLE001
                            failures.append(f"{start.isoformat()}:{code}: {one_exc}")
        return total


def run(
    start: date = date(2016, 8, 1),
    end: date = date(2026, 8, 6),
    window_days: int = 30,
) -> dict:
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    total = 0
    failures: list[str] = []
    current = start
    while current <= end:
        window_end = min(current + timedelta(days=window_days - 1), end)
        total += _recover_range(current, window_end, failures)
        current = window_end + timedelta(days=1)
    result = {"inserted": total, "failures": failures}
    print(result, flush=True)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2016, 8, 1))
    parser.add_argument("--end-date", type=date.fromisoformat, default=date(2026, 8, 6))
    parser.add_argument("--window-days", type=int, default=30)
    args = parser.parse_args()
    result = run(args.start_date, args.end_date, args.window_days)
    raise SystemExit(1 if result["failures"] else 0)
