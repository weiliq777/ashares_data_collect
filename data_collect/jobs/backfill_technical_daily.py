"""Bounded, resumable historical backfill for technical_daily_v1."""
from __future__ import annotations

import argparse
from calendar import monthrange
from datetime import date, timedelta

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.wave3_repository import Wave3Repository
from data_collect.features.technical_daily import calculate_technical_features, to_feature_rows

# Keep this boundary explicit: each output month is one resumable unit, while
# the calculation reads a bounded warm-up window for indicator history.
LOOKBACK_DAYS = 60
JOB_NAME = "backfill_technical_daily"
STOCK_BATCH_SIZE = 250


def month_windows(start: date, end: date) -> list[tuple[date, date]]:
    if start > end:
        raise ValueError("start cannot be after end")
    windows: list[tuple[date, date]] = []
    cursor = start.replace(day=1)
    while cursor <= end:
        month_end = date(cursor.year, cursor.month, monthrange(cursor.year, cursor.month)[1])
        windows.append((max(start, cursor), min(end, month_end)))
        cursor = month_end + timedelta(days=1)
    return windows


def bounded_windows(start: date, end: date, *, max_days: int = 7) -> list[tuple[date, date]]:
    """Split one output month so calculation memory stays bounded."""
    if max_days <= 0:
        raise ValueError("max_days must be positive")
    windows: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        window_end = min(end, cursor + timedelta(days=max_days - 1))
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def stock_batches(codes: list[str], size: int = STOCK_BATCH_SIZE):
    if size <= 0:
        raise ValueError("stock batch size must be positive")
    for offset in range(0, len(codes), size):
        yield codes[offset : offset + size]


def run(start: str | date, end: str | date, *, run_id: str | None = None) -> dict[str, int]:
    output_start = date.fromisoformat(start) if isinstance(start, str) else start
    output_end = date.fromisoformat(end) if isinstance(end, str) else end
    run_id = run_id or f"technical-daily-backfill:{output_start}:{output_end}"
    context = RunContext(
        run_id=run_id, job_name=JOB_NAME, code_version="unknown",
        config_hash="unknown", pipeline_version="data-access-v1",
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = Wave3Repository(kernel)
    completed = skipped = written = 0
    try:
        for index, (chunk_start, chunk_end) in enumerate(month_windows(output_start, output_end)):
            scope_key = f"technical_daily_v1:{chunk_start}:{chunk_end}"
            state = kernel.checkpoint.load(run_id, JOB_NAME, scope_key)
            if state is not None and state.done:
                skipped += 1
                continue

            for sub_index, (sub_start, sub_end) in enumerate(
                bounded_windows(chunk_start, chunk_end), start=1
            ):
                sub_scope_key = f"technical_daily_v1:{sub_start}:{sub_end}"
                sub_state = kernel.checkpoint.load(run_id, JOB_NAME, sub_scope_key)
                if sub_state is not None and sub_state.done:
                    continue
                input_start = sub_start - timedelta(days=LOOKBACK_DAYS * 2)
                codes = repository.daily_kline_stock_codes(sub_start, sub_end, context)
                if not codes:
                    kernel.checkpoint.mark_done(run_id, JOB_NAME, sub_scope_key)
                    continue
                for code_batch in stock_batches(codes):
                    prices = pd.DataFrame(
                        repository.daily_kline_for_codes(
                            input_start, sub_end, code_batch, context
                        )
                    )
                    if prices.empty:
                        continue
                    dividends = pd.DataFrame(
                        repository.divid_factors(
                            code_batch, context, start_date=input_start, end_date=sub_end
                        )
                    )
                    features = calculate_technical_features(prices, dividends)
                    features = features[
                        (features["trade_date"] >= sub_start)
                        & (features["trade_date"] <= sub_end)
                    ]
                    written += repository.upsert_technical_features(
                        to_feature_rows(features), sub_start, sub_end, context
                    )
                    del prices, dividends, features
                kernel.checkpoint.mark_progress(run_id, JOB_NAME, sub_scope_key, sub_index)
                kernel.checkpoint.mark_done(run_id, JOB_NAME, sub_scope_key)
            kernel.checkpoint.mark_progress(run_id, JOB_NAME, scope_key, index + 1)
            kernel.checkpoint.mark_done(run_id, JOB_NAME, scope_key)
            completed += 1
        return {"completed_chunks": completed, "skipped_chunks": skipped, "written": written}
    finally:
        kernel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("start")
    parser.add_argument("end")
    args = parser.parse_args()
    print(run(args.start, args.end))
