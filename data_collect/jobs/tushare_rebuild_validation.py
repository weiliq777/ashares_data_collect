"""从 Tushare Raw 重建到指定验证表，不触碰生产 Standard 表。"""
from __future__ import annotations

import argparse

from data_collect.jobs.tushare_raw_to_standard import _payload_frames
from data_collect.normalize.tushare_daily import normalize_daily
from data_collect.utils.db import save_to_postgres


def rebuild_daily_validation(
    source_table: str = "tushare_daily_raw",
    target_table: str = "daily_kline_rebuild_validation",
    chunk_size: int = 5000,
) -> tuple[int, int, int]:
    attempted = inserted = chunks = 0
    for raw in _payload_frames(source_table, chunk_size=chunk_size):
        frame = normalize_daily(raw)
        if frame.empty:
            continue
        current, affected = save_to_postgres(frame, table_name=target_table)
        attempted += current
        inserted += affected
        chunks += 1
        if chunks % 50 == 0:
            print(
                f"progress chunks={chunks} attempted={attempted} inserted={inserted}",
                flush=True,
            )
    return chunks, attempted, inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Raw 到验证 Standard 表的重建任务")
    parser.add_argument("--source-table", default="tushare_daily_raw")
    parser.add_argument("--target-table", default="daily_kline_rebuild_validation")
    parser.add_argument("--chunk-size", type=int, default=5000)
    args = parser.parse_args()
    result = rebuild_daily_validation(
        source_table=args.source_table,
        target_table=args.target_table,
        chunk_size=args.chunk_size,
    )
    print(f"completed chunks={result[0]} attempted={result[1]} inserted={result[2]}")


if __name__ == "__main__":
    main()
