"""Bounded, resumable migration of public.daily_kline to std.daily_kline."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, timedelta
from uuid import UUID

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.schema_migration_repository import SchemaMigrationRepository


MIGRATION_ID = "7c7e6f3d-6e6e-4f3b-88c2-f7eb4cc8bb19"
JOB_NAME = "migrate_daily_kline"
WINDOW_DAYS = 7
SOURCE_START = date(2016, 8, 1)
SOURCE_END = date(2026, 8, 14)


def windows(start: date, end_inclusive: date):
    if start > end_inclusive:
        raise ValueError("start cannot be after end")
    cursor = start
    while cursor <= end_inclusive:
        end_exclusive = min(end_inclusive + timedelta(days=1), cursor + timedelta(days=WINDOW_DAYS))
        yield cursor, end_exclusive
        cursor = end_exclusive


def run(
    start: str | date = "2016-08-01",
    end: str | date = "2026-08-14",
    *,
    auto_cutover: bool = True,
) -> dict:
    start_date = date.fromisoformat(start) if isinstance(start, str) else start
    end_date = date.fromisoformat(end) if isinstance(end, str) else end
    # This UUID identifies one physical migration, not an arbitrary slice.
    # Reusing it for a partial range could incorrectly mark a partial copy as
    # the complete migration in ops.foundation_table_migration.
    if start_date != SOURCE_START or end_date != SOURCE_END:
        raise ValueError(
            "daily_kline migration range must be the registered full range "
            f"{SOURCE_START}..{SOURCE_END}"
        )
    UUID(MIGRATION_ID)
    run_id = f"schema-migration-daily-kline:{start_date}:{end_date}"
    context = RunContext(
        run_id=run_id, job_name=JOB_NAME, code_version="unknown",
        config_hash="unknown", pipeline_version="data-foundation-stage3-large-table",
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = SchemaMigrationRepository(kernel)
    migration_lock = kernel.lock_manager.session_exclusive(
        "std.daily_kline.partition-migration", MIGRATION_ID,
    )
    lock_acquired = False
    completed = skipped = 0
    aggregate_source = hashlib.sha256()
    aggregate_target = hashlib.sha256()
    source_total = target_total = 0
    try:
        # Prevent two supervisors from copying the same bounded window into
        # the same target primary key at the same time.
        migration_lock.__enter__()
        lock_acquired = True
        current = repository.get(context, MIGRATION_ID)
        if not current:
            repository.register(
                context, migration_id=MIGRATION_ID,
                source_table="public.daily_kline", target_table="std.daily_kline",
                notes="bounded partitioned copy; public source retained until validated cutover",
            )
        else:
            if (
                current["source_table"] != "public.daily_kline"
                or current["target_table"] != "std.daily_kline"
            ):
                raise RuntimeError(
                    f"migration {MIGRATION_ID} is registered for an unexpected table pair: {current}"
                )
            if current["status"] in {"CUTOVER", "ROLLED_BACK", "ABORTED"}:
                raise RuntimeError(
                    f"migration {MIGRATION_ID} is in terminal state {current['status']}; refusing to copy"
                )
        for index, (window_start, window_end_exclusive) in enumerate(windows(start_date, end_date), start=1):
            scope_key = f"daily_kline:{window_start}:{window_end_exclusive}"
            state = kernel.checkpoint.load(run_id, JOB_NAME, scope_key)
            if state is not None and state.done:
                skipped += 1
                # A checkpoint only skips the copy. It never skips current
                # source/target validation, so stale or externally modified
                # target rows are still detected on resume.
                evidence = repository.validate_daily_kline_window(
                    context, MIGRATION_ID, window_start, window_end_exclusive,
                )
                if (
                    evidence["source_row_count"] != evidence["target_row_count"]
                    or evidence["source_content_hash"] != evidence["target_content_hash"]
                ):
                    # A completed checkpoint is not proof that the target is
                    # still intact. Repair the bounded window, then revalidate.
                    repository.prepare_daily_kline_window(
                        context, MIGRATION_ID, window_start, window_end_exclusive,
                    )
                    evidence = repository.validate_daily_kline_window(
                        context, MIGRATION_ID, window_start, window_end_exclusive,
                    )
                    if (
                        evidence["source_row_count"] != evidence["target_row_count"]
                        or evidence["source_content_hash"] != evidence["target_content_hash"]
                    ):
                        raise RuntimeError(
                            f"daily_kline repaired window mismatch: {evidence}"
                        )
                    completed += 1
            else:
                repository.prepare_daily_kline_window(
                    context, MIGRATION_ID, window_start, window_end_exclusive,
                )
                evidence = repository.validate_daily_kline_window(
                    context, MIGRATION_ID, window_start, window_end_exclusive,
                )
                if (
                    evidence["source_row_count"] != evidence["target_row_count"]
                    or evidence["source_content_hash"] != evidence["target_content_hash"]
                ):
                    raise RuntimeError(f"daily_kline window mismatch: {evidence}")
                kernel.checkpoint.mark_progress(run_id, JOB_NAME, scope_key, index)
                kernel.checkpoint.mark_done(run_id, JOB_NAME, scope_key)
                completed += 1
            source_total += evidence["source_row_count"]
            target_total += evidence["target_row_count"]
            aggregate_source.update(
                f"{window_start}:{evidence['source_row_count']}:{evidence['source_content_hash']}\n".encode()
            )
            aggregate_target.update(
                f"{window_start}:{evidence['target_row_count']}:{evidence['target_content_hash']}\n".encode()
            )
            print(json.dumps({
                "window_start": window_start,
                "window_end_exclusive": window_end_exclusive,
                "source_rows": evidence["source_row_count"],
                "target_rows": evidence["target_row_count"],
                "completed_windows": completed,
                "skipped_windows": skipped,
            }, ensure_ascii=False, default=str), flush=True)
        final = {
            "source_row_count": source_total,
            "target_row_count": target_total,
            "source_content_hash": aggregate_source.hexdigest(),
            "target_content_hash": aggregate_target.hexdigest(),
        }
        if final["source_row_count"] != final["target_row_count"] or final["source_content_hash"] != final["target_content_hash"]:
            raise RuntimeError(f"daily_kline aggregate mismatch: {final}")
        repository.record_validation(context, migration_id=MIGRATION_ID, **final)
        backup_table = None
        status = "VALIDATED"
        if auto_cutover:
            backup_table = repository.cutover_daily_kline_partitioned(
                context, MIGRATION_ID, approved_by="automated:content-hash-validated",
            )
            status = "CUTOVER"
        result = {
            "completed_windows": completed, "skipped_windows": skipped,
            "status": status, "backup_table": backup_table, **final,
        }
        print(json.dumps(result, ensure_ascii=False, default=str))
        return result
    finally:
        if lock_acquired:
            migration_lock.__exit__(None, None, None)
        kernel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2016-08-01")
    parser.add_argument("--end", default="2026-08-14")
    parser.add_argument("--no-cutover", action="store_true")
    args = parser.parse_args()
    run(args.start, args.end, auto_cutover=not args.no_cutover)
