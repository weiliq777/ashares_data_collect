"""Bounded, resumable migration of public.valuation_daily to Standard."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, timedelta
from uuid import UUID

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.schema_migration_repository import SchemaMigrationRepository

MIGRATION_ID = "6cb3c9bd-e5b5-4d7a-bf9e-5e4f0b1b9f49"
JOB_NAME = "migrate_valuation_daily"
WINDOW_DAYS = 7
SOURCE_START = date(2016, 8, 1)
SOURCE_END = date(2026, 8, 14)


def windows(start: date, end_inclusive: date):
    cursor = start
    while cursor <= end_inclusive:
        end_exclusive = min(end_inclusive + timedelta(days=1), cursor + timedelta(days=WINDOW_DAYS))
        yield cursor, end_exclusive
        cursor = end_exclusive


def run(start: str | date = SOURCE_START, end: str | date = SOURCE_END) -> dict:
    start_date = date.fromisoformat(start) if isinstance(start, str) else start
    end_date = date.fromisoformat(end) if isinstance(end, str) else end
    if (start_date, end_date) != (SOURCE_START, SOURCE_END):
        raise ValueError(f"valuation_daily migration range must be {SOURCE_START}..{SOURCE_END}")
    UUID(MIGRATION_ID)
    run_id = f"schema-migration-valuation-daily:{start_date}:{end_date}"
    context = RunContext(run_id=run_id, job_name=JOB_NAME, code_version="unknown",
                         config_hash="unknown", pipeline_version="data-foundation-stage3-large-table")
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = SchemaMigrationRepository(kernel)
    lock = kernel.lock_manager.session_exclusive("std.valuation_daily.partition-migration", MIGRATION_ID)
    acquired = False
    completed = skipped = 0
    source_total = target_total = 0
    source_hash = hashlib.sha256(); target_hash = hashlib.sha256()
    try:
        lock.__enter__(); acquired = True
        current = repository.get(context, MIGRATION_ID)
        if not current:
            repository.register(context, migration_id=MIGRATION_ID,
                                source_table="public.valuation_daily", target_table="std.valuation_daily",
                                notes="bounded partitioned copy; public source retained until validated cutover")
        elif current["status"] in {"CUTOVER", "ROLLED_BACK", "ABORTED"}:
            raise RuntimeError(f"migration {MIGRATION_ID} is in terminal state {current['status']}")
        for index, (window_start, window_end_exclusive) in enumerate(windows(start_date, end_date), start=1):
            scope_key = f"valuation_daily:{window_start}:{window_end_exclusive}"
            state = kernel.checkpoint.load(run_id, JOB_NAME, scope_key)
            if state is not None and state.done:
                skipped += 1
            else:
                repository.prepare_valuation_daily_window(context, MIGRATION_ID, window_start, window_end_exclusive)
                kernel.checkpoint.mark_progress(run_id, JOB_NAME, scope_key, index)
                kernel.checkpoint.mark_done(run_id, JOB_NAME, scope_key)
                completed += 1
            evidence = repository.validate_valuation_daily_window(context, MIGRATION_ID, window_start, window_end_exclusive)
            if evidence["source_row_count"] != evidence["target_row_count"] or evidence["source_content_hash"] != evidence["target_content_hash"]:
                raise RuntimeError(f"valuation_daily window mismatch: {evidence}")
            source_total += evidence["source_row_count"]; target_total += evidence["target_row_count"]
            source_hash.update(f"{window_start}:{evidence['source_row_count']}:{evidence['source_content_hash']}\n".encode())
            target_hash.update(f"{window_start}:{evidence['target_row_count']}:{evidence['target_content_hash']}\n".encode())
            print(json.dumps({"window_start": window_start, "window_end_exclusive": window_end_exclusive,
                              "source_rows": evidence["source_row_count"], "target_rows": evidence["target_row_count"],
                              "completed_windows": completed, "skipped_windows": skipped}, default=str), flush=True)
        final = {"source_row_count": source_total, "target_row_count": target_total,
                 "source_content_hash": source_hash.hexdigest(), "target_content_hash": target_hash.hexdigest()}
        if final["source_row_count"] != final["target_row_count"] or final["source_content_hash"] != final["target_content_hash"]:
            raise RuntimeError(f"valuation_daily aggregate mismatch: {final}")
        repository.record_validation(context, migration_id=MIGRATION_ID, **final)
        result = {"status": "VALIDATED", "completed_windows": completed, "skipped_windows": skipped, **final}
        print(json.dumps(result, default=str)); return result
    finally:
        if acquired: lock.__exit__(None, None, None)
        kernel.close()


def cutover_only(*, approved_by: str = "automated:content-hash-validated") -> dict:
    UUID(MIGRATION_ID)
    context = RunContext(run_id=f"schema-cutover-valuation-daily:{MIGRATION_ID}", job_name=JOB_NAME,
                         code_version="unknown", config_hash="unknown", pipeline_version="data-foundation-stage3-large-table")
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = SchemaMigrationRepository(kernel)
    lock = kernel.lock_manager.session_exclusive("std.valuation_daily.partition-migration", MIGRATION_ID)
    acquired = False
    try:
        lock.__enter__(); acquired = True
        backup = repository.cutover_valuation_daily_partitioned(context, MIGRATION_ID, approved_by)
        result = {"status": "CUTOVER", "backup_table": backup}
        print(json.dumps(result, default=str)); return result
    finally:
        if acquired: lock.__exit__(None, None, None)
        kernel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(); parser.add_argument("--start", default=str(SOURCE_START)); parser.add_argument("--end", default=str(SOURCE_END)); parser.add_argument("--cutover-only", action="store_true")
    args = parser.parse_args(); cutover_only() if args.cutover_only else run(args.start, args.end)
