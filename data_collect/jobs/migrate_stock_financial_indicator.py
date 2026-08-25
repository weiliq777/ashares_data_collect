"""Bounded migration of the legacy financial indicator compatibility table."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, timedelta
from uuid import UUID

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.schema_migration_repository import SchemaMigrationRepository
from data_collect.jobs.tushare_job_lock import WAREHOUSE_WRITE_LOCK

MIGRATION_ID = "c61c07a4-f3e1-4cb0-8aa8-cb0e1ec5a5b8"
JOB_NAME = "migrate_stock_financial_indicator"
WINDOW_DAYS = 7


def run() -> dict:
    UUID(MIGRATION_ID)
    context = RunContext(
        run_id=f"schema-migration:stock-financial-indicator:{MIGRATION_ID}",
        job_name=JOB_NAME, code_version="unknown", config_hash="unknown",
        pipeline_version="data-foundation-stage3-financial-compatibility",
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = SchemaMigrationRepository(kernel)
    lock = kernel.lock_manager.session_exclusive("warehouse-write", WAREHOUSE_WRITE_LOCK)
    acquired = False
    completed = skipped = 0
    source_total = target_total = 0
    source_hash = hashlib.sha256(); target_hash = hashlib.sha256()
    try:
        lock.__enter__(); acquired = True
        current = repository.get(context, MIGRATION_ID)
        if not current:
            repository.register(
                context, migration_id=MIGRATION_ID,
                source_table="public.stock_financial_indicator",
                target_table="std.stock_financial_indicator",
                notes="legacy compatibility table; canonical financial v2 uses versioned std tables",
            )
        elif current["status"] in {"CUTOVER", "ROLLED_BACK", "ABORTED"}:
            raise RuntimeError(f"migration {MIGRATION_ID} is terminal: {current['status']}")

        bounds = repository.stock_financial_indicator_bounds(context, MIGRATION_ID)
        if bounds is None:
            raise RuntimeError("stock_financial_indicator has no report-date bounds")
        start, end = bounds
        cursor = start
        window_number = 0
        while cursor <= end:
            window_end = min(end + timedelta(days=1), cursor + timedelta(days=WINDOW_DAYS))
            window_number += 1
            scope_key = f"stock_financial_indicator:{cursor}:{window_end}"
            state = kernel.checkpoint.load(context.run_id, JOB_NAME, scope_key)
            if state is not None and state.done:
                skipped += 1
            else:
                repository.prepare_stock_financial_indicator_window(
                    context, MIGRATION_ID, cursor, window_end,
                )
                kernel.checkpoint.mark_progress(context.run_id, JOB_NAME, scope_key, window_number)
                kernel.checkpoint.mark_done(context.run_id, JOB_NAME, scope_key)
                completed += 1
            evidence = repository.validate_stock_financial_indicator_window(
                context, MIGRATION_ID, cursor, window_end,
            )
            if evidence["source_row_count"] != evidence["target_row_count"] or evidence["source_content_hash"] != evidence["target_content_hash"]:
                raise RuntimeError(f"stock_financial_indicator window mismatch: {evidence}")
            source_total += evidence["source_row_count"]; target_total += evidence["target_row_count"]
            source_hash.update(f"{cursor}:{evidence['source_row_count']}:{evidence['source_content_hash']}\n".encode())
            target_hash.update(f"{cursor}:{evidence['target_row_count']}:{evidence['target_content_hash']}\n".encode())
            print(json.dumps({"window_start": cursor, "window_end_exclusive": window_end,
                              "source_rows": evidence["source_row_count"], "target_rows": evidence["target_row_count"],
                              "completed_windows": completed, "skipped_windows": skipped}, default=str), flush=True)
            cursor = window_end

        final = {"source_row_count": source_total, "target_row_count": target_total,
                 "source_content_hash": source_hash.hexdigest(), "target_content_hash": target_hash.hexdigest()}
        if final["source_row_count"] != final["target_row_count"] or final["source_content_hash"] != final["target_content_hash"]:
            raise RuntimeError(f"stock_financial_indicator aggregate mismatch: {final}")
        repository.record_validation(context, migration_id=MIGRATION_ID, **final)
        result = {"status": "VALIDATED", "completed_windows": completed, "skipped_windows": skipped, **final}
        print(json.dumps(result, default=str), flush=True); return result
    finally:
        if acquired: lock.__exit__(None, None, None)
        kernel.close()


def cutover_only() -> dict:
    context = RunContext(
        run_id=f"schema-cutover:stock-financial-indicator:{MIGRATION_ID}",
        job_name=JOB_NAME, code_version="unknown", config_hash="unknown",
        pipeline_version="data-foundation-stage3-financial-compatibility",
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = SchemaMigrationRepository(kernel)
    lock = kernel.lock_manager.session_exclusive("warehouse-write", WAREHOUSE_WRITE_LOCK)
    acquired = False
    try:
        lock.__enter__(); acquired = True
        backup = repository.cutover_financial_standard(
            context, MIGRATION_ID, "stock_financial_indicator", "automated:content-hash-validated",
        )
        result = {"status": "CUTOVER", "backup_table": backup}
        print(json.dumps(result, default=str), flush=True); return result
    finally:
        if acquired: lock.__exit__(None, None, None)
        kernel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutover-only", action="store_true")
    args = parser.parse_args()
    cutover_only() if args.cutover_only else run()
