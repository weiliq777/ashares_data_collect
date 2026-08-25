"""Resumable primary-key migration of one financial Standard table to ``std``.

The job deliberately validates one bounded id window at a time.  It does not
run a table-sized string_agg/json hash and it does not cut over automatically;
application consumers are switched only after validation has completed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from uuid import UUID

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.schema_migration_repository import SchemaMigrationRepository
from data_collect.jobs.tushare_job_lock import WAREHOUSE_WRITE_LOCK


TABLES = frozenset(SchemaMigrationRepository.FINANCIAL_STANDARD_SPECS)
MIGRATION_IDS = {
    "financial_income_standard": "c3b8cc76-2e53-4f1a-9dcf-ea0d4ec10a01",
    "financial_balance_sheet_standard": "35b25b76-a2ea-45fb-bc89-91ca7edb7e02",
    "financial_cash_flow_standard": "e09a3b23-2d42-43cd-bf7e-fb2e64da5c03",
    "fina_indicator_standard": "f0efb823-5d9f-41b2-9489-a4e0c83b8d04",
}
WINDOW_IDS = 5_000


def run(table: str = "financial_income_standard") -> dict:
    if table not in TABLES:
        raise ValueError(f"unsupported financial Standard table: {table}")
    migration_id = MIGRATION_IDS[table]
    UUID(migration_id)
    spec = SchemaMigrationRepository.FINANCIAL_STANDARD_SPECS[table]
    context = RunContext(
        run_id=f"schema-migration:{table}:{migration_id}",
        job_name="migrate_financial_standard",
        code_version="unknown", config_hash="unknown",
        pipeline_version="data-foundation-stage3-financial-standard",
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = SchemaMigrationRepository(kernel)
    # A schema migration changes the same Standard relations that the
    # financial incremental/derived jobs write.  Use the shared warehouse
    # lock, not a migration-only lock domain.
    lock = kernel.lock_manager.session_exclusive("warehouse-write", WAREHOUSE_WRITE_LOCK)
    acquired = False
    completed = skipped = 0
    source_total = target_total = 0
    source_hash = hashlib.sha256()
    target_hash = hashlib.sha256()
    try:
        lock.__enter__(); acquired = True
        current = repository.get(context, migration_id)
        if not current:
            repository.register(
                context, migration_id=migration_id,
                source_table=f"public.{table}", target_table=spec["target"],
                notes="bounded primary-key copy; source retained until consumer cutover",
            )
        elif current["status"] in {"CUTOVER", "ROLLED_BACK", "ABORTED"}:
            raise RuntimeError(f"migration {migration_id} is terminal: {current['status']}")

        upper_id = repository.financial_standard_upper_id(context, table)
        cursor = 0
        window_number = 0
        while cursor < upper_id:
            start_id = cursor
            end_id = min(upper_id, start_id + WINDOW_IDS)
            window_number += 1
            scope_key = f"{table}:{start_id}:{end_id}"
            state = kernel.checkpoint.load(context.run_id, context.job_name, scope_key)
            if state is not None and state.done:
                skipped += 1
            else:
                repository.prepare_financial_standard_window(
                    context, migration_id, table, start_id, end_id,
                )
                kernel.checkpoint.mark_progress(
                    context.run_id, context.job_name, scope_key, window_number,
                )
                kernel.checkpoint.mark_done(
                    context.run_id, context.job_name, scope_key,
                )
                completed += 1

            evidence = repository.validate_financial_standard_window(
                context, migration_id, table, start_id, end_id,
            )
            if (
                evidence["source_row_count"] != evidence["target_row_count"]
                or evidence["source_content_hash"] != evidence["target_content_hash"]
            ):
                raise RuntimeError(f"{table} window mismatch: {evidence}")
            source_total += evidence["source_row_count"]
            target_total += evidence["target_row_count"]
            source_hash.update(
                f"{start_id}:{evidence['source_row_count']}:{evidence['source_content_hash']}\n".encode(),
            )
            target_hash.update(
                f"{start_id}:{evidence['target_row_count']}:{evidence['target_content_hash']}\n".encode(),
            )
            print(json.dumps({
                "table": table, "start_id": start_id, "end_id": end_id,
                "source_rows": evidence["source_row_count"],
                "target_rows": evidence["target_row_count"],
                "completed_windows": completed, "skipped_windows": skipped,
            }, default=str), flush=True)
            cursor = end_id

        final = {
            "source_row_count": source_total,
            "target_row_count": target_total,
            "source_content_hash": source_hash.hexdigest(),
            "target_content_hash": target_hash.hexdigest(),
        }
        if (
            final["source_row_count"] != final["target_row_count"]
            or final["source_content_hash"] != final["target_content_hash"]
        ):
            raise RuntimeError(f"{table} aggregate mismatch: {final}")
        repository.record_validation(context, migration_id=migration_id, **final)
        result = {"status": "VALIDATED", "table": table,
                  "completed_windows": completed, "skipped_windows": skipped, **final}
        print(json.dumps(result, default=str), flush=True)
        return result
    finally:
        if acquired:
            lock.__exit__(None, None, None)
        kernel.close()


def cutover_only(table: str) -> dict:
    if table not in TABLES:
        raise ValueError(f"unsupported financial Standard table: {table}")
    migration_id = MIGRATION_IDS[table]
    context = RunContext(
        run_id=f"schema-cutover:{table}:{migration_id}",
        job_name="migrate_financial_standard",
        code_version="unknown", config_hash="unknown",
        pipeline_version="data-foundation-stage3-financial-standard",
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = SchemaMigrationRepository(kernel)
    lock = kernel.lock_manager.session_exclusive("warehouse-write", WAREHOUSE_WRITE_LOCK)
    acquired = False
    try:
        lock.__enter__(); acquired = True
        backup = repository.cutover_financial_standard(context, migration_id, table, "automated:content-hash-validated")
        result = {"status": "CUTOVER", "table": table, "backup_table": backup}
        print(json.dumps(result, default=str), flush=True)
        return result
    finally:
        if acquired:
            lock.__exit__(None, None, None)
        kernel.close()


def finalize_only(table: str) -> dict:
    """Repair the control-plane status after a completed catalog cutover."""
    if table not in TABLES:
        raise ValueError(f"unsupported financial Standard table: {table}")
    migration_id = MIGRATION_IDS[table]
    context = RunContext(
        run_id=f"schema-finalize:{table}:{migration_id}",
        job_name="migrate_financial_standard",
        code_version="unknown", config_hash="unknown",
        pipeline_version="data-foundation-stage3-financial-standard",
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = SchemaMigrationRepository(kernel)
    lock = kernel.lock_manager.session_exclusive("warehouse-write", WAREHOUSE_WRITE_LOCK)
    acquired = False
    try:
        lock.__enter__(); acquired = True
        current = repository.get(context, migration_id)
        if not current or current["status"] != "VALIDATED":
            raise ValueError("financial Standard status finalization requires VALIDATED migration")
        repository.cutover(context, migration_id, "automated:content-hash-validated")
        result = {"status": "CUTOVER", "table": table, "control_plane": "updated"}
        print(json.dumps(result, default=str), flush=True)
        return result
    finally:
        if acquired:
            lock.__exit__(None, None, None)
        kernel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", choices=sorted(TABLES), default="financial_income_standard")
    parser.add_argument("--cutover-only", action="store_true")
    parser.add_argument("--finalize-only", action="store_true")
    args = parser.parse_args()
    finalize_only(args.table) if args.finalize_only else (cutover_only(args.table) if args.cutover_only else run(args.table))
