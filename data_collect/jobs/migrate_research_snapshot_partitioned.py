"""Resumable, one-day-at-a-time migration of the research snapshot table.

The legacy table is retained until every day has matching count/content
evidence.  Each copy is one governed transaction; no job-level transaction
spans the historical table.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, timedelta
from uuid import UUID

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.schema_migration_repository import SchemaMigrationRepository


MIGRATION_ID = "f1baf86d-f2bb-47e7-95be-6f11f39d6b3b"
JOB_NAME = "migrate_research_snapshot_partitioned"
WINDOW_DAYS = 1


def windows(start: date, end_inclusive: date):
    cursor = start
    while cursor <= end_inclusive:
        end_exclusive = min(end_inclusive + timedelta(days=1), cursor + timedelta(days=WINDOW_DAYS))
        yield cursor, end_exclusive
        cursor = end_exclusive


def run(
    start: str | date | None = None,
    end: str | date | None = None,
    *,
    auto_cutover: bool = True,
) -> dict:
    UUID(MIGRATION_ID)
    context = RunContext(
        run_id=f"schema-migration-research-snapshot:{MIGRATION_ID}",
        job_name=JOB_NAME, code_version="unknown", config_hash="unknown",
        pipeline_version="data-foundation-partitioned-snapshot-v1",
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = SchemaMigrationRepository(kernel)
    migration_lock = kernel.lock_manager.session_exclusive(
        "research.snapshot.partition-migration", MIGRATION_ID,
    )
    lock_acquired = False
    try:
        # A resumable migration must still be single-instance.  Without a
        # session lock, two operators/process supervisors can both observe an
        # incomplete window and race into the same target primary key.
        migration_lock.__enter__()
        lock_acquired = True
        bounds = repository.snapshot_date_range(context)
        if bounds is None:
            raise RuntimeError("research.stock_snapshot_daily has no rows; refusing empty migration")
        actual_start, actual_end = bounds
        start_date = date.fromisoformat(start) if isinstance(start, str) else start or actual_start
        end_date = date.fromisoformat(end) if isinstance(end, str) else end or actual_end
        if (start_date, end_date) != (actual_start, actual_end):
            raise ValueError(
                "migration range must match the observed legacy snapshot range "
                f"{actual_start}..{actual_end}"
            )
        current = repository.get(context, MIGRATION_ID)
        if not current:
            repository.register(
                context, migration_id=MIGRATION_ID,
                source_table="research.stock_snapshot_daily",
                target_table="research.stock_snapshot_daily_partitioned",
                notes=f"one-day bounded copy; observed_range={start_date}:{end_date}",
            )
        elif current["status"] in {"CUTOVER", "ROLLED_BACK", "ABORTED"}:
            raise RuntimeError(f"migration is in terminal state {current['status']}")

        completed = skipped = 0
        resume_copy = False
        for index, (window_start, window_end_exclusive) in enumerate(windows(start_date, end_date), start=1):
            scope_key = f"research-snapshot:{window_start}:{window_end_exclusive}"
            checkpoint = kernel.checkpoint.load(context.run_id, JOB_NAME, scope_key)
            if not resume_copy and checkpoint is not None and checkpoint.done:
                skipped += 1
                continue

            # Resume at the first incomplete window.  Validate first so a
            # copy committed before a process crash is recognized without an
            # expensive partition-wide ON CONFLICT probe.
            resume_copy = True
            evidence = repository.validate_research_snapshot_window(
                context, MIGRATION_ID, window_start, window_end_exclusive,
            )
            if (
                evidence["source_row_count"] != evidence["target_row_count"]
                or evidence["source_content_hash"] != evidence["target_content_hash"]
            ):
                if evidence["target_row_count"] == 0:
                    repository.prepare_research_snapshot_window(
                        context, MIGRATION_ID, window_start, window_end_exclusive,
                    )
                else:
                    repository.repair_research_snapshot_window(
                        context, MIGRATION_ID, window_start, window_end_exclusive,
                    )
                evidence = repository.validate_research_snapshot_window(
                    context, MIGRATION_ID, window_start, window_end_exclusive,
                )
                if (
                    evidence["source_row_count"] != evidence["target_row_count"]
                    or evidence["source_content_hash"] != evidence["target_content_hash"]
                ):
                    raise RuntimeError(f"snapshot partition window mismatch: {evidence}")
            kernel.checkpoint.mark_progress(context.run_id, JOB_NAME, scope_key, index)
            kernel.checkpoint.mark_done(context.run_id, JOB_NAME, scope_key)
            completed += 1
            print(json.dumps({
                "window_start": window_start,
                "window_end_exclusive": window_end_exclusive,
                "source_rows": evidence["source_row_count"],
                "target_rows": evidence["target_row_count"],
                "completed_windows": completed,
                "skipped_windows": skipped,
            }, ensure_ascii=False, default=str), flush=True)

        # Checkpoints control copy resumption, not final proof.  After the
        # missing suffix is copied, validate every window and build final
        # source/target evidence from current contents before cutover.
        source_total = target_total = 0
        aggregate_source = hashlib.sha256()
        aggregate_target = hashlib.sha256()
        final_validation_windows = 0
        for window_start, window_end_exclusive in windows(start_date, end_date):
            evidence = repository.validate_research_snapshot_window(
                context, MIGRATION_ID, window_start, window_end_exclusive,
            )
            if (
                evidence["source_row_count"] != evidence["target_row_count"]
                or evidence["source_content_hash"] != evidence["target_content_hash"]
            ):
                raise RuntimeError(
                    "snapshot partition final validation mismatch: "
                    f"{evidence}"
                )
            source_total += evidence["source_row_count"]
            target_total += evidence["target_row_count"]
            aggregate_source.update(
                f"{window_start}:{evidence['source_row_count']}:{evidence['source_content_hash']}\n".encode()
            )
            aggregate_target.update(
                f"{window_start}:{evidence['target_row_count']}:{evidence['target_content_hash']}\n".encode()
            )
            final_validation_windows += 1

        final = {
            "source_row_count": source_total,
            "target_row_count": target_total,
            "source_content_hash": aggregate_source.hexdigest(),
            "target_content_hash": aggregate_target.hexdigest(),
        }
        if final["source_row_count"] != final["target_row_count"] or final["source_content_hash"] != final["target_content_hash"]:
            raise RuntimeError(f"snapshot partition aggregate mismatch: {final}")
        repository.record_validation(context, migration_id=MIGRATION_ID, **final)
        backup_table = None
        status = "VALIDATED"
        if auto_cutover:
            backup_table = repository.cutover_research_snapshot_partitioned(
                context, MIGRATION_ID, approved_by="automated:content-hash-validated",
            )
            status = "CUTOVER"
        result = {
            "status": status, "completed_windows": completed,
            "skipped_windows": skipped,
            "final_validation_windows": final_validation_windows,
            "backup_table": backup_table, **final,
        }
        print(json.dumps(result, ensure_ascii=False, default=str))
        return result
    finally:
        if lock_acquired:
            migration_lock.__exit__(None, None, None)
        kernel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()
    run(args.start, args.end)
