"""Automated, bounded stock-master migration and cutover."""
from __future__ import annotations

import json
from uuid import UUID

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.schema_migration_repository import SchemaMigrationRepository


MIGRATION_ID = "b5fbe4a4-e6e9-49c8-ae34-3347e6b0b5aa"


def main() -> None:
    UUID(MIGRATION_ID)
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"schema-migration-stock-basic-{MIGRATION_ID}",
        job_name="migrate_stock_basic", code_version="unknown",
        config_hash="unknown", pipeline_version="data-foundation-stage3-pilot",
    )
    repository = SchemaMigrationRepository(kernel)
    try:
        current = repository.get(context, MIGRATION_ID)
        copied = 0
        if not current:
            repository.register(
                context, migration_id=MIGRATION_ID,
                source_table="public.stock_basic_info",
                target_table="std.stock_basic_info",
                notes="automated validated cutover; public source retained for rollback",
            )
            copied = repository.prepare_stock_basic(context, MIGRATION_ID)
            validation = repository.validate_stock_basic(context, MIGRATION_ID)
        elif current["status"] == "REGISTERED":
            copied = repository.prepare_stock_basic(context, MIGRATION_ID)
            validation = repository.validate_stock_basic(context, MIGRATION_ID)
        elif current["status"] == "VALIDATED":
            validation = repository.validate_stock_basic(
                context, MIGRATION_ID, persist=True,
            )
        elif current["status"] == "CUTOVER":
            validation = repository.validate_stock_basic(
                context, MIGRATION_ID, persist=False,
            )
        else:
            raise RuntimeError(f"migration cannot resume from status={current['status']}")
        if validation["source_content_hash"] != validation["target_content_hash"]:
            raise RuntimeError(f"stock basic hash mismatch: {validation}")
        if (repository.get(context, MIGRATION_ID) or {}).get("status") == "VALIDATED":
            repository.cutover(context, MIGRATION_ID, "automated-validation")
        print(json.dumps({"copied": copied, "status": "CUTOVER", **validation}, ensure_ascii=False, default=str))
    finally:
        kernel.close()


if __name__ == "__main__":
    main()
