"""Run the bounded, reversible trading-calendar migration pilot."""
from __future__ import annotations

import json
from uuid import UUID

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.schema_migration_repository import SchemaMigrationRepository


MIGRATION_ID = "2f0f4f8a-3de7-4c28-9e96-1d2fd0fbb8d1"


def main() -> None:
    UUID(MIGRATION_ID)
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"schema-migration-trading-calendar-{MIGRATION_ID}",
        job_name="migrate_trading_calendar", code_version="unknown",
        config_hash="unknown", pipeline_version="data-foundation-stage3-pilot",
    )
    repository = SchemaMigrationRepository(kernel)
    try:
        current = repository.get(context, MIGRATION_ID)
        copied = 0
        if not current:
            repository.register(
                context, migration_id=MIGRATION_ID,
                source_table="public.trading_calendar",
                target_table="std.trading_calendar",
                notes="automated validated cutover; public source retained for rollback",
            )
            copied = repository.prepare_trading_calendar(context, MIGRATION_ID)
            validation = repository.validate_trading_calendar(context, MIGRATION_ID)
        elif current["status"] == "REGISTERED":
            copied = repository.prepare_trading_calendar(context, MIGRATION_ID)
            validation = repository.validate_trading_calendar(context, MIGRATION_ID)
        elif current["status"] == "VALIDATED":
            validation = repository.validate_trading_calendar(
                context, MIGRATION_ID, persist=True,
            )
        elif current["status"] == "CUTOVER":
            validation = repository.validate_trading_calendar(
                context, MIGRATION_ID, persist=False,
            )
        else:
            raise RuntimeError(f"migration cannot resume from status={current['status']}")
        if validation["source_content_hash"] != validation["target_content_hash"]:
            raise RuntimeError(f"trading calendar pilot hash mismatch: {validation}")
        if (repository.get(context, MIGRATION_ID) or {}).get("status") == "VALIDATED":
            repository.cutover(context, MIGRATION_ID, "automated-validation")
        print(json.dumps({"copied": copied, "status": "CUTOVER", **validation}, ensure_ascii=False, default=str))
    finally:
        kernel.close()


if __name__ == "__main__":
    main()
