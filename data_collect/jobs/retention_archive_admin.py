"""Audited control-plane operations for retention archive candidates.

The command only changes the archive registry metadata.  It never drops,
deletes, truncates, or renames a source relation.
"""
from __future__ import annotations

import argparse
import json
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.retention_archive_repository import (
    RetentionArchiveRepository,
)
from data_collect.jobs.tushare_job_lock import warehouse_write_job


def _service() -> tuple[DataAccessKernel, RetentionArchiveRepository, RunContext]:
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"retention-archive-admin-{uuid4().hex}",
        job_name="retention_archive_admin",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    return kernel, RetentionArchiveRepository(kernel), context


def list_candidates(object_type: str | None = None, limit: int = 1000) -> list[dict]:
    kernel, repository, context = _service()
    try:
        return repository.list_candidates(context, object_type=object_type, limit=limit)
    finally:
        kernel.close()


def archive_plan(limit: int = 1000) -> dict[str, object]:
    """Build a read-only preflight; no physical archive operation exists here."""
    kernel, repository, context = _service()
    try:
        items = repository.list_archive_plan(context, limit=limit)
        plan_items = []
        for item in items:
            references = int(item.get("current_reference_count") or 0)
            item["current_reference_count"] = references
            item["preflight_status"] = _preflight_status(
                item.get("object_type"), references,
            )
            item["physical_archive_executed"] = False
            plan_items.append(item)
        return {
            "action": "plan",
            "read_only": True,
            "physical_archive_executed": False,
            "items": plan_items,
        }
    finally:
        kernel.close()


def _preflight_status(object_type: str | None, references: int) -> str:
    """Never treat an unsupported object type as physically archivable."""
    if object_type not in {"DATASET", "SNAPSHOT"}:
        return "BLOCKED_UNSUPPORTED_OBJECT_TYPE"
    return "READY_FOR_SEPARATE_REVIEW" if references == 0 else "BLOCKED_DEPENDENCIES"


@warehouse_write_job
def transition(
    action: str,
    registry_ids: list[str],
    actor: str,
    reason: str,
) -> dict[str, object]:
    kernel, repository, context = _service()
    try:
        changed = repository.transition(
            context,
            action=action,
            registry_ids=registry_ids,
            actor=actor,
            reason=reason,
        )
        return {
            "action": action,
            "registry_ids": registry_ids,
            "changed": changed,
            "actor": actor,
        }
    finally:
        kernel.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="审计并管理归档候选登记")
    parser.add_argument("--action", choices=("list", "plan", "approve", "reject", "restore"), default="list")
    parser.add_argument("--registry-id", dest="registry_ids", action="append")
    parser.add_argument("--object-type", choices=("SNAPSHOT", "DATASET", "ROLLBACK_BACKUP", "COMPATIBILITY_TABLE"))
    parser.add_argument("--archive-state", choices=("CANDIDATE", "APPROVED", "ARCHIVED", "RESTORED", "REJECTED"), default="CANDIDATE")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--actor")
    parser.add_argument("--reason")
    args = parser.parse_args()
    if args.action == "list":
        kernel, repository, context = _service()
        try:
            rows = repository.list_candidates(
                context,
                object_type=args.object_type,
                limit=args.limit,
                archive_state=args.archive_state,
            )
        finally:
            kernel.close()
        print(json.dumps(rows, ensure_ascii=False, default=str), flush=True)
        return
    if args.action == "plan":
        print(json.dumps(archive_plan(args.limit), ensure_ascii=False, default=str), flush=True)
        return
    if not args.registry_ids or not args.actor or not args.reason:
        parser.error("transition actions require --registry-id, --actor, and --reason")
    print(transition(args.action, args.registry_ids, args.actor, args.reason), flush=True)


if __name__ == "__main__":
    main()
