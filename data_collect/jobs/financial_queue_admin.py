"""Audited administrative transitions for quarantined financial queue rows.

This command is intentionally separate from the production consumer.  A
quarantine row is never requeued or resolved automatically: an operator must
provide bounded queue IDs, an identity, and a reason that is persisted with
the transition.
"""
from __future__ import annotations

import argparse
from collections.abc import Sequence
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_vip_repository import FinancialVipRepository
from data_collect.jobs.tushare_job_lock import warehouse_write_job


def _service():
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"financial-queue-admin-{uuid4().hex}",
        job_name="financial_queue_admin",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    return kernel, FinancialVipRepository(kernel), context


@warehouse_write_job
def run(
    action: str, queue_ids: Sequence[int], approved_by: str, reason: str,
    reset_attempts: bool = True,
) -> dict[str, object]:
    """Apply one explicit, audited quarantine transition."""
    if action not in {"requeue", "resolve"}:
        raise ValueError("action must be requeue or resolve")
    ids = tuple(dict.fromkeys(int(value) for value in queue_ids))
    if not ids:
        raise ValueError("at least one queue_id is required")
    if len(ids) > 1000:
        raise ValueError("at most 1000 queue IDs may be transitioned per command")
    kernel, repository, context = _service()
    try:
        if action == "requeue":
            changed = repository.requeue_quarantined(
                list(ids), approved_by, reason, context,
                reset_attempts=reset_attempts,
            )
            target_status = "PENDING"
        else:
            changed = repository.resolve_quarantined(
                list(ids), approved_by, reason, context,
            )
            target_status = "RESOLVED"
        return {
            "action": action,
            "queue_ids": list(ids),
            "changed": changed,
            "status": target_status,
            "approved_by": approved_by,
        }
    finally:
        kernel.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="审计并恢复财务变更队列隔离记录")
    parser.add_argument("--action", choices=("requeue", "resolve"), required=True)
    parser.add_argument("--queue-id", dest="queue_ids", action="append", type=int, required=True)
    parser.add_argument("--approved-by", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--keep-attempts", action="store_true")
    args = parser.parse_args()
    print(run(
        action=args.action,
        queue_ids=args.queue_ids,
        approved_by=args.approved_by,
        reason=args.reason,
        reset_attempts=not args.keep_attempts,
    ), flush=True)


if __name__ == "__main__":
    main()
