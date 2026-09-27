"""Consume pending financial-change checkpoints in the unified pipeline."""
from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4

from data_collect.jobs.tushare_financial_vip_incremental import (
    _consume_pending_changes,
    _service,
)
from data_collect.jobs.tushare_job_lock import warehouse_write_job


def _parse_date(value) -> date:
    if value is None:
        return date.today()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value).replace("-", ""), "%Y%m%d").date()


@warehouse_write_job
def run(
    run_date=None, dry_run: bool = False, execution_id=None, attempt=1, **_kwargs,
) -> dict:
    output_end = _parse_date(run_date)
    if dry_run:
        return {"status": "dry_run", "output_end": output_end.isoformat()}
    execution_id = execution_id or f"production-derived-{uuid4().hex}"
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 1:
        raise ValueError("attempt must be a positive integer")
    kernel, repository, context = _service(execution_id)
    try:
        claim_owner_id = f"{execution_id}:attempt:{attempt}:{uuid4().hex}"
        summary = _consume_pending_changes(
            repository, context, output_end,
            claim_owner_id=claim_owner_id, takeover=attempt > 1,
        )
        if summary["processed_batches"] == 0:
            return {"status": "no_pending_financial_changes", "stocks": 0}
        return {
            "status": "rebuilt",
            "stocks": summary["processed_stocks"],
            "batches": summary["processed_batches"],
            "queue_rows": summary["processed_queue_rows"],
            "result": summary["last_result"],
        }
    finally:
        kernel.close()


if __name__ == "__main__":
    print(run())
