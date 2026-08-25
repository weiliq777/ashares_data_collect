"""Consume pending financial-change checkpoints in the unified pipeline."""
from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4

from data_collect.jobs.tushare_financial_vip_incremental import (
    _legacy_pending_codes,
    _pending_queue_codes,
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
def run(run_date=None, dry_run: bool = False, **_kwargs) -> dict:
    from data_collect.jobs.tushare_financial_raw_to_standard import _run_unlocked as standard_run
    from data_collect.jobs.tushare_fina_indicator_version import _rebuild_standard_unlocked as indicator_run
    from data_collect.jobs.tushare_foundation_features import rebuild_company_financial_v2
    from data_collect.jobs.tushare_foundation_pipeline import OUTPUT_START, WARMUP_START

    output_end = _parse_date(run_date)
    if dry_run:
        return {"status": "dry_run", "output_end": output_end.isoformat()}
    kernel, repository, context = _service(f"production-derived-{uuid4().hex}")
    try:
        queue_ids, queue_codes = _pending_queue_codes(repository, context)
        legacy_codes = _legacy_pending_codes(repository, context)
        codes = sorted(set(queue_codes) | set(legacy_codes))
        if not codes:
            return {"status": "no_pending_financial_changes", "stocks": 0}
        try:
            standard_run("all", 2000, codes, WARMUP_START, output_end)
            indicator_run(2000, codes, WARMUP_START, output_end)
            result = rebuild_company_financial_v2(
                None, WARMUP_START, OUTPUT_START, output_end,
                requested_codes=codes, target_table="feature.company_financial_v2",
                batch_stocks=5,
            )
            repository.finish_queue(queue_ids, "DONE", None, context)
            repository.finish_legacy(legacy_codes, "done", None, context)
            return {"status": "rebuilt", "stocks": len(codes), "result": result}
        except Exception as exc:
            repository.finish_queue(queue_ids, "FAILED", str(exc)[:500], context)
            repository.finish_legacy(legacy_codes, "failed", str(exc)[:500], context)
            raise
    finally:
        kernel.close()


if __name__ == "__main__":
    print(run())
