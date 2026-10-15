"""Bounded market/PIT/Feature derivation for the unified production pipeline."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from uuid import uuid4

from data_collect.data_access import DataAccessKernel
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.phase2_raw_repository import Phase2RawRepository
from data_collect.data_access.run_context import build_run_context
from data_collect.jobs.tushare_derived_rebuild import rebuild_after_market
from data_collect.jobs.tushare_job_lock import warehouse_write_job


MAX_PRODUCTION_LOOKBACK_DAYS = 31


def _parse_date(value: date | str | None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value).replace("-", ""), "%Y%m%d").date()


def run(
    run_date=None, lookback_days: int = 5, dry_run: bool = False,
    execution_id=None, **_kwargs,
) -> dict:
    if not 1 <= int(lookback_days) <= MAX_PRODUCTION_LOOKBACK_DAYS:
        raise ValueError(
            "production market-derived lookback_days must be in 1..31 "
            "to keep output rebuilds bounded"
        )
    lookback_days = int(lookback_days)
    end_date = _parse_date(run_date)
    if dry_run:
        return {
            "status": "dry_run",
            "end_date": end_date.isoformat(),
            "lookback_days": lookback_days,
        }
    return _run_locked(end_date, lookback_days, execution_id)


@warehouse_write_job
def _run_locked(end_date: date, lookback_days: int, execution_id=None) -> dict:
    start_date = end_date - timedelta(days=max(lookback_days * 3, 10))
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = build_run_context(
        "tushare_production_market_derived",
        pipeline_version="data-access-v1",
        run_id=f"production-market-derived-{uuid4().hex}",
    )
    try:
        repository = Phase2RawRepository(kernel)
        trade_days = repository.trade_dates(start_date, end_date, context, lookback_days)
        kernel.release_connection()
        if not trade_days:
            return {"status": "no_trading_days", "start_date": None, "end_date": None}
        first = datetime.strptime(trade_days[0], "%Y%m%d").date()
        last = datetime.strptime(trade_days[-1], "%Y%m%d").date()
        result = rebuild_after_market(first, last, execution_id=execution_id)
        return {
            "status": "rebuilt",
            "start_date": first.isoformat(),
            "end_date": last.isoformat(),
            **result,
        }
    except BaseException as exc:
        kernel.record_run_failure(context, exc)
        raise
    finally:
        kernel.close()


if __name__ == "__main__":
    print(run())
