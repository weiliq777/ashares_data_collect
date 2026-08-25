"""Bounded quality gate for one unified production-pipeline window."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.phase2_raw_repository import Phase2RawRepository
from data_collect.data_access.repository.production_quality_repository import ProductionQualityRepository


def _parse_date(value: date | str | None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value).replace("-", ""), "%Y%m%d").date()


def run(run_date=None, lookback_days: int = 5, dry_run: bool = False, **_kwargs) -> dict:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be > 0")
    end_date = _parse_date(run_date)
    if dry_run:
        return {
            "status": "dry_run",
            "end_date": end_date.isoformat(),
            "lookback_days": lookback_days,
        }

    start_date = end_date - timedelta(days=max(lookback_days * 3, 10))
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"production-quality-{uuid4().hex}",
        job_name="tushare_production_quality",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    try:
        trade_days = Phase2RawRepository(kernel).trade_dates(
            start_date, end_date, context, lookback_days,
        )
        if not trade_days:
            return {"status": "no_trading_days", "failures": []}
        first = datetime.strptime(trade_days[0], "%Y%m%d").date()
        last = datetime.strptime(trade_days[-1], "%Y%m%d").date()
        report = ProductionQualityRepository(kernel).inspect_window(first, last, context)
    finally:
        kernel.close()

    if report["failures"]:
        raise RuntimeError("production quality gate failed: " + "; ".join(report["failures"]))
    return {"status": "passed", "start_date": first.isoformat(), "end_date": last.isoformat(), "report": report}


if __name__ == "__main__":
    print(run())
