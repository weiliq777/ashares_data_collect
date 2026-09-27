"""Build one daily Market Activity Observation persistence snapshot.

This is a write job for the new observation tables only.  It does not modify
Raw, Standard, PIT, Feature, Research Snapshot or Backtest data.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
from typing import Sequence

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_activity_storage_repository import (
    MarketActivityStorageRepository,
)
from data_collect.market_activity import MarketActivityBuilder, MarketActivityRequest


JOB_NAME = "market_activity_daily"


def run(
    trade_date: date,
    data_baseline_id: str,
    *,
    top_n: int = 20,
    lookback_trading_days: int = 20,
    calendar_lookback_days: int = 45,
    run_id: str | None = None,
    freeze: bool = False,
) -> dict:
    request = MarketActivityRequest(
        trade_date=trade_date,
        top_n=top_n,
        lookback_trading_days=lookback_trading_days,
        calendar_lookback_days=calendar_lookback_days,
        data_baseline_id=data_baseline_id,
    )
    stable_run_id = run_id or (
        f"market-activity-build-{trade_date.isoformat()}-"
        f"{request.methodology_hash[:16]}-{str(data_baseline_id).strip()}"
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=stable_run_id,
        job_name=JOB_NAME,
        code_version="market-activity-v1",
        config_hash=request.methodology_hash,
        pipeline_version="data-access-v1",
    )
    try:
        result = MarketActivityBuilder(
            MarketActivityStorageRepository(kernel)
        ).build(request, context, freeze=freeze)
        return {
            "activity_snapshot_id": result.activity_snapshot_id,
            "status": result.status,
            "skipped": result.skipped,
            "validation": result.validation,
            "report": result.report.as_dict() if result.report else None,
        }
    finally:
        kernel.close()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Persist one daily market activity snapshot")
    parser.add_argument("--trade-date", required=True, help="交易日，格式 YYYY-MM-DD")
    parser.add_argument("--data-baseline-id", required=True)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--lookback-trading-days", type=int, default=20)
    parser.add_argument("--calendar-lookback-days", type=int, default=45)
    parser.add_argument("--run-id")
    parser.add_argument("--freeze", action="store_true")
    args = parser.parse_args(argv)
    print(json.dumps(
        run(
            date.fromisoformat(args.trade_date),
            args.data_baseline_id,
            top_n=args.top_n,
            lookback_trading_days=args.lookback_trading_days,
            calendar_lookback_days=args.calendar_lookback_days,
            run_id=args.run_id,
            freeze=args.freeze,
        ),
        ensure_ascii=False,
        indent=2,
        default=str,
    ))


if __name__ == "__main__":
    main()
