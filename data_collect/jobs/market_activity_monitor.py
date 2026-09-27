"""CLI for the read-only Market Activity Monitor（市场成交监控）."""
from __future__ import annotations

import argparse
from datetime import date
import json
from typing import Sequence

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_activity_repository import (
    MarketActivityRepository,
)
from data_collect.market_activity import (
    MarketActivityMonitor,
    MarketActivityRequest,
)


JOB_NAME = "market_activity_monitor"


def run(
    trade_date: date,
    *,
    top_n: int = 20,
    lookback_trading_days: int = 20,
    calendar_lookback_days: int = 45,
) -> dict:
    """Build one report without writing business data rows.

    The Kernel may record the normal ``ops.run_manifest`` control-plane
    record; Standard/Raw/Feature and other business tables remain read-only.
    """
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"market-activity-{trade_date.isoformat()}",
        job_name=JOB_NAME,
        code_version="market-activity-v1",
        config_hash="market-activity-v1",
        pipeline_version="data-access-v1",
    )
    try:
        report = MarketActivityMonitor(MarketActivityRepository(kernel)).daily(
            MarketActivityRequest(
                trade_date=trade_date,
                top_n=top_n,
                lookback_trading_days=lookback_trading_days,
                calendar_lookback_days=calendar_lookback_days,
            ),
            context,
        )
        return report.as_dict()
    finally:
        kernel.close()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Read-only market activity report")
    parser.add_argument("--trade-date", required=True, help="交易日，格式 YYYY-MM-DD")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--lookback-trading-days", type=int, default=20)
    parser.add_argument("--calendar-lookback-days", type=int, default=45)
    args = parser.parse_args(argv)
    payload = run(
        date.fromisoformat(args.trade_date),
        top_n=args.top_n,
        lookback_trading_days=args.lookback_trading_days,
        calendar_lookback_days=args.calendar_lookback_days,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
