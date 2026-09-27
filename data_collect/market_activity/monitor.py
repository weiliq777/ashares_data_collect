"""Deep interface for read-only market activity monitoring."""
from __future__ import annotations

from datetime import timedelta

from data_collect.data_access.context import RunContext
from data_collect.data_access.repository.market_activity_repository import (
    MarketActivityRepository,
)
from data_collect.market_activity.contracts import MarketActivityReport, MarketActivityRequest
from data_collect.market_activity.metrics import build_daily_report


class MarketActivityMonitor:
    """One small interface hiding bounded SQL and activity calculations."""

    def __init__(self, repository: MarketActivityRepository) -> None:
        self._repository = repository

    def daily(
        self,
        request: MarketActivityRequest,
        context: RunContext,
    ) -> MarketActivityReport:
        """Return one date's observed volume/amount report.

        This method is read-only.  It does not alter Standard, Feature,
        Snapshot or Strategy data and it does not claim to measure true
        capital direction（真实资金净流向）.
        """
        request.validate()
        start = request.trade_date - timedelta(days=request.calendar_lookback_days)
        batches = self._repository.stream_daily_bars(
            start,
            request.trade_date,
            context,
            limit_rows=request.max_bars + 1,
        )
        return build_daily_report(batches, request)
