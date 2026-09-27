"""Market activity monitoring（市场成交活跃度监控）深模块。

This module deliberately describes observed trading activity, not actual
capital direction.  It reads the canonical Standard daily bars through a
Repository and never writes to the data foundation.
"""

from data_collect.market_activity.contracts import (
    MARKET_ACTIVITY_CONTRACT_HASH,
    MARKET_ACTIVITY_CONTRACT_VERSION,
    MARKET_ACTIVITY_METHODOLOGY_ID,
    MarketActivityReport,
    MarketActivityRequest,
    StockActivity,
)
from data_collect.market_activity.monitor import MarketActivityMonitor


def __getattr__(name: str):
    """Load persistence orchestration lazily to keep Repository imports acyclic."""
    if name in {"MarketActivityBuildResult", "MarketActivityBuilder"}:
        from data_collect.market_activity.persistence import (
            MarketActivityBuildResult,
            MarketActivityBuilder,
        )
        return {
            "MarketActivityBuildResult": MarketActivityBuildResult,
            "MarketActivityBuilder": MarketActivityBuilder,
        }[name]
    raise AttributeError(name)
from data_collect.data_access.repository.market_activity_repository import (
    MarketActivityRepository,
)

__all__ = [
    "MARKET_ACTIVITY_CONTRACT_VERSION",
    "MARKET_ACTIVITY_CONTRACT_HASH",
    "MARKET_ACTIVITY_METHODOLOGY_ID",
    "MarketActivityBuildResult",
    "MarketActivityBuilder",
    "MarketActivityMonitor",
    "MarketActivityReport",
    "MarketActivityRepository",
    "MarketActivityRequest",
    "StockActivity",
]
