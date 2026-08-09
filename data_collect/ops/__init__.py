"""阶段三运行质量门禁。"""

from .market_coverage import (
    CoverageGateResult,
    assert_market_coverage_publishable,
    assert_market_coverage_range_publishable,
    evaluate_market_coverage,
    latest_market_coverage_failures,
    latest_market_coverage_failures_in_range,
    persist_market_coverage_blocked,
    persist_market_coverage_gate,
)

__all__ = [
    "CoverageGateResult",
    "evaluate_market_coverage",
    "persist_market_coverage_gate",
    "persist_market_coverage_blocked",
    "latest_market_coverage_failures",
    "latest_market_coverage_failures_in_range",
    "assert_market_coverage_publishable",
    "assert_market_coverage_range_publishable",
]
