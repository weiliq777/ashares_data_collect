"""阶段二增量后的派生依赖重建编排。

该模块只读 Raw/Standard，按依赖顺序重建，不访问 Tushare，也不修改 Raw。
当前优先保证一致性，采用可审计的全表重建；后续可在不改变依赖关系的前提下优化为分区重建。
"""
from __future__ import annotations

from datetime import date, datetime


def rebuild_after_market(
    start_date: date | str | None = None,
    end_date: date | str | None = None,
) -> dict:
    if start_date is None or end_date is None:
        raise ValueError("行情派生重建必须显式提供 start_date/end_date")
    from data_collect.jobs.tushare_phase2_standard import run as standard_run
    from data_collect.jobs.tushare_phase2_features import run as feature_run

    standard = standard_run("status", start_date, end_date)
    standard.update(standard_run("universe", start_date, end_date))
    standard.update(standard_run("tradeability", start_date, end_date))
    # price_daily_v1 is a read-only legacy compatibility table.  It remains
    # available for explicitly requested historical rebuilds, but must not be
    # part of the production incremental path.  New production consumers use
    # the point-in-time-safe price_daily_v2 contract.
    features = {}
    features.update(feature_run("price_v2", start_date, end_date))
    features.update(feature_run("valuation", start_date, end_date))
    return {"standard": standard, "features": features}


def rebuild_after_phase2(
    datasets: set[str],
    start_date: date | str | None = None,
    end_date: date | str | None = None,
) -> dict:
    from data_collect.jobs.tushare_phase2_incremental import _rebuild_standard

    # _rebuild_standard 负责 Standard/PIT/Feature 的依赖顺序；保留一个统一入口
    # 方便日后调度器和质量门禁调用。
    return _rebuild_standard(datasets, start_date, end_date)


def rebuild_after_financial(
    stock_codes: list[str],
    output_end: date | str,
) -> dict:
    """Rebuild financial derivatives only for explicitly changed securities."""
    if not stock_codes:
        raise ValueError("财务派生重建必须显式提供 changed stock_codes")
    if isinstance(output_end, date):
        parsed_end = output_end
    else:
        raw_end = str(output_end)
        parsed_end = (
            date.fromisoformat(raw_end)
            if "-" in raw_end
            else datetime.strptime(raw_end, "%Y%m%d").date()
        )
    from data_collect.jobs.tushare_financial_incremental import _rebuild_financial_derived

    _rebuild_financial_derived(sorted(set(stock_codes)), parsed_end)
    return {
        "status": "rebuilt",
        "stocks": len(set(stock_codes)),
        "output_end": parsed_end.isoformat(),
    }
