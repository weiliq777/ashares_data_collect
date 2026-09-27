"""阶段二增量后的派生依赖重建编排。

该模块只读 Raw/Standard，按依赖顺序重建，不访问 Tushare，也不修改 Raw。
输出严格限制在显式日期范围；部分特征为计算滚动窗口会读取有界预热历史
（估值分位数约 5 年），但不会把整个目标表全量重写。
"""
from __future__ import annotations

from datetime import date, datetime


MAX_MARKET_DERIVED_SPAN_DAYS = 45


def _parse_date(value: date | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    return datetime.strptime(text.replace("-", ""), "%Y%m%d").date()


def rebuild_after_market(
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    *,
    execution_id: str | None = None,
) -> dict:
    if start_date is None or end_date is None:
        raise ValueError("行情派生重建必须显式提供 start_date/end_date")
    start, end = _parse_date(start_date), _parse_date(end_date)
    if start > end:
        raise ValueError("行情派生重建 start_date 不能晚于 end_date")
    if (end - start).days > MAX_MARKET_DERIVED_SPAN_DAYS:
        raise ValueError(
            "生产行情派生单次输出窗口不能超过 45 个自然日；"
            "历史回建请使用专用分批任务"
        )
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
    features.update(feature_run(
        "price_v2", start_date, end_date, execution_id=execution_id,
    ))
    features.update(feature_run("valuation", start_date, end_date))
    from data_collect.jobs.technical_daily import build as build_technical_daily

    features["technical_daily_v1"] = build_technical_daily(
        start_date,
        end_date,
        run_id=f"technical-daily-market-derived-{start_date}-{end_date}",
    )
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
