"""阶段二 Standard、历史状态和 PIT 构建任务。

本任务只读取既有 Raw/Standard 并写入可重建的 std/pit 派生表，不访问 Tushare。
所有目标表均可安全重跑；不会修改或删除任何 ``*_raw`` 表。
"""
from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date, datetime, timedelta

from data_collect.data_access import DataAccessKernel, WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_repository import MarketRepository
from data_collect.data_access.repository.pit_repository import PitRepository
from data_collect.data_access.repository.security_repository import SecurityRepository
from data_collect.data_access.run_context import build_run_context


logger = logging.getLogger("tushare_phase2_standard")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())


def _bounded_dates(start_date=None, end_date=None) -> tuple[date, date] | None:
    if start_date is None and end_date is None:
        return None
    if start_date is None or end_date is None:
        raise ValueError("start_date 和 end_date 必须同时提供")

    def parse(value) -> date:
        if isinstance(value, date):
            return value
        return datetime.strptime(str(value).replace("-", ""), "%Y%m%d").date()

    bounds = parse(start_date), parse(end_date)
    if bounds[0] > bounds[1]:
        raise ValueError("start_date 不能晚于 end_date")
    return bounds


def _date_bounds() -> tuple[date, date] | None:
    """读交易日范围（经 Repository → Kernel，避免对派生大表做 MIN/MAX 全扫描）。"""
    kernel, sec_repo, _, _ = _repositories()
    try:
        return sec_repo.load_trade_date_range(build_run_context(_JOB_NAME))
    finally:
        kernel.close()


def _month_windows(bounds: tuple[date, date]):
    current = bounds[0]
    while current <= bounds[1]:
        next_month = (
            current.replace(year=current.year + 1, month=1, day=1)
            if current.month == 12
            else current.replace(month=current.month + 1, day=1)
        )
        yield current, min(bounds[1], next_month - timedelta(days=1))
        current = next_month


_JOB_NAME = "tushare_phase2_standard"


def _repositories():
    """构造 Kernel + 领域仓储（连接由 Kernel 私有持有，不暴露给业务层）。"""
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    return kernel, SecurityRepository(kernel), PitRepository(kernel), MarketRepository(kernel)


def _scoped_rebuild(bounds, which: str) -> int:
    """有界日期的 scoped 重建：走 Repository → Kernel，替代旧的 DELETE+INSERT 手写 SQL。"""
    kernel, sec_repo, pit_repo, _ = _repositories()
    try:
        scope = WorkScope(date_from=bounds[0], date_to=bounds[1])
        context = build_run_context(_JOB_NAME)
        if which == "status":
            return sec_repo.rebuild_status_daily(scope, context)
        if which == "universe":
            return pit_repo.rebuild_universe(scope, context)
        if which == "tradeability":
            return pit_repo.rebuild_tradeability(scope, context)
        raise ValueError(f"unknown scoped rebuild: {which}")
    finally:
        kernel.close()


def rebuild_lifecycle() -> int:
    kernel, sec_repo, _, _ = _repositories()
    try:
        return sec_repo.rebuild_lifecycle(build_run_context(_JOB_NAME))
    finally:
        kernel.close()


def rebuild_status_daily(start_date=None, end_date=None) -> int:
    bounds = _bounded_dates(start_date, end_date)
    if bounds is None:
        source_bounds = _date_bounds()
        return 0 if source_bounds is None else sum(
            rebuild_status_daily(first_day, last_day)
            for first_day, last_day in _month_windows(source_bounds)
        )
    return _scoped_rebuild(bounds, "status")


def rebuild_universe(start_date=None, end_date=None) -> int:
    bounds = _bounded_dates(start_date, end_date)
    if bounds is None:
        source_bounds = _date_bounds()
        return 0 if source_bounds is None else sum(
            rebuild_universe(first_day, last_day)
            for first_day, last_day in _month_windows(source_bounds)
        )
    return _scoped_rebuild(bounds, "universe")


def rebuild_tradeability(start_date=None, end_date=None) -> int:
    bounds = _bounded_dates(start_date, end_date)
    if bounds is None:
        source_bounds = _date_bounds()
        return 0 if source_bounds is None else sum(
            rebuild_tradeability(first_day, last_day)
            for first_day, last_day in _month_windows(source_bounds)
        )
    return _scoped_rebuild(bounds, "tradeability")


def rebuild_dividend(codes=None) -> int:
    kernel, sec_repo, _, _ = _repositories()
    try:
        context = build_run_context(_JOB_NAME)
        if codes is not None:
            return sec_repo.rebuild_dividend_codes(context, codes)
        return sec_repo.rebuild_dividend(context)
    finally:
        kernel.close()


def rebuild_index_daily() -> int:
    kernel, _, _, market_repo = _repositories()
    try:
        return market_repo.rebuild_index_daily(build_run_context(_JOB_NAME))
    finally:
        kernel.close()


def rebuild_financial_asof(batch_days: int = 30) -> int:
    kernel, _, pit_repo, _ = _repositories()
    try:
        return pit_repo.rebuild_financial_asof(batch_days, build_run_context(_JOB_NAME))
    finally:
        kernel.close()


def run(
    task: str = "all",
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    allow_full_rebuild: bool = False,
    dividend_codes=None,
) -> dict[str, int]:
    tasks = {
        "lifecycle": rebuild_lifecycle,
        "status": rebuild_status_daily,
        "universe": rebuild_universe,
        "tradeability": rebuild_tradeability,
        "dividend": rebuild_dividend,
        "index_daily": rebuild_index_daily,
        "financial_asof": rebuild_financial_asof,
    }
    if task == "all":
        selected = list(tasks)
    elif task in tasks:
        selected = [task]
    else:
        raise ValueError(f"不支持的 task: {task}")
    if (
        not allow_full_rebuild
        and start_date is None
        and end_date is None
        and any(name in {"status", "universe", "tradeability"} for name in selected)
    ):
        raise RuntimeError(
            "拒绝隐式全市场 Standard/PIT 重建；请提供 start_date/end_date，"
            "或显式设置 allow_full_rebuild=True"
        )
    result = {}
    for name in selected:
        logger.info("phase2 standard task started=%s", name)
        if name == "dividend" and dividend_codes is not None:
            result[name] = tasks[name](dividend_codes)
        elif name in {"status", "universe", "tradeability"}:
            result[name] = tasks[name](start_date, end_date)
        else:
            result[name] = tasks[name]()
        logger.info("phase2 standard task finished=%s affected=%s", name, result[name])
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="阶段二 Standard/PIT 构建")
    parser.add_argument("--task", choices=["all", "lifecycle", "status", "universe", "tradeability", "dividend", "index_daily", "financial_asof"], default="all")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--allow-full-rebuild", action="store_true")
    args = parser.parse_args()
    print(run(
        args.task, args.start_date, args.end_date, args.allow_full_rebuild
    ))
