"""阶段二 Feature v1 计算任务。

只读取 PostgreSQL Raw/Standard/PIT，不访问 Tushare；目标表可重复重建。
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
from datetime import date, timedelta

from data_collect.data_access import DataAccessKernel, WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.feature_repository import FeatureRepository
from data_collect.data_access.run_context import build_run_context


_JOB_NAME = "tushare_phase2_features"


def _feature_repo():
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    return kernel, FeatureRepository(kernel)




logger = logging.getLogger("tushare_phase2_features")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())


FEATURE_CODE_BATCH_SIZE = max(
    1, min(10, int(os.getenv("ASHARES_FEATURE_CODE_BATCH_SIZE", "5")))
)


def _batches(values, size=FEATURE_CODE_BATCH_SIZE):
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def _bounded_dates(start_date=None, end_date=None) -> tuple[date, date] | None:
    if start_date is None and end_date is None:
        return None
    if start_date is None or end_date is None:
        raise ValueError("start_date and end_date must be provided together")

    def parse(value) -> date:
        if isinstance(value, date):
            return value
        from datetime import datetime

        return datetime.strptime(str(value).replace("-", ""), "%Y%m%d").date()

    bounds = parse(start_date), parse(end_date)
    if bounds[0] > bounds[1]:
        raise ValueError("start_date cannot be later than end_date")
    return bounds


def _all_price_date_bounds() -> tuple[date, date] | None:
    kernel, repo = _feature_repo()
    try:
        return repo.load_price_date_range(build_run_context(_JOB_NAME))
    finally:
        kernel.close()


def rebuild_price(start_date=None, end_date=None) -> int:
    bounds = _bounded_dates(start_date, end_date) or _all_price_date_bounds()
    if bounds is None:
        return 0
    kernel, repo = _feature_repo()
    try:
        context = build_run_context(_JOB_NAME)
        scope = WorkScope(date_from=bounds[0], date_to=bounds[1])
        codes = repo.load_codes_in_window(scope, context)
        return repo.rebuild_price(scope, context, codes)
    finally:
        kernel.close()


def rebuild_price_v2(start_date=None, end_date=None) -> int:
    bounds = _bounded_dates(start_date, end_date) or _all_price_date_bounds()
    if bounds is None:
        return 0
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        repo = FeatureRepository(kernel)
        base_context = build_run_context(_JOB_NAME)
        # The interval and code identity make retries reuse the same
        # per-batch checkpoint.  Include the code commit so a later code
        # version cannot silently skip work completed by an older version.
        run_identity = "|".join((
            bounds[0].isoformat(), bounds[1].isoformat(),
            base_context.code_version, base_context.config_hash,
        ))
        stable_run_id = "price-v2-" + hashlib.sha256(run_identity.encode("utf-8")).hexdigest()[:48]
        from data_collect.data_access.context import RunContext

        context = RunContext(
            run_id=stable_run_id,
            job_name=_JOB_NAME,
            code_version=base_context.code_version,
            config_hash=base_context.config_hash,
            pipeline_version=base_context.pipeline_version,
        )
        scope = WorkScope(date_from=bounds[0], date_to=bounds[1])
        codes = repo.load_codes_in_window(scope, context)
        return repo.rebuild_price_v2(
            scope, context, codes, batch_size=FEATURE_CODE_BATCH_SIZE,
        )
    finally:
        kernel.close()


def rebuild_valuation(start_date=None, end_date=None) -> int:
    from data_collect.jobs.tushare_foundation_features import rebuild_valuation_features

    kernel, repo = _feature_repo()
    try:
        context = build_run_context(_JOB_NAME)
        bounds = repo.load_valuation_date_range(context)
        if not bounds:
            return 0
        first_day, last_day = bounds
        bounds = _bounded_dates(start_date, end_date)
        output_start, output_end = bounds or (first_day, last_day)
        if output_start > output_end:
            raise ValueError("valuation start_date 不能晚于 end_date")
        warmup_start = max(first_day, output_start - timedelta(days=5 * 366 + 31))
        result = rebuild_valuation_features(
            None, warmup_start, output_start, output_end,
            target_table="feature.valuation_daily_v1",
        )
        return int(result["written"])
    finally:
        kernel.close()


def rebuild_company_financial() -> int:
    kernel, repo = _feature_repo()
    try:
        return repo.rebuild_company_financial(build_run_context(_JOB_NAME))
    finally:
        kernel.close()


def rebuild_shareholder_return(start_date=None, end_date=None) -> int:
    bounds = _bounded_dates(start_date, end_date) or _all_price_date_bounds()
    if bounds is None:
        return 0
    kernel, repo = _feature_repo()
    try:
        context = build_run_context(_JOB_NAME)
        scope = WorkScope(date_from=bounds[0], date_to=bounds[1])
        codes = repo.load_codes_in_window(scope, context)
        return repo.rebuild_shareholder_return(scope, context, codes)
    finally:
        kernel.close()


def register_features() -> int:
    kernel, repo = _feature_repo()
    try:
        return repo.register_features(build_run_context(_JOB_NAME))
    finally:
        kernel.close()


def run(
    task: str = "all",
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    allow_full_rebuild: bool = False,
) -> dict[str, int]:
    tasks = {
        "price": rebuild_price,
        "price_v2": rebuild_price_v2,
        "valuation": rebuild_valuation,
        "company_financial": rebuild_company_financial,
        "shareholder_return": rebuild_shareholder_return,
        "registry": register_features,
    }
    selected = list(tasks) if task == "all" else [task]
    if any(name not in tasks for name in selected):
        raise ValueError(f"不支持的 task: {task}")
    high_volume = {"price", "price_v2", "valuation", "shareholder_return"}
    if (
        not allow_full_rebuild
        and start_date is None
        and end_date is None
        and any(name in high_volume for name in selected)
    ):
        raise RuntimeError(
            "拒绝隐式全市场 Feature 重建；请提供 start_date/end_date，"
            "或显式设置 allow_full_rebuild=True"
        )
    if not allow_full_rebuild and "company_financial" in selected:
        raise RuntimeError(
            "company_financial_v1 已是兼容层；请使用 company_financial_v2，"
            "或显式设置 allow_full_rebuild=True"
        )
    result = {}
    for name in selected:
        logger.info("phase2 feature task started=%s", name)
        if name in {"price", "price_v2", "valuation", "shareholder_return"}:
            result[name] = tasks[name](start_date, end_date)
        else:
            result[name] = tasks[name]()
        logger.info("phase2 feature task finished=%s affected=%s", name, result[name])
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="阶段二 Feature v1 构建")
    parser.add_argument("--task", choices=["all", "price", "price_v2", "valuation", "company_financial", "shareholder_return", "registry"], default="all")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--allow-full-rebuild", action="store_true")
    args = parser.parse_args()
    print(run(
        args.task, args.start_date, args.end_date, args.allow_full_rebuild
    ))
