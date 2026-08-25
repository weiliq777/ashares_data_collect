"""修复历史日线存在但 adj_factor Raw 不完整的股票。

该任务只处理数据库质量检查发现的缺口，不清理或覆盖任何 Raw；每只股票按
``adj_factor_repair`` checkpoint 续传。修复完成后应独立重建 Standard/Feature。
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime

import data_collect.jobs.tushare_history_init as history_shared
from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_repository import MarketRepository


logger = logging.getLogger("tushare_adj_factor_gap_repair")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())


def _missing_codes(start_date: str, end_date: str) -> list[tuple[str, int]]:
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id="adj-factor-gap-repair", job_name="tushare_adj_factor_gap_repair",
        code_version="unknown", config_hash="unknown", pipeline_version="v1",
    )
    try:
        return MarketRepository(kernel).missing_adj_factor_codes(
            history_shared._date(start_date), history_shared._date(end_date), context
        )
    finally:
        kernel.close()


def run(start_date: str = "20210802", end_date: str = "20260731", pause: float = 1.0, limit: int | None = None) -> str:
    missing = _missing_codes(start_date, end_date)
    if limit:
        missing = missing[: int(limit)]
    if not missing:
        return "adj_factor 没有发现历史缺口"

    pro = get_pro()
    total_rows = 0
    failed = 0
    logger.info("adj_factor gap repair started start=%s end=%s stocks=%s", start_date, end_date, len(missing))
    for index, (code, expected_missing) in enumerate(missing, 1):
        key = f"{code}:{start_date}:{end_date}"
        if history_shared._done("adj_factor_repair", key):
            continue
        try:
            frame = call_api(pro.adj_factor, ts_code=code, start_date=start_date, end_date=end_date)
            rows = 0 if frame is None else len(frame)
            if rows == 0:
                raise RuntimeError(f"接口返回空数据，预计缺口 {expected_missing} 行")
            if rows < expected_missing:
                raise RuntimeError(f"接口返回 {rows} 行，仍少于预计缺口 {expected_missing} 行")
            history_shared._save_market_raw("tushare_adj_factor_raw", frame, ["ts_code", "trade_date"])
            total_rows += rows
            history_shared._checkpoint("adj_factor_repair", key, "done", rows)
            if index % 25 == 0 or index == len(missing):
                logger.info("adj_factor repair progress=%s/%s api_rows=%s", index, len(missing), total_rows)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            logger.exception("adj_factor repair failed code=%s", code)
            history_shared._checkpoint("adj_factor_repair", key, "failed", 0, str(exc)[:500])
        time.sleep(float(pause))
    if failed:
        raise RuntimeError(f"adj_factor 缺口修复完成但有 {failed} 只股票失败，请按 checkpoint 重试")
    return f"adj_factor 缺口修复完成 stocks={len(missing)} api_rows={total_rows}"


def main() -> None:
    parser = argparse.ArgumentParser(description="修复历史 adj_factor 缺口")
    parser.add_argument("--start-date", default="20210802")
    parser.add_argument("--end-date", default="20260731")
    parser.add_argument("--pause", type=float, default=1.0)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    print(run(**vars(args)))


if __name__ == "__main__":
    main()
