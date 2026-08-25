"""Retry incomplete adj_factor windows through typed market repositories."""
from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.history_repository import HistoryRepository
from data_collect.data_access.repository.market_repository import MarketRepository
from data_collect.jobs.tushare_daily_incremental import _date, _rebuild_market_date, _trade_dates
from data_collect.providers.tushare_client import call_api, get_pro

logger = logging.getLogger("tushare_adj_factor_incremental_retry")


def run(end_date=None, lookback_days=5, batch_size=100, batch_pause=2.0):
    end = end_date or date.today().strftime("%Y%m%d")
    batch_id = f"adj-retry-{date.today().strftime('%Y%m%d')}-{int(time.time())}"
    pro = get_pro()
    calendar = _trade_dates(pro, end, int(lookback_days))
    if calendar is None or calendar.empty:
        return "没有找到需要同步的交易日"
    days = calendar["cal_date"].astype(str).tolist()
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    history = HistoryRepository(kernel)
    market = MarketRepository(kernel)
    context = RunContext(batch_id, "tushare-adj-factor-retry", "unknown", "unknown", "v1")
    try:
        codes = history.active_stock_codes(context)
        total = 0
        logger.info("adj retry started batch_id=%s dates=%s stocks=%s", batch_id, days, len(codes))
        for index, code in enumerate(codes, 1):
            key = f"{code}:{days[0]}:{days[-1]}"
            if history.status("adj_factor_retry", key, context) == "done":
                continue
            try:
                frame = call_api(pro.adj_factor, ts_code=code, start_date=days[0], end_date=days[-1])
                rows = [] if frame is None else frame.astype(object).where(pd.notna(frame), None).to_dict("records")
                versions, _raw = market.append_market_versions("adj_factor", rows, batch_id, context)
                total += versions
                history.save("adj_factor_retry", key, "done", versions, context)
                if index % int(batch_size) == 0:
                    logger.info("adj retry progress=%s/%s versions=%s", index, len(codes), total)
                    time.sleep(float(batch_pause))
            except Exception as exc:  # noqa: BLE001
                logger.exception("adj retry failed code=%s", code)
                history.save("adj_factor_retry", key, "failed", total, context, str(exc)[:500])
                raise

        for day in days:
            version_rows = market.latest_market_versions("adj_factor", _date(day), context)
            if not version_rows:
                raise RuntimeError(f"adj_factor {day} has no usable Raw version")
            payload = pd.json_normalize([
                row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
                for row in version_rows
            ])
            _rebuild_market_date(payload, "adj_factor", _date(day))
            actual = market.standard_count("adj_factor_daily", _date(day), context)
            logger.info("adj retry standard date=%s rows=%s", day, actual)
        return f"adj_factor retry complete dates={days}, stocks={len(codes)}, versions={total}"
    finally:
        kernel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retry Tushare adj_factor incremental gaps")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--lookback-days", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--batch-pause", type=float, default=2.0)
    args = parser.parse_args()
    print(run(args.end_date, args.lookback_days, args.batch_size, args.batch_pause))
