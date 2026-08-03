"""单独重试每日增量中失败的 adj_factor 阶段。

按股票记录 checkpoint；失败后只重试未完成股票，完成后按交易日重建 Standard。
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date, timedelta

import pandas as pd

from data_collect.jobs.tushare_daily_incremental import (
    _assert_standard_count,
    _codes,
    _date,
    _done,
    _checkpoint,
    _rebuild_market_date,
    _trade_dates,
    _version_and_snapshot,
)
from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.utils.db import get_connection

logger = logging.getLogger("tushare_adj_factor_incremental_retry")


def run(end_date=None, lookback_days=5, batch_size=100, batch_pause=2.0):
    end = end_date or date.today().strftime("%Y%m%d")
    batch_id = f"adj-retry-{date.today().strftime('%Y%m%d')}-{int(time.time())}"
    pro = get_pro()
    calendar = _trade_dates(pro, end, int(lookback_days))
    if calendar is None or calendar.empty:
        return "没有找到需要同步的交易日"
    days = calendar["cal_date"].astype(str).tolist()
    codes = _codes()
    total = 0
    logger.info("adj retry started batch_id=%s dates=%s stocks=%s", batch_id, days, len(codes))
    for index, code in enumerate(codes, 1):
        key = f"{code}:{days[0]}:{days[-1]}"
        if _done("adj_factor_retry", key):
            continue
        try:
            frame = call_api(pro.adj_factor, ts_code=code, start_date=days[0], end_date=days[-1])
            versions, _ = _version_and_snapshot("adj_factor", frame, batch_id)
            total += versions
            _checkpoint("adj_factor_retry", key, "done", versions)
            if index % int(batch_size) == 0:
                logger.info("adj retry progress=%s/%s versions=%s", index, len(codes), total)
                time.sleep(float(batch_pause))
        except Exception as exc:  # noqa: BLE001
            logger.exception("adj retry failed code=%s", code)
            _checkpoint("adj_factor_retry", key, "failed", total, str(exc)[:500])
            raise
    for day in days:
        with get_connection() as conn:
            frame = pd.read_sql_query("""SELECT DISTINCT ON (ts_code,trade_date) payload
                FROM tushare_market_raw_version WHERE dataset='adj_factor'
                AND trade_date=%s ORDER BY ts_code,trade_date,fetched_at DESC,record_id DESC""", conn, params=(_date(day),))
        if frame.empty:
            raise RuntimeError(f"adj_factor {day} 没有可用 Raw 版本")
        payload = pd.json_normalize(frame["payload"].map(lambda x: x if isinstance(x, dict) else json.loads(x)))
        _rebuild_market_date(payload, "adj_factor", _date(day))
        actual = _assert_standard_count("adj_factor", _date(day), "adj_factor_daily")
        logger.info("adj retry standard date=%s rows=%s", day, actual)
    return f"adj_factor 重试完成：dates={days}, stocks={len(codes)}, versions={total}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="单独重试 Tushare adj_factor 增量")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--lookback-days", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--batch-pause", type=float, default=2.0)
    args = parser.parse_args()
    print(run(args.end_date, args.lookback_days, args.batch_size, args.batch_pause))
