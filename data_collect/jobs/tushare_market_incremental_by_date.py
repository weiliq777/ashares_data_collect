"""按交易日批量同步 Tushare 行情 Raw，并按日重建 Standard。

历史初始化中的 ``adj_factor`` 继续按股票请求，便于长任务按股票断点续传；
日常增量使用 Tushare 的 ``trade_date`` 全市场接口，每个交易日每个数据集只
请求一次。Raw 只追加版本，不修改或删除已有 Raw 记录。
"""
from __future__ import annotations

import argparse
import logging
import time
import uuid
from datetime import date
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd

from data_collect.jobs.tushare_daily_incremental import (
    _assert_standard_count,
    _checkpoint,
    _date,
    _rebuild_market_date,
    _trade_dates,
    _version_and_snapshot,
)
from data_collect.normalize.tushare_daily import normalize_daily, normalize_daily_basic
from data_collect.providers.tushare_client import call_api, get_pro


logger = logging.getLogger("tushare_market_incremental_by_date")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    log_dir = Path(__file__).resolve().parents[2] / "logs"
    log_dir.mkdir(exist_ok=True)
    handler = RotatingFileHandler(
        log_dir / "tushare_market_incremental_by_date.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())


DATASETS = {
    "daily": ("daily_kline", "daily"),
    "daily_basic": ("valuation_daily", "daily_basic"),
    "adj_factor": ("adj_factor_daily", "adj_factor"),
}


def _normalize_for_count(dataset: str, frame: pd.DataFrame) -> pd.DataFrame:
    if dataset == "daily":
        return normalize_daily(frame)
    if dataset == "daily_basic":
        return normalize_daily_basic(frame)
    return frame


def _filter_sample(frame: pd.DataFrame, sample_codes: set[str] | None) -> pd.DataFrame:
    if not sample_codes or frame is None or frame.empty:
        return frame
    column = "ts_code" if "ts_code" in frame.columns else "stock_code"
    return frame[frame[column].isin(sample_codes)].copy()


def _sync_one_date(
    pro,
    dataset: str,
    day: str,
    batch_id: str,
    sample_codes: set[str] | None = None,
    dry_run: bool = False,
) -> int:
    standard_table, api_name = DATASETS[dataset]
    api = getattr(pro, api_name)
    checkpoint_dataset = f"{dataset}_incremental_by_date"

    # 关键点：三类接口均按 trade_date 请求全市场，不再传入 ts_code 循环股票。
    frame = call_api(api, trade_date=day)
    frame = _filter_sample(frame, sample_codes)
    rows = 0 if frame is None else len(frame)
    if frame is None or frame.empty:
        message = "Tushare 返回空数据，未删除已有 Standard"
        logger.warning("dataset=%s trade_date=%s rows=0: %s", dataset, day, message)
        if not dry_run:
            _checkpoint(checkpoint_dataset, day, "empty", 0, message)
        return 0

    if dry_run:
        transformed = _normalize_for_count(dataset, frame)
        logger.info(
            "dry-run dataset=%s trade_date=%s api_rows=%s normalized_rows=%s",
            dataset,
            day,
            rows,
            len(transformed),
        )
        return rows

    versions, snapshots = _version_and_snapshot(dataset, frame, batch_id)
    _rebuild_market_date(frame, dataset, _date(day))
    actual = _assert_standard_count(dataset, _date(day), standard_table)
    _checkpoint(checkpoint_dataset, day, "done", versions)
    logger.info(
        "dataset=%s trade_date=%s api_rows=%s versions=%s raw_attempted=%s standard_rows=%s",
        dataset,
        day,
        rows,
        versions,
        snapshots,
        actual,
    )
    return versions


def run(
    end_date: str | None = None,
    lookback_days: int = 5,
    batch_pause: float = 1.0,
    dataset: str = "all",
    limit_stocks: int | None = None,
    dry_run: bool = False,
) -> str:
    """同步最近交易日。

    ``limit_stocks`` 和 ``dry_run`` 仅用于小范围验证，正式任务不应使用
    ``limit_stocks``，否则该批次不是完整的全市场快照。
    """
    if int(lookback_days) <= 0:
        raise ValueError("lookback_days 必须大于 0")
    if dataset not in {"all", *DATASETS}:
        raise ValueError(f"不支持的数据集: {dataset}")
    if limit_stocks is not None and int(limit_stocks) <= 0:
        raise ValueError("limit_stocks 必须大于 0")

    end = end_date or date.today().strftime("%Y%m%d")
    pro = get_pro()
    calendar = _trade_dates(pro, end, int(lookback_days))
    if calendar is None or calendar.empty:
        return "没有找到需要同步的交易日"

    days = calendar["cal_date"].astype(str).tolist()
    selected = list(DATASETS) if dataset == "all" else [dataset]
    sample_codes = None
    if limit_stocks:
        from data_collect.jobs.tushare_daily_incremental import _codes

        sample_codes = set(_codes()[: int(limit_stocks)])
        logger.warning("sample mode enabled: only %s stocks", len(sample_codes))

    batch_id = uuid.uuid4().hex
    logger.info(
        "date-batch incremental started batch_id=%s dates=%s datasets=%s",
        batch_id,
        days,
        selected,
    )
    total = 0
    for day in days:
        for current in selected:
            try:
                total += _sync_one_date(
                    pro,
                    current,
                    day,
                    batch_id,
                    sample_codes=sample_codes,
                    dry_run=dry_run,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("date-batch failed dataset=%s trade_date=%s", current, day)
                if not dry_run:
                    _checkpoint(
                        f"{current}_incremental_by_date",
                        day,
                        "failed",
                        0,
                        str(exc)[:500],
                    )
                raise
            time.sleep(float(batch_pause))

    return f"按交易日增量完成 batch_id={batch_id} dates={days} versions={total}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Tushare 全市场按交易日增量同步")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--lookback-days", type=int, default=5)
    parser.add_argument("--batch-pause", type=float, default=1.0)
    parser.add_argument("--dataset", choices=["all", *DATASETS], default="all")
    parser.add_argument("--limit-stocks", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(
        run(
            end_date=args.end_date,
            lookback_days=args.lookback_days,
            batch_pause=args.batch_pause,
            dataset=args.dataset,
            limit_stocks=args.limit_stocks,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
