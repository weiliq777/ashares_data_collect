"""Phase-2 Raw ingestion orchestration.

API pagination and retry decisions stay in the job; checkpoint, date/code
selection and append-only persistence are owned by ``Phase2RawRepository``.
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

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.history_repository import HistoryRepository
from data_collect.data_access.repository.phase2_raw_repository import Phase2RawRepository
from data_collect.providers.tushare_client import call_api, get_pro


logger = logging.getLogger("tushare_phase2_raw_init")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    log_dir = Path(__file__).resolve().parents[2] / "logs"
    log_dir.mkdir(exist_ok=True)
    handler = RotatingFileHandler(log_dir / "tushare_phase2_raw_init.log", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())


DATE_DATASETS = {"stock_st": ("stock_st", "tushare_stock_st_raw"), "suspend_d": ("suspend_d", "tushare_suspend_d_raw"), "stk_limit": ("stk_limit", "tushare_stk_limit_raw")}
EVENT_DATASETS = {"namechange": ("namechange", "tushare_namechange_raw"), "dividend": ("dividend", "tushare_dividend_raw")}
INDEX_CODES = ("000300.SH", "000905.SH", "000852.SH", "000016.SH")


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"phase2-raw-{label}-{uuid.uuid4().hex}", job_name="tushare_phase2_raw_init",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, Phase2RawRepository(kernel), HistoryRepository(kernel), context


def _date(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    parsed = pd.to_datetime(value, format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _checkpoint(dataset: str, key: str, status: str, rows: int = 0, error: str | None = None):
    kernel, _, history, context = _service("checkpoint")
    try:
        return history.save(dataset, key, status, rows, context, error)
    finally:
        kernel.close()


def _checkpoint_status(dataset: str, key: str) -> str | None:
    kernel, _, history, context = _service("checkpoint-status")
    try:
        return history.status(dataset, key, context)
    finally:
        kernel.close()


def _checkpoint_done(dataset: str, key: str) -> bool:
    return _checkpoint_status(dataset, key) in {"done", "empty"}


def _trade_dates(start: str, end: str, lookback_days: int | None = None) -> list[str]:
    kernel, repository, _, context = _service("trade-dates")
    try:
        return repository.trade_dates(start, end, context, lookback_days)
    finally:
        kernel.close()


def _stock_codes(limit_stocks: int | None = None) -> list[str]:
    kernel, repository, _, context = _service("stock-codes")
    try:
        return repository.stock_codes(context, limit_stocks)
    finally:
        kernel.close()


def _save_frame(table: str, frame: pd.DataFrame, batch_id: str) -> int:
    if frame is None or frame.empty:
        return 0
    kernel, repository, _, context = _service("save")
    try:
        return repository.save_frame(table, frame.to_dict("records"), batch_id, context)
    finally:
        kernel.close()


def _run_date_dataset(dataset, pro, start, end, lookback_days, pause, batch_id, retry_empty=False):
    api_name, table = DATE_DATASETS[dataset]
    total = 0
    for day in _trade_dates(start, end, lookback_days):
        if lookback_days is None and not retry_empty and _checkpoint_done(dataset, day):
            continue
        if retry_empty and _checkpoint_status(dataset, day) != "empty":
            continue
        try:
            frame = call_api(getattr(pro, api_name), trade_date=day)
            rows = 0 if frame is None else len(frame)
            if rows == 0:
                _checkpoint(dataset, day, "empty", 0, "API returned no rows")
            else:
                inserted = _save_frame(table, frame, batch_id)
                total += inserted
                _checkpoint(dataset, day, "done", inserted)
        except Exception as exc:
            logger.exception("dataset failed dataset=%s trade_date=%s", dataset, day)
            _checkpoint(dataset, day, "failed", 0, str(exc)[:500])
            raise
        time.sleep(float(pause))
    return total


def _run_event_dataset(dataset, pro, start, end, limit_stocks, pause, batch_id):
    api_name, table = EVENT_DATASETS[dataset]
    total = 0
    for index, code in enumerate(_stock_codes(limit_stocks), 1):
        if _checkpoint_done(dataset, code):
            continue
        try:
            frame = call_api(getattr(pro, api_name), ts_code=code)
            rows = 0 if frame is None else len(frame)
            if rows == 0:
                _checkpoint(dataset, code, "empty", 0, "API returned no rows")
            else:
                inserted = _save_frame(table, frame, batch_id)
                total += inserted
                _checkpoint(dataset, code, "done", inserted)
            if index % 100 == 0:
                logger.info("dataset=%s progress=%s inserted=%s", dataset, index, total)
        except Exception as exc:
            _checkpoint(dataset, code, "failed", 0, str(exc)[:500])
            raise
        time.sleep(float(pause))
    return total


def _run_index(pro, start, end, pause, batch_id):
    total = 0
    for code in INDEX_CODES:
        if _checkpoint_done("index_daily", code):
            continue
        frame = call_api(pro.index_daily, ts_code=code, start_date=start, end_date=end)
        rows = 0 if frame is None else len(frame)
        if rows == 0:
            _checkpoint("index_daily", code, "empty", 0, "API returned no rows")
        else:
            inserted = _save_frame("tushare_index_daily_raw", frame, batch_id)
            total += inserted
            _checkpoint("index_daily", code, "done", inserted)
        time.sleep(float(pause))
    return total


def run(dataset: str, start_date: str = "20210802", end_date: str | None = None, lookback_days: int | None = None, limit_stocks: int | None = None, pause: float = 0.5, retry_empty: bool = False) -> str:
    end = end_date or date.today().strftime("%Y%m%d")
    allowed = set(DATE_DATASETS) | set(EVENT_DATASETS) | {"index_daily"}
    if dataset not in allowed:
        raise ValueError(f"unsupported dataset: {dataset}")
    if dataset in DATE_DATASETS and not _trade_dates(start_date, end, lookback_days):
        return f"{dataset} no trading dates"
    pro = get_pro()
    batch_id = f"phase2-{dataset}-{uuid.uuid4().hex}"
    if dataset in DATE_DATASETS:
        count = _run_date_dataset(dataset, pro, start_date, end, lookback_days, pause, batch_id, retry_empty)
    elif dataset in EVENT_DATASETS:
        count = _run_event_dataset(dataset, pro, start_date, end, limit_stocks, pause, batch_id)
    else:
        count = _run_index(pro, start_date, end, pause, batch_id)
    return f"phase2 Raw sync complete dataset={dataset} inserted={count}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase-2 Tushare Raw sync")
    parser.add_argument("--dataset", required=True, choices=["stock_st", "suspend_d", "stk_limit", "namechange", "dividend", "index_daily"])
    parser.add_argument("--start-date", default="20210802")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--lookback-days", type=int)
    parser.add_argument("--limit-stocks", type=int)
    parser.add_argument("--pause", type=float, default=0.5)
    parser.add_argument("--retry-empty", action="store_true")
    args = parser.parse_args()
    print(run(args.dataset, args.start_date, args.end_date, args.lookback_days, args.limit_stocks, args.pause, args.retry_empty))


if __name__ == "__main__":
    main()
