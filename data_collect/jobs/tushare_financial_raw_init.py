"""Initialize immutable financial Raw and version layers through Kernel."""
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
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.data_access.repository.history_repository import HistoryRepository
from data_collect.providers.tushare_client import call_api, get_pro


DATASETS = {
    "fina_indicator": ("tushare_fina_indicator_raw", "financial_fina_indicator"),
    "income": ("tushare_income_raw", "financial_income"),
    "balancesheet": ("tushare_balancesheet_raw", "financial_balancesheet"),
    "cashflow": ("tushare_cashflow_raw", "financial_cashflow"),
}
DEFAULT_FINANCIAL_HISTORY_START = "20160801"


def _logger():
    logger = logging.getLogger("tushare_financial_raw_init")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    logs = Path(__file__).resolve().parents[2] / "logs"
    logs.mkdir(exist_ok=True)
    handler = RotatingFileHandler(logs / "tushare_financial_raw_init.log", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())
    return logger


logger = _logger()


def _service(token: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(token, "tushare-financial-raw-init", "unknown", "unknown", "v1")
    return kernel, HistoryRepository(kernel), FinancialRepository(kernel), context


def _done(dataset: str, key: str, repository: HistoryRepository | None = None, context: RunContext | None = None) -> bool:
    owned = repository is None or context is None
    kernel = history = None
    if owned:
        kernel, history, _financial, context = _service(f"financial-done-{uuid.uuid4().hex}")
        repository = history
    try:
        assert repository is not None and context is not None
        if repository.status(dataset, key, context) == "done":
            return True
        parts = key.rsplit(":", 2)
        return len(parts) == 3 and repository.completed_covering_window(dataset, parts[0], parts[1], parts[2], context)
    finally:
        if kernel is not None:
            kernel.close()


def _checkpoint(
    dataset: str, key: str, status: str = "done", rows: int = 0, error: str | None = None,
    repository: HistoryRepository | None = None, context: RunContext | None = None,
):
    kernel = None
    if repository is None or context is None:
        kernel, repository, _financial, context = _service(f"financial-checkpoint-{uuid.uuid4().hex}")
    try:
        assert repository is not None and context is not None
        repository.save(dataset, key, status, rows, context, error)
    finally:
        if kernel is not None:
            kernel.close()


def _save_raw(
    table: str, frame: pd.DataFrame, batch_id: str | None = None,
    repository: FinancialRepository | None = None, context: RunContext | None = None,
) -> int:
    if frame is None or frame.empty:
        return 0
    api_name = next((name for name, (target, _checkpoint_name) in DATASETS.items() if target == table), None)
    if api_name is None:
        raise ValueError(f"unsupported financial raw table: {table}")
    kernel = None
    if repository is None or context is None:
        kernel, _history, repository, context = _service(f"financial-save-{uuid.uuid4().hex}")
    try:
        records = frame.astype(object).where(pd.notna(frame), None).to_dict("records")
        return repository.append_api_rows(api_name, records, [str(record.get("ts_code")) for record in records], batch_id or uuid.uuid4().hex, context)
    finally:
        if kernel is not None:
            kernel.close()


def _codes(repository: HistoryRepository | None = None, context: RunContext | None = None) -> list[str]:
    kernel = None
    if repository is None or context is None:
        kernel, repository, _financial, context = _service(f"financial-codes-{uuid.uuid4().hex}")
    try:
        assert repository is not None and context is not None
        return repository.active_stock_codes(context)
    finally:
        if kernel is not None:
            kernel.close()


def run(
    dataset: str, start_date: str | None = None, end_date: str | None = None,
    limit_stocks: int | None = None, batch_size: int = 25, batch_pause: float = 5.0,
) -> str:
    if dataset not in DATASETS:
        raise ValueError(f"unsupported financial dataset: {dataset}")
    start = start_date or DEFAULT_FINANCIAL_HISTORY_START
    end = end_date or date.today().strftime("%Y%m%d")
    table, checkpoint_dataset = DATASETS[dataset]
    batch_id = f"financial-raw-{uuid.uuid4().hex}"
    kernel, history, financial, context = _service(batch_id)
    try:
        codes = _codes(history, context)[: int(limit_stocks or 10**9)]
        api = getattr(get_pro(), dataset)
        total = 0
        logger.info("financial raw init started dataset=%s table=%s stocks=%s", dataset, table, len(codes))
        for index, code in enumerate(codes, 1):
            key = f"{code}:{start}:{end}"
            if _done(checkpoint_dataset, key, history, context):
                continue
            try:
                frame = call_api(api, ts_code=code, start_date=start, end_date=end)
                rows = _save_raw(table, frame, batch_id, financial, context)
                total += rows
                _checkpoint(checkpoint_dataset, key, "done", rows, repository=history, context=context)
                if index % int(batch_size) == 0:
                    logger.info("%s progress=%s/%s rows=%s", dataset, index, len(codes), total)
                    time.sleep(float(batch_pause))
            except Exception as exc:  # noqa: BLE001
                logger.exception("%s failed code=%s", dataset, code)
                _checkpoint(checkpoint_dataset, key, "failed", total, str(exc)[:500], history, context)
                raise
        return f"{dataset} raw complete stocks={len(codes)}, rows={total}"
    finally:
        kernel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize Tushare financial Raw")
    parser.add_argument("dataset", choices=sorted(DATASETS))
    parser.add_argument("--start-date", default=DEFAULT_FINANCIAL_HISTORY_START)
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--limit-stocks", type=int)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--batch-pause", type=float, default=5.0)
    args = parser.parse_args()
    print(run(args.dataset, args.start_date, args.end_date, args.limit_stocks, args.batch_size, args.batch_pause))
