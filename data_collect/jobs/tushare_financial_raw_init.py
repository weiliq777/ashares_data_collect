"""按单个 Tushare 财务接口初始化 Raw 表。

每次只处理一张表，使用独立 checkpoint，避免四个财务接口相互拖累或重复请求。
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date
from pathlib import Path
from logging.handlers import RotatingFileHandler

import pandas as pd

from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.utils.db import get_connection


DATASETS = {
    "fina_indicator": ("tushare_fina_indicator_raw", "financial_fina_indicator"),
    "income": ("tushare_income_raw", "financial_income"),
    "balancesheet": ("tushare_balancesheet_raw", "financial_balancesheet"),
    "cashflow": ("tushare_cashflow_raw", "financial_cashflow"),
}


def _logger():
    logger = logging.getLogger("tushare_financial_raw_init")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(Path(__file__).resolve().parents[2] / "logs" / "tushare_financial_raw_init.log", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())
    return logger


logger = _logger()


def _date_value(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    parsed = pd.to_datetime(value, format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _done(dataset, key):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM tushare_history_checkpoint WHERE dataset=%s AND checkpoint_key=%s", (dataset, key))
        row = cur.fetchone()
    return bool(row and row[0] == "done")


def _checkpoint(dataset, key, status, rows=0, error=None):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO tushare_history_checkpoint
            (dataset, checkpoint_key, status, rows_written, error_message, updated_at)
            VALUES (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
            ON CONFLICT (dataset, checkpoint_key) DO UPDATE SET
              status=EXCLUDED.status, rows_written=EXCLUDED.rows_written,
              error_message=EXCLUDED.error_message, updated_at=CURRENT_TIMESTAMP""",
                    (dataset, key, status, rows, error))
        conn.commit()


def _save_raw(table, frame) -> int:
    if frame is None or frame.empty:
        return 0
    with get_connection() as conn, conn.cursor() as cur:
        for row in frame.astype(object).where(pd.notna(frame), None).to_dict("records"):
            clean = {key: value for key, value in row.items()}
            cur.execute(f'''INSERT INTO "{table}" (ts_code, ann_date, end_date, payload)
                VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
                        (clean.get("ts_code"), _date_value(clean.get("ann_date")), _date_value(clean.get("end_date")),
                         json.dumps(clean, ensure_ascii=False, default=str, allow_nan=False)))
        conn.commit()
    return len(frame)


def _codes():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT stock_code FROM stock_basic_info WHERE list_status IN ('L','D','P','G') ORDER BY stock_code")
        return [row[0] for row in cur.fetchall()]


def run(dataset: str, start_date: str | None = None, end_date: str | None = None,
        limit_stocks: int | None = None, batch_size: int = 25, batch_pause: float = 5.0) -> str:
    if dataset not in DATASETS:
        raise ValueError(f"不支持的财务接口: {dataset}")
    start = start_date or "20210802"
    end = end_date or date.today().strftime("%Y%m%d")
    table, checkpoint_dataset = DATASETS[dataset]
    codes = _codes()[: int(limit_stocks or 10**9)]
    pro = get_pro()
    api = getattr(pro, dataset)
    total = 0
    logger.info("financial raw init started dataset=%s table=%s stocks=%s start=%s end=%s", dataset, table, len(codes), start, end)
    for index, code in enumerate(codes, 1):
        key = f"{code}:{start}:{end}"
        if _done(checkpoint_dataset, key):
            continue
        try:
            frame = call_api(api, ts_code=code, start_date=start, end_date=end)
            rows = _save_raw(table, frame)
            total += rows
            _checkpoint(checkpoint_dataset, key, "done", rows)
            if index % int(batch_size) == 0:
                logger.info("%s progress=%s/%s rows=%s", dataset, index, len(codes), total)
                time.sleep(float(batch_pause))
        except Exception as exc:  # noqa: BLE001
            logger.exception("%s failed code=%s", dataset, code)
            _checkpoint(checkpoint_dataset, key, "failed", total, str(exc)[:500])
            raise
    return f"{dataset} raw 完成：stocks={len(codes)}, rows={total}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="按单个接口初始化 Tushare 财务 Raw 表")
    parser.add_argument("dataset", choices=sorted(DATASETS))
    parser.add_argument("--start-date", default="20210802")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--limit-stocks", type=int)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--batch-pause", type=float, default=5.0)
    args = parser.parse_args()
    print(run(args.dataset, args.start_date, args.end_date, args.limit_stocks, args.batch_size, args.batch_pause))
