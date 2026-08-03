"""阶段二公司状态/公司行为/指数 Raw 同步任务。

所有接口响应先以完整 JSON 写入对应 Raw 表，再由后续 Standard/PIT 任务转换。
历史状态类接口按交易日获取；名称变化和分红按股票获取，均使用 checkpoint 续传。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
import uuid
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd

from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.utils.db import get_connection, require_psycopg2


logger = logging.getLogger("tushare_phase2_raw_init")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    log_dir = Path(__file__).resolve().parents[2] / "logs"
    log_dir.mkdir(exist_ok=True)
    handler = RotatingFileHandler(
        log_dir / "tushare_phase2_raw_init.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())


DATE_DATASETS = {
    "stock_st": ("stock_st", "tushare_stock_st_raw"),
    "suspend_d": ("suspend_d", "tushare_suspend_d_raw"),
    "stk_limit": ("stk_limit", "tushare_stk_limit_raw"),
}
EVENT_DATASETS = {
    "namechange": ("namechange", "tushare_namechange_raw"),
    "dividend": ("dividend", "tushare_dividend_raw"),
}
INDEX_CODES = ("000300.SH", "000905.SH", "000852.SH", "000016.SH")


def _date(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    parsed = pd.to_datetime(value, format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _clean(value):
    return None if pd.isna(value) else value


def _payload(record: dict) -> tuple[str, str]:
    clean = {key: _clean(value) for key, value in record.items()}
    payload = json.dumps(clean, ensure_ascii=False, default=str, allow_nan=False)
    return payload, hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _checkpoint(dataset: str, key: str, status: str, rows: int = 0, error: str | None = None):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO tushare_history_checkpoint
            (dataset, checkpoint_key, status, rows_written, error_message, updated_at)
            VALUES (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
            ON CONFLICT (dataset, checkpoint_key) DO UPDATE SET
              status=EXCLUDED.status, rows_written=EXCLUDED.rows_written,
              error_message=EXCLUDED.error_message, updated_at=CURRENT_TIMESTAMP""",
            (dataset, key, status, rows, error),
        )
        conn.commit()


def _checkpoint_done(dataset: str, key: str) -> bool:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM tushare_history_checkpoint WHERE dataset=%s AND checkpoint_key=%s",
            (dataset, key),
        )
        row = cur.fetchone()
    return bool(row and row[0] in {"done", "empty"})


def _trade_dates(start: str, end: str, lookback_days: int | None = None) -> list[str]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT cal_date FROM tushare_trade_cal_raw
               WHERE exchange='SSE' AND cal_date BETWEEN %s AND %s
                 AND (payload->>'is_open')::int = 1
               ORDER BY cal_date""",
            (_date(start), _date(end)),
        )
        dates = [row[0].strftime("%Y%m%d") for row in cur.fetchall()]
    if lookback_days:
        dates = dates[-int(lookback_days) :]
    return dates


def _stock_codes(limit_stocks: int | None = None) -> list[str]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT stock_code FROM stock_basic_info
               WHERE list_status IN ('L','D','P','G') ORDER BY stock_code"""
        )
        codes = [row[0] for row in cur.fetchall()]
    return codes[: int(limit_stocks)] if limit_stocks else codes


def _save_frame(table: str, frame: pd.DataFrame, batch_id: str) -> int:
    if frame is None or frame.empty:
        return 0
    psycopg2, execute_values = require_psycopg2()
    rows = []
    for record in frame.to_dict("records"):
        payload, digest = _payload(record)
        dataset = table.replace("tushare_", "").replace("_raw", "")
        if table == "tushare_stock_st_raw":
            values = (record.get("ts_code"), _date(record.get("trade_date")), record.get("type"), payload, "tushare", batch_id, digest)
            columns = ("ts_code", "trade_date", "type", "payload", "source", "batch_id", "payload_hash")
        elif table == "tushare_namechange_raw":
            values = (record.get("ts_code"), _date(record.get("start_date")), _date(record.get("end_date")), _date(record.get("ann_date")), payload, "tushare", batch_id, digest)
            columns = ("ts_code", "start_date", "end_date", "ann_date", "payload", "source", "batch_id", "payload_hash")
        elif table == "tushare_suspend_d_raw":
            values = (record.get("ts_code"), _date(record.get("trade_date")), record.get("suspend_type"), payload, "tushare", batch_id, digest)
            columns = ("ts_code", "trade_date", "suspend_type", "payload", "source", "batch_id", "payload_hash")
        elif table == "tushare_stk_limit_raw":
            values = (record.get("ts_code"), _date(record.get("trade_date")), payload, "tushare", batch_id, digest)
            columns = ("ts_code", "trade_date", "payload", "source", "batch_id", "payload_hash")
        elif table == "tushare_dividend_raw":
            values = (record.get("ts_code"), _date(record.get("end_date")), _date(record.get("ann_date")), record.get("div_proc"), _date(record.get("record_date")), _date(record.get("ex_date")), payload, "tushare", batch_id, digest)
            columns = ("ts_code", "end_date", "ann_date", "div_proc", "record_date", "ex_date", "payload", "source", "batch_id", "payload_hash")
        elif table == "tushare_index_daily_raw":
            values = (record.get("ts_code"), _date(record.get("trade_date")), payload, "tushare", batch_id, digest)
            columns = ("ts_code", "trade_date", "payload", "source", "batch_id", "payload_hash")
        else:
            raise ValueError(f"不支持的 Raw 表: {table}")
        rows.append(values)
    placeholders = ", ".join(["%s"] * len(rows[0]))
    sql = f'INSERT INTO "{table}" ({", ".join(columns)}) VALUES %s ON CONFLICT DO NOTHING'
    with get_connection() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
        inserted = cur.rowcount
        conn.commit()
    logger.debug("saved table=%s attempted=%s inserted=%s", table, len(rows), inserted)
    return inserted


def _run_date_dataset(dataset: str, pro, start: str, end: str, lookback_days: int | None, pause: float, batch_id: str):
    api_name, table = DATE_DATASETS[dataset]
    dates = _trade_dates(start, end, lookback_days)
    total = 0
    logger.info("phase2 date dataset started dataset=%s dates=%s", dataset, len(dates))
    for index, day in enumerate(dates, 1):
        if _checkpoint_done(dataset, day):
            logger.info("skip completed dataset=%s key=%s", dataset, day)
            continue
        try:
            frame = call_api(getattr(pro, api_name), trade_date=day)
            rows = 0 if frame is None else len(frame)
            if rows == 0:
                _checkpoint(dataset, day, "empty", 0, "接口返回空数据")
                logger.warning("dataset=%s trade_date=%s rows=0", dataset, day)
            else:
                inserted = _save_frame(table, frame, batch_id)
                total += inserted
                _checkpoint(dataset, day, "done", inserted)
                logger.info("dataset=%s trade_date=%s api_rows=%s inserted=%s", dataset, day, rows, inserted)
        except Exception as exc:  # noqa: BLE001
            logger.exception("dataset failed dataset=%s trade_date=%s", dataset, day)
            _checkpoint(dataset, day, "failed", 0, str(exc)[:500])
            raise
        time.sleep(float(pause))
    return total


def _run_event_dataset(dataset: str, pro, start: str, end: str, limit_stocks: int | None, pause: float, batch_id: str):
    api_name, table = EVENT_DATASETS[dataset]
    codes = _stock_codes(limit_stocks)
    total = 0
    logger.info("phase2 event dataset started dataset=%s stocks=%s", dataset, len(codes))
    for index, code in enumerate(codes, 1):
        if _checkpoint_done(dataset, code):
            continue
        try:
            frame = call_api(getattr(pro, api_name), ts_code=code)
            rows = 0 if frame is None else len(frame)
            if rows == 0:
                _checkpoint(dataset, code, "empty", 0, "接口返回空数据")
            else:
                inserted = _save_frame(table, frame, batch_id)
                total += inserted
                _checkpoint(dataset, code, "done", inserted)
            if index % 100 == 0:
                logger.info("dataset=%s progress=%s/%s inserted=%s", dataset, index, len(codes), total)
        except Exception as exc:  # noqa: BLE001
            logger.exception("dataset failed dataset=%s ts_code=%s", dataset, code)
            _checkpoint(dataset, code, "failed", 0, str(exc)[:500])
            raise
        time.sleep(float(pause))
    return total


def _run_index(pro, start: str, end: str, pause: float, batch_id: str):
    total = 0
    for index, code in enumerate(INDEX_CODES, 1):
        if _checkpoint_done("index_daily", code):
            continue
        try:
            frame = call_api(pro.index_daily, ts_code=code, start_date=start, end_date=end)
            rows = 0 if frame is None else len(frame)
            if rows == 0:
                _checkpoint("index_daily", code, "empty", 0, "接口返回空数据")
            else:
                inserted = _save_frame("tushare_index_daily_raw", frame, batch_id)
                total += inserted
                _checkpoint("index_daily", code, "done", inserted)
            logger.info("dataset=index_daily code=%s/%s rows=%s", index, len(INDEX_CODES), rows)
        except Exception as exc:  # noqa: BLE001
            logger.exception("dataset failed dataset=index_daily ts_code=%s", code)
            _checkpoint("index_daily", code, "failed", 0, str(exc)[:500])
            raise
        time.sleep(float(pause))
    return total


def run(
    dataset: str,
    start_date: str = "20210802",
    end_date: str | None = None,
    lookback_days: int | None = None,
    limit_stocks: int | None = None,
    pause: float = 0.5,
) -> str:
    end = end_date or date.today().strftime("%Y%m%d")
    allowed = {"stock_st", "suspend_d", "stk_limit", "namechange", "dividend", "index_daily"}
    if dataset not in allowed:
        raise ValueError(f"不支持的数据集: {dataset}")
    if dataset in DATE_DATASETS and not _trade_dates(start_date, end, lookback_days):
        return f"{dataset} 没有可同步的交易日"
    pro = get_pro()
    batch_id = f"phase2-{dataset}-{uuid.uuid4().hex}"
    if dataset in DATE_DATASETS:
        count = _run_date_dataset(dataset, pro, start_date, end, lookback_days, pause, batch_id)
    elif dataset in EVENT_DATASETS:
        count = _run_event_dataset(dataset, pro, start_date, end, limit_stocks, pause, batch_id)
    else:
        count = _run_index(pro, start_date, end, pause, batch_id)
    return f"阶段二 Raw 同步完成 dataset={dataset} inserted={count}"


def main() -> None:
    parser = argparse.ArgumentParser(description="阶段二 Tushare Raw 同步")
    parser.add_argument("--dataset", required=True, choices=["stock_st", "suspend_d", "stk_limit", "namechange", "dividend", "index_daily"])
    parser.add_argument("--start-date", default="20210802")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--lookback-days", type=int)
    parser.add_argument("--limit-stocks", type=int)
    parser.add_argument("--pause", type=float, default=0.5)
    args = parser.parse_args()
    print(run(args.dataset, args.start_date, args.end_date, args.lookback_days, args.limit_stocks, args.pause))


if __name__ == "__main__":
    main()
