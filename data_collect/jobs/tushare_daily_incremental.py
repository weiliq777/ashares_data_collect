"""Tushare 每日增量：先保存 Raw 版本，再重建对应日期的 Standard。

现有 *_raw 表只做首次业务记录的幂等追加；每次抓取的完整响应进入
tushare_market_raw_version，后续修正不会覆盖历史 Raw。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
import uuid
from datetime import date, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd

from data_collect.normalize.tushare_daily import normalize_daily, normalize_daily_basic
from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.utils.db import get_connection, replace_day_then_insert


logger = logging.getLogger("tushare_daily_incremental")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    log_dir = Path(__file__).resolve().parents[2] / "logs"
    log_dir.mkdir(exist_ok=True)
    handler = RotatingFileHandler(log_dir / "tushare_daily_incremental.log", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())


def _clean(row):
    return {key: (None if pd.isna(value) else value) for key, value in row.items()}


def _json(row):
    return json.dumps(_clean(row), ensure_ascii=False, default=str, allow_nan=False)


def _version_and_snapshot(dataset, frame, batch_id):
    """写入不可变版本，并将首次出现的业务键追加到现有 Raw 快照。"""
    if frame is None or frame.empty:
        return 0, 0
    version_rows = []
    snapshot_rows = []
    for row in frame.to_dict("records"):
        clean = _clean(row)
        payload = _json(clean)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        if dataset == "trade_cal":
            business_key = f"{clean.get('exchange')}:{clean.get('cal_date')}"
            ts_code = trade_date = None
            exchange, cal_date = clean.get("exchange"), _date(clean.get("cal_date"))
        else:
            business_key = f"{clean.get('ts_code')}:{clean.get('trade_date')}"
            ts_code, trade_date = clean.get("ts_code"), _date(clean.get("trade_date"))
            exchange = cal_date = None
        version_rows.append((dataset, business_key, ts_code, trade_date, exchange, cal_date, payload, "tushare", batch_id, digest))
        snapshot_rows.append((clean, payload))
    with get_connection() as conn, conn.cursor() as cur:
        for values in version_rows:
            cur.execute("""INSERT INTO tushare_market_raw_version
                (dataset,business_key,ts_code,trade_date,exchange,cal_date,payload,source,batch_id,payload_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (dataset,business_key,payload_hash) DO NOTHING""", values)
        table = {"daily": "tushare_daily_raw", "daily_basic": "tushare_daily_basic_raw", "adj_factor": "tushare_adj_factor_raw", "trade_cal": "tushare_trade_cal_raw"}[dataset]
        keys = ("exchange", "cal_date") if dataset == "trade_cal" else ("ts_code", "trade_date")
        for clean, payload in snapshot_rows:
            values = tuple(_date(clean.get(key)) if key in ("trade_date", "cal_date") else clean.get(key) for key in keys)
            cur.execute(f'INSERT INTO "{table}" ({", ".join(keys)},payload,source) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING', values + (payload, "tushare"))
        conn.commit()
    return len(version_rows), len(snapshot_rows)


def _date(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    parsed = pd.to_datetime(value, format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _checkpoint(dataset, key, status, rows=0, error=None):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO tushare_history_checkpoint
            (dataset,checkpoint_key,status,rows_written,error_message,updated_at)
            VALUES (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
            ON CONFLICT (dataset,checkpoint_key) DO UPDATE SET status=EXCLUDED.status,
              rows_written=EXCLUDED.rows_written,error_message=EXCLUDED.error_message,updated_at=CURRENT_TIMESTAMP""",
                    (dataset, key, status, rows, error))
        conn.commit()


def _done(dataset, key):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM tushare_history_checkpoint WHERE dataset=%s AND checkpoint_key=%s", (dataset, key))
        row = cur.fetchone()
    return bool(row and row[0] == "done")


def _trade_dates(pro, end, lookback):
    start = (pd.to_datetime(end) - timedelta(days=max(lookback * 3, 10))).strftime("%Y%m%d")
    frame = call_api(pro.trade_cal, exchange="SSE", start_date=start, end_date=end, is_open="1")
    if frame is None or frame.empty:
        return frame
    return frame.sort_values("cal_date").tail(lookback)


def _codes():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT stock_code FROM stock_basic_info WHERE list_status IN ('L','D','P','G') ORDER BY stock_code")
        return [row[0] for row in cur.fetchall()]


def _rebuild_market_date(frame, dataset, trade_date):
    if dataset == "daily":
        out = normalize_daily(frame)
        if out.empty:
            raise RuntimeError(f"daily {trade_date} 转换后为空，保留既有 Standard")
        return replace_day_then_insert("daily_kline", trade_date, out)
    if dataset == "daily_basic":
        out = normalize_daily_basic(frame)
        if out.empty:
            raise RuntimeError(f"daily_basic {trade_date} 转换后为空，保留既有 Standard")
        keep = ["stock_code", "trade_date", "turnover_rate", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "total_mv", "circ_mv"]
        out = out[[c for c in keep if c in out.columns]].copy()
        out["source"], out["fetched_at"] = "tushare", pd.Timestamp.now()
        return replace_day_then_insert("valuation_daily", trade_date, out)
    out = frame.rename(columns={"ts_code": "stock_code"}).copy()
    out["trade_date"] = out["trade_date"].map(_date)
    out["adj_factor"] = pd.to_numeric(out["adj_factor"], errors="coerce")
    out = out[["stock_code", "trade_date", "adj_factor"]].dropna(subset=["stock_code", "trade_date"])
    out["source"], out["fetched_at"] = "tushare", pd.Timestamp.now()
    return replace_day_then_insert("adj_factor_daily", trade_date, out)


def _assert_standard_count(dataset, trade_date, standard_table):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""SELECT COUNT(*) FROM (
            SELECT DISTINCT ON (ts_code, trade_date) ts_code, trade_date
            FROM tushare_market_raw_version
            WHERE dataset=%s AND trade_date=%s
            ORDER BY ts_code, trade_date, fetched_at DESC, record_id DESC
        ) latest""", (dataset, trade_date))
        expected = cur.fetchone()[0]
        cur.execute(f'SELECT COUNT(*) FROM "{standard_table}" WHERE trade_date=%s', (trade_date,))
        actual = cur.fetchone()[0]
    if expected != actual:
        raise RuntimeError(f"{dataset} {trade_date} Raw最新版本={expected}，Standard={actual}，数量不一致")
    return actual


def run(end_date=None, lookback_days=5, batch_size=100, batch_pause=2.0, adj_batch_size=100, adj_batch_pause=2.0,
        limit_stocks=None, dry_run=False):
    end = end_date or date.today().strftime("%Y%m%d")
    batch_id = uuid.uuid4().hex
    pro = get_pro()
    calendar = _trade_dates(pro, end, int(lookback_days))
    if calendar is None or calendar.empty:
        return "没有找到需要同步的交易日"
    if dry_run:
        logger.info("dry-run enabled: no database writes")
    else:
        _version_and_snapshot("trade_cal", calendar, batch_id)
    days = calendar["cal_date"].astype(str).tolist()
    logger.info("incremental started batch_id=%s dates=%s", batch_id, days)
    sample_codes = set(_codes()[:int(limit_stocks)]) if limit_stocks else None
    dry_results = []
    for day in days:
        for dataset, api in (("daily", pro.daily), ("daily_basic", pro.daily_basic)):
            try:
                frame = call_api(api, trade_date=day)
                if frame is None or frame.empty:
                    _checkpoint(f"{dataset}_incremental", day, "empty", 0, "接口返回空数据，保留既有 Standard")
                    continue
                if sample_codes:
                    code_column = "ts_code" if "ts_code" in frame.columns else "stock_code"
                    frame = frame[frame[code_column].isin(sample_codes)].copy()
                if dry_run:
                    transformed = normalize_daily(frame) if dataset == "daily" else normalize_daily_basic(frame)
                    dry_results.append((dataset, day, len(frame), len(transformed)))
                else:
                    version_rows, snapshot_rows = _version_and_snapshot(dataset, frame, batch_id)
                    _rebuild_market_date(frame, dataset, _date(day))
                    _assert_standard_count(dataset, _date(day), "daily_kline" if dataset == "daily" else "valuation_daily")
                    _checkpoint(f"{dataset}_incremental", day, "done", version_rows)
                    logger.info("%s date=%s versions=%s snapshot_attempted=%s", dataset, day, version_rows, snapshot_rows)
            except Exception as exc:  # noqa: BLE001
                logger.exception("%s failed date=%s", dataset, day)
                _checkpoint(f"{dataset}_incremental", day, "failed", 0, str(exc)[:500])
                raise
            time.sleep(float(batch_pause))
    codes = _codes()
    if limit_stocks:
        codes = codes[:int(limit_stocks)]
    adj_key = f"{days[0]}:{days[-1]}"
    total = 0
    if dry_run:
        for code in codes:
            frame = call_api(pro.adj_factor, ts_code=code, start_date=days[0], end_date=days[-1])
            dry_results.append(("adj_factor", code, 0 if frame is None else len(frame), 0 if frame is None else len(frame)))
        return f"dry-run 完成：dates={days}, stocks={len(codes)}, results={dry_results}"
    try:
        for index, code in enumerate(codes, 1):
            frame = call_api(pro.adj_factor, ts_code=code, start_date=days[0], end_date=days[-1])
            version_rows, _ = _version_and_snapshot("adj_factor", frame, batch_id)
            total += version_rows
            if index % int(adj_batch_size) == 0:
                logger.info("adj_factor date_range=%s/%s versions=%s", index, len(codes), total)
                time.sleep(float(adj_batch_pause))
        for day in days:
            with get_connection() as conn:
                frame = pd.read_sql_query("""SELECT DISTINCT ON (ts_code,trade_date) payload
                    FROM tushare_market_raw_version WHERE dataset='adj_factor'
                    AND trade_date=%s ORDER BY ts_code,trade_date,fetched_at DESC,record_id DESC""", conn, params=(_date(day),))
            if not frame.empty:
                payload = pd.json_normalize(frame["payload"].map(lambda x: x if isinstance(x, dict) else json.loads(x)))
                _rebuild_market_date(payload, "adj_factor", _date(day))
                _assert_standard_count("adj_factor", _date(day), "adj_factor_daily")
        _checkpoint("adj_factor_incremental", adj_key, "done", total)
    except Exception as exc:  # noqa: BLE001
        logger.exception("adj_factor failed range=%s", adj_key)
        _checkpoint("adj_factor_incremental", adj_key, "failed", total, str(exc)[:500])
        raise
    return f"增量完成 batch_id={batch_id} dates={days}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tushare Raw-first 每日增量")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--lookback-days", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--batch-pause", type=float, default=2.0)
    parser.add_argument("--adj-batch-size", type=int, default=100)
    parser.add_argument("--adj-batch-pause", type=float, default=2.0)
    parser.add_argument("--limit-stocks", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(run(args.end_date, args.lookback_days, args.batch_size, args.batch_pause, args.adj_batch_size, args.adj_batch_pause, args.limit_stocks, args.dry_run))
