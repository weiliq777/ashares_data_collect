"""Tushare 历史库初始化：按交易日/股票分批、可重跑、可断点续传。

默认覆盖近五年。该任务只负责历史事实入库，不生成策略观点。
"""
from __future__ import annotations

import argparse
from datetime import datetime
import time
import json
import logging
import uuid
from logging.handlers import RotatingFileHandler
from pathlib import Path
import pandas as pd

from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.normalize.tushare_daily import normalize_daily, normalize_daily_basic
from data_collect.utils.db import get_connection, save_to_postgres


def _get_logger():
    logger = logging.getLogger("tushare_history_init")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    log_dir = Path(__file__).resolve().parents[2] / "logs"
    log_dir.mkdir(exist_ok=True)
    handler = RotatingFileHandler(log_dir / "tushare_history_init.log", maxBytes=10 * 1024 * 1024,
                                  backupCount=3, encoding="utf-8")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())
    return logger


logger = _get_logger()


# Tushare 按交易日接口达到返回上限时可能已经被截断；历史初始化必须
# 改用代码分片，不能把截断结果标记为完成。
HISTORY_MARKET_ROW_LIMITS = {"daily": 6000, "valuation": 6000}
HISTORY_CODE_CHUNK_SIZE = 1000


def _date(v):
    if not v:
        return None
    return pd.to_datetime(v, format="%Y%m%d", errors="coerce").date()


def _checkpoint(dataset, key, status="done", rows=0, error=None):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO tushare_history_checkpoint
            (dataset, checkpoint_key, status, rows_written, error_message, updated_at)
            VALUES (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
            ON CONFLICT (dataset, checkpoint_key) DO UPDATE SET
              status=EXCLUDED.status, rows_written=EXCLUDED.rows_written,
              error_message=EXCLUDED.error_message, updated_at=CURRENT_TIMESTAMP""",
                    (dataset, key, status, rows, error))
        conn.commit()


def _done(dataset, key):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT status,rows_written FROM tushare_history_checkpoint WHERE dataset=%s AND checkpoint_key=%s",
                    (dataset, key))
        row = cur.fetchone()
        if not row or row[0] != "done":
            return False
        # A historical checkpoint is not proof that Raw still contains the
        # snapshot.  Validate date-based market checkpoints against Raw so a
        # stale checkpoint cannot permanently hide a missing trade date.
        raw_table = {
            "daily": "tushare_daily_raw",
            "valuation": "tushare_daily_basic_raw",
        }.get(dataset)
        if raw_table:
            cur.execute(f"SELECT COUNT(*) FROM {raw_table} WHERE trade_date=%s", (_date(key),))
            actual_rows = int(cur.fetchone()[0])
            # Older Raw-only checkpoints stored 0.  New checkpoints store the
            # API row count, so a partially lost day can no longer be skipped.
            expected_rows = int(row[1] or 0)
            return actual_rows > 0 and (expected_rows <= 0 or actual_rows >= expected_rows)
    return True


def _codes_for_history():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT stock_code FROM stock_basic_info "
            "WHERE list_status IN ('L','D','P','G') ORDER BY stock_code"
        )
        return [row[0] for row in cur.fetchall()]


def _request_history_market_frame(pro, dataset, day, codes):
    """请求完整交易日；命中接口上限时自动按代码分片。"""
    api = pro.daily if dataset == "daily" else pro.daily_basic
    frame = call_api(api, trade_date=day)
    limit = HISTORY_MARKET_ROW_LIMITS[dataset]
    if frame is None or len(frame) < limit:
        return frame

    logger.warning(
        "history dataset=%s trade_date=%s rows=%s reached API limit=%s; "
        "retrying by code chunks",
        dataset, day, len(frame), limit,
    )
    chunks = []
    for offset in range(0, len(codes), HISTORY_CODE_CHUNK_SIZE):
        code_batch = ",".join(codes[offset:offset + HISTORY_CODE_CHUNK_SIZE])
        if not code_batch:
            continue
        part = call_api(api, ts_code=code_batch, trade_date=day)
        if part is not None and not part.empty:
            chunks.append(part)
    if not chunks:
        return pd.DataFrame(columns=frame.columns)
    result = pd.concat(chunks, ignore_index=True)
    key_columns = [column for column in ("ts_code", "trade_date") if column in result.columns]
    return result.drop_duplicates(subset=key_columns) if key_columns else result


def _save_market_raw(table, df, key_fields):
    if df is None or df.empty:
        return 0
    with get_connection() as conn, conn.cursor() as cur:
        for row in df.to_dict("records"):
            clean = {k: (None if pd.isna(v) else v) for k, v in row.items()}
            values = []
            for field in key_fields:
                value = clean.get(field)
                if field in ("trade_date", "cal_date"):
                    value = _date(value)
                values.append(value)
            columns = key_fields + ["payload", "source"]
            placeholders = ", ".join(["%s"] * len(columns))
            cur.execute(
                f'INSERT INTO "{table}" ({", ".join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING',
                tuple(values) + (json.dumps(clean, ensure_ascii=False, default=str, allow_nan=False), "tushare"),
            )
        conn.commit()
    return len(df)


def _save_calendar(df, normalize_standard=False):
    if df is None or df.empty:
        return 0
    _save_market_raw("tushare_trade_cal_raw", df, ["exchange", "cal_date"])
    out = pd.DataFrame({
        "exchange": df["exchange"], "trade_date": df["cal_date"].map(_date),
        "is_open": df["is_open"].astype(bool), "pretrade_date": df["pretrade_date"].map(_date),
        "source": "tushare", "fetched_at": datetime.now(),
    })
    return save_to_postgres(out, pre_aligned_df=out, table_name="trading_calendar")[1] if normalize_standard else 0


def _save_adj_factor(pro, codes, start, end, limit_stocks=None, batch_size=100, batch_pause=2.0, normalize_standard=False, batch_id=None):
    total = 0
    selected = codes[: int(limit_stocks or len(codes))]
    for index, code in enumerate(selected, 1):
        key = f"{code}:{start}:{end}"
        if _done("adj_factor", key):
            continue
        try:
            df = call_api(pro.adj_factor, ts_code=code, start_date=start, end_date=end)
            from data_collect.jobs.tushare_daily_incremental import _version_and_snapshot
            version_rows, _ = _version_and_snapshot(
                "adj_factor", df, batch_id or f"history-{uuid.uuid4().hex}"
            )
            if normalize_standard and df is not None and not df.empty:
                out = pd.DataFrame({"stock_code": df.ts_code, "trade_date": df.trade_date.map(_date),
                                    "adj_factor": df.adj_factor, "source": "tushare", "fetched_at": datetime.now()})
                total += save_to_postgres(out, pre_aligned_df=out, table_name="adj_factor_daily")[1]
            _checkpoint("adj_factor", key, rows=version_rows)
            if index % int(batch_size) == 0:
                time.sleep(float(batch_pause))
                logger.info("adj_factor progress=%s/%s", index, len(selected))
        except Exception as exc:  # noqa: BLE001
            logger.exception("adj_factor failed code=%s start=%s end=%s", code, start, end)
            _checkpoint("adj_factor", key, "failed", total, str(exc)[:500])
            raise
    return total


def _save_financial(pro, codes, start, end, limit_stocks=None, batch_size=25, batch_pause=5.0, normalize_standard=False):
    from data_collect.jobs.tushare_financial_raw_init import _save_raw
    batch_id = f"history-financial-{uuid.uuid4().hex}"
    total = 0
    selected = codes[: int(limit_stocks or len(codes))]
    for index, code in enumerate(selected, 1):
        key = f"{code}:{start}:{end}"
        if _done("financial", key):
            continue
        try:
            indicator = call_api(
                pro.fina_indicator, ts_code=code, start_date=start, end_date=end
            )
            if indicator is not None and not indicator.empty:
                _save_raw("tushare_fina_indicator_raw", indicator, batch_id=batch_id)
                out = indicator.rename(columns={"ts_code": "stock_code", "end_date": "report_date",
                                                "ann_date": "announce_date"}).copy()
                out["report_date"] = out.report_date.map(_date); out["announce_date"] = out.announce_date.map(_date)
                out["source"] = "tushare"; out["fetched_at"] = datetime.now()
                keep = ["stock_code", "report_date", "announce_date", "roe", "roa", "debt_to_assets",
                        "grossprofit_margin", "netprofit_margin", "ocf_to_or", "eps", "bps", "source", "fetched_at"]
                if normalize_standard:
                    standard = out[[c for c in keep if c in out]]
                    total += save_to_postgres(standard, pre_aligned_df=standard,
                                              table_name="stock_financial_indicator")[1]
            for api, table in ((pro.income, "tushare_income_raw"), (pro.balancesheet, "tushare_balancesheet_raw"),
                               (pro.cashflow, "tushare_cashflow_raw")):
                raw = call_api(api, ts_code=code, start_date=start, end_date=end)
                if raw is not None and not raw.empty:
                    _save_raw(table, raw, batch_id=batch_id)
            _checkpoint("financial", key, rows=total)
            if index % int(batch_size) == 0:
                time.sleep(float(batch_pause))
                logger.info("financial progress=%s/%s", index, len(selected))
        except Exception as exc:  # noqa: BLE001
            logger.exception("financial failed code=%s start=%s end=%s", code, start, end)
            _checkpoint("financial", key, "failed", total, str(exc)[:500])
            raise
    return total


def run(start_date=None, end_date=None, limit_stocks=None, max_days=None, include_financial=True,
        normalize_standard=False,
        batch_size=100, batch_pause=2.0, financial_batch_size=25, financial_batch_pause=5.0, **kwargs):
    if normalize_standard:
        raise ValueError("历史初始化已固定为 raw-only；请先完成 Raw 初始化，再运行 tushare_raw_to_standard")
    today = datetime.now().date()
    start = start_date or today.replace(year=today.year - 5).strftime("%Y%m%d")
    end = end_date or today.strftime("%Y%m%d")
    logger.info("history init started start=%s end=%s normalize_standard=%s include_financial=%s",
                start, end, normalize_standard, include_financial)
    pro = get_pro()
    batch_id = f"history-market-{uuid.uuid4().hex}"
    cal = call_api(pro.trade_cal, exchange="SSE", start_date=start, end_date=end)
    from data_collect.jobs.tushare_daily_incremental import _version_and_snapshot
    _version_and_snapshot("trade_cal", cal, batch_id)
    if normalize_standard:
        _save_calendar(cal, normalize_standard)
    days = cal.loc[cal.is_open == 1, "cal_date"].astype(str).tolist() if cal is not None and not cal.empty else []
    if max_days:
        days = days[:int(max_days)]
    codes = _codes_for_history()
    daily_rows = valuation_rows = 0
    for day in days:
        for dataset, api, normalizer, table in (("daily", pro.daily, normalize_daily, "daily_kline"),
                                                  ("valuation", pro.daily_basic, normalize_daily_basic, "valuation_daily")):
            if _done(dataset, day):
                continue
            try:
                raw_df = _request_history_market_frame(pro, dataset, day, codes)
                raw_table = "tushare_daily_raw" if dataset == "daily" else "tushare_daily_basic_raw"
                if raw_df is None or raw_df.empty:
                    message = "Tushare 返回空响应；不标记为 done，等待缺口补采"
                    logger.warning("market dataset empty dataset=%s trade_date=%s", dataset, day)
                    _checkpoint(dataset, day, "empty", 0, message)
                    continue
                raw_dataset = "daily" if dataset == "daily" else "daily_basic"
                raw_versions, _ = _version_and_snapshot(raw_dataset, raw_df, batch_id)
                df = normalizer(raw_df)
                if limit_stocks and not df.empty and normalize_standard:
                    df = df.head(int(limit_stocks))
                if normalize_standard and dataset == "valuation" and not df.empty:
                    keep = ["stock_code", "trade_date", "turnover_rate", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "total_mv", "circ_mv"]
                    df = df[[c for c in keep if c in df]]
                count = 0
                if normalize_standard and not df.empty:
                    if dataset == "valuation":
                        df["source"] = "tushare"; df["fetched_at"] = datetime.now()
                    count = save_to_postgres(df, pre_aligned_df=df, table_name=table)[1]
                    daily_rows += count if dataset == "daily" else 0
                    valuation_rows += count if dataset == "valuation" else 0
                _checkpoint(dataset, day, rows=raw_versions if raw_df is not None else 0)
            except Exception as exc:  # noqa: BLE001
                logger.exception("market dataset failed dataset=%s trade_date=%s", dataset, day)
                _checkpoint(dataset, day, "failed", 0, str(exc)[:500])
                raise
    with get_connection() as conn:
        with conn.cursor() as cur:
            # 历史初始化必须保留 L/D/P/G 全部股票；研究范围和可交易性由后续 PIT 层判断。
            cur.execute("SELECT stock_code FROM stock_basic_info WHERE list_status IN ('L','D','P','G') ORDER BY stock_code")
            codes = [r[0] for r in cur.fetchall()]
    adj_rows = _save_adj_factor(pro, codes, start, end, limit_stocks, batch_size, batch_pause, normalize_standard, batch_id)
    financial_rows = (_save_financial(pro, codes, start, end, limit_stocks, financial_batch_size, financial_batch_pause, normalize_standard)
                      if include_financial else 0)
    return (f"历史初始化完成: days={len(days)}, daily={daily_rows}, valuation={valuation_rows}, "
            f"adj_factor={adj_rows}, financial={financial_rows}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Tushare 历史 Raw 初始化/续传")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--limit-stocks", type=int)
    parser.add_argument("--max-days", type=int)
    parser.add_argument("--include-financial", action="store_true")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--batch-pause", type=float, default=2.0)
    args = parser.parse_args()
    print(run(
        start_date=args.start_date, end_date=args.end_date,
        limit_stocks=args.limit_stocks, max_days=args.max_days,
        include_financial=args.include_financial, normalize_standard=False,
        batch_size=args.batch_size, batch_pause=args.batch_pause,
    ))


if __name__ == "__main__":
    main()
