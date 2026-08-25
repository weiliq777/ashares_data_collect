"""Historical market and financial Raw initialization through typed repositories."""
from __future__ import annotations

import argparse
import logging
import time
import uuid
from datetime import date, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.data_access.repository.history_repository import HistoryRepository
from data_collect.data_access.repository.market_repository import MarketRepository
from data_collect.normalize.tushare_daily import normalize_daily, normalize_daily_basic
from data_collect.providers.tushare_client import call_api, get_pro


HISTORY_MARKET_ROW_LIMITS = {"daily": 6000, "valuation": 6000}
HISTORY_CODE_CHUNK_SIZE = 1000


def _get_logger():
    logger = logging.getLogger("tushare_history_init")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    log_dir = Path(__file__).resolve().parents[2] / "logs"
    log_dir.mkdir(exist_ok=True)
    handler = RotatingFileHandler(log_dir / "tushare_history_init.log", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())
    return logger


logger = _get_logger()


def _service(token: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(token, "tushare-history-init", "unknown", "unknown", "v1")
    return kernel, HistoryRepository(kernel), MarketRepository(kernel), FinancialRepository(kernel), context


def _date(value):
    if not value:
        return None
    parsed = pd.to_datetime(value, format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _checkpoint(dataset, key, status="done", rows=0, error=None, history=None, context=None):
    kernel = None
    if history is None or context is None:
        kernel, history, _market, _financial, context = _service(f"history-checkpoint-{uuid.uuid4().hex}")
    try:
        history.save(dataset, key, status, rows, context, error)
    finally:
        if kernel is not None:
            kernel.close()


def _done(dataset, key, history=None, context=None):
    kernel = None
    if history is None or context is None:
        kernel, history, _market, _financial, context = _service(f"history-done-{uuid.uuid4().hex}")
    try:
        if history.status(dataset, key, context) != "done":
            return False
        raw_table = {"daily": "tushare_daily_raw", "valuation": "tushare_daily_basic_raw"}.get(dataset)
        if not raw_table:
            return True
        # A checkpoint is valid only while the immutable Raw snapshot still has rows.
        day = _date(key)
        return history.raw_snapshot_count(raw_table, day, context) > 0
    finally:
        if kernel is not None:
            kernel.close()


def _codes_for_history(history=None, context=None):
    kernel = None
    if history is None or context is None:
        kernel, history, _market, _financial, context = _service("history-codes")
    try:
        return history.active_stock_codes(context)
    finally:
        if kernel is not None:
            kernel.close()


def _request_history_market_frame(pro, dataset, day, codes):
    api = pro.daily if dataset == "daily" else pro.daily_basic
    frame = call_api(api, trade_date=day)
    limit = HISTORY_MARKET_ROW_LIMITS[dataset]
    if frame is None or len(frame) < limit:
        return frame
    logger.warning("history dataset=%s trade_date=%s reached API limit; retrying by code chunks", dataset, day)
    chunks = []
    for offset in range(0, len(codes), HISTORY_CODE_CHUNK_SIZE):
        code_batch = ",".join(codes[offset:offset + HISTORY_CODE_CHUNK_SIZE])
        part = call_api(api, ts_code=code_batch, trade_date=day)
        if part is not None and not part.empty:
            chunks.append(part)
    if not chunks:
        return pd.DataFrame(columns=frame.columns)
    result = pd.concat(chunks, ignore_index=True)
    key_columns = [column for column in ("ts_code", "trade_date") if column in result.columns]
    return result.drop_duplicates(subset=key_columns) if key_columns else result


def _save_market_raw(table, df, key_fields, market=None, context=None, batch_id=None):
    if df is None or df.empty:
        return 0
    dataset = {
        "tushare_daily_raw": "daily", "tushare_daily_basic_raw": "daily_basic",
        "tushare_adj_factor_raw": "adj_factor", "tushare_trade_cal_raw": "trade_cal",
    }.get(table)
    if dataset is None:
        raise ValueError(f"unsupported historical market raw table: {table}")
    kernel = None
    if market is None or context is None:
        kernel, _history, market, _financial, context = _service(f"history-market-raw-{uuid.uuid4().hex}")
    try:
        rows = df.astype(object).where(pd.notna(df), None).to_dict("records")
        _versions, raw_rows = market.append_market_versions(dataset, rows, batch_id or uuid.uuid4().hex, context)
        return raw_rows
    finally:
        if kernel is not None:
            kernel.close()


def _save_calendar(df, normalize_standard=False, market=None, context=None, batch_id=None):
    if df is None or df.empty:
        return 0
    _save_market_raw("tushare_trade_cal_raw", df, ["exchange", "cal_date"], market, context, batch_id)
    if not normalize_standard:
        return 0
    rows = [{
        "exchange": row.get("exchange"), "trade_date": _date(row.get("cal_date")),
        "is_open": bool(row.get("is_open")), "pretrade_date": _date(row.get("pretrade_date")),
        "source": "tushare", "fetched_at": datetime.now(),
    } for row in df.astype(object).where(pd.notna(df), None).to_dict("records")]
    return market.upsert_calendar(rows, context)


def _save_adj_factor(pro, codes, start, end, limit_stocks=None, batch_size=100, batch_pause=2.0,
                     normalize_standard=False, batch_id=None, history=None, market=None, context=None):
    selected = codes[: int(limit_stocks or len(codes))]
    kernel = None
    if history is None or market is None or context is None:
        kernel, history, market, _financial, context = _service(f"history-adj-{uuid.uuid4().hex}")
    total = 0
    try:
        for index, code in enumerate(selected, 1):
            key = f"{code}:{start}:{end}"
            if _done("adj_factor", key, history, context):
                continue
            try:
                frame = call_api(pro.adj_factor, ts_code=code, start_date=start, end_date=end)
                rows = [] if frame is None else frame.astype(object).where(pd.notna(frame), None).to_dict("records")
                versions, _raw = market.append_market_versions("adj_factor", rows, batch_id or uuid.uuid4().hex, context)
                total += versions
                if normalize_standard and frame is not None and not frame.empty:
                    for day, part in frame.groupby(frame["trade_date"].map(_date)):
                        from data_collect.jobs.tushare_daily_incremental import _rebuild_market_date
                        _rebuild_market_date(part, "adj_factor", day)
                _checkpoint("adj_factor", key, rows=versions, history=history, context=context)
                if index % int(batch_size) == 0:
                    time.sleep(float(batch_pause))
            except Exception as exc:  # noqa: BLE001
                _checkpoint("adj_factor", key, "failed", total, str(exc)[:500], history, context)
                raise
        return total
    finally:
        if kernel is not None:
            kernel.close()


def _save_financial(pro, codes, start, end, limit_stocks=None, batch_size=25, batch_pause=5.0,
                    normalize_standard=False, history=None, financial=None, context=None):
    selected = codes[: int(limit_stocks or len(codes))]
    kernel = None
    if history is None or financial is None or context is None:
        kernel, history, _market, financial, context = _service(f"history-financial-{uuid.uuid4().hex}")
    batch_id = f"history-financial-{uuid.uuid4().hex}"
    total = 0
    try:
        for index, code in enumerate(selected, 1):
            key = f"{code}:{start}:{end}"
            if _done("financial", key, history, context):
                continue
            try:
                indicator = call_api(pro.fina_indicator, ts_code=code, start_date=start, end_date=end)
                if indicator is not None and not indicator.empty:
                    records = indicator.astype(object).where(pd.notna(indicator), None).to_dict("records")
                    total += financial.append_api_rows("fina_indicator", records, [code], batch_id, context)
                for api_name in ("income", "balancesheet", "cashflow"):
                    frame = call_api(getattr(pro, api_name), ts_code=code, start_date=start, end_date=end)
                    if frame is not None and not frame.empty:
                        records = frame.astype(object).where(pd.notna(frame), None).to_dict("records")
                        total += financial.append_api_rows(api_name, records, [code], batch_id, context)
                _checkpoint("financial", key, rows=total, history=history, context=context)
                if index % int(batch_size) == 0:
                    time.sleep(float(batch_pause))
            except Exception as exc:  # noqa: BLE001
                _checkpoint("financial", key, "failed", total, str(exc)[:500], history, context)
                raise
        return total
    finally:
        if kernel is not None:
            kernel.close()


def run(start_date=None, end_date=None, limit_stocks=None, max_days=None, include_financial=True,
        normalize_standard=False, batch_size=100, batch_pause=2.0,
        financial_batch_size=25, financial_batch_pause=5.0, **kwargs):
    if normalize_standard:
        raise ValueError("historical initialization is raw-only; run the standard projection separately")
    today = datetime.now().date()
    start = start_date or today.replace(year=today.year - 5).strftime("%Y%m%d")
    end = end_date or today.strftime("%Y%m%d")
    pro = get_pro()
    batch_id = f"history-market-{uuid.uuid4().hex}"
    kernel, history, market, financial, context = _service(batch_id)
    try:
        calendar = call_api(pro.trade_cal, exchange="SSE", start_date=start, end_date=end)
        _save_calendar(calendar, False, market, context, batch_id)
        days = calendar.loc[calendar.is_open == 1, "cal_date"].astype(str).tolist() if calendar is not None and not calendar.empty else []
        if max_days:
            days = days[:int(max_days)]
        codes = _codes_for_history(history, context)
        daily_rows = valuation_rows = 0
        for day in days:
            for dataset in ("daily", "valuation"):
                if _done(dataset, day, history, context):
                    continue
                raw_df = _request_history_market_frame(pro, dataset, day, codes)
                if raw_df is None or raw_df.empty:
                    _checkpoint(dataset, day, "empty", 0, "API returned empty response", history, context)
                    continue
                raw_dataset = "daily" if dataset == "daily" else "daily_basic"
                versions, _raw = market.append_market_versions(
                    raw_dataset,
                    raw_df.astype(object).where(pd.notna(raw_df), None).to_dict("records"),
                    batch_id, context,
                )
                _checkpoint(dataset, day, rows=versions, history=history, context=context)
                daily_rows += versions if dataset == "daily" else 0
                valuation_rows += versions if dataset == "valuation" else 0
        adj_rows = _save_adj_factor(pro, codes, start, end, limit_stocks, batch_size, batch_pause, False, batch_id, history, market, context)
        financial_rows = _save_financial(pro, codes, start, end, limit_stocks, financial_batch_size, financial_batch_pause, False, history, financial, context) if include_financial else 0
        return f"history initialization complete days={len(days)}, daily={daily_rows}, valuation={valuation_rows}, adj_factor={adj_rows}, financial={financial_rows}"
    finally:
        kernel.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize Tushare historical Raw")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--limit-stocks", type=int)
    parser.add_argument("--max-days", type=int)
    parser.add_argument("--include-financial", action="store_true")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--batch-pause", type=float, default=2.0)
    args = parser.parse_args()
    print(run(**vars(args), normalize_standard=False))


if __name__ == "__main__":
    main()
