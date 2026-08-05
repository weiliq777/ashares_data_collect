"""财务 Raw 日常增量。

报表接口按公告日期窗口请求；fina_indicator 按股票、报告期窗口请求，
遇到单次 100 条上限时自动按年度拆分。所有响应先写入 Raw，再写入不可变
版本层；任务可重复运行，Raw 和版本表均按 payload_hash 幂等。
"""
from __future__ import annotations

import argparse
import logging
import time
import uuid
from datetime import date, timedelta

import pandas as pd

from data_collect.jobs.tushare_financial_raw_init import _codes, _save_raw
from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.utils.db import get_connection

logger = logging.getLogger("tushare_financial_incremental")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

REPORT_APIS = ("income", "balancesheet", "cashflow")
INDICATOR_FIELDS = (
    "ts_code,ann_date,end_date,roe,roa,debt_to_assets,grossprofit_margin,"
    "netprofit_margin,ocf_to_or,eps,bps,or_yoy,tr_yoy,q_sales_yoy,netprofit_yoy"
)


def _checkpoint(dataset: str, key: str, status: str, rows: int = 0, error: str | None = None):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO tushare_history_checkpoint
            (dataset, checkpoint_key, status, rows_written, error_message, updated_at)
            VALUES (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
            ON CONFLICT (dataset, checkpoint_key) DO UPDATE SET
              status=EXCLUDED.status, rows_written=EXCLUDED.rows_written,
              error_message=EXCLUDED.error_message, updated_at=CURRENT_TIMESTAMP""",
                    (dataset, key, status, rows, error))
        conn.commit()


def _completed(dataset: str, key: str) -> bool:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM tushare_history_checkpoint WHERE dataset=%s AND checkpoint_key=%s", (dataset, key))
        row = cur.fetchone()
    return bool(row and row[0] == "done")


def _year_windows(start: str, end: str):
    start_date = pd.to_datetime(start, format="%Y%m%d").date()
    end_date = pd.to_datetime(end, format="%Y%m%d").date()
    current = start_date
    while current <= end_date:
        window_end = min(date(current.year, 12, 31), end_date)
        yield current.strftime("%Y%m%d"), window_end.strftime("%Y%m%d")
        current = window_end + timedelta(days=1)


def _indicator_windows(start: str, end: str):
    """按报告期季度切片，避免 fina_indicator 单次最多 100 条被截断。"""
    start_date = pd.to_datetime(start, format="%Y%m%d").date()
    end_date = pd.to_datetime(end, format="%Y%m%d").date()
    current = start_date
    while current <= end_date:
        quarter = (current.month - 1) // 3 + 1
        quarter_end_month = quarter * 3
        if quarter_end_month == 12:
            quarter_end = date(current.year, 12, 31)
        else:
            quarter_end = date(current.year, quarter_end_month + 1, 1) - timedelta(days=1)
        window_end = min(quarter_end, end_date)
        yield current.strftime("%Y%m%d"), window_end.strftime("%Y%m%d")
        current = window_end + timedelta(days=1)


def _rebuild_financial_derived():
    from data_collect.jobs.tushare_financial_raw_to_standard import run as standard_run
    from data_collect.jobs.tushare_fina_indicator_version import rebuild_standard as rebuild_indicator
    from data_collect.jobs.tushare_phase2_features import run as feature_run
    from data_collect.jobs.tushare_phase2_standard import run as standard_phase2_run

    standard_run("all")
    rebuild_indicator()
    standard_phase2_run("financial_asof")
    feature_run("company_financial")


def run(end_date: str | None = None, lookback_days: int = 30,
        report_lookback_years: int = 3, limit_stocks: int | None = None,
        batch_pause: float = 1.0, dataset: str = "all", rebuild_derived: bool = True,
        dry_run: bool = False) -> str:
    if dataset not in {"all", *REPORT_APIS, "fina_indicator"}:
        raise ValueError(f"不支持的数据集: {dataset}")
    end = end_date or date.today().strftime("%Y%m%d")
    end_dt = pd.to_datetime(end, format="%Y%m%d").date()
    announcement_start = (end_dt - timedelta(days=max(lookback_days - 1, 0))).strftime("%Y%m%d")
    report_start = date(end_dt.year - max(report_lookback_years - 1, 0), 1, 1).strftime("%Y%m%d")
    codes = _codes()[: int(limit_stocks or 10**9)]
    pro = get_pro()
    batch_id = f"financial-incremental-{uuid.uuid4().hex}"
    selected = list(REPORT_APIS) + ["fina_indicator"] if dataset == "all" else [dataset]
    total = 0

    for api_name in selected:
        api = getattr(pro, api_name)
        for code_index, code in enumerate(codes, 1):
            windows = [(announcement_start, end)] if api_name in REPORT_APIS else list(_indicator_windows(report_start, end))
            for window_start, window_end in windows:
                key = f"{code}:{window_start}:{window_end}"
                checkpoint_dataset = f"financial_{api_name}_incremental"
                if _completed(checkpoint_dataset, key):
                    continue
                try:
                    kwargs = {"ts_code": code, "start_date": window_start, "end_date": window_end}
                    if api_name == "fina_indicator":
                        kwargs["fields"] = INDICATOR_FIELDS
                    frame = call_api(api, **kwargs)
                    rows = 0 if frame is None else len(frame)
                    if rows >= 100 and api_name == "fina_indicator":
                        raise RuntimeError(f"fina_indicator 达到单次100条上限: {code} {window_start}-{window_end}")
                    if dry_run:
                        logger.info("dry-run api=%s code=%s window=%s:%s rows=%s", api_name, code, window_start, window_end, rows)
                    elif rows == 0:
                        _checkpoint(checkpoint_dataset, key, "empty", 0, "接口返回空数据")
                    else:
                        table = "tushare_fina_indicator_raw" if api_name == "fina_indicator" else f"tushare_{api_name}_raw"
                        inserted = _save_raw(table, frame, batch_id=batch_id)
                        total += inserted
                        _checkpoint(checkpoint_dataset, key, "done", inserted)
                    time.sleep(float(batch_pause))
                except Exception as exc:  # noqa: BLE001
                    logger.exception("financial incremental failed api=%s code=%s window=%s:%s", api_name, code, window_start, window_end)
                    if not dry_run:
                        _checkpoint(checkpoint_dataset, key, "failed", 0, str(exc)[:500])
                    raise
            if code_index % 100 == 0:
                logger.info("financial incremental progress api=%s stocks=%s/%s rows=%s", api_name, code_index, len(codes), total)

    if rebuild_derived and not dry_run and total:
        _rebuild_financial_derived()
    return f"财务增量完成 batch_id={batch_id} stocks={len(codes)} rows={total}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tushare 财务增量同步")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--report-lookback-years", type=int, default=3)
    parser.add_argument("--limit-stocks", type=int)
    parser.add_argument("--batch-pause", type=float, default=1.0)
    parser.add_argument("--dataset", choices=["all", *REPORT_APIS, "fina_indicator"], default="all")
    parser.add_argument("--no-rebuild-derived", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(run(args.end_date, args.lookback_days, args.report_lookback_years, args.limit_stocks,
              args.batch_pause, args.dataset, not args.no_rebuild_derived, args.dry_run))
