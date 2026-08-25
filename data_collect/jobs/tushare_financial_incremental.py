"""Incremental financial API ingestion through typed repositories."""
from __future__ import annotations

import argparse
import logging
import time
import uuid
from datetime import date, timedelta

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.data_access.repository.history_repository import HistoryRepository
from data_collect.jobs.tushare_job_lock import warehouse_write_job
from data_collect.providers.tushare_client import call_api, get_pro

logger = logging.getLogger("tushare_financial_incremental")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

REPORT_APIS = ("income", "balancesheet", "cashflow")


def _year_windows(start: str, end: str):
    current = pd.to_datetime(start, format="%Y%m%d").date()
    end_date = pd.to_datetime(end, format="%Y%m%d").date()
    while current <= end_date:
        window_end = min(date(current.year, 12, 31), end_date)
        yield current.strftime("%Y%m%d"), window_end.strftime("%Y%m%d")
        current = window_end + timedelta(days=1)


def _indicator_windows(start: str, end: str):
    current = pd.to_datetime(start, format="%Y%m%d").date()
    end_date = pd.to_datetime(end, format="%Y%m%d").date()
    while current <= end_date:
        quarter_end_month = ((current.month - 1) // 3 + 1) * 3
        quarter_end = date(current.year, quarter_end_month, 31) if quarter_end_month == 12 else (
            date(current.year, quarter_end_month + 1, 1) - timedelta(days=1)
        )
        window_end = min(quarter_end, end_date)
        yield current.strftime("%Y%m%d"), window_end.strftime("%Y%m%d")
        current = window_end + timedelta(days=1)


def _pending_derived_codes(repository: HistoryRepository, context: RunContext) -> list[str]:
    """Return durable per-stock work left by Raw/Standard/Feature failures."""
    return repository.pending_keys("financial_derived_v2", ("pending", "failed"), context)


def _rebuild_financial_derived(stock_codes: list[str], output_end: date):
    if not stock_codes:
        return
    from data_collect.jobs.tushare_financial_raw_to_standard import _run_unlocked as standard_run
    from data_collect.jobs.tushare_fina_indicator_version import _rebuild_standard_unlocked as rebuild_indicator
    from data_collect.jobs.tushare_foundation_features import rebuild_company_financial_v2
    from data_collect.jobs.tushare_foundation_pipeline import OUTPUT_START, WARMUP_START

    standard_run("all", 2000, stock_codes, WARMUP_START, output_end)
    rebuild_indicator(2000, stock_codes, WARMUP_START, output_end)
    rebuild_company_financial_v2(
        None, WARMUP_START, OUTPUT_START, output_end,
        requested_codes=stock_codes, target_table="feature.company_financial_v2", batch_stocks=5,
    )


@warehouse_write_job
def run(
    end_date: str | None = None, lookback_days: int = 30,
    report_lookback_years: int = 3, limit_stocks: int | None = None,
    batch_pause: float = 1.0, dataset: str = "all", rebuild_derived: bool = True,
    dry_run: bool = False, run_date: str | None = None,
) -> str:
    if dataset not in {"all", *REPORT_APIS, "fina_indicator"}:
        raise ValueError(f"unsupported dataset: {dataset}")
    if end_date and run_date and str(end_date).replace("-", "") != str(run_date).replace("-", ""):
        raise ValueError("end_date and run_date must match")
    end = (end_date or run_date or date.today().strftime("%Y%m%d")).replace("-", "")
    end_dt = pd.to_datetime(end, format="%Y%m%d").date()
    announcement_start = (end_dt - timedelta(days=max(lookback_days - 1, 0))).strftime("%Y%m%d")
    report_start = date(end_dt.year - max(report_lookback_years - 1, 0), 1, 1).strftime("%Y%m%d")
    batch_id = f"financial-incremental-{uuid.uuid4().hex}"
    context = RunContext(batch_id, "tushare-financial-incremental", "unknown", "unknown", "v1")
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    history = HistoryRepository(kernel)
    financial = FinancialRepository(kernel)
    try:
        codes = history.active_stock_codes(context)[: int(limit_stocks or 10**9)]
        selected = list(REPORT_APIS) + ["fina_indicator"] if dataset == "all" else [dataset]
        pro = get_pro()
        total = 0
        for api_name in selected:
            api = getattr(pro, api_name)
            for code_index, code in enumerate(codes, 1):
                windows = (
                    [(announcement_start, end)]
                    if api_name in REPORT_APIS
                    else list(_indicator_windows(report_start, end))
                )
                for window_start, window_end in windows:
                    key = f"{code}:{window_start}:{window_end}"
                    checkpoint_dataset = f"financial_{api_name}_incremental"
                    if history.status(checkpoint_dataset, key, context) == "done":
                        continue
                    try:
                        frame = call_api(api, ts_code=code, start_date=window_start, end_date=window_end)
                        rows = 0 if frame is None else len(frame)
                        if rows >= 100 and api_name == "fina_indicator":
                            raise RuntimeError(f"fina_indicator reached 100-row limit: {code} {window_start}-{window_end}")
                        if dry_run:
                            logger.info("dry-run api=%s code=%s window=%s:%s rows=%s", api_name, code, window_start, window_end, rows)
                        elif rows == 0:
                            history.save(checkpoint_dataset, key, "done", 0, context, "API returned no rows")
                        else:
                            records = frame.astype(object).where(pd.notna(frame), None).to_dict("records")
                            inserted = financial.append_api_rows(api_name, records, [code], batch_id, context)
                            total += inserted
                            if inserted:
                                history.save("financial_derived_v2", code, "pending", inserted, context)
                            history.save(checkpoint_dataset, key, "done", inserted, context)
                        time.sleep(float(batch_pause))
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("financial incremental failed api=%s code=%s", api_name, code)
                        if not dry_run:
                            history.save(checkpoint_dataset, key, "failed", 0, context, str(exc)[:500])
                        raise
                if code_index % 100 == 0:
                    logger.info("financial incremental progress api=%s stocks=%s/%s rows=%s", api_name, code_index, len(codes), total)

        pending_codes = _pending_derived_codes(history, context) if not dry_run else []
        if rebuild_derived and not dry_run and pending_codes:
            try:
                _rebuild_financial_derived(pending_codes, end_dt)
                for code in pending_codes:
                    history.save("financial_derived_v2", code, "done", 0, context)
            except Exception as exc:  # noqa: BLE001
                for code in pending_codes:
                    history.save("financial_derived_v2", code, "failed", 0, context, str(exc)[:500])
                raise
        return f"financial incremental complete batch_id={batch_id} stocks={len(codes)} rows={total}"
    finally:
        kernel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tushare financial incremental sync")
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
