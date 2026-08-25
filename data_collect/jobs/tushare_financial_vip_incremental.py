"""Incremental financial VIP ingestion with atomic version/queue semantics."""
from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date, timedelta

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_vip_repository import FinancialVipRepository
from data_collect.jobs.tushare_financial_raw_to_standard import _run_unlocked as standard_run
from data_collect.jobs.tushare_foundation_features import rebuild_company_financial_v2
from data_collect.jobs.tushare_job_lock import warehouse_write_job
from data_collect.providers.tushare_client import call_api, get_pro

logger = logging.getLogger("tushare_financial_vip_incremental")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

PAGE_SIZE = 2000
DISCOVERY_FIELDS = "ts_code,ann_date,end_date,pre_date,actual_date"
REPORT_APIS = {
    "income_vip": ("income", "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,update_flag,revenue,total_revenue,oper_cost,total_cogs,operate_profit,total_profit,income_tax,n_income,n_income_attr_p,basic_eps,diluted_eps"),
    "balancesheet_vip": ("balancesheet", "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,update_flag,total_assets,total_liab,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,money_cap,accounts_receiv,inventories,goodwill,total_cur_assets,total_cur_liab,st_borr,lt_borr"),
    "cashflow_vip": ("cashflow", "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,update_flag,n_cashflow_act,n_cashflow_inv_act,n_cash_flows_fnc_act,c_fr_sale_sg,c_paid_goods_s,c_paid_for_taxes,c_pay_acq_const_fiolta,c_recp_cap_contrib,c_recp_borrow,c_pay_debt,n_incr_cash_cash_equ,c_cash_equ_beg_period,c_cash_equ_end_period,free_cashflow,net_profit"),
}
INDICATOR_API = "fina_indicator", "ts_code,ann_date,end_date,update_flag,roe,roe_yearly,q_roe,roa,debt_to_assets,grossprofit_margin,netprofit_margin,ocf_to_or,eps,bps,or_yoy,tr_yoy,q_sales_yoy,netprofit_yoy"


class CoverageBlocked(RuntimeError):
    """A successful response was not complete enough to publish."""


def _service(token: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(token, "tushare-financial-vip", "unknown", "unknown", "v1")
    return kernel, FinancialVipRepository(kernel), context


def _as_yyyymmdd(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed.strftime("%Y%m%d")


def _clean_records(frame: pd.DataFrame) -> list[dict]:
    if frame is None or frame.empty:
        return []
    cleaned = frame.astype(object).where(pd.notna(frame), None)
    return [{key: value for key, value in row.items() if not str(key).startswith("_")} for row in cleaned.to_dict("records")]


def _recent_report_periods(end_dt: date, count: int) -> list[str]:
    if end_dt.month <= 3:
        cursor = date(end_dt.year - 1, 12, 31)
    elif end_dt.month <= 6:
        cursor = date(end_dt.year, 3, 31)
    elif end_dt.month <= 9:
        cursor = date(end_dt.year, 6, 30)
    else:
        cursor = date(end_dt.year, 9, 30)
    periods = []
    for _ in range(max(1, int(count))):
        periods.append(cursor.strftime("%Y%m%d"))
        if cursor.month == 12:
            cursor = date(cursor.year, 9, 30)
        elif cursor.month == 9:
            cursor = date(cursor.year, 6, 30)
        elif cursor.month == 6:
            cursor = date(cursor.year, 3, 31)
        else:
            cursor = date(cursor.year - 1, 12, 31)
    return periods


def _dates(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _checkpoint(dataset: str, key: str, status: str, rows: int = 0, error: str | None = None,
                repository: FinancialVipRepository | None = None, context: RunContext | None = None):
    kernel = None
    if repository is None or context is None:
        kernel, repository, context = _service(f"vip-checkpoint-{uuid.uuid4().hex}")
    try:
        return repository.save_checkpoint(dataset, key, status, rows, context, error)
    finally:
        if kernel is not None:
            kernel.close()


def _checkpoint_state(dataset: str, key: str, repository=None, context=None):
    kernel = None
    if repository is None or context is None:
        kernel, repository, context = _service(f"vip-state-{uuid.uuid4().hex}")
    try:
        return repository.checkpoint_status(dataset, key, context)
    finally:
        if kernel is not None:
            kernel.close()


def _record_anomaly(dataset: str, api_name: str, period: str | None, batch_id: str, reason: str,
                    handling: str, details: dict, repository=None, context=None):
    kernel = None
    if repository is None or context is None:
        kernel, repository, context = _service(f"vip-anomaly-{uuid.uuid4().hex}")
    try:
        return repository.record_anomaly(dataset, api_name, period, batch_id, reason, handling, details, context)
    finally:
        if kernel is not None:
            kernel.close()


def _validate_and_prepare(frame, api_name, dataset, period, batch_id, *, record_anomalies=True):
    if frame is None or frame.empty:
        return []
    frame = frame.copy()
    if "end_date" not in frame.columns:
        if record_anomalies:
            _record_anomaly(dataset, api_name, period, batch_id, "MISSING_END_DATE", "QUARANTINE", {"columns": list(frame.columns)})
        return []
    frame["_normalized_end_date"] = frame["end_date"].map(_as_yyyymmdd)
    outside = frame[frame["_normalized_end_date"] != period]
    if not outside.empty and record_anomalies:
        _record_anomaly(dataset, api_name, period, batch_id, "PERIOD_MISMATCH", "WARNING", {"rows": len(outside), "expected": period})
    frame = frame[frame["_normalized_end_date"] == period].drop(columns=["_normalized_end_date"])
    if frame.empty:
        return []
    required = {"ts_code", "ann_date", "end_date", "update_flag"}
    if dataset != "fina_indicator":
        required.update({"report_type", "comp_type", "end_type"})
    missing = sorted(required - set(frame.columns))
    if missing:
        if record_anomalies:
            _record_anomaly(dataset, api_name, period, batch_id, "MISSING_IDENTITY_COLUMNS", "QUARANTINE", {"columns": missing})
        return []
    frame = frame[frame["update_flag"].astype(str) == "1"].copy()
    identity = ["ts_code", "ann_date", "end_date"] + ([] if dataset == "fina_indicator" else ["report_type", "comp_type", "end_type"])
    invalid = frame[identity].isna().any(axis=1)
    if invalid.any():
        if record_anomalies:
            _record_anomaly(dataset, api_name, period, batch_id, "MISSING_IDENTITY_VALUE", "QUARANTINE", {"rows": int(invalid.sum()), "fields": identity})
        frame = frame[~invalid].copy()
    if dataset == "fina_indicator":
        counts = frame.groupby(["ts_code", "end_date", "ann_date"], dropna=False).size()
        if (counts > 1).any():
            if record_anomalies:
                _record_anomaly(dataset, api_name, period, batch_id, "INDICATOR_IDENTITY_AMBIGUOUS", "QUARANTINE", {"duplicate_groups": int((counts > 1).sum())})
            duplicate_keys = set(counts[counts > 1].index)
            frame = frame[~frame.apply(lambda row: (row["ts_code"], row["end_date"], row["ann_date"]) in duplicate_keys, axis=1)].copy()
    return _clean_records(frame)


def _write_versions_and_queue(dataset, records, batch_id, repository=None, context=None):
    kernel = None
    if repository is None or context is None:
        kernel, repository, context = _service(f"vip-write-{batch_id}")
    try:
        return repository.write_versions_and_queue(dataset, records, batch_id, context)
    finally:
        if kernel is not None:
            kernel.close()


def discover_report_periods(pro, end_dt, lookback_days, report_periods_back, batch_id, *, record_anomalies=True, repository=None, context=None):
    periods = set(_recent_report_periods(end_dt, report_periods_back))
    start_dt = end_dt - timedelta(days=max(lookback_days - 1, 0))
    for current in _dates(start_dt, end_dt):
        frame = call_api(pro.disclosure_date, ann_date=current.strftime("%Y%m%d"), fields=DISCOVERY_FIELDS)
        if frame is None or frame.empty:
            continue
        if len(frame) >= 6000 and record_anomalies:
            _record_anomaly("disclosure_date", "disclosure_date", None, batch_id, "DISCOVERY_COVERAGE_LIMIT", "WARNING", {"rows": len(frame), "ann_date": current.strftime("%Y%m%d")}, repository, context)
        periods.update(value for value in frame.get("end_date", pd.Series(dtype=object)).map(_as_yyyymmdd).dropna() if value <= end_dt.strftime("%Y%m%d"))
    return sorted(periods)


def _period_pages(pro, api_name, dataset, fields, period, batch_id, dry_run, repository=None, context=None):
    checkpoint_dataset = f"financial_vip_{dataset}"
    offset = 0
    while True:
        key = f"{period}:offset:{offset}"
        state = None if dry_run else _checkpoint_state(checkpoint_dataset, key, repository, context)
        if state and state[0] in {"DONE", "EMPTY_CONFIRMED"}:
            if int(state[1] or 0) < PAGE_SIZE:
                return
            offset += PAGE_SIZE
            continue
        frame = call_api(getattr(pro, api_name), period=period, fields=fields, limit=PAGE_SIZE, offset=offset)
        raw_rows = 0 if frame is None else len(frame)
        if raw_rows == 0:
            if offset == 0:
                if not dry_run:
                    _checkpoint(checkpoint_dataset, key, "COVERAGE_BLOCKED", 0, "VIP period returned empty", repository, context)
                    _record_anomaly(dataset, api_name, period, batch_id, "EMPTY_VIP_PERIOD", "QUARANTINE", {"offset": offset}, repository, context)
                raise CoverageBlocked(f"{api_name} period={period} returned empty")
            if not dry_run:
                _checkpoint(checkpoint_dataset, key, "EMPTY_CONFIRMED", 0, repository=repository, context=context)
            return
        records = _validate_and_prepare(frame, api_name, dataset, period, batch_id, record_anomalies=not dry_run)
        if not records:
            if not dry_run:
                _checkpoint(checkpoint_dataset, key, "COVERAGE_BLOCKED", raw_rows, "No safe records after identity validation", repository, context)
            raise CoverageBlocked(f"{api_name} period={period} has no safe records")
        inserted = 0 if dry_run else _write_versions_and_queue(dataset, records, batch_id, repository, context)
        if not dry_run:
            _checkpoint(checkpoint_dataset, key, "DONE", inserted, repository=repository, context=context)
        if raw_rows < PAGE_SIZE:
            return
        offset += PAGE_SIZE


def _pending_queue_codes(repository=None, context=None):
    kernel = None
    if repository is None or context is None:
        kernel, repository, context = _service(f"vip-queue-{uuid.uuid4().hex}")
    try:
        return repository.pending_queue(context)
    finally:
        if kernel is not None:
            kernel.close()


def _legacy_pending_codes(repository=None, context=None):
    kernel = None
    if repository is None or context is None:
        kernel, repository, context = _service(f"vip-legacy-{uuid.uuid4().hex}")
    try:
        return repository.pending_legacy_codes(context)
    finally:
        if kernel is not None:
            kernel.close()


@warehouse_write_job
def run(end_date=None, run_date=None, lookback_days=30, report_periods_back=4, rebuild_derived=True, dry_run=False):
    if end_date and run_date and str(end_date).replace("-", "") != str(run_date).replace("-", ""):
        raise ValueError("end_date and run_date must match")
    end = (end_date or run_date or date.today().strftime("%Y%m%d")).replace("-", "")
    end_dt = pd.to_datetime(end, format="%Y%m%d").date()
    batch_id = f"financial-vip-{uuid.uuid4().hex}"
    kernel, repository, context = _service(batch_id)
    try:
        pro = get_pro()
        periods = discover_report_periods(pro, end_dt, lookback_days, report_periods_back, batch_id, record_anomalies=not dry_run, repository=repository, context=context)
        blocked = []
        for period in periods:
            for api_name, (dataset, fields) in [*REPORT_APIS.items(), ("fina_indicator_vip", INDICATOR_API)]:
                try:
                    _period_pages(pro, api_name, dataset, fields, period, batch_id, dry_run, repository, context)
                except CoverageBlocked as exc:
                    blocked.append(str(exc))
        if blocked:
            raise CoverageBlocked("; ".join(blocked[:10]))
        if not dry_run and rebuild_derived:
            queue_ids, queue_codes = _pending_queue_codes(repository, context)
            legacy_codes = _legacy_pending_codes(repository, context)
            codes = sorted(set(queue_codes) | set(legacy_codes))
            if codes:
                try:
                    from data_collect.jobs.tushare_foundation_pipeline import OUTPUT_START, WARMUP_START
                    standard_run("all", 2000, codes, WARMUP_START, end_dt)
                    from data_collect.jobs.tushare_fina_indicator_version import _rebuild_standard_unlocked as rebuild_indicator
                    rebuild_indicator(2000, codes, WARMUP_START, end_dt)
                    rebuild_company_financial_v2(None, WARMUP_START, OUTPUT_START, end_dt, requested_codes=codes, target_table="feature.company_financial_v2", batch_stocks=5)
                    repository.finish_queue(queue_ids, "DONE", None, context)
                    repository.finish_legacy(legacy_codes, "done", None, context)
                except Exception as exc:
                    repository.finish_queue(queue_ids, "FAILED", str(exc)[:500], context)
                    repository.finish_legacy(legacy_codes, "failed", str(exc)[:500], context)
                    raise
        return f"financial VIP incremental completed batch_id={batch_id} periods={len(periods)}"
    finally:
        kernel.close()


def repair_orphan_versions(batch_size=2000):
    batch_id = f"financial-vip-repair-{uuid.uuid4().hex}"
    kernel, repository, context = _service(batch_id)
    try:
        return repository.repair_orphan_versions(batch_size, context)
    finally:
        kernel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tushare VIP financial incremental ingestion")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--report-periods-back", type=int, default=4)
    parser.add_argument("--no-rebuild-derived", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(run(end_date=args.end_date, lookback_days=args.lookback_days, report_periods_back=args.report_periods_back, rebuild_derived=not args.no_rebuild_derived, dry_run=args.dry_run))
