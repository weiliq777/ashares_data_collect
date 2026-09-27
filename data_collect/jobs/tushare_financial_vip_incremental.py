"""Incremental financial VIP ingestion with atomic version/queue semantics."""
from __future__ import annotations

import argparse
import gc
import logging
import threading
import time
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
# One queue row normally maps to one stock. A consumer batch triggers a
# historical Standard + Feature rebuild, so this is also a hard bound on the
# amount of historical work one worker may start at once.
QUEUE_CLAIM_LIMIT = 100
QUEUE_LEASE_SECONDS = 4 * 60 * 60
QUEUE_HEARTBEAT_SECONDS = 5 * 60
# Individual SQL statements have a TransactionManager deadline. This second
# circuit breaker bounds the aggregate work held by one queue claim.
QUEUE_BATCH_DEADLINE_SECONDS = 30 * 60
# A financial queue batch is deliberately smaller than the API page size.
# This bounds the temporary pandas object frame used by Raw -> Standard.
DERIVATION_PAGE_SIZE = 500
DISCOVERY_FIELDS = "ts_code,ann_date,end_date,pre_date,actual_date"
REPORT_APIS = {
    "income_vip": ("income", "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,update_flag,revenue,total_revenue,oper_cost,total_cogs,operate_profit,total_profit,income_tax,n_income,n_income_attr_p,basic_eps,diluted_eps"),
    "balancesheet_vip": ("balancesheet", "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,update_flag,total_assets,total_liab,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,money_cap,accounts_receiv,inventories,goodwill,total_cur_assets,total_cur_liab,st_borr,lt_borr"),
    "cashflow_vip": ("cashflow", "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,update_flag,n_cashflow_act,n_cashflow_inv_act,n_cash_flows_fnc_act,c_fr_sale_sg,c_paid_goods_s,c_paid_for_taxes,c_pay_acq_const_fiolta,c_recp_cap_contrib,c_recp_borrow,c_pay_debt,n_incr_cash_cash_equ,c_cash_equ_beg_period,c_cash_equ_end_period,free_cashflow,net_profit"),
}
INDICATOR_API = "fina_indicator", "ts_code,ann_date,end_date,update_flag,roe,roe_yearly,q_roe,roa,debt_to_assets,grossprofit_margin,netprofit_margin,ocf_to_or,eps,bps,or_yoy,tr_yoy,q_sales_yoy,netprofit_yoy"


class CoverageBlocked(RuntimeError):
    """A successful response was not complete enough to publish."""


class _QueueLeaseHeartbeat:
    """Renew one queue claim from an independent Kernel connection."""

    def __init__(self, queue_ids, context, claim_owner_id):
        self._queue_ids = tuple(int(value) for value in queue_ids)
        self._context = context
        self._claim_owner_id = claim_owner_id
        self._stop = threading.Event()
        self._error = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"financial-queue-heartbeat-{claim_owner_id[-12:]}",
            daemon=True,
        )

    def start(self):
        self._thread.start()

    def _run(self):
        if self._stop.is_set():
            return
        heartbeat_kernel = DataAccessKernel(policy_registry=build_default_registry())
        heartbeat_repository = FinancialVipRepository(heartbeat_kernel)
        try:
            while not self._stop.wait(QUEUE_HEARTBEAT_SECONDS):
                try:
                    heartbeat_repository.renew_queue_lease(
                        list(self._queue_ids), self._context,
                        QUEUE_LEASE_SECONDS, self._claim_owner_id,
                    )
                except Exception as exc:  # surface to the owning worker
                    self._error = exc
                    self._stop.set()
                    return
        finally:
            heartbeat_kernel.close()

    def raise_if_failed(self):
        if self._error is not None:
            raise RuntimeError("financial queue lease heartbeat failed") from self._error

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=10)


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
        # A page checkpoint belongs to one fetch attempt, not forever to a
        # report period.  The same batch_id can resume after a failure; a new
        # scheduled run gets a new batch_id and re-requests recent periods so
        # later Tushare revisions cannot be hidden by an old DONE marker.
        key = f"{batch_id}:{period}:offset:{offset}"
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
            # Pagination depends on the provider row count, not on the number
            # of newly inserted hashes.  A full page of already-known rows can
            # legitimately have inserted=0 and must still advance the offset.
            _checkpoint(checkpoint_dataset, key, "DONE", raw_rows, repository=repository, context=context)
        if raw_rows < PAGE_SIZE:
            return
        offset += PAGE_SIZE


def _pending_queue_codes(
    repository=None, context=None, *, limit=QUEUE_CLAIM_LIMIT,
    claim_owner_id=None, takeover=False, through_date=None,
):
    kernel = None
    if repository is None or context is None:
        kernel, repository, context = _service(f"vip-queue-{uuid.uuid4().hex}")
    try:
        return repository.pending_queue(
            context, limit=limit, claim_owner_id=claim_owner_id, takeover=takeover,
            through_date=through_date,
        )
    finally:
        if kernel is not None:
            kernel.close()


def _legacy_pending_codes(repository=None, context=None, *, limit=QUEUE_CLAIM_LIMIT):
    kernel = None
    if repository is None or context is None:
        kernel, repository, context = _service(f"vip-legacy-{uuid.uuid4().hex}")
    try:
        return repository.pending_legacy_codes(context, limit=limit)
    finally:
        if kernel is not None:
            kernel.close()


def _consume_pending_changes(
    repository, context, output_end, *, claim_owner_id=None, takeover=False,
):
    """Drain financial changes in leased, bounded batches.

    A successful return means there is no pending, failed, running, blocked,
    or quarantined queue work left at the end of this consumer run.  Derived
    rebuilds happen outside the claim transaction; the lease is renewed at
    a short periodic heartbeat and completion is owner-checked by the Gateway.
    """
    from data_collect.jobs.tushare_fina_indicator_version import _rebuild_standard_unlocked as indicator_run
    from data_collect.jobs.tushare_foundation_features import rebuild_company_financial_v2
    from data_collect.jobs.tushare_foundation_pipeline import OUTPUT_START, WARMUP_START

    processed_batches = 0
    processed_queue_rows = 0
    processed_stocks = 0
    last_result = None
    claim_owner_id = context.run_id if claim_owner_id is None else claim_owner_id
    while True:
        queue_ids, queue_codes = _pending_queue_codes(
            repository, context, limit=QUEUE_CLAIM_LIMIT,
            claim_owner_id=claim_owner_id, takeover=takeover,
            through_date=output_end,
        )
        legacy_limit = max(0, QUEUE_CLAIM_LIMIT - len(queue_ids))
        legacy_codes = (
            _legacy_pending_codes(repository, context, limit=legacy_limit)
            if legacy_limit else []
        )
        if not queue_ids and not legacy_codes:
            break
        if queue_ids and not queue_codes:
            raise RuntimeError("financial queue claim returned ids without stock codes")
        changed_datasets = set(
            repository.claimed_queue_datasets(queue_ids, context, claim_owner_id)
            if queue_ids else ()
        )
        allowed_datasets = {"income", "balancesheet", "cashflow", "fina_indicator"}
        unknown_datasets = changed_datasets - allowed_datasets
        if unknown_datasets:
            raise RuntimeError(
                "financial queue contains unsupported datasets: "
                f"{sorted(unknown_datasets)}"
            )
        codes = sorted(set(queue_codes) | set(legacy_codes))
        if not codes:
            raise RuntimeError("financial change consumer found work without stock codes")
        if len(codes) > QUEUE_CLAIM_LIMIT:
            raise RuntimeError(
                "financial change consumer batch exceeds stock-code budget: "
                f"codes={len(codes)} limit={QUEUE_CLAIM_LIMIT}"
            )
        batch_started = time.monotonic()

        def check_batch_deadline(stage: str) -> None:
            elapsed = time.monotonic() - batch_started
            if elapsed > QUEUE_BATCH_DEADLINE_SECONDS:
                raise TimeoutError(
                    "financial change consumer batch exceeded deadline: "
                    f"stage={stage} elapsed={elapsed:.1f}s "
                    f"limit={QUEUE_BATCH_DEADLINE_SECONDS}s"
                )

        heartbeat = _QueueLeaseHeartbeat(queue_ids, context, claim_owner_id) if queue_ids else None
        try:
            if heartbeat:
                heartbeat.start()
            check_batch_deadline("before_standard")
            # Legacy checkpoints do not carry a typed dataset, so retain the
            # conservative all-dataset rebuild only for that compatibility
            # path. Typed queue rows rebuild the affected Standard projection.
            standard_datasets = (
                {"income", "balancesheet", "cashflow"}
                if legacy_codes else changed_datasets & {"income", "balancesheet", "cashflow"}
            )
            if standard_datasets:
                for dataset in sorted(standard_datasets):
                    standard_run(dataset, DERIVATION_PAGE_SIZE, codes, WARMUP_START, output_end)
                    gc.collect()
            if heartbeat:
                heartbeat.raise_if_failed()
            check_batch_deadline("after_standard")
            if legacy_codes or "fina_indicator" in changed_datasets:
                indicator_run(DERIVATION_PAGE_SIZE, codes, WARMUP_START, output_end)
                gc.collect()
            if heartbeat:
                heartbeat.raise_if_failed()
            check_batch_deadline("after_indicator")
            last_result = rebuild_company_financial_v2(
                None, WARMUP_START, OUTPUT_START, output_end,
                requested_codes=codes, target_table="feature.company_financial_v2",
                batch_stocks=5,
            )
            if heartbeat:
                heartbeat.raise_if_failed()
            check_batch_deadline("after_feature")
            if queue_ids:
                repository.finish_queue(
                    queue_ids, "DONE", None, context, claim_owner_id,
                    processed_through=output_end,
                )
            repository.finish_legacy(legacy_codes, "done", None, context)
        except Exception as exc:
            if queue_ids:
                repository.finish_queue(
                    queue_ids, "FAILED", str(exc)[:500], context, claim_owner_id,
                )
            repository.finish_legacy(legacy_codes, "failed", str(exc)[:500], context)
            raise
        finally:
            if heartbeat:
                heartbeat.stop()
        processed_batches += 1
        processed_queue_rows += len(queue_ids)
        processed_stocks += len(codes)

    queue_status = repository.queue_status_counts(context, output_end)
    unresolved = {
        status: count for status, count in queue_status.items()
        if status in {"PENDING", "FAILED", "RUNNING", "COVERAGE_BLOCKED", "QUARANTINED"}
        and count
    }
    if unresolved:
        raise RuntimeError(f"financial change queue is not drained: {unresolved}")
    return {
        "processed_batches": processed_batches,
        "processed_queue_rows": processed_queue_rows,
        "processed_stocks": processed_stocks,
        "queue_status": queue_status,
        "last_result": last_result,
    }


@warehouse_write_job
def run(end_date=None, run_date=None, lookback_days=30, report_periods_back=4,
        rebuild_derived=True, dry_run=False, batch_id=None, attempt=1):
    if end_date and run_date and str(end_date).replace("-", "") != str(run_date).replace("-", ""):
        raise ValueError("end_date and run_date must match")
    end = (end_date or run_date or date.today().strftime("%Y%m%d")).replace("-", "")
    end_dt = pd.to_datetime(end, format="%Y%m%d").date()
    batch_id = batch_id or f"financial-vip-{uuid.uuid4().hex}"
    if not str(batch_id).strip():
        raise ValueError("batch_id must not be empty")
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 1:
        raise ValueError("attempt must be a positive integer")
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
            claim_owner_id = f"{batch_id}:attempt:{attempt}:{uuid.uuid4().hex}"
            _consume_pending_changes(
                repository, context, end_dt,
                claim_owner_id=claim_owner_id, takeover=attempt > 1,
            )
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
    parser.add_argument("--batch-id")
    args = parser.parse_args()
    print(run(end_date=args.end_date, lookback_days=args.lookback_days, report_periods_back=args.report_periods_back, rebuild_derived=not args.no_rebuild_derived, dry_run=args.dry_run, batch_id=args.batch_id))
