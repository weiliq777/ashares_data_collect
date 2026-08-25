"""Orchestrate fina_indicator version backfill and Standard projection."""
from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from datetime import datetime

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.fina_indicator_repository import FinaIndicatorRepository
from data_collect.jobs.tushare_job_lock import warehouse_write_job


def _date(value):
    if value in (None, ""):
        return None
    parsed = pd.to_datetime(value, format="%Y%m%d", errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _clean(value):
    if value is None:
        return None
    try:
        return None if pd.isna(value) else value
    except (TypeError, ValueError):
        return value


def _payload_and_hash(record: dict) -> tuple[str, str]:
    clean = {key: _clean(value) for key, value in record.items()}
    encoded = json.dumps(clean, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)
    return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _version_values(record: dict, batch_id: str, fetched_at=None):
    payload, payload_hash = _payload_and_hash(record)
    return (
        ":".join(str(record.get(key) or "") for key in ("ts_code", "end_date", "report_type", "comp_type", "end_type")),
        record.get("ts_code"), _date(record.get("ann_date")), _date(record.get("f_ann_date")), _date(record.get("end_date")),
        record.get("report_type"), record.get("comp_type"), record.get("end_type"), record.get("update_flag"),
        payload, "tushare", batch_id, payload_hash, fetched_at or datetime.now(),
    )


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"fina-indicator-{label}-{uuid.uuid4().hex}", job_name="tushare_fina_indicator_version",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, FinaIndicatorRepository(kernel), context


def save_indicator_versions(records: list[dict], batch_id: str | None = None) -> int:
    kernel, repository, context = _service("save")
    try:
        return repository.save_versions(records, batch_id or f"fina-indicator-{uuid.uuid4().hex}", context)
    finally:
        kernel.close()


def backfill_from_raw(batch_size: int = 2000) -> int:
    kernel, repository, context = _service("backfill")
    try:
        return repository.backfill_from_raw(batch_size, f"fina-indicator-backfill-{uuid.uuid4().hex}", context)
    finally:
        kernel.close()


def _rebuild_standard_unlocked(batch_size: int = 5000, stock_codes=None, report_start=None, report_end=None) -> int:
    kernel, repository, context = _service("standard")
    try:
        return repository.rebuild_standard(batch_size, context, stock_codes, report_start, report_end)
    finally:
        kernel.close()


@warehouse_write_job
def rebuild_standard(batch_size: int = 5000, stock_codes=None, report_start=None, report_end=None) -> int:
    return _rebuild_standard_unlocked(batch_size, stock_codes, report_start, report_end)


def _run_unlocked(backfill: bool = True, convert: bool = True, batch_size: int = 2000, stock_codes=None, report_start=None, report_end=None) -> dict:
    result = {}
    if backfill:
        result["version_backfill"] = backfill_from_raw(batch_size)
    if convert:
        result["standard_conversion"] = _rebuild_standard_unlocked(batch_size, stock_codes, report_start, report_end)
    return result


@warehouse_write_job
def run(backfill: bool = True, convert: bool = True, batch_size: int = 2000, stock_codes=None, report_start=None, report_end=None) -> dict:
    return _run_unlocked(backfill, convert, batch_size, stock_codes, report_start, report_end)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fina_indicator Raw version and Standard rebuild")
    parser.add_argument("--no-backfill", action="store_true")
    parser.add_argument("--no-convert", action="store_true")
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--codes", nargs="*")
    parser.add_argument("--report-start")
    parser.add_argument("--report-end")
    args = parser.parse_args()
    print(run(not args.no_backfill, not args.no_convert, args.batch_size, args.codes, args.report_start, args.report_end))
