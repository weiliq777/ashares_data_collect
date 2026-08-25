"""Backfill the immutable financial Raw version layer through Repository APIs."""
from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from datetime import datetime

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.jobs.tushare_financial_raw_to_standard import CONFIG


def _date(value):
    parsed = pd.to_datetime(value, format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _payload_values(data: dict, dataset: str, batch_id: str, fetched_at=None):
    encoded = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    business_key = ":".join(str(data.get(key) or "") for key in ("ts_code", "end_date", "report_type", "comp_type", "end_type"))
    return (dataset, business_key, data.get("ts_code"), _date(data.get("ann_date")), _date(data.get("f_ann_date")), _date(data.get("end_date")), data.get("report_type"), data.get("comp_type"), data.get("end_type"), data.get("update_flag"), encoded, "tushare", batch_id, digest, fetched_at or datetime.now())


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"financial-version-backfill-{label}-{uuid.uuid4().hex}", job_name="tushare_financial_raw_version_backfill",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, FinancialRepository(kernel), context


def save_financial_versions(dataset: str, records: list[dict], batch_id: str | None = None) -> int:
    if dataset not in {"income", "balancesheet", "cashflow"} or not records:
        return 0
    kernel, repository, context = _service("save")
    try:
        return repository.save_financial_versions(dataset, records, batch_id or f"financial-{dataset}-{uuid.uuid4().hex}", context)
    finally:
        kernel.close()


def run(dataset="all", batch_size=1000):
    if batch_size <= 0 or batch_size > 5000:
        raise ValueError("batch_size must be in 1..5000")
    selected = CONFIG if dataset == "all" else {dataset: CONFIG[dataset]}
    batch_id = f"historical-backfill-{uuid.uuid4().hex}"
    result = {}
    for name, (source, _, _) in selected.items():
        kernel, repository, context = _service(f"{name}")
        try:
            result[name] = repository.backfill_financial_versions(name, source, batch_size, batch_id, context)
        finally:
            kernel.close()
    return {"batch_id": batch_id, "inserted": result}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill immutable financial versions")
    parser.add_argument("--dataset", choices=["income", "balancesheet", "cashflow", "all"], default="all")
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    print(run(args.dataset, args.batch_size))
