"""Compare a small Tushare sample with the immutable Raw payloads.

This is an audit-only command.  It never writes the database and never calls
the application database connection directly; database reads go through
SourceAuditRepository -> DataAccessKernel.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_collect.data_access import DataAccessKernel
from data_collect.data_access.context import RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.source_audit_repository import SourceAuditRepository
from data_collect.providers.tushare_client import call_api, get_pro


DATASET_FIELDS = {
    "daily": (
        "ts_code", "trade_date", "open", "high", "low", "close",
        "pre_close", "change", "pct_chg", "vol", "amount",
    ),
    "daily_basic": (
        "ts_code", "trade_date", "turnover_rate", "turnover_rate_f",
        "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm",
        "dv_ratio", "dv_ttm", "total_share", "float_share", "free_share",
        "total_mv", "circ_mv",
    ),
    "adj_factor": ("ts_code", "trade_date", "adj_factor"),
}


def _missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _canonical(field: str, value: Any) -> Any:
    if _missing(value) or value == "":
        return None
    if field == "trade_date":
        if isinstance(value, (date, datetime)):
            return value.strftime("%Y%m%d")
        return str(value).replace("-", "")[:8]
    if field == "ts_code":
        return str(value)
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            return None
        return number
    except (InvalidOperation, ValueError, TypeError):
        return str(value)


def _equal(field: str, left: Any, right: Any) -> bool:
    left = _canonical(field, left)
    right = _canonical(field, right)
    if left is None or right is None:
        return left is right
    if isinstance(left, Decimal) and isinstance(right, Decimal):
        return math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-9)
    return left == right


def _api_row(pro, dataset: str, code: str, trade_date: date) -> dict[str, Any] | None:
    frame = call_api(
        getattr(pro, dataset),
        ts_code=code,
        trade_date=trade_date.strftime("%Y%m%d"),
    )
    if frame is None or frame.empty:
        return None
    records = frame.to_dict("records")
    for record in records:
        if str(record.get("ts_code")) == code:
            return record
    return records[0] if records else None


def audit_sample(
    trade_date: date,
    sample_size: int = 10,
    datasets: tuple[str, ...] = ("daily", "daily_basic", "adj_factor"),
    seed: str = "default",
    requested_codes: list[str] | None = None,
) -> dict[str, Any]:
    if not 1 <= sample_size <= 100:
        raise ValueError("sample_size must be between 1 and 100")
    if not datasets or any(dataset not in DATASET_FIELDS for dataset in datasets):
        raise ValueError("unsupported audit dataset")

    context = RunContext(
        run_id=f"tushare-source-audit-{trade_date.isoformat()}-{seed}",
        job_name="audit_tushare_sample",
        code_version="source-audit-v1",
        config_hash=seed,
        pipeline_version="data-access-v1",
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        repository = SourceAuditRepository(kernel)
        codes = repository.sample_codes(
            trade_date, sample_size, seed, context, requested_codes
        )
        pro = get_pro()
        report: dict[str, Any] = {
            "audit_version": "source-audit-v1",
            "trade_date": trade_date.isoformat(),
            "requested_sample_size": sample_size,
            "sample_size": len(codes),
            "seed": seed,
            "codes": codes,
            "datasets": {},
        }
        for dataset in datasets:
            raw_rows = repository.latest_raw_payloads(dataset, trade_date, codes, context)
            dataset_report = {"matched": 0, "different": 0, "missing_db": 0, "missing_api": 0, "rows": []}
            for code in codes:
                db_record = raw_rows.get(code)
                db_payload = db_record["payload"] if db_record else None
                try:
                    api_payload = _api_row(pro, dataset, code, trade_date)
                    api_error = None
                except Exception as exc:  # noqa: BLE001
                    api_payload = None
                    api_error = str(exc)
                if db_payload is None:
                    dataset_report["missing_db"] += 1
                    status = "MISSING_DB"
                elif api_error:
                    status = "API_ERROR"
                elif api_payload is None:
                    dataset_report["missing_api"] += 1
                    status = "MISSING_API"
                else:
                    differences = {}
                    for field in DATASET_FIELDS[dataset]:
                        db_value = db_payload.get(field)
                        api_value = api_payload.get(field)
                        if not _equal(field, db_value, api_value):
                            differences[field] = {"db": db_value, "api": api_value}
                    if differences:
                        dataset_report["different"] += 1
                        status = "DIFFERENT"
                    else:
                        dataset_report["matched"] += 1
                        status = "MATCH"
                row = {"ts_code": code, "status": status}
                if api_error:
                    row["error"] = api_error[:500]
                if status == "DIFFERENT":
                    row["differences"] = differences
                dataset_report["rows"].append(row)
            report["datasets"][dataset] = dataset_report
        report["summary"] = {
            "matched": sum(item["matched"] for item in report["datasets"].values()),
            "different": sum(item["different"] for item in report["datasets"].values()),
            "missing_db": sum(item["missing_db"] for item in report["datasets"].values()),
            "missing_api": sum(item["missing_api"] for item in report["datasets"].values()),
            "api_errors": sum(
                sum(row.get("status") == "API_ERROR" for row in item["rows"])
                for item in report["datasets"].values()
            ),
        }
        return report
    finally:
        kernel.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare a bounded Tushare sample with database Raw payloads")
    parser.add_argument("--trade-date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--dataset", choices=["all", *DATASET_FIELDS], default="all")
    parser.add_argument("--seed", default="default")
    parser.add_argument("--codes", nargs="*", help="optional explicit ts_code list")
    parser.add_argument("--output", type=Path, help="optional JSON report path")
    args = parser.parse_args()
    parsed = datetime.strptime(args.trade_date.replace("-", ""), "%Y%m%d").date()
    datasets = tuple(DATASET_FIELDS) if args.dataset == "all" else (args.dataset,)
    report = audit_sample(parsed, args.sample_size, datasets, args.seed, args.codes)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return 0 if report["summary"]["different"] == 0 and report["summary"]["api_errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
