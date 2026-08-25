"""Tushare release orchestration; persistence belongs to ReleaseRepository."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.context import WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.release_repository import ReleaseRepository


_LEGACY_ONLY_INVARIANTS = frozenset({"financial_feature_missing_revenue_mapping"})


def _invariant_blocks_release(name: str, *, include_foundation_v2: bool) -> bool:
    return not (include_foundation_v2 and name in _LEGACY_ONLY_INVARIANTS)


def _json_safe(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"release-{label}-{uuid4().hex}", job_name="tushare_release",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, ReleaseRepository(kernel), context


def _source_manifest(cutoff: date):
    kernel, repository, context = _service("manifest")
    try:
        return repository.source_manifest(cutoff, context)
    finally:
        kernel.close()


def _quality_gate(cutoff: date | None = None, *, include_foundation_v2: bool = False) -> dict:
    kernel, repository, context = _service("quality")
    try:
        return _json_safe(repository.quality_gate(cutoff, include_foundation_v2, context))
    finally:
        kernel.close()


def _locked_quality_and_source_manifest(cutoff: date, *, include_foundation_v2: bool = False):
    kernel, repository, context = _service("locked-quality")
    try:
        with kernel.lock_manager.session_exclusive("tushare-release", "validate"):
            gate = repository.quality_gate(cutoff, include_foundation_v2, context)
            manifest, digest = repository.source_manifest(cutoff, context)
            return _json_safe(gate), manifest, digest
    finally:
        kernel.close()


def create(tag: str, cutoff: str, notes: str = "", schema_version: str = "", data_cutoff_timestamp: str | None = None) -> None:
    kernel, repository, context = _service("create")
    try:
        observed_at = datetime.fromisoformat(data_cutoff_timestamp) if data_cutoff_timestamp else None
        repository.create(tag, datetime.strptime(cutoff, "%Y%m%d").date(), notes, schema_version, context, data_cutoff_timestamp=observed_at)
    finally:
        kernel.close()


def validate(tag: str, quality_report_hash: str = "") -> None:
    kernel, repository, context = _service("validate")
    try:
        result = repository.validate(tag, quality_report_hash, context)
        changed = repository.commit_validation(result, context)
        if changed != 1:
            raise RuntimeError(f"release state changed concurrently: {tag}")
    finally:
        kernel.close()


def status() -> None:
    kernel, repository, context = _service("status")
    try:
        for row in repository.status_rows(context):
            print(" | ".join("" if value is None else str(value) for value in row.values()))
    finally:
        kernel.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage Tushare release baselines")
    sub = parser.add_subparsers(dest="command", required=True)
    create_parser = sub.add_parser("create")
    create_parser.add_argument("--tag", required=True)
    create_parser.add_argument("--cutoff", required=True)
    create_parser.add_argument("--notes", default="")
    create_parser.add_argument("--schema-version", default="")
    create_parser.add_argument("--data-cutoff-timestamp", default=None, help="ISO-8601 timezone-aware warehouse visibility cutoff")
    validate_parser = sub.add_parser("validate")
    validate_parser.add_argument("--tag", required=True)
    validate_parser.add_argument("--quality-report-hash", default="")
    sub.add_parser("status")
    args = parser.parse_args()
    if args.command == "create":
        create(args.tag, args.cutoff, args.notes, args.schema_version, args.data_cutoff_timestamp)
    elif args.command == "validate":
        validate(args.tag, args.quality_report_hash)
    else:
        status()


if __name__ == "__main__":
    main()
