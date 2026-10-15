"""Create a new Backtest Dataset from an immutable date-based replacement map.

The source Dataset and every Research Snapshot remain immutable.  This job is
used when a repaired Research Snapshot series replaces selected dates in an
existing frozen Dataset, for example after a Foundation contract repair.
Selection and identity checks stay inside the Repository -> Kernel boundary;
the JSON file is only an audit instruction and never a source of trust by
itself.
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.jobs.backtest_dataset import create_dataset
from data_collect.jobs.tushare_job_lock import warehouse_write_job


REPLACEMENT_VERSION = "foundation-financial-replacement-v1"


def _iso_day(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)).isoformat()


def select_replacement_snapshot_ids(
    source_contracts: Sequence[dict[str, Any]],
    replacement_rows: Sequence[dict[str, Any]],
) -> list[str]:
    """Return one immutable snapshot ID per source Dataset date.

    This function deliberately validates the old identity before replacing it
    and rejects duplicate/missing dates.  It is pure so the mapping contract
    can be tested without touching PostgreSQL.
    """
    source_by_date: dict[str, dict[str, Any]] = {}
    for row in source_contracts:
        day = _iso_day(row["research_date"])
        if day in source_by_date:
            raise ValueError(f"source Dataset has duplicate research date: {day}")
        source_by_date[day] = row
    if not source_by_date:
        raise ValueError("source Dataset has no Research Snapshot members")

    replacements_by_date: dict[str, dict[str, Any]] = {}
    for row in replacement_rows:
        day = _iso_day(row["research_date"])
        if day in replacements_by_date:
            raise ValueError(f"replacement manifest has duplicate research date: {day}")
        old_id = str(row.get("old_snapshot_id") or "")
        new_id = str(row.get("new_snapshot_id") or "")
        if not old_id or not new_id or old_id == new_id:
            raise ValueError(f"replacement identity is invalid: {day}")
        source = source_by_date.get(day)
        if source is None:
            raise ValueError(f"replacement date is not a source Dataset member: {day}")
        if str(source["research_snapshot_id"]) != old_id:
            raise ValueError(
                f"replacement old snapshot mismatch for {day}: "
                f"manifest={old_id}, source={source['research_snapshot_id']}"
            )
        replacements_by_date[day] = row

    return [
        str(replacements_by_date.get(day, {}).get("new_snapshot_id")
            or source_by_date[day]["research_snapshot_id"])
        for day in sorted(source_by_date)
    ]


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"backtest-dataset-replacement-{label}-{uuid4().hex}",
        job_name="backtest_dataset_replacement",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    return kernel, DatasetRepository(kernel), context


def _load_replacement_file(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("repair_version") != REPLACEMENT_VERSION:
        raise ValueError(
            f"unsupported replacement version: {payload.get('repair_version')!r}"
        )
    if payload.get("status") != "FROZEN":
        raise ValueError("replacement snapshots must be FROZEN before Dataset creation")
    rows = list(payload.get("replacements") or [])
    if not rows or int(payload.get("replacement_count") or 0) != len(rows):
        raise ValueError("replacement manifest count is missing or inconsistent")
    if payload.get("replacement_schema_version") != "research-v2.1":
        raise ValueError("replacement manifest is not the Foundation v2.1 repair")
    return payload


def create_replacement_dataset(
    source_dataset_id: str,
    replacement_manifest_path: str | Path,
    *,
    dataset_name: str,
    dataset_version: str,
    dataset_id: str | None = None,
) -> str:
    """Create a DRAFT Dataset using verified old/new snapshot identities."""
    replacement = _load_replacement_file(replacement_manifest_path)
    kernel, repository, context = _service("select")
    try:
        source = repository.manifest(source_dataset_id, context)
        if source["status"] != "FROZEN":
            raise RuntimeError(
                f"source Dataset must remain FROZEN: {source['status']}"
            )
        source_contracts = repository.research_snapshot_contracts(
            source_dataset_id, context
        )
        snapshot_ids = select_replacement_snapshot_ids(
            source_contracts, replacement["replacements"]
        )
        new_ids = [
            str(row["new_snapshot_id"])
            for row in replacement["replacements"]
        ]
        target_rows = repository.snapshot_manifests(new_ids, context)
        target_by_id = {str(row["snapshot_id"]): row for row in target_rows}
        if len(target_by_id) != len(new_ids):
            missing = [item for item in new_ids if item not in target_by_id]
            raise RuntimeError(f"replacement Snapshot manifest missing: {missing[:5]}")
        for item in replacement["replacements"]:
            target = target_by_id[str(item["new_snapshot_id"])]
            if target["status"] != "FROZEN":
                raise RuntimeError(
                    f"replacement Snapshot is not FROZEN: {item['research_date']}"
                )
            if target["data_baseline_tag"] != source["baseline"]:
                raise RuntimeError(
                    f"replacement baseline mismatch: {item['research_date']}"
                )
            if target["schema_version"] != "research-v2.1":
                raise RuntimeError(
                    f"replacement schema mismatch: {item['research_date']}"
                )
    finally:
        kernel.close()

    # create_dataset performs the full immutable baseline, calendar, PIT,
    # Foundation financial contract, and member identity gates.
    created_id = create_dataset(
        snapshot_ids=snapshot_ids,
        dataset_name=dataset_name,
        dataset_version=dataset_version,
        baseline_tag=source["baseline"],
        warmup_start=source["warmup_start"],
        evaluation_start=source["evaluation_start"],
        evaluation_end=source["evaluation_end"],
        out_of_sample_start=source["out_of_sample_start"],
        calendar_rule_version=source["calendar_rule_version"],
        market_rule_version=source["market_rule_version"],
        fee_rule_version=source["fee_rule_version"],
        benchmark_code=source["benchmark_code"],
        dataset_id=dataset_id,
    )
    # A validated baseline may have a small, explicit late-arriving benchmark
    # repair.  Bind it before the batched builder starts; otherwise the new
    # Dataset would silently lose the same benchmark coverage correction that
    # was already bound to the source Dataset.
    kernel, repository, context = _service("benchmark-repair")
    try:
        with repository.lifecycle_lock(created_id):
            changed = repository.transition_manifest(
                context,
                created_id,
                ("DRAFT",),
                "BUILDING",
                {"failure_reason": None, "failed_at": None},
            )
            if changed != 1:
                raise RuntimeError(
                    "new replacement Dataset lifecycle changed before benchmark repair: "
                    f"{created_id}"
                )
            repository.bind_benchmark_source_repair(created_id, context)
    finally:
        kernel.close()
    return created_id


@warehouse_write_job
def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Create a Dataset with date-scoped immutable Snapshot replacements"
    )
    parser.add_argument("--source-dataset-id", required=True)
    parser.add_argument("--replacement-manifest", required=True)
    parser.add_argument("--dataset-name", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--dataset-id")
    args = parser.parse_args(argv)
    dataset_id = create_replacement_dataset(
        args.source_dataset_id,
        args.replacement_manifest,
        dataset_name=args.dataset_name,
        dataset_version=args.dataset_version,
        dataset_id=args.dataset_id,
    )
    print(f"DATASET_ID={dataset_id}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
