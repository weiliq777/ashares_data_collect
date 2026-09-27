"""Create immutable Foundation-v2.1 replacements for invalid Snapshot dates.

This job never updates or deletes an existing Research Snapshot/Dataset.  It
consumes the read-only audit JSON, verifies that the listed snapshot IDs still
match the frozen Dataset, and then delegates creation/build/validation/freeze
to the existing Research Snapshot series workflow.
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.data_access.repository.research_snapshot_repository import (
    ResearchSnapshotRepository,
)
from data_collect.data_access.research_contract import (
    STRATEGY_NEUTRAL_SCHEMA_VERSION,
    is_foundation_schema,
)
from data_collect.jobs.research_snapshot import (
    _baseline,
    freeze_snapshots_batch,
    validate_snapshots_batch,
)
from data_collect.jobs.research_snapshot_series import create_snapshot_series


AUDIT_VERSION = "foundation-financial-contract-v1"


def _read_dataset_contract(dataset_id: str) -> tuple[dict, list[dict]]:
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"repair-foundation-snapshot-read-{uuid4().hex}",
        job_name="repair_foundation_snapshot_financial",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    try:
        repository = DatasetRepository(kernel)
        return (
            repository.manifest(dataset_id, context),
            repository.research_snapshot_contracts(dataset_id, context),
        )
    finally:
        kernel.close()


def _load_issue_dates(audit_path: str | Path, dataset_id: str) -> list[dict]:
    payload = json.loads(Path(audit_path).read_text(encoding="utf-8"))
    if payload.get("audit_version") != AUDIT_VERSION:
        raise ValueError(
            "unsupported audit version: "
            f"{payload.get('audit_version')!r}; expected {AUDIT_VERSION!r}"
        )
    if str(payload.get("dataset_id")) != str(dataset_id):
        raise ValueError("audit dataset_id does not match the requested Dataset")
    if payload.get("status") != "FAIL":
        raise ValueError("repair requires a failing audit result")
    rows = list(payload.get("issue_rows") or [])
    if not rows:
        raise ValueError("audit has no issue rows to repair")
    rows.sort(key=lambda row: (str(row.get("research_date")), str(row.get("snapshot_id"))))
    dates = [row.get("research_date") for row in rows]
    if any(not value for value in dates) or len(set(dates)) != len(dates):
        raise ValueError("audit issue dates must be non-empty and unique")
    if any(int(row.get("non_v2_source_rows") or 0) <= 0 for row in rows):
        raise ValueError("audit contains an issue without a non-V2 source row")
    return rows


def prepare_repair(dataset_id: str, audit_path: str | Path) -> dict:
    manifest, members = _read_dataset_contract(dataset_id)
    if manifest["status"] != "FROZEN":
        raise RuntimeError(
            f"source Dataset must remain FROZEN during repair: {manifest['status']}"
        )
    member_by_date = {
        row["research_date"].isoformat(): row for row in members
    }
    issue_rows = _load_issue_dates(audit_path, dataset_id)
    missing = [row["research_date"] for row in issue_rows if row["research_date"] not in member_by_date]
    if missing:
        raise RuntimeError(f"audit dates are not Dataset members: {missing[:5]}")
    for row in issue_rows:
        member = member_by_date[row["research_date"]]
        if str(member["research_snapshot_id"]) != str(row["snapshot_id"]):
            raise RuntimeError(
                "audit snapshot identity changed for "
                f"{row['research_date']}: {row['snapshot_id']} != "
                f"{member['research_snapshot_id']}"
            )
        if member["snapshot_status"] != "FROZEN":
            raise RuntimeError(
                f"source snapshot is no longer FROZEN: {row['research_date']}"
            )
        if not is_foundation_schema(str(member["research_schema_version"])):
            raise RuntimeError(
                f"source snapshot schema is not Foundation: {row['research_date']}"
            )
    return {
        "dataset_id": str(dataset_id),
        "source_dataset_status": manifest["status"],
        "baseline_tag": manifest["baseline"],
        "source_schema_versions": sorted({
            str(row["research_schema_version"]) for row in members
        }),
        "target_schema_version": STRATEGY_NEUTRAL_SCHEMA_VERSION,
        "issue_count": len(issue_rows),
        "issue_rows": issue_rows,
    }


def repair_dataset(
    dataset_id: str,
    audit_path: str | Path,
    *,
    batch_size: int = 10,
    freeze: bool = True,
    code_archive_path: str | None = None,
    output_path: str | Path | None = None,
) -> dict:
    plan = prepare_repair(dataset_id, audit_path)
    target_schema = plan["target_schema_version"]
    if target_schema != "research-v2.1":
        raise RuntimeError(f"unexpected replacement schema: {target_schema}")
    issue_rows = plan["issue_rows"]
    dates = [date.fromisoformat(row["research_date"]) for row in issue_rows]
    print(
        f"creating immutable Foundation replacement manifests: "
        f"{len(dates)} dates, schema={target_schema}",
        flush=True,
    )
    # Manifest creation is cheap and resumable.  The actual materialization
    # uses the specialized clone/enrich path below; the generic builder would
    # rescan every input group for every date.
    created = create_snapshot_series(
        dates,
        plan["baseline_tag"],
        schema_version=target_schema,
        notes_prefix=(
            f"versioned replacement for Dataset {dataset_id}; "
            "source audit foundation-financial-contract-v1"
        ),
        build=False,
        validate=False,
        freeze=False,
        code_archive_path=None,
        max_snapshots=None,
        batch_size=batch_size,
        verify_source_manifest=False,
        check_building_completeness=False,
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"repair-foundation-snapshot-build-{uuid4().hex}",
        job_name="repair_foundation_snapshot_financial",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    repository = ResearchSnapshotRepository(kernel)
    replacements_by_date = {
        item["research_cutoff_date"]: item["snapshot_id"] for item in created
    }
    source_by_date = {row["research_date"]: row["snapshot_id"] for row in issue_rows}
    statuses = {}
    quality_reports: dict[str, tuple[dict, str]] = {}
    try:
        target_ids = [item["snapshot_id"] for item in created]
        target_manifests = repository.snapshot_manifests(target_ids, context)
        if len(target_manifests) != len(target_ids):
            missing = [item for item in target_ids if item not in target_manifests]
            raise RuntimeError(
                f"replacement Snapshot manifest missing: {missing[:5]}"
            )

        # Check all existing targets and immutable sources in bounded batches.
        # The old implementation performed three reads per target, which made
        # a restart spend hours in preflight before any useful work resumed.
        integrity_pairs = []
        for row in issue_rows:
            target_id = replacements_by_date[row["research_date"]]
            target_manifest = target_manifests[target_id]
            if target_manifest["status"] in {"BUILDING", "FAILED"}:
                integrity_pairs.extend([
                    (row["snapshot_id"], target_manifest["cutoff"]),
                    (target_id, target_manifest["cutoff"]),
                ])
        materialization = {}
        for offset in range(
            0, len(integrity_pairs),
            repository.REPLACEMENT_INTEGRITY_BATCH_SIZE,
        ):
            materialization.update(
                repository.snapshot_materialization_counts_batch(
                    integrity_pairs[
                        offset:offset + repository.REPLACEMENT_INTEGRITY_BATCH_SIZE
                    ],
                    context,
                )
            )

        pending_build = []
        for row in issue_rows:
            target_id = replacements_by_date[row["research_date"]]
            source_id = source_by_date[row["research_date"]]
            target_manifest = target_manifests[target_id]
            statuses[target_id] = target_manifest["status"]
            if not target_manifest.get("quality_report") or not target_manifest.get("quality_hash"):
                raise RuntimeError(
                    f"replacement Snapshot lacks its immutable quality evidence: {target_id}"
                )
            quality_reports[target_id] = (
                target_manifest["quality_report"],
                target_manifest["quality_hash"],
            )
            if target_manifest["status"] in {"BUILDING", "FAILED"}:
                # A previous run can commit all children and then stop before
                # lifecycle validation.  Conversely, it can stop halfway
                # through the first child table.  Distinguish those states by
                # bounded child counts so resume never rebuilds a complete
                # replacement and never validates a partial one.
                source_counts = materialization.get(source_id, {
                    "stock_rows": 0, "bundle_rows": 0, "input_rows": 0,
                })
                target_counts = materialization.get(target_id, {
                    "stock_rows": 0, "bundle_rows": 0, "input_rows": 0,
                })
                complete = (
                    source_counts["stock_rows"] > 0
                    and target_counts["stock_rows"] == source_counts["stock_rows"]
                    and target_counts["bundle_rows"] == source_counts["stock_rows"]
                    and target_counts["input_rows"] == source_counts["input_rows"]
                )
                if complete and target_manifest["status"] == "FAILED":
                    changed = kernel.write.transition_snapshot_manifest(
                        context,
                        repository._snapshot_scope(target_id, target_manifest["cutoff"]),
                        target_id,
                        ("FAILED",),
                        "BUILDING",
                        {"failure_reason": None},
                    )
                    if changed != 1:
                        raise RuntimeError(
                            f"replacement lifecycle changed while resuming: {target_id}"
                        )
                    statuses[target_id] = "BUILDING"
                elif not complete and target_manifest["status"] == "BUILDING":
                    changed = kernel.write.transition_snapshot_manifest(
                        context,
                        repository._snapshot_scope(target_id, target_manifest["cutoff"]),
                        target_id,
                        ("BUILDING",),
                        "FAILED",
                        {"failure_reason": "replacement materialization incomplete; rebuild required"},
                    )
                    if changed != 1:
                        raise RuntimeError(
                            f"replacement lifecycle changed while resetting: {target_id}"
                        )
                    statuses[target_id] = "FAILED"
                if not complete:
                    pending_build.append((target_id, source_id))
            elif target_manifest["status"] == "DRAFT":
                pending_build.append((target_id, source_id))
        for offset in range(0, len(pending_build), batch_size):
            batch = pending_build[offset:offset + batch_size]
            for target_id, source_id in batch:
                result = repository.build_financial_replacement_snapshot(
                    target_id, source_id, context,
                )
                if result["stock_rows"] != result["lineage_rows"]:
                    raise RuntimeError(
                        "replacement lineage row count mismatch: "
                        f"snapshot_id={target_id}, result={result}"
                    )
                statuses[target_id] = "BUILDING"
            print(
                f"replacement snapshot build progress "
                f"{min(offset + len(batch), len(pending_build))}/{len(pending_build)}",
                flush=True,
            )
    finally:
        kernel.close()

    baseline = _baseline(max(dates), plan["baseline_tag"], verify_source_manifest=False)
    pending_validate = [
        item["snapshot_id"] for item in created
        if statuses[item["snapshot_id"]] == "BUILDING"
    ]
    for offset in range(0, len(pending_validate), batch_size):
        batch = pending_validate[offset:offset + batch_size]
        validate_snapshots_batch(
            batch,
            baseline=baseline,
            quality_reports={target_id: quality_reports[target_id] for target_id in batch},
        )
        for target_id in batch:
            statuses[target_id] = "VALIDATED"
        print(
            f"replacement snapshot validate progress "
            f"{min(offset + len(batch), len(pending_validate))}/{len(pending_validate)}",
            flush=True,
        )
    pending_freeze = [
        item["snapshot_id"] for item in created
        if statuses[item["snapshot_id"]] == "VALIDATED"
    ]
    if freeze:
        for offset in range(0, len(pending_freeze), batch_size):
            batch = pending_freeze[offset:offset + batch_size]
            freeze_snapshots_batch(
                batch,
                code_archive_path=code_archive_path,
                baseline=baseline,
                quality_report=next(iter(quality_reports.values()))[0],
                verify_source_manifest=False,
            )
            print(
                f"replacement snapshot freeze progress "
                f"{min(offset + len(batch), len(pending_freeze))}/{len(pending_freeze)}",
                flush=True,
            )
    replacements = [
        {
            "research_date": item["research_cutoff_date"],
            "old_snapshot_id": row["snapshot_id"],
            "new_snapshot_id": item["snapshot_id"],
            "new_schema_version": target_schema,
        }
        for row, item in zip(issue_rows, created)
    ]
    result = {
        "repair_version": "foundation-financial-replacement-v1",
        "dataset_id": str(dataset_id),
        "source_dataset_status": plan["source_dataset_status"],
        "baseline_tag": plan["baseline_tag"],
        "source_schema_versions": plan["source_schema_versions"],
        "replacement_schema_version": target_schema,
        "freeze": freeze,
        "replacement_count": len(replacements),
        "replacements": replacements,
        "status": "FROZEN" if freeze else "VALIDATED",
    }
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Create immutable Foundation-v2.1 replacement Snapshots"
    )
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--audit", required=True)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--no-freeze", action="store_true")
    parser.add_argument("--code-archive-path")
    parser.add_argument("--output")
    args = parser.parse_args(argv)
    if args.batch_size <= 0 or args.batch_size > 20:
        parser.error("--batch-size must be between 1 and 20")
    result = repair_dataset(
        args.dataset_id,
        args.audit,
        batch_size=args.batch_size,
        freeze=not args.no_freeze,
        code_archive_path=args.code_archive_path,
        output_path=args.output,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
