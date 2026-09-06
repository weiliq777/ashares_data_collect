"""Read-only audit for Foundation-v2 financial identity in a Dataset.

The audit never changes a frozen Snapshot or Dataset.  It processes a bounded
number of dates per Repository read so a large historical Dataset can be
checked without a cross-series transaction or an unbounded lineage scan.
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
from data_collect.data_access.research_contract import is_foundation_schema


def audit_dataset(dataset_id: str, *, batch_size: int = 10) -> dict:
    if not str(dataset_id).strip():
        raise ValueError("dataset_id must not be blank")
    if batch_size <= 0 or batch_size > 20:
        raise ValueError("batch_size must be between 1 and 20")

    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"audit-foundation-snapshot-contract-{uuid4().hex}",
        job_name="audit_foundation_snapshot_contract",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    try:
        datasets = DatasetRepository(kernel)
        dataset = datasets.manifest(dataset_id, context)
        members = datasets.research_snapshot_contracts(dataset_id, context)
        if not members:
            raise RuntimeError(f"Dataset has no Research Snapshot members: {dataset_id}")
        schema_versions = {str(row["research_schema_version"]) for row in members}
        if len(schema_versions) != 1 or not all(
            is_foundation_schema(value) for value in schema_versions
        ):
            raise RuntimeError(
                "Foundation-v2 audit requires one Foundation Research schema; "
                f"actual={sorted(schema_versions)}"
            )

        repository = ResearchSnapshotRepository(kernel)
        issues: list[dict] = []
        total = len(members)
        for offset in range(0, total, batch_size):
            batch = members[offset : offset + batch_size]
            manifests = [
                {
                    "snapshot_id": row["research_snapshot_id"],
                    "cutoff": row["research_date"],
                    "schema_version": row["research_schema_version"],
                }
                for row in batch
            ]
            batch_issues = repository.foundation_financial_contract_issues(
                manifests, context,
            )
            by_id = {str(row["research_snapshot_id"]): row for row in batch}
            for snapshot_id, details in batch_issues.items():
                row = by_id[snapshot_id]
                issues.append({
                    "research_date": row["research_date"].isoformat(),
                    "snapshot_id": snapshot_id,
                    **details,
                })
            print(
                f"foundation snapshot contract audit {min(offset + len(batch), total)}/{total}",
                flush=True,
            )

        return {
            "audit_version": "foundation-financial-contract-v1",
            "dataset_id": str(dataset_id),
            "dataset_status": dataset["status"],
            "member_count": total,
            "batch_size": batch_size,
            "research_schema_versions": sorted(schema_versions),
            "issue_snapshot_count": len(issues),
            "issue_rows": issues,
            "status": "PASS" if not issues else "FAIL",
        }
    finally:
        kernel.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only Foundation-v2 Research Snapshot contract audit",
    )
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--output")
    args = parser.parse_args(argv)
    result = audit_dataset(args.dataset_id, batch_size=args.batch_size)
    encoded = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    if args.output:
        Path(args.output).write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
