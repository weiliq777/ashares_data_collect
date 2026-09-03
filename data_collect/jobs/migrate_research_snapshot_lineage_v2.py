"""Switch and rebuild one Research Snapshot series to compact lineage v2.

This is a one-time, pre-freeze migration for an existing Foundation-v2
series.  It changes only Research Snapshot manifests and children; Raw,
Standard, PIT and Feature are read-only inputs and are never rebuilt here.
The operation is resumable: the first metadata/lifecycle switch is atomic,
and subsequent invocations preserve already VALIDATED v2 snapshots.
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.checkpoint import Checkpoint, PostgresCheckpointStore
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.snapshot_series_repository import SnapshotSeriesRepository
from data_collect.data_access.research_contract import is_foundation_schema
from data_collect.jobs.research_snapshot import (
    _baseline,
    _quality_report,
    build_snapshots_batch,
    validate_snapshots_batch,
)
from data_collect.jobs.snapshot_series_control import (
    SeriesSelector,
    SnapshotSeriesMonitor,
    SnapshotSeriesResume,
    MonitorMode,
)


JOB_NAME = "migrate_research_snapshot_lineage_v2"


def _quality_mapping(ids: list[str], report: dict, quality_hash: str):
    return {snapshot_id: (report, quality_hash) for snapshot_id in ids}


def run(
    start: str | date,
    end: str | date,
    baseline_tag: str,
    schema_version: str,
    expected_snapshots: int,
    *,
    exchange: str = "SSE",
    batch_size: int = 2,
    run_id: str | None = None,
) -> dict:
    """Atomically switch to v2 and resume the existing series to VALIDATED."""
    if not is_foundation_schema(schema_version):
        raise ValueError("compact lineage v2 migration requires a Foundation schema")
    selector = SeriesSelector(
        start=date.fromisoformat(start) if isinstance(start, str) else start,
        end=date.fromisoformat(end) if isinstance(end, str) else end,
        baseline_tag=baseline_tag,
        schema_version=schema_version,
        expected_snapshots=expected_snapshots,
        exchange=exchange,
    )
    if batch_size < 1 or batch_size > 20:
        raise ValueError("batch_size must be between 1 and 20")

    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"{JOB_NAME}-{uuid4().hex}",
        job_name=JOB_NAME,
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-foundation-lineage-v2",
    )
    repository = SnapshotSeriesRepository(kernel)
    monitor = SnapshotSeriesMonitor(repository, context)
    try:
        baseline = _baseline(
            selector.end, selector.baseline_tag, verify_source_manifest=False,
        )
        quality_report, quality_hash = _quality_report(
            baseline[1], selector.schema_version, baseline,
        )
        if quality_report["failures"]:
            raise RuntimeError(
                f"baseline quality gate failed: {quality_report['failures']}"
            )

        with kernel.lock_manager.session_exclusive(
            "research.snapshot.series", selector.digest,
        ):
            report = monitor.inspect(selector, mode=MonitorMode.PROGRESS)
            if report.blocking:
                raise RuntimeError(
                    f"snapshot series migration preflight failed: {report.to_dict()}"
                )
            if any(record.status == "FROZEN" for record in report.records):
                raise RuntimeError(
                    "cannot change lineage storage for a series containing FROZEN snapshots"
                )
            invalid_versions = [
                (record.snapshot_id, record.lineage_storage_version)
                for record in report.records
                if record.lineage_storage_version not in {"v1", "v2"}
            ]
            if invalid_versions:
                raise RuntimeError(f"unknown lineage storage versions: {invalid_versions[:10]}")

            ids = [record.snapshot_id for record in report.records]
            v1_ids = [
                record.snapshot_id for record in report.records
                if record.lineage_storage_version == "v1"
            ]
            if v1_ids:
                changed = repository.prepare_lineage_v2_rebuild(
                    v1_ids,
                    "compact partitioned lineage rebuild; Raw/Standard/PIT/Feature remain read-only",
                    context,
                )
                if changed != len(v1_ids):
                    raise RuntimeError(
                        f"lineage v2 metadata switch mismatch: expected {len(v1_ids)}, got {changed}"
                    )

            checkpoint = Checkpoint(PostgresCheckpointStore())
            runner = SnapshotSeriesResume(
                monitor,
                checkpoint,
                build_batch=lambda batch_ids: build_snapshots_batch(
                    batch_ids,
                    baseline=baseline,
                    quality_reports=_quality_mapping(
                        batch_ids, quality_report, quality_hash,
                    ),
                ),
                validate_batch=lambda batch_ids: validate_snapshots_batch(
                    batch_ids,
                    baseline=baseline,
                    quality_reports=_quality_mapping(
                        batch_ids, quality_report, quality_hash,
                    ),
                ),
            )
            final_report = runner.run(
                selector,
                batch_size=batch_size,
                run_id=run_id,
            )
            result = final_report.to_dict()
            result["lineage_storage_version"] = "v2"
            result["metadata_switched"] = len(v1_ids)
            result["raw_standard_pit_feature_rebuilt"] = False
            print(json.dumps(result, ensure_ascii=False, default=str), flush=True)
            return result
    finally:
        kernel.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="One-time Research Snapshot compact lineage v2 migration"
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--baseline-tag", required=True)
    parser.add_argument("--schema-version", required=True)
    parser.add_argument("--expected-snapshots", required=True, type=int)
    parser.add_argument("--exchange", default="SSE")
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--run-id")
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    run(
        args.start_date,
        args.end_date,
        args.baseline_tag,
        args.schema_version,
        args.expected_snapshots,
        exchange=args.exchange,
        batch_size=args.batch_size,
        run_id=args.run_id,
    )
