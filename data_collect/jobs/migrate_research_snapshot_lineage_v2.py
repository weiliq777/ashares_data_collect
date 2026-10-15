"""Switch one Research Snapshot series to compact lineage v2.

This is a one-time, pre-freeze migration for an existing Foundation-v2
series.  It changes only the Research Snapshot lineage representation; Raw,
Standard, PIT and Feature are read-only inputs and are never rebuilt here.
Existing stock snapshot rows are preserved.  Legacy lineage is aggregated
directly into the compact bundle in bounded code slices.  The operation is
resumable: already VALIDATED v2 snapshots are skipped.  A date with no
legacy lineage is treated as an incomplete snapshot and routed to the normal
v2 builder only for that date.
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.checkpoint import Checkpoint, PostgresCheckpointStore
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.research_snapshot_repository import (
    ResearchBaseline,
    ResearchSnapshotRepository,
)
from data_collect.data_access.repository.snapshot_series_repository import SnapshotSeriesRepository
from data_collect.data_access.research_contract import is_foundation_schema
from data_collect.data_access.snapshot_lifecycle import (
    MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE,
    snapshot_id_batches,
)
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


def _validate_legacy_lineage_v2_batch(
    snapshot_repository: ResearchSnapshotRepository,
    snapshot_ids: list[str],
    baseline: ResearchBaseline,
    quality_hash: str,
    context: RunContext,
) -> None:
    with snapshot_repository.lifecycle_locks(snapshot_ids):
        _validate_legacy_lineage_v2_batch_locked(
            snapshot_repository, snapshot_ids, baseline, quality_hash, context,
        )


def _validate_legacy_lineage_v2_batch_locked(
    snapshot_repository: ResearchSnapshotRepository,
    snapshot_ids: list[str],
    baseline: ResearchBaseline,
    quality_hash: str,
    context: RunContext,
) -> None:
    """Accept the physical lineage conversion without current input drift.

    The ordinary snapshot validator compares ``snapshot_input`` against the
    current builder definition.  That is correct for a new snapshot, but a
    format migration must preserve the historical manifest exactly.  This
    validator checks only the conversion invariants and then uses the normal
    lifecycle transition.
    """
    issues_by_snapshot: dict[str, list[dict]] = {}
    for snapshot_id in snapshot_ids:
        manifest = snapshot_repository.manifest(snapshot_id, context)
        issues = snapshot_repository.check_legacy_lineage_v2(manifest, context)
        if issues:
            issues_by_snapshot[snapshot_id] = issues
            snapshot_repository.mark_failed(
                snapshot_id,
                manifest["cutoff"],
                json.dumps(issues, ensure_ascii=False, default=str),
                context,
            )
        else:
            snapshot_repository.validate_snapshot(
                manifest, baseline, quality_hash, [], context,
            )
    if issues_by_snapshot:
        raise RuntimeError(
            f"legacy lineage v2 validation failed: {issues_by_snapshot}"
        )


def _build_migration_batch(
    snapshot_repository: ResearchSnapshotRepository,
    snapshot_ids: list[str],
    baseline: ResearchBaseline,
    quality_report: dict,
    quality_hash: str,
    context: RunContext,
) -> None:
    """Route legacy rows to conversion and incomplete dates to v2 rebuild.

    The migration must not manufacture lineage for a date that has no old
    lineage.  Such a date is not a format conversion; it is an incomplete
    snapshot and may use the ordinary bounded v2 builder.  Complete v2 child
    data is left untouched and is only sent through the ordinary validator.
    """
    legacy_ids: list[str] = []
    rebuild_ids: list[str] = []
    for snapshot_id in snapshot_ids:
        manifest = snapshot_repository.manifest(snapshot_id, context)
        cutoff = manifest["cutoff"]
        if snapshot_repository.has_legacy_lineage(snapshot_id, cutoff, context):
            legacy_ids.append(snapshot_id)
            continue
        counts = snapshot_repository.snapshot_materialization_counts(
            snapshot_id, cutoff, context,
        )
        complete_v2 = (
            counts["stock_rows"] > 0
            and counts["input_rows"] > 0
            and counts["bundle_rows"] == counts["stock_rows"]
        )
        if not complete_v2:
            rebuild_ids.append(snapshot_id)

    if legacy_ids:
        snapshot_repository.materialize_legacy_lineage_v2(legacy_ids, context)
    if rebuild_ids:
        build_snapshots_batch(
            rebuild_ids,
            baseline=baseline,
            quality_reports={
                snapshot_id: (quality_report, quality_hash)
                for snapshot_id in rebuild_ids
            },
        )


def _validate_migration_batch(
    snapshot_repository: ResearchSnapshotRepository,
    snapshot_ids: list[str],
    baseline: ResearchBaseline,
    quality_report: dict,
    quality_hash: str,
    context: RunContext,
) -> None:
    """Validate legacy conversions and ordinary v2 repairs separately."""
    legacy_ids: list[str] = []
    ordinary_ids: list[str] = []
    for snapshot_id in snapshot_ids:
        manifest = snapshot_repository.manifest(snapshot_id, context)
        if snapshot_repository.has_legacy_lineage(
            snapshot_id, manifest["cutoff"], context,
        ):
            legacy_ids.append(snapshot_id)
        else:
            ordinary_ids.append(snapshot_id)

    if legacy_ids:
        _validate_legacy_lineage_v2_batch(
            snapshot_repository, legacy_ids, baseline, quality_hash, context,
        )
    if ordinary_ids:
        validate_snapshots_batch(
            ordinary_ids,
            baseline=baseline,
            quality_reports={
                snapshot_id: (quality_report, quality_hash)
                for snapshot_id in ordinary_ids
            },
        )


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
    if batch_size < 1 or batch_size > MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE:
        raise ValueError(
            "batch_size must be between 1 and "
            f"{MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE}"
        )

    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"{JOB_NAME}-{uuid4().hex}",
        job_name=JOB_NAME,
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-foundation-lineage-v2",
    )
    repository = SnapshotSeriesRepository(kernel)
    snapshot_repository = ResearchSnapshotRepository(kernel)
    monitor = SnapshotSeriesMonitor(repository, context)
    try:
        baseline = _baseline(
            selector.end, selector.baseline_tag, verify_source_manifest=False,
        )
        quality_report, quality_hash = _quality_report(
            baseline.baseline_cutoff_date, selector.schema_version, baseline,
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

            v1_ids = [
                record.snapshot_id for record in report.records
                if record.lineage_storage_version == "v1"
            ]
            if v1_ids:
                metadata_switched = 0
                for batch in snapshot_id_batches(v1_ids, batch_size):
                    changed = repository.prepare_lineage_v2_rebuild(
                        batch,
                        (
                            "compact partitioned lineage rebuild; "
                            "Raw/Standard/PIT/Feature remain read-only"
                        ),
                        context,
                    )
                    if changed != len(batch):
                        raise RuntimeError(
                            "lineage v2 metadata switch mismatch: "
                            f"expected {len(batch)}, got {changed}"
                        )
                    metadata_switched += changed
            else:
                metadata_switched = 0

            checkpoint = Checkpoint(PostgresCheckpointStore())
            runner = SnapshotSeriesResume(
                monitor,
                checkpoint,
                build_batch=lambda batch_ids: _build_migration_batch(
                    snapshot_repository,
                    batch_ids,
                    baseline,
                    quality_report,
                    quality_hash,
                    context,
                ),
                validate_batch=lambda batch_ids: _validate_migration_batch(
                    snapshot_repository,
                    batch_ids,
                    baseline,
                    quality_report,
                    quality_hash,
                    context,
                ),
            )
            final_report = runner.run(
                selector,
                batch_size=batch_size,
                run_id=run_id,
            )
            result = final_report.to_dict()
            result["lineage_storage_version"] = "v2"
            result["metadata_switched"] = metadata_switched
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
