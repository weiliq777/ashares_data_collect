"""Operational CLI for bounded Research Snapshot series acceptance and resume."""
from __future__ import annotations

import argparse
from datetime import date, timedelta
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.checkpoint import Checkpoint, PostgresCheckpointStore
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.snapshot_series_repository import SnapshotSeriesRepository
from data_collect.data_access.research_contract import is_foundation_schema
from data_collect.data_access.snapshot_lifecycle import (
    MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE,
    snapshot_id_batches,
)
from data_collect.jobs.research_snapshot import (
    _baseline,
    _git_metadata,
    _quality_report,
    _sha256_file,
    build_snapshots_batch,
    freeze_snapshots_batch,
    inspect_snapshots_batch,
    validate_snapshots_batch,
)
from data_collect.jobs.snapshot_series_control import (
    MonitorMode,
    SeriesSelector,
    SnapshotSeriesFinalize,
    SnapshotSeriesMonitor,
    SnapshotSeriesResume,
    report_json,
)


def _add_selector_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--baseline-tag", required=True)
    parser.add_argument("--schema-version", required=True)
    parser.add_argument("--expected-snapshots", required=True, type=int)
    parser.add_argument("--exchange", default="SSE")


def _selector(args) -> SeriesSelector:
    return SeriesSelector(
        start=date.fromisoformat(args.start_date),
        end=date.fromisoformat(args.end_date),
        baseline_tag=args.baseline_tag,
        schema_version=args.schema_version,
        expected_snapshots=args.expected_snapshots,
        exchange=args.exchange,
    )


def _runtime(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"snapshot-series-ops-{label}-{uuid4().hex}",
        job_name="research_snapshot_series_ops",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    repository = SnapshotSeriesRepository(kernel)
    return kernel, SnapshotSeriesMonitor(repository, context)


def _shared_evidence(selector: SeriesSelector):
    """Load one bounded baseline/quality object for the complete operation."""
    baseline = _baseline(
        selector.end,
        selector.baseline_tag,
        verify_source_manifest=False,
    )
    if is_foundation_schema(selector.schema_version):
        quality_report, quality_hash = _quality_report(
            baseline.baseline_cutoff_date, selector.schema_version, baseline,
        )
        if quality_report["failures"]:
            raise RuntimeError(f"baseline quality gate failed: {quality_report['failures']}")
        return baseline, quality_report, quality_hash
    return baseline, None, None


def _quality_mapping(ids: list[str], report, quality_hash):
    if report is None or quality_hash is None:
        return None
    return {snapshot_id: (report, quality_hash) for snapshot_id in ids}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research Snapshot 序列监控、自动续跑和分批冻结",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    monitor = commands.add_parser("monitor", help="只读检查快照序列和日志尾部")
    _add_selector_arguments(monitor)
    monitor.add_argument(
        "--mode", choices=[item.value for item in MonitorMode],
        default=MonitorMode.PROGRESS.value,
    )
    monitor.add_argument("--log-file", action="append", default=[])
    monitor.add_argument("--stale-building-hours", type=float)

    resume = commands.add_parser(
        "resume", help="先校验 BUILDING，再逐批构建和校验 DRAFT/FAILED",
    )
    _add_selector_arguments(resume)
    resume.add_argument("--batch-size", type=int, default=2)
    resume.add_argument("--run-id")
    resume.add_argument(
        "--rebuild-dirty", action="store_true",
        help="仅重建明确选出的、尚未冻结且缺少代码归档的 dirty VALIDATED 快照",
    )

    finalize = commands.add_parser(
        "finalize", help="全系列深度验收通过后分批进入 FROZEN",
    )
    _add_selector_arguments(finalize)
    finalize.add_argument("--batch-size", type=int, default=10)
    finalize.add_argument("--run-id")
    finalize.add_argument("--code-archive-path")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    selector = _selector(args)
    if args.command != "resume" and getattr(args, "rebuild_dirty", False):
        build_parser().error("--rebuild-dirty 只能用于 resume")
    if args.command in {"resume", "finalize"} and not (
        1 <= args.batch_size <= MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE
    ):
        build_parser().error(
            "--batch-size must be between 1 and "
            f"{MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE}"
        )
    kernel, monitor = _runtime(args.command)
    try:
        if args.command == "monitor":
            stale_after = (
                timedelta(hours=args.stale_building_hours)
                if args.stale_building_hours is not None else None
            )
            report = monitor.inspect(
                selector,
                mode=MonitorMode(args.mode),
                log_paths=args.log_file,
                stale_building_after=stale_after,
            )
            print(report_json(report), flush=True)
            return 2 if report.blocking else 0

        checkpoint = Checkpoint(PostgresCheckpointStore())
        baseline, quality_report, quality_hash = _shared_evidence(selector)
        freeze_metadata = None
        archive_evidence = None
        if args.command == "finalize":
            freeze_metadata = _git_metadata()
            if args.code_archive_path:
                archive_evidence = _sha256_file(args.code_archive_path)
        lock_name = "research.snapshot.series"
        with kernel.lock_manager.session_exclusive(lock_name, selector.digest):
            if args.command == "resume":
                if args.rebuild_dirty:
                    preflight = monitor.inspect(selector, mode=MonitorMode.PROGRESS)
                    dirty_ids = [
                        record.snapshot_id
                        for record in preflight.records
                        if record.status == "VALIDATED"
                        and record.git_dirty
                        and not record.code_archive_path
                    ]
                    if dirty_ids:
                        for batch in snapshot_id_batches(dirty_ids):
                            monitor.repository.reopen_validated_snapshots(
                                batch,
                                "原快照由 dirty 工作区构建且缺少对应代码归档，执行一次性清洁代码重建",
                                monitor.context,
                            )
                runner = SnapshotSeriesResume(
                    monitor,
                    checkpoint,
                    build_batch=lambda ids: build_snapshots_batch(
                        ids,
                        baseline=baseline,
                        quality_reports=_quality_mapping(ids, quality_report, quality_hash),
                    ),
                    validate_batch=lambda ids: validate_snapshots_batch(
                        ids,
                        baseline=baseline,
                        quality_reports=_quality_mapping(ids, quality_report, quality_hash),
                    ),
                )
                report = runner.run(
                    selector, batch_size=args.batch_size, run_id=args.run_id,
                )
            else:
                runner = SnapshotSeriesFinalize(
                    monitor,
                    checkpoint,
                    inspect_batch=lambda ids: inspect_snapshots_batch(
                        ids,
                        baseline=baseline,
                        quality_reports=_quality_mapping(ids, quality_report, quality_hash),
                        allowed_statuses=("VALIDATED", "FROZEN"),
                        verify_source_manifest=False,
                    ),
                    immediate_inspect_batch=lambda ids: inspect_snapshots_batch(
                        ids,
                        baseline=baseline,
                        quality_reports=_quality_mapping(ids, quality_report, quality_hash),
                        allowed_statuses=("VALIDATED", "FROZEN"),
                        verify_source_manifest=False,
                        verify_global_checks=False,
                    ),
                    freeze_batch=lambda ids: freeze_snapshots_batch(
                        ids,
                        args.code_archive_path,
                        baseline=baseline,
                        quality_report=quality_report,
                        verify_source_manifest=False,
                        verify_global_checks=False,
                        metadata=freeze_metadata,
                        code_archive_evidence=archive_evidence,
                    ),
                    code_archive_available=bool(args.code_archive_path),
                )
                report = runner.run(
                    selector, batch_size=args.batch_size, run_id=args.run_id,
                )
        print(report_json(report), flush=True)
        return 0
    finally:
        kernel.close()


if __name__ == "__main__":
    raise SystemExit(main())
