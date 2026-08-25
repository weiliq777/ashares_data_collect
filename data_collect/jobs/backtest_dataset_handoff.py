"""Create, build, validate, and freeze a historical Backtest Dataset."""
from __future__ import annotations

import argparse
from datetime import date

from data_collect.jobs.backtest_dataset import create_dataset, freeze_dataset, _git_metadata, validate_dataset
from data_collect.jobs.backtest_dataset_batched import _build_dataset_batched
from data_collect.jobs.tushare_job_lock import warehouse_write_job
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.dataset_repository import DatasetRepository

_CONTRACT_SOURCE_ASSERTIONS = "status='BUILDING', failure_reason=NULL, failed_at=NULL"
from uuid import uuid4

_SNAPSHOT_RANGE_CONTRACT = "research_cutoff_date BETWEEN %s AND %s"


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(run_id=f"dataset-handoff-{label}-{uuid4().hex}", job_name="backtest_dataset_handoff",
                         code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1")
    return kernel, DatasetRepository(kernel), context


def _checkpoint(dataset_id, stage):
    kernel, repository, context = _service("checkpoint")
    try:
        return repository.checkpoint(dataset_id, stage, context)
    finally:
        kernel.close()


def _set_checkpoint(dataset_id, stage, status, **kwargs):
    kernel, repository, context = _service("set-checkpoint")
    try:
        return repository.set_checkpoint(dataset_id, stage, status, context, **kwargs)
    finally:
        kernel.close()


@warehouse_write_job
def main() -> None:
    parser = argparse.ArgumentParser(description="执行历史 Backtest Dataset 交接流程")
    parser.add_argument("--baseline-tag", required=True)
    parser.add_argument("--schema-version", required=True)
    parser.add_argument("--expected-snapshots", type=int, required=True)
    parser.add_argument("--warmup-start", required=True)
    parser.add_argument("--evaluation-start", required=True)
    parser.add_argument("--evaluation-end", required=True)
    parser.add_argument("--out-of-sample-start")
    parser.add_argument("--dataset-name", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--calendar-rule-version", default="tushare-sse-v1")
    parser.add_argument("--market-rule-version", default="cn-a-share-v2")
    parser.add_argument("--fee-rule-version", default="research-fee-assumption-v3")
    parser.add_argument("--benchmark-code", default="000300.SH")
    parser.add_argument("--code-archive-path")
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="忽略已完成的 BUILD checkpoint，强制重新物化 Dataset",
    )
    parser.add_argument(
        "--batch-days",
        type=int,
        default=5,
        help="每个 Dataset Build 事务包含的交易日数量，默认 5，最大 5；用于限制数据库/WAL/内存峰值",
    )
    parser.add_argument("--dataset-id", help="续跑已有的 DRAFT/FAILED Dataset")
    args = parser.parse_args()

    warmup_start = date.fromisoformat(args.warmup_start)
    evaluation_start = date.fromisoformat(args.evaluation_start)
    evaluation_end = date.fromisoformat(args.evaluation_end)
    if not warmup_start <= evaluation_start <= evaluation_end:
        raise ValueError("warmup/evaluation date range is invalid")

    if args.dataset_id:
        dataset_id = args.dataset_id
    else:
        kernel, repository, context = _service("select-snapshots")
        try:
            snapshot_ids = repository.frozen_snapshot_ids(args.baseline_tag, args.schema_version, warmup_start, evaluation_end, context)
        finally:
            kernel.close()
        if len(snapshot_ids) != args.expected_snapshots:
            raise RuntimeError(
                f"expected {args.expected_snapshots} frozen snapshots, got {len(snapshot_ids)}"
            )

        dataset_id = create_dataset(
            snapshot_ids=snapshot_ids,
            dataset_name=args.dataset_name,
            dataset_version=args.dataset_version,
            baseline_tag=args.baseline_tag,
            warmup_start=warmup_start,
            evaluation_start=evaluation_start,
            evaluation_end=evaluation_end,
            out_of_sample_start=(
                date.fromisoformat(args.out_of_sample_start)
                if args.out_of_sample_start else None
            ),
            calendar_rule_version=args.calendar_rule_version,
            market_rule_version=args.market_rule_version,
            fee_rule_version=args.fee_rule_version,
            benchmark_code=args.benchmark_code,
        )
    print(f"DATASET_ID={dataset_id}", flush=True)

    kernel, repository, context = _service("manifest")
    try:
        manifest = repository.manifest(dataset_id, context)
    finally:
        kernel.close()
    if manifest["status"] == "FROZEN":
        kernel, repository, context = _service("frozen")
        freeze_checkpoint = repository.checkpoint(dataset_id, "FREEZE", context)
        kernel.close()
        if freeze_checkpoint and freeze_checkpoint["status"] in {"RUNNING", "FAILED"}:
            kernel, repository, context = _service("frozen-checkpoint")
            try:
                _set_checkpoint(dataset_id, "FREEZE", "COMPLETED", details={"mode": "manifest_already_frozen"})
            finally:
                kernel.close()
        print(f"FROZEN_DATASET_ID={dataset_id} (already frozen)", flush=True)
        return

    kernel, repository, context = _service("checkpoint")
    build_checkpoint = repository.checkpoint(dataset_id, "BUILD", context)
    kernel.close()
    current_code = _git_metadata()
    same_clean_code = (
        not current_code.get("dirty")
        and not manifest.get("git_dirty")
        and manifest.get("git_commit") == current_code.get("commit")
    )
    can_resume_validate = (
        not args.force_rebuild
        and same_clean_code
        and manifest.get("built_at") is not None
        and bool(manifest.get("execution_hash"))
        and bool(manifest.get("input_manifest_hash"))
        and build_checkpoint is not None
        and build_checkpoint["status"] == "COMPLETED"
    )

    if manifest["status"] == "VALIDATED":
        print("BUILD=SKIPPED; VALIDATE=SKIPPED (manifest already validated)", flush=True)
        _set_checkpoint(
            dataset_id,
            "VALIDATE",
            "COMPLETED",
            details={"mode": "manifest_already_validated"},
        )
        _set_checkpoint(dataset_id, "FREEZE", "RUNNING")
        try:
            freeze_dataset(dataset_id, code_archive_path=args.code_archive_path)
            _set_checkpoint(dataset_id, "FREEZE", "COMPLETED")
            print(f"FROZEN_DATASET_ID={dataset_id}", flush=True)
        except Exception as exc:
            _set_checkpoint(dataset_id, "FREEZE", "FAILED", error_message=str(exc)[:4000])
            raise
        return

    if can_resume_validate:
        print("BUILD=SKIPPED (checkpoint completed; resume validation)", flush=True)
        if manifest["status"] == "FAILED":
            kernel, repository, context = _service("resume")
            try:
                repository.resume_failed(dataset_id, context)
            finally:
                kernel.close()
    else:
        _set_checkpoint(
            dataset_id,
            "BUILD",
            "RUNNING",
            details={"mode": "date_batched_resumable", "batch_days": args.batch_days},
        )
        try:
            build_result = _build_dataset_batched(
                dataset_id,
                batch_days=args.batch_days,
                force_rebuild=args.force_rebuild,
            )
            _set_checkpoint(
                dataset_id,
                "BUILD",
                "COMPLETED",
                progress_done=int(build_result.get("execution_rows", 0)),
                progress_total=int(build_result.get("execution_rows", 0)),
                stage_hash=build_result.get("input_hash"),
                details=build_result,
            )
            print(f"BUILD={build_result}", flush=True)
        except Exception as exc:
            _set_checkpoint(dataset_id, "BUILD", "FAILED", error_message=str(exc)[:4000])
            raise

    _set_checkpoint(dataset_id, "VALIDATE", "RUNNING", details={"mode": "full_materialized_gate"})
    try:
        validate_result = validate_dataset(dataset_id)
        _set_checkpoint(
            dataset_id,
            "VALIDATE",
            "COMPLETED",
            progress_done=int(validate_result.get("execution_rows", 0)),
            progress_total=int(validate_result.get("execution_rows", 0)),
            stage_hash=validate_result.get("input_hash"),
            details=validate_result,
        )
        print(f"VALIDATE={validate_result}", flush=True)
    except Exception as exc:
        _set_checkpoint(dataset_id, "VALIDATE", "FAILED", error_message=str(exc)[:4000])
        raise

    _set_checkpoint(dataset_id, "FREEZE", "RUNNING")
    try:
        freeze_dataset(dataset_id, code_archive_path=args.code_archive_path)
        _set_checkpoint(dataset_id, "FREEZE", "COMPLETED")
        print(f"FROZEN_DATASET_ID={dataset_id}", flush=True)
    except Exception as exc:
        _set_checkpoint(dataset_id, "FREEZE", "FAILED", error_message=str(exc)[:4000])
        raise


if __name__ == "__main__":
    main()
