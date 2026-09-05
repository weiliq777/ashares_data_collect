"""Resource-safe, resumable Backtest Dataset materialization.

The legacy builder keeps one repeatable-read transaction open while deleting and
materializing millions of rows.  This module keeps the same source selection,
unit conversion, quality rules, and hashes, but commits one bounded trading-day
batch at a time.  A failed batch is safe to repeat; completed batches are
skipped only when they were produced by the current code version.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from typing import Any

from data_collect.jobs.backtest_dataset import (
    ADAPTER_VERSION,
    FEATURE_VERSION,
    HASH_ALGORITHM,
    _canonical,
    _check_baseline,
    _execution_limit_safety,
    _git_metadata,
    _json_param,
    _manifest,
    _materialized_hashes,
    _quality_report_hash,
    _set_checkpoint,
    _IncrementalRowHasher,
    hash_rows,
)
from data_collect.jobs.tushare_job_lock import warehouse_write_job
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.context import WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.dataset_repository import DatasetRepository

_CONTRACT_SOURCE_ASSERTIONS = "m.research_date BETWEEN %s AND %s"
# Contract text: DELETE FROM backtest.dataset_execution_bar
# Contract text: trade_date BETWEEN %s AND %s


def _source_service(dataset_id: str, batch_start: date):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"backtest-batched-source-{dataset_id}-{batch_start.isoformat()}",
        job_name="backtest_dataset_batched", code_version="unknown",
        config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, DatasetRepository(kernel), context


def _transform_row(row: tuple[Any, ...], dataset_id: str) -> tuple[tuple[Any, ...], str]:
    (
        day, code, market, opn, high, low, close, volume, amount, up, down,
        suspended, can_buy, can_sell, status_source_present, limit_status,
        payload, version, payload_hash, limit_version, limit_hash,
        suspend_version, suspend_hash,
    ) = row
    up, down, can_buy, can_sell, limit_unknown = _execution_limit_safety(
        up, down, suspended, can_buy, can_sell
    )
    computed_hash = payload_hash
    if computed_hash is None and payload is not None:
        computed_hash = hashlib.sha256(
            json.dumps(
                _canonical(payload), ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
    has_ohlc = all(value is not None and float(value) > 0 for value in (opn, high, low, close))
    geometry_valid = (
        has_ohlc
        and float(low) <= min(float(opn), float(close))
        <= max(float(opn), float(close)) <= float(high)
    )
    has_activity = (
        volume is not None and math.isfinite(float(volume)) and float(volume) > 0
        and amount is not None and math.isfinite(float(amount)) and float(amount) > 0
    )
    amount_volume_valid = True
    if has_activity and has_ohlc:
        average_price = float(amount) / float(volume)
        amount_volume_valid = (
            math.isfinite(average_price)
            and float(low) * 0.5 <= average_price <= float(high) * 1.5
        )
    if not status_source_present:
        quality = "WARNING" if suspended or (not can_buy and not can_sell) else "MISSING_SOURCE"
    elif suspended and geometry_valid and computed_hash is not None:
        quality = "WARNING"
    elif geometry_valid and computed_hash is not None and not limit_unknown and has_activity and amount_volume_valid:
        quality = "VALID"
    elif geometry_valid and computed_hash is not None and (limit_unknown or not has_activity):
        quality = "WARNING"
    elif geometry_valid and computed_hash is not None and not amount_volume_valid:
        quality = "INVALID"
    elif has_ohlc and not geometry_valid:
        quality = "INVALID"
    elif suspended:
        quality = "WARNING"
    else:
        quality = "MISSING_SOURCE"
    values = (
        dataset_id, day, code, market, opn, high, low, close, volume, amount, up, down,
        suspended, can_buy, can_sell, limit_status, f"{code}:{day}", version,
        computed_hash, limit_version, limit_hash, suspend_version, suspend_hash, quality,
    )
    return values, quality


def _quality_counter_key(quality: str) -> str:
    """Map persisted row quality to the bounded per-day counter names."""
    return "missing" if quality == "MISSING_SOURCE" else quality.lower()


def _aggregate_execution_quality(quality_counts: dict[str, int]) -> str:
    """Derive member quality from the actual persisted execution rows."""
    if quality_counts.get("INVALID", 0):
        return "INVALID"
    if quality_counts.get("MISSING_SOURCE", 0):
        return "MISSING_SOURCE"
    if quality_counts.get("WARNING", 0):
        return "WARNING"
    return "VALID" if quality_counts.get("VALID", 0) else "MISSING_SOURCE"


def _batch_checkpoint(dataset_id: str, batch_start: date, repository: DatasetRepository, context: RunContext) -> dict[str, Any] | None:
    return repository.batch_checkpoint(dataset_id, batch_start, context)


def _set_batch_checkpoint(dataset_id: str, batch_start: date, batch_end: date, status: str, *, row_count: int = 0, batch_hash: str | None = None, code_version: str | None = None, details: dict[str, Any] | None = None, error_message: str | None = None, repository: DatasetRepository, context: RunContext) -> None:
    repository.set_batch_checkpoint(dataset_id, batch_start, batch_end, status, context, row_count=row_count, batch_hash=batch_hash, code_version=code_version, details=details or {}, error_message=error_message)


def _trading_batches(dataset_id: str, batch_days: int, repository: DatasetRepository, context: RunContext) -> list[tuple[date, date, list[date]]]:
    dates = repository.member_dates(dataset_id, context)
    if not dates:
        raise RuntimeError("Dataset 没有 dataset_member，不能构建执行行情")
    return [(chunk[0], chunk[-1], chunk) for offset in range(0, len(dates), batch_days) for chunk in [dates[offset:offset + batch_days]]]


def _prepare_manifest(dataset_id: str, repository: DatasetRepository, context: RunContext) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest = repository.manifest(dataset_id, context)
    if manifest["status"] not in {"DRAFT", "FAILED", "BUILDING"}:
        raise RuntimeError(f"只有 DRAFT/FAILED/BUILDING Dataset 可分批构建: {manifest['status']}")
    baseline = _check_baseline(manifest["baseline"], manifest["evaluation_end"], expected_quality_report_hash=manifest.get("baseline_quality_report_hash"), expected_source_manifest_hash=manifest.get("baseline_source_manifest_hash"))
    source, append_only = baseline["source_manifest"], baseline["source_manifest"].get("append_only_sources", {})
    boundaries = {"daily": source.get("market_versions", {}).get("daily", {}).get("max_record_id"), "legacy_daily": source.get("legacy_market_raw", {}).get("tushare_daily_raw", {}).get("max_fetched_at"), "limit": append_only.get("tushare_stk_limit_raw", {}).get("max_record_id"), "suspend": append_only.get("tushare_suspend_d_raw", {}).get("max_record_id"), "dividend": append_only.get("tushare_dividend_raw", {}).get("max_record_id"), "trade_cal": source.get("market_versions", {}).get("trade_cal", {}).get("max_record_id"), "legacy_trade_cal": source.get("legacy_market_raw", {}).get("tushare_trade_cal_raw", {}).get("max_fetched_at"), "index": append_only.get("tushare_index_daily_raw", {}).get("max_record_id")}
    missing = [key for key, value in boundaries.items() if value is None]
    if missing:
        raise RuntimeError("Baseline 缺少 Dataset 所需 Raw 高水位: " + ", ".join(missing))
    if manifest["status"] != "BUILDING":
        repository.transition_manifest(
            context, dataset_id, (manifest["status"],), "BUILDING",
            {"failure_reason": None, "failed_at": None},
        )
    return manifest, boundaries, _git_metadata()


def _build_execution_batch(dataset_id: str, batch_start: date, batch_end: date, boundaries: dict[str, Any], code_version: str, repository: DatasetRepository, context: RunContext) -> dict[str, Any]:
    _set_batch_checkpoint(dataset_id, batch_start, batch_end, "RUNNING", code_version=code_version, details={"mode": "date_batch"}, repository=repository, context=context)
    try:
        repository.delete_execution_range(dataset_id, batch_start, batch_end, context)
        batch_hasher, per_day, pending, total = _IncrementalRowHasher(), {}, [], 0
        columns = ("dataset_id", "trade_date", "ts_code", "market", "raw_open", "raw_high", "raw_low", "raw_close", "volume", "amount", "up_limit", "down_limit", "is_suspended", "can_buy", "can_sell", "limit_status", "source_record_key", "source_record_version", "source_payload_hash", "limit_source_record_id", "limit_source_payload_hash", "suspend_source_record_id", "suspend_source_payload_hash", "quality_status")
        for source_rows in repository.execution_source_batches(dataset_id, boundaries, batch_start, batch_end, context, 5000):
            for source_row in source_rows:
                values, quality = _transform_row(source_row, dataset_id)
                pending.append(dict(zip(columns, values)))
                batch_hasher.update(values[1:])
                total += 1
                stats = per_day.setdefault(values[1], {"hasher": _IncrementalRowHasher(), "count": 0, "valid": 0, "missing": 0, "invalid": 0, "warning": 0})
                stats["hasher"].update(values[1:])
                stats["count"] += 1
                key = _quality_counter_key(quality)
                stats[key] += 1
                if len(pending) >= 5000:
                    repository.write_execution_rows(pending, context, dataset_id)
                    pending.clear()
        if pending:
            repository.write_execution_rows(pending, context, dataset_id)
        for day, stats in per_day.items():
            member = repository.batch_member_inputs(dataset_id, day, context)
            if not member:
                raise RuntimeError(f"执行批次出现未绑定 Dataset 成员: {day}")
            quality = "INVALID" if stats["invalid"] else "MISSING_SOURCE" if stats["missing"] else "WARNING" if stats["warning"] else "VALID"
            repository.update_batch_member(dataset_id, day, {"observation_row_count": member["observation_row_count"], "execution_row_count": stats["count"], "execution_hash": stats["hasher"].hexdigest(), "quality_status": quality}, context)
        result = {"batch_start": batch_start, "batch_end": batch_end, "rows": total, "hash": batch_hasher.hexdigest()}
        _set_batch_checkpoint(dataset_id, batch_start, batch_end, "COMPLETED", row_count=total, batch_hash=result["hash"], code_version=code_version, details={"dates": len(per_day), "rows": total}, repository=repository, context=context)
        return result
    except Exception as exc:
        _set_batch_checkpoint(dataset_id, batch_start, batch_end, "FAILED", code_version=code_version, error_message=str(exc)[:4000], repository=repository, context=context)
        raise


def _finalize_build(dataset_id: str, manifest: dict[str, Any], metadata: dict[str, Any], repository: DatasetRepository, context: RunContext) -> dict[str, Any]:
    materialized = _materialized_hashes(dataset_id)
    members = repository.member_validation_rows(dataset_id, context)
    updates, member_stats = [], []
    for day, _role, snapshot_hash, _status, _schema, obs_count, _exec_count, _exec_hash, _corporate_hash, _quality in members:
        execution = materialized["execution_by_day"].get(
            day,
            {"hasher": _IncrementalRowHasher(), "count": 0, "quality_counts": {}},
        )
        corporate = hash_rows(materialized["dividend_by_day"].get(day, []))
        updates.append({"dataset_id": dataset_id, "research_date": day, "observation_row_count": obs_count, "execution_row_count": execution["count"], "execution_hash": execution["hasher"].hexdigest(), "corporate_action_hash": corporate, "quality_status": _aggregate_execution_quality(execution["quality_counts"])})
        member_stats.append((dataset_id, day, snapshot_hash, execution["hasher"].hexdigest(), corporate, execution["count"]))
    if updates:
        repository.update_member_stats(dataset_id, updates, context)
    input_hash = hash_rows(member_stats + [("calendar", materialized["calendar_hash"]), ("rules", materialized["rules_hash"]), ("execution", materialized["execution_hash"]), ("corporate_action", materialized["corporate_action_hash"]), ("benchmark", materialized["benchmark_hash"]), ("benchmark_source_manifest", manifest.get("benchmark_source_manifest_hash") or ""), ("baseline_quality_report", manifest["baseline_quality_report_hash"]), ("baseline_source_manifest", manifest["baseline_source_manifest_hash"]), ("out_of_sample_start", manifest["out_of_sample_start"])])
    quality = {"execution_rows": sum(item["count"] for item in materialized["execution_by_day"].values()), "calendar_rows": len(materialized["calendar_rows"]), "corporate_action_rows": len(materialized["dividend_rows"]), "rule_rows": len(materialized["rule_rows"]), "benchmark_rows": len(materialized["benchmark_rows"]), "member_count": len(members), "baseline_quality_report_hash": manifest["baseline_quality_report_hash"], "baseline_source_manifest_hash": manifest["baseline_source_manifest_hash"], "benchmark_code": manifest["benchmark_code"], "benchmark_hash": materialized["benchmark_hash"], "benchmark_source_manifest_hash": manifest.get("benchmark_source_manifest_hash"), "out_of_sample_start": manifest["out_of_sample_start"], "input_hash": input_hash, "build_mode": "date_batched_resumable"}
    repository.update_manifest(
        context, dataset_id, ("BUILDING",), {
            "calendar_hash": materialized["calendar_hash"],
            "rules_hash": materialized["rules_hash"],
            "execution_hash": materialized["execution_hash"],
            "corporate_action_hash": materialized["corporate_action_hash"],
            "benchmark_hash": materialized["benchmark_hash"],
            "benchmark_source_manifest": _json_param(manifest["benchmark_source_manifest"]) if manifest.get("benchmark_source_manifest") else None,
            "benchmark_source_manifest_hash": manifest.get("benchmark_source_manifest_hash"),
            "input_manifest_hash": input_hash,
            "quality_report": _json_param(quality),
            "quality_report_hash": _quality_report_hash(quality),
            "code_version": metadata["code_version"],
            "git_commit": metadata["commit"],
            "git_dirty": metadata["dirty"],
            "diff_hash": metadata["diff_hash"],
        },
    )
    return quality


def _build_dataset_batched(dataset_id: str, *, batch_days: int = 5, force_rebuild: bool = False) -> dict[str, Any]:
    if batch_days < 1 or batch_days > 5:
        raise ValueError("batch_days 必须在 1..5 之间")
    kernel, repository, context = _source_service(dataset_id, date.today())
    try:
        manifest, boundaries, metadata = _prepare_manifest(dataset_id, repository, context)
        batches = _trading_batches(dataset_id, batch_days, repository, context)
        if force_rebuild:
            repository.delete_batch_checkpoints(dataset_id, context)
        results = []
        for index, (batch_start, batch_end, _days) in enumerate(batches, start=1):
            checkpoint = _batch_checkpoint(dataset_id, batch_start, repository, context)
            checkpoint_is_eligible = (
                not force_rebuild
                and checkpoint
                and checkpoint["status"] == "COMPLETED"
                and checkpoint["batch_end"] == batch_end
                and checkpoint["code_version"] == metadata["code_version"]
                and checkpoint["batch_hash"]
            )
            if checkpoint_is_eligible:
                expected_rows = int(checkpoint["row_count"] or 0)
                actual_rows = repository.execution_row_count(
                    dataset_id, batch_start, batch_end, context
                )
                if actual_rows == expected_rows:
                    results.append({"batch_start": batch_start, "batch_end": batch_end, "rows": expected_rows, "skipped": True})
                    _set_checkpoint(dataset_id, "BUILD", "RUNNING", progress_done=index, progress_total=len(batches), last_trade_date=batch_end, details={"mode": "date_batched_resumable", "batch_days": batch_days})
                    continue
            results.append(_build_execution_batch(dataset_id, batch_start, batch_end, boundaries, metadata["code_version"], repository, context))
            _set_checkpoint(dataset_id, "BUILD", "RUNNING", progress_done=index, progress_total=len(batches), last_trade_date=batch_end, details={"mode": "date_batched_resumable", "batch_days": batch_days})
        repository.rebuild_auxiliary(dataset_id, manifest, boundaries, context)
        result = _finalize_build(dataset_id, manifest, metadata, repository, context)
        result.update({"batch_days": batch_days, "batches": len(batches), "skipped_batches": sum(1 for item in results if item.get("skipped"))})
        return result
    finally:
        kernel.close()


@warehouse_write_job
def build_dataset_batched(
    dataset_id: str,
    *,
    batch_days: int = 5,
    force_rebuild: bool = False,
) -> dict[str, Any]:
    """Build one Dataset under the warehouse writer lock."""
    return _build_dataset_batched(
        dataset_id, batch_days=batch_days, force_rebuild=force_rebuild
    )
