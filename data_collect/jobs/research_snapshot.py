"""Research 快照 Manifest、构建、校验和冻结。

第一版快照粒度固定为：一个 ``snapshot_id`` 对应一个 ``research_cutoff_date``。
生成器只读取通过质量门禁的 PIT/Feature，使用 PostgreSQL REPEATABLE READ
事务读取一致输入；Research/Backtest 后续只能使用 FROZEN 快照。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from data_collect.ops.market_coverage import (
    assert_market_coverage_publishable,
    assert_market_coverage_range_publishable,
)
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.research_snapshot_repository import ResearchSnapshotRepository
from data_collect.data_access.repository.pit_validation_repository import PitValidationRepository
from data_collect.data_access.research_contract import (
    FOUNDATION_SCHEMA_VERSION,
    FOUNDATION_SCHEMA_VERSIONS,
    STRATEGY_NEUTRAL_SCHEMA_VERSION,
    assert_strategy_neutral_inputs,
    is_foundation_schema,
)


SCHEMA_VERSION = "research-v1.3"
FOUNDATION_FEATURE_CONTRACT = "feature-contract-v2.0"
LEGACY_V12_SCHEMA_VERSION = "research-v1.2"
REQUIRED_LINEAGE_GROUPS = (
    "universe", "tradeability", "price", "valuation", "financial",
    "shareholder_return", "lifecycle", "instrument", "market_facts",
    "fundamental_history",
)

def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"research-snapshot-{label}-{uuid.uuid4().hex}",
        job_name="research_snapshot", code_version="unknown",
        config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, ResearchSnapshotRepository(kernel), context


# 这些表名和字段均为代码内固定定义，不接受命令行直接拼接，避免把哈希范围变成隐式变化。
INPUTS = (
    {
        "schema": "pit", "table": "universe_daily",
        "columns": ("trade_date", "ts_code", "is_listed_asof", "is_st_asof", "is_suspended_asof", "has_price_history", "eligible_by_market", "quality_status", "source_asof_date"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code", "date_column": "trade_date",
    },
    {
        "schema": "pit", "table": "security_tradeability_daily",
        "columns": ("trade_date", "ts_code", "is_tradeable", "can_buy", "can_sell", "is_suspended", "limit_status", "quality_status", "source_asof_date"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code", "date_column": "trade_date",
    },
    {
        "schema": "feature", "table": "price_daily_v2",
        "columns": ("ts_code", "trade_date", "raw_close", "asof_close", "asof_adjustment_factor", "return_1d", "return_20d", "return_60d", "ma20", "ma60", "ma120", "volume_ratio_20d", "available_observation_count", "ma20_observation_count", "ma60_observation_count", "ma120_observation_count", "feature_set_version", "adjustment_mode", "quality_status", "source_asof_date"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code", "date_column": "trade_date",
    },
    {
        "schema": "feature", "table": "valuation_daily_v1",
        "columns": ("ts_code", "trade_date", "pe_ttm", "pb", "ps_ttm", "dv_ratio", "total_mv", "circ_mv", "pe_pct_3y", "pe_pct_5y", "pb_pct_3y", "pb_pct_5y", "is_pe_meaningful", "valuation_observation_count", "pe_window_start_date_5y", "pe_observation_count_5y", "has_full_pe_5y_history", "quality_status", "source_asof_date"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code", "date_column": "trade_date",
    },
    {
        "schema": "pit", "table": "financial_asof",
        "columns": ("decision_date", "ts_code", "report_date", "effective_announce_date", "version_available_date", "vintage_quality", "source_observed_at", "record_version", "source_record_key", "roe", "roa", "debt_to_assets", "revenue_yoy", "tr_yoy", "q_sales_yoy", "netprofit_yoy", "quality_status", "payload_hash"),
        "predicate": "decision_date = :research_cutoff_date",
        "where": "decision_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "decision_date, ts_code", "date_column": "decision_date",
    },
    {
        "schema": "std", "table": "daily_kline",
        "columns": ("stock_code", "trade_date", "open", "high", "low", "close", "volume", "amount"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, stock_code", "date_column": "trade_date",
    },
    {
        "schema": "std", "table": "security_status_daily",
        "columns": ("trade_date", "ts_code", "up_limit", "down_limit", "close_price", "limit_status", "quality_status"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code", "date_column": "trade_date",
    },
    {
        "schema": "feature", "table": "shareholder_return_v1",
        "columns": ("ts_code", "asof_date", "cash_div_12m", "dividend_event_count_3y", "dividend_year_count_3y", "quality_status"),
        "predicate": "asof_date = :research_cutoff_date",
        "where": "asof_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "asof_date, ts_code", "date_column": "asof_date",
    },
    {
        "schema": "public", "table": "tushare_stock_basic_raw_version",
        "columns": ("record_id", "ts_code", "business_key", "payload", "payload_hash", "fetched_at"),
        "predicate": "record_id <= :baseline_stock_basic_max_record_id",
        "where": "record_id <= %s",
        "params_from_manifest": lambda cutoff, manifest: (
            manifest["append_only_sources"]["tushare_stock_basic_raw_version"]["max_record_id"],
        ),
        "order": "record_id", "date_column": None,
    },
    {
        "schema": "std", "table": "security_lifecycle",
        "columns": ("ts_code", "name", "market", "list_status", "effective_from", "effective_to", "source", "source_record_key", "record_version", "quality_status"),
        "predicate": "effective_from <= :research_cutoff_date AND (effective_to IS NULL OR effective_to >= :research_cutoff_date)",
        "where": "effective_from <= %s AND (effective_to IS NULL OR effective_to >= %s)", "params": lambda cutoff: (cutoff, cutoff),
        "order": "ts_code, effective_from, effective_to", "date_column": "effective_from",
    },
)


def _inputs_for_schema(schema_version: str):
    from data_collect.data_access.repository.research_snapshot_repository import ResearchSnapshotRepository
    if not is_foundation_schema(schema_version):
        return INPUTS
    definitions = ResearchSnapshotRepository.foundation_inputs(INPUTS)
    if schema_version == STRATEGY_NEUTRAL_SCHEMA_VERSION:
        assert_strategy_neutral_inputs(definitions)
    return definitions


def _qualified(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def _canonical(value):
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _canonical(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    return value


def _row_bytes(row) -> bytes:
    return (json.dumps([_canonical(value) for value in row], ensure_ascii=False,
                       sort_keys=False, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n")


def _input_stats(definition: dict, cutoff: date, source_manifest: dict | None = None) -> dict:
    kernel, repository, context = _service("input-stats")
    try:
        return repository.input_stats(definition, cutoff, source_manifest, context, _row_bytes)
    finally:
        kernel.close()

def _manifest_hash(stats: list[dict]) -> str:
    payload = []
    for item in stats:
        payload.append({key: _canonical(value) for key, value in item.items() if key not in {"created_at"}})
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

def _git_metadata() -> dict:
    root = Path(__file__).resolve().parents[2]
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip()
        status = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=root, text=True,
        )
        dirty = bool(status.strip())
        diff_hash = None
        if dirty:
            digest = hashlib.sha256()
            digest.update(subprocess.check_output(
                ["git", "diff", "--binary", "HEAD"], cwd=root
            ))
            untracked = subprocess.check_output(
                ["git", "ls-files", "--others", "--exclude-standard", "-z"],
                cwd=root,
            )
            for raw_path in untracked.split(b"\0"):
                if not raw_path:
                    continue
                path = root / raw_path.decode("utf-8", "surrogateescape")
                digest.update(raw_path)
                digest.update(b"\0")
                digest.update(path.read_bytes())
            diff_hash = digest.hexdigest()
        return {
            "commit": commit,
            "dirty": dirty,
            "diff_hash": diff_hash,
            "code_version": commit + ("+dirty" if dirty else ""),
        }
    except (OSError, subprocess.CalledProcessError):
        return {
            "commit": None, "dirty": True,
            "diff_hash": "unavailable", "code_version": "unknown+dirty",
        }


def _sha256_file(path: str | Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


def _quality_report(
    cutoff: date | None = None,
    schema_version: str = SCHEMA_VERSION,
    baseline=None,
) -> tuple[dict, str]:
    # A validated release already contains quality evidence for its
    # immutable baseline. Reuse it instead of rescanning large legacy tables
    # once per Snapshot; build/validate still perform cutoff-specific checks.
    if is_foundation_schema(schema_version) and baseline is not None:
        stored = baseline[7] if len(baseline) > 7 else None
        if isinstance(stored, dict) and stored.get("failures") == {}:
            return dict(stored), baseline[3]
    from data_collect.jobs.tushare_release import _quality_gate

    report = _quality_gate(
        cutoff, include_foundation_v2=is_foundation_schema(schema_version)
    )
    encoded = json.dumps(
        _canonical(report), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )
    return report, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _stored_quality_cutoff(snapshot_cutoff: date, stored_report: dict | None) -> date:
    raw = (stored_report or {}).get("cutoff_date")
    quality_cutoff = date.fromisoformat(raw) if raw else snapshot_cutoff
    if quality_cutoff < snapshot_cutoff:
        raise RuntimeError(
            "\u8d28\u91cf\u8bc1\u636e\u622a\u6b62\u65e5\u65e9\u4e8e\u7814\u7a76\u622a\u6b62\u65e5: "
            f"quality={quality_cutoff}, snapshot={snapshot_cutoff}"
        )
    return quality_cutoff


def _current_quality_for_manifest(manifest: dict) -> tuple[dict, str]:
    return _quality_report(
        _stored_quality_cutoff(manifest["cutoff"], manifest.get("quality_report")),
        manifest["schema_version"],
    )


def _assert_baseline_schema(baseline, snapshot_schema_version: str) -> None:
    if (
        is_foundation_schema(snapshot_schema_version)
        and baseline[6] != FOUNDATION_FEATURE_CONTRACT
    ):
        raise RuntimeError(
            "Foundation research snapshots require a feature-contract-v2.0 validated release; "
            f"actual release schema={baseline[6]!r}"
        )


def _quality_evidence_cutoff(
    baseline, snapshot_cutoff: date, snapshot_schema_version: str
) -> date:
    if is_foundation_schema(snapshot_schema_version):
        return baseline[1]
    return snapshot_cutoff


def _assert_adj_factor_vintage(cutoff: date) -> None:
    kernel, repository, context = _service("price-vintage")
    try:
        repository.price_vintage(cutoff, LEGACY_V12_SCHEMA_VERSION, context)
    finally:
        kernel.close()

def _assert_price_vintage_for_schema(cutoff: date, schema_version: str) -> None:
    kernel, repository, context = _service("price-vintage")
    try:
        repository.price_vintage(cutoff, schema_version, context)
    finally:
        kernel.close()

def _assert_price_vintage_for_dates(cutoffs: list[date], snapshot_ids: list[str], schema_version: str = SCHEMA_VERSION) -> None:
    kernel, repository, context = _service("price-vintage-batch")
    try:
        repository.price_vintage_dates(cutoffs, snapshot_ids, schema_version, context)
    finally:
        kernel.close()

def _baseline(cutoff: date, tag: str, *, verify_source_manifest: bool = True):
    kernel, repository, context = _service("baseline")
    try:
        return repository.baseline(cutoff, tag, context, verify_source_manifest)
    finally:
        kernel.close()

def create(cutoff: str, baseline_tag: str, schema_version: str = SCHEMA_VERSION, notes: str = "") -> str:
    cutoff_date = date.fromisoformat(cutoff)
    baseline = _baseline(cutoff_date, baseline_tag)
    _assert_baseline_schema(baseline, schema_version)
    _assert_price_vintage_for_schema(cutoff_date, schema_version)
    quality_report, quality_hash = _quality_report(
        _quality_evidence_cutoff(baseline, cutoff_date, schema_version),
        schema_version, baseline,
    )
    if quality_report["failures"]:
        raise RuntimeError(f"quality gate failed; cannot create Research Snapshot: {quality_report['failures']}")
    metadata = _git_metadata()
    snapshot_id = str(uuid.uuid4())
    kernel, repository, context = _service("create")
    try:
        repository.create_manifest(
            snapshot_id, cutoff_date, baseline_tag, schema_version, metadata,
            quality_hash, baseline[3], baseline[4], quality_report, notes, context,
        )
    finally:
        kernel.close()
    return snapshot_id

def existing_snapshot(cutoff: str, baseline_tag: str, schema_version: str = SCHEMA_VERSION):
    cutoff_date = date.fromisoformat(cutoff)
    kernel, repository, context = _service("existing-snapshot")
    try:
        return repository.existing_snapshot(cutoff_date, baseline_tag, schema_version, context)
    finally:
        kernel.close()

def _manifest(snapshot_id: str, *, for_update: bool = False):
    kernel, repository, context = _service("manifest")
    try:
        return repository.manifest(snapshot_id, context, for_update=for_update)
    finally:
        kernel.close()

def _stock_snapshot_sql():
    return ResearchSnapshotRepository.stock_snapshot_sql()

def _lineage_sql():
    return ResearchSnapshotRepository.lineage_sql()

def _stock_snapshot_batch_sql():
    return ResearchSnapshotRepository.stock_snapshot_batch_sql()

def _lineage_batch_sql():
    return ResearchSnapshotRepository.lineage_batch_sql()

def create_snapshots_batch(
    cutoff_dates: list[date], baseline_tag: str,
    schema_version: str = SCHEMA_VERSION, notes_prefix: str = "",
    verify_source_manifest: bool = True,
) -> tuple[dict[str, str], ...]:
    dates = tuple(sorted(set(cutoff_dates)))
    if not dates:
        return ()
    baseline = _baseline(max(dates), baseline_tag, verify_source_manifest=verify_source_manifest)
    _assert_baseline_schema(baseline, schema_version)
    quality_report, _ = _quality_report(
        _quality_evidence_cutoff(baseline, max(dates), schema_version),
        schema_version, baseline,
    )
    if quality_report["failures"]:
        raise RuntimeError(f"quality gate failed: {quality_report['failures']}")
    metadata = _git_metadata()
    kernel, repository, context = _service("create-batch")
    try:
        existing = repository.existing_snapshots(list(dates), baseline_tag, schema_version, context)
        result: dict[str, str] = {}
        candidates = []
        for cutoff in dates:
            if cutoff in existing:
                snapshot_id, status = existing[cutoff]
                if status == "BUILDING":
                    raise RuntimeError(f"snapshot is already BUILDING: {snapshot_id}")
                result[cutoff.isoformat()] = snapshot_id
                continue
            snapshot_id = str(uuid.uuid4())
            report = dict(quality_report)
            report["cutoff_date"] = cutoff.isoformat()
            encoded = json.dumps(_canonical(report), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            candidates.append({
                "snapshot_id": snapshot_id, "research_cutoff_date": cutoff,
                "data_baseline_tag": baseline_tag, "schema_version": schema_version,
                "metadata": metadata, "quality_report_hash": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
                "baseline_quality_report_hash": baseline[3], "baseline_source_manifest_hash": baseline[4],
                "quality_report": report, "notes": (f"{notes_prefix} cutoff={cutoff.isoformat()}").strip(),
            })
            result[cutoff.isoformat()] = snapshot_id
        if candidates:
            repository.create_manifests(candidates, context)
        return tuple({"snapshot_id": result[cutoff.isoformat()], "research_cutoff_date": cutoff.isoformat()} for cutoff in dates)
    finally:
        kernel.close()

def build_snapshots_batch(
    snapshot_ids: list[str],
    *,
    baseline=None,
    quality_reports: dict[str, tuple[dict, str]] | None = None,
) -> None:
    """Build several snapshots through one typed Repository path per snapshot."""
    ids = tuple(str(value) for value in snapshot_ids)
    if not ids:
        return
    metadata = _git_metadata()
    kernel, repository, context = _service("build-batch")
    try:
        manifests = [_manifest(snapshot_id) for snapshot_id in ids]
        baseline_tags = {item["baseline"] for item in manifests}
        schema_versions = {item["schema_version"] for item in manifests}
        if len(baseline_tags) != 1 or len(schema_versions) != 1:
            raise ValueError("同一批次必须使用同一 baseline_tag/schema_version")
        baseline_value = baseline or _baseline(max(item["cutoff"] for item in manifests), next(iter(baseline_tags)))
        schema_version = next(iter(schema_versions))
        _assert_price_vintage_for_dates([item["cutoff"] for item in manifests], [], schema_version)
        for manifest in manifests:
            if manifest["status"] not in {"DRAFT", "FAILED"}:
                raise RuntimeError(f"批量构建只允许 DRAFT/FAILED Snapshot: {manifest['snapshot_id']}")
            if baseline_value[3] != manifest["baseline_quality_hash"] or baseline_value[4] != manifest["baseline_source_manifest_hash"]:
                raise RuntimeError("数据基线 Manifest 已变化，拒绝批量构建")
            report, quality_hash = quality_reports[str(manifest["snapshot_id"])] if quality_reports else _quality_report(manifest["cutoff"], schema_version)
            if report["failures"]:
                raise RuntimeError(f"质量门禁未通过: {manifest['cutoff']} {report['failures']}")
            repository.build_snapshot(
                str(manifest["snapshot_id"]), manifest, baseline_value, report, quality_hash,
                metadata, _inputs_for_schema(schema_version), _row_bytes, _manifest_hash, context,
            )
    finally:
        kernel.close()


def build(snapshot_id: str) -> None:
    metadata = _git_metadata()
    manifest = _manifest(snapshot_id)
    _assert_price_vintage_for_schema(manifest["cutoff"], manifest["schema_version"])
    if manifest["status"] not in {"DRAFT", "FAILED"}:
        raise RuntimeError(f"只有 DRAFT/FAILED 快照可构建: {snapshot_id} ({manifest['status']})")
    quality_report, quality_hash = _current_quality_for_manifest(manifest)
    if quality_report["failures"]:
        raise RuntimeError(f"质量门禁未通过，不能构建快照: {quality_report['failures']}")
    baseline = _baseline(manifest["cutoff"], manifest["baseline"])
    if baseline[3] != manifest["baseline_quality_hash"] or baseline[4] != manifest["baseline_source_manifest_hash"]:
        raise RuntimeError("数据基线 Manifest 已变化，拒绝使用旧 Manifest 构建")
    kernel, repository, context = _service("build")
    try:
        repository.build_snapshot(
            snapshot_id, manifest, baseline, quality_report, quality_hash, metadata,
            _inputs_for_schema(manifest["schema_version"]), _row_bytes, _manifest_hash, context,
        )
    finally:
        kernel.close()


def inspect_snapshots_batch(
    snapshot_ids: list[str],
    *,
    baseline=None,
    quality_reports: dict[str, tuple[dict, str]] | None = None,
    allowed_statuses: tuple[str, ...] = ("BUILDING", "VALIDATED", "FROZEN"),
    verify_source_manifest: bool = False,
) -> dict[str, list[dict]]:
    """Run the snapshot acceptance checks without changing lifecycle state.

    Callers must keep batches small.  Every stock/lineage query is scoped by
    ``snapshot_id`` and uses the existing primary/lookup indexes; this helper
    intentionally does not join or aggregate the whole historical series in a
    single statement.
    """
    ids = tuple(str(value) for value in snapshot_ids)
    if not ids:
        return {}
    if len(ids) > 100:
        raise ValueError("snapshot inspection batches are limited to 100")
    allowed = frozenset(str(value) for value in allowed_statuses)
    if not allowed:
        raise ValueError("allowed_statuses must not be empty")

    kernel, repository, context = _service("inspect-batch")
    try:
        manifests = [repository.manifest(snapshot_id, context) for snapshot_id in ids]
        baseline_tags = {item["baseline"] for item in manifests}
        schema_versions = {item["schema_version"] for item in manifests}
        if len(baseline_tags) != 1 or len(schema_versions) != 1:
            raise ValueError("同一验收批次必须使用同一 baseline_tag/schema_version")
        schema_version = next(iter(schema_versions))
        baseline_value = baseline or _baseline(
            max(item["cutoff"] for item in manifests),
            next(iter(baseline_tags)),
            verify_source_manifest=verify_source_manifest,
        )
        _assert_baseline_schema(baseline_value, schema_version)

        issues_by_snapshot: dict[str, list[dict]] = {}
        try:
            repository.price_vintage_dates(
                [item["cutoff"] for item in manifests], list(ids), schema_version, context,
            )
        except Exception as exc:
            issues_by_snapshot["__batch__"] = [{"price_vintage": str(exc)}]
        try:
            PitValidationRepository(kernel).verify_batch(
                [item["cutoff"] for item in manifests], list(ids), schema_version, context,
            )
        except Exception as exc:
            issues_by_snapshot.setdefault("__batch__", []).append(
                {"pit_cutoff": str(exc)}
            )

        for manifest in manifests:
            snapshot_id = str(manifest["snapshot_id"])
            issues: list[dict] = []
            if manifest["status"] not in allowed:
                issues.append({
                    "unexpected_status": {
                        "actual": manifest["status"],
                        "allowed": sorted(allowed),
                    }
                })
            if (
                baseline_value[3] != manifest["baseline_quality_hash"]
                or baseline_value[4] != manifest["baseline_source_manifest_hash"]
            ):
                issues.append({"baseline_hash_changed": True})
            report, quality_hash = (
                quality_reports[snapshot_id]
                if quality_reports and snapshot_id in quality_reports
                else _quality_report(manifest["cutoff"], schema_version, baseline_value)
            )
            if report["failures"] or quality_hash != manifest["quality_hash"]:
                issues.append({
                    "quality_report_changed": {
                        "failures": report["failures"],
                        "stored_hash": manifest["quality_hash"],
                        "current_hash": quality_hash,
                    }
                })
            issues.extend(repository.check_snapshot(
                manifest,
                baseline_value[5],
                _inputs_for_schema(schema_version),
                _row_bytes,
                _manifest_hash,
                len(REQUIRED_LINEAGE_GROUPS),
                context,
            ))
            if issues:
                issues_by_snapshot[snapshot_id] = issues
        return issues_by_snapshot
    finally:
        kernel.close()


def validate_snapshots_batch(
    snapshot_ids: list[str],
    *,
    baseline=None,
    quality_reports: dict[str, tuple[dict, str]] | None = None,
) -> None:
    """Validate several BUILDING snapshots through the typed Repository."""
    ids = tuple(str(value) for value in snapshot_ids)
    if not ids:
        return
    kernel, repository, context = _service("validate-batch")
    try:
        manifests = [repository.manifest(snapshot_id, context) for snapshot_id in ids]
        baseline_tags = {item["baseline"] for item in manifests}
        schema_versions = {item["schema_version"] for item in manifests}
        if len(baseline_tags) != 1 or len(schema_versions) != 1:
            raise ValueError("同一批次必须使用同一 baseline_tag/schema_version")
        baseline_value = baseline or _baseline(max(item["cutoff"] for item in manifests), next(iter(baseline_tags)))
        schema_version = next(iter(schema_versions))
        _assert_price_vintage_for_dates([item["cutoff"] for item in manifests], ids, schema_version)
        issues_by_date = {}
        for manifest in manifests:
            if manifest["status"] != "BUILDING":
                raise RuntimeError(f"只有 BUILDING 快照可批量校验: {manifest['snapshot_id']} ({manifest['status']})")
            if baseline_value[3] != manifest["baseline_quality_hash"] or baseline_value[4] != manifest["baseline_source_manifest_hash"]:
                raise RuntimeError("数据基线质量哈希已变化，拒绝批量校验")
            report, quality_hash = quality_reports[str(manifest["snapshot_id"])] if quality_reports else _quality_report(manifest["cutoff"], schema_version)
            if report["failures"] or quality_hash != manifest["quality_hash"]:
                raise RuntimeError(f"质量报告已变化或门禁失败: {manifest['cutoff']}")
            issues = _check_snapshot(manifest, baseline_value[5])
            pit_report = _pit_report(manifest["cutoff"], str(manifest["snapshot_id"]))
            if not pit_report["passed"]:
                issues.append({"pit_cutoff": pit_report})
            if issues:
                issues_by_date[str(manifest["cutoff"])] = issues
            else:
                repository.validate_snapshot(manifest, baseline_value, quality_hash, [], context)
        if issues_by_date:
            raise RuntimeError(f"Research 快照批量校验失败: {issues_by_date}")
    except Exception as exc:
        for snapshot_id in ids:
            try:
                manifest = repository.manifest(snapshot_id, context)
                repository.mark_failed(snapshot_id, manifest["cutoff"], str(exc), context)
            except Exception:
                pass
        raise
    finally:
        kernel.close()


def _check_snapshot(manifest: dict, source_manifest: dict) -> list[dict]:
    kernel, repository, context = _service("check-snapshot")
    try:
        return repository.check_snapshot(
            manifest, source_manifest, _inputs_for_schema(manifest["schema_version"]),
            _row_bytes, _manifest_hash, len(REQUIRED_LINEAGE_GROUPS), context,
        )
    finally:
        kernel.close()

def validate(snapshot_id: str) -> None:
    manifest = _manifest(snapshot_id)
    _assert_price_vintage_for_schema(manifest["cutoff"], manifest["schema_version"])
    if manifest["status"] != "BUILDING":
        raise RuntimeError(f"只有 BUILDING 快照可校验: {snapshot_id} ({manifest['status']})")
    quality_report, quality_hash = _quality_report(manifest["cutoff"], manifest["schema_version"])
    baseline = _baseline(manifest["cutoff"], manifest["baseline"])
    if baseline[3] != manifest["baseline_quality_hash"] or baseline[4] != manifest["baseline_source_manifest_hash"]:
        raise RuntimeError("数据基线质量哈希已变化，必须重新创建快照")
    if quality_report["failures"] or quality_hash != manifest["quality_hash"]:
        raise RuntimeError("质量报告已变化或门禁失败，必须重新 build 快照")
    issues = _check_snapshot(manifest, baseline[5])
    pit_report = _pit_report(manifest["cutoff"], snapshot_id)
    if not pit_report["passed"]:
        issues.append({"pit_cutoff": pit_report})
    kernel, repository, context = _service("validate")
    try:
        if issues:
            repository.mark_failed(snapshot_id, manifest["cutoff"], str(issues), context)
            raise RuntimeError(f"Research 快照校验失败: {issues}")
        repository.validate_snapshot(manifest, baseline, quality_hash, [], context)
    finally:
        kernel.close()


def freeze(snapshot_id: str, code_archive_path: str | None = None) -> None:
    metadata = _git_metadata()
    if metadata["dirty"] and not code_archive_path:
        raise RuntimeError(
            "正式冻结要求 Git 工作区干净；脏工作区必须显式提供存在的 code_archive_path"
        )
    if code_archive_path and not Path(code_archive_path).exists():
        raise FileNotFoundError(f"代码归档不存在: {code_archive_path}")
    archive_hash = archive_size = None
    if code_archive_path:
        archive_hash, archive_size = _sha256_file(code_archive_path)
    manifest = _manifest(snapshot_id)
    _assert_price_vintage_for_schema(manifest["cutoff"], manifest["schema_version"])
    if manifest["status"] != "VALIDATED":
        raise RuntimeError(f"只有 VALIDATED 快照可冻结: {snapshot_id} ({manifest['status']})")
    baseline = _baseline(manifest["cutoff"], manifest["baseline"])
    if baseline[3] != manifest["baseline_quality_hash"] or baseline[4] != manifest["baseline_source_manifest_hash"]:
        raise RuntimeError("数据基线哈希已变化，拒绝冻结")
    quality_report, quality_hash = _quality_report(manifest["cutoff"], manifest["schema_version"])
    if quality_report["failures"] or quality_hash != manifest["quality_hash"]:
        raise RuntimeError("质量报告已变化或门禁失败，必须重新 build/validate 快照")
    pit_report = _pit_report(manifest["cutoff"], snapshot_id)
    if not pit_report["passed"]:
        raise RuntimeError(f"正式冻结前 PIT 截面校验失败: {pit_report['issues']}")
    assert_market_coverage_publishable(manifest["cutoff"])
    kernel, repository, context = _service("freeze")
    try:
        repository.freeze_snapshot(
            manifest, metadata,
            {"path": code_archive_path, "sha256": archive_hash, "size": archive_size}, context,
        )
    finally:
        kernel.close()


def _assert_pit_cutoff_dates_batch(
    cutoffs: list[date], snapshot_ids: list[str],
    schema_version: str = SCHEMA_VERSION,
) -> None:
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = PitValidationRepository(kernel)
    context = RunContext(
        run_id=f"snapshot-pit-batch-{uuid.uuid4().hex}", job_name="research_snapshot",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    try:
        repository.verify_batch(cutoffs, snapshot_ids, schema_version, context)
    finally:
        kernel.close()

def _pit_report(cutoff: date, snapshot_id: str | None = None) -> dict:
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = PitValidationRepository(kernel)
    context = RunContext(
        run_id=f"snapshot-pit-{uuid.uuid4().hex}", job_name="research_snapshot",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    try:
        return repository.verify(cutoff, snapshot_id, context)
    finally:
        kernel.close()

def freeze_snapshots_batch(
    snapshot_ids: list[str],
    code_archive_path: str | None = None,
    *,
    baseline=None,
    quality_report: dict | None = None,
    verify_source_manifest: bool = False,
) -> None:
    """Freeze validated snapshots sharing one baseline with bounded IO.

    This is the production path for a historical series.  It keeps the
    per-snapshot immutable rows and audit metadata, while avoiding one full
    Raw-manifest/quality scan for every cutoff date.
    """
    ids = tuple(str(value) for value in snapshot_ids)
    if not ids:
        return
    metadata = _git_metadata()
    if metadata["dirty"] and not code_archive_path:
        raise RuntimeError("批量正式冻结要求 Git 工作区干净")
    if code_archive_path and not Path(code_archive_path).exists():
        raise FileNotFoundError(f"代码归档不存在: {code_archive_path}")
    archive_hash = archive_size = None
    if code_archive_path:
        archive_hash, archive_size = _sha256_file(code_archive_path)

    kernel, repository, context = _service("freeze-batch")
    try:
        manifests = [repository.manifest(snapshot_id, context) for snapshot_id in ids]
        if len(manifests) != len(ids) or any(item["status"] != "VALIDATED" for item in manifests):
            raise RuntimeError("批量冻结只允许存在且状态为 VALIDATED 的 Snapshot")
        baseline_tags = {item["baseline"] for item in manifests}
        schema_versions = {item["schema_version"] for item in manifests}
        if len(baseline_tags) != 1 or len(schema_versions) != 1:
            raise ValueError("批量冻结必须使用同一 baseline_tag/schema_version")
        cutoffs = [item["cutoff"] for item in manifests]
        baseline_value = baseline or _baseline(
            max(cutoffs), next(iter(baseline_tags)),
            verify_source_manifest=verify_source_manifest,
        )
        for manifest in manifests:
            if baseline_value[3] != manifest["baseline_quality_hash"] or baseline_value[4] != manifest["baseline_source_manifest_hash"]:
                raise RuntimeError("Snapshot 绑定的基线哈希已变化，拒绝冻结")
            if not manifest["quality_hash"] or not manifest["input_hash"]:
                raise RuntimeError("Snapshot 缺少质量哈希或输入哈希")
        schema_version = next(iter(schema_versions))
        _assert_price_vintage_for_dates(cutoffs, list(ids), schema_version)
        report = quality_report
        if report is None:
            report, _ = _quality_report(max(cutoffs), schema_version, baseline_value)
        if report["failures"]:
            raise RuntimeError(f"质量门禁未通过，拒绝批量冻结: {report['failures']}")
        assert_market_coverage_range_publishable(min(cutoffs), max(cutoffs))
        _assert_pit_cutoff_dates_batch(cutoffs, list(ids), schema_version)
        for manifest in manifests:
            repository.freeze_snapshot(
                manifest, metadata,
                {"path": code_archive_path, "sha256": archive_hash, "size": archive_size}, context,
            )
    finally:
        kernel.close()


def status() -> list[tuple]:
    kernel, repository, context = _service("status")
    try:
        rows = repository.status_rows(context)
    finally:
        kernel.close()
    for row in rows:
        print(" | ".join("" if value is None else str(value) for value in row))
    return rows

def main():
    parser = argparse.ArgumentParser(description="Research 快照 Manifest 管理")
    sub = parser.add_subparsers(dest="command", required=True)
    create_parser = sub.add_parser("create")
    create_parser.add_argument("--cutoff-date", required=True, help="YYYY-MM-DD")
    create_parser.add_argument("--baseline-tag", required=True)
    create_parser.add_argument("--schema-version", default=SCHEMA_VERSION)
    create_parser.add_argument("--notes", default="")
    build_parser = sub.add_parser("build")
    build_parser.add_argument("--snapshot-id", required=True)
    validate_parser = sub.add_parser("validate")
    validate_parser.add_argument("--snapshot-id", required=True)
    freeze_parser = sub.add_parser("freeze")
    freeze_parser.add_argument("--snapshot-id", required=True)
    freeze_parser.add_argument("--code-archive-path")
    sub.add_parser("status")
    args = parser.parse_args()
    if args.command == "create":
        print(create(args.cutoff_date, args.baseline_tag, args.schema_version, args.notes))
    elif args.command == "build":
        build(args.snapshot_id)
    elif args.command == "validate":
        validate(args.snapshot_id)
    elif args.command == "freeze":
        freeze(args.snapshot_id, args.code_archive_path)
    else:
        status()


if __name__ == "__main__":
    main()
