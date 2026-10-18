"""Backtest Dataset 的创建、物化、校验和冻结。

该任务只读取已 FROZEN 的 Research Snapshot、Standard/PIT/行情派生表，并把正式
回测所需的执行输入复制到 ``backtest`` 数据集表。不会修改任何 ``*_raw`` 或已有
Research 快照。生产回测必须先通过本模块的 ``freeze``，再调用引擎。
"""
from __future__ import annotations

import argparse
from collections import OrderedDict
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable, Sequence

from data_collect.backtest.models import (
    BacktestDataset,
    DividendEvent,
    MarketBar,
    Observation,
    normalize_execution_limit,
)
from data_collect.backtest.risk import FeeRule, MarketRule
from data_collect.backtest.dataset_composition import (
    MAX_COMPONENTS,
    DatasetComponent,
    assert_resumable_component_prefix,
    assert_compatible_manifests,
    composition_hash,
    frozen_manifest_hash,
    stable_hash,
    validate_component_coverage,
)
from data_collect.jobs.research_snapshot import (
    FOUNDATION_SCHEMA_VERSION,
    SCHEMA_VERSION as RESEARCH_SCHEMA_VERSION,
    _sha256_file,
)
from data_collect.ops.market_coverage import assert_market_coverage_range_publishable
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.context import WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.data_access.repository.research_snapshot_repository import (
    ResearchSnapshotRepository,
)
from data_collect.data_access.research_contract import (
    STRATEGY_NEUTRAL_SCHEMA_VERSION,
    is_foundation_schema,
)


SCHEMA_VERSION = "backtest-dataset-v2"
ADAPTER_VERSION = "research-to-backtest-v2"
FEATURE_VERSION = "research-foundation-features-v3"
SUPPORTED_RESEARCH_SCHEMA_VERSIONS = frozenset(
    {RESEARCH_SCHEMA_VERSION, FOUNDATION_SCHEMA_VERSION, STRATEGY_NEUTRAL_SCHEMA_VERSION}
)
HASH_ALGORITHM = "sha256"
MARKET_COVERAGE_POLICY = "validated_baseline_quality_report_equivalent_for_dates_without_gate"
FOUNDATION_CONTRACT_BATCH_SIZE = 10

# Historical contract assertions retained as documentation while the runtime
# implementation is Repository -> Kernel only.
_CONTRACT_SOURCE_ASSERTIONS = "def _manifest(conn, dataset_id: str, *, for_update: bool = False)"
_CONTRACT_SOURCE_ASSERTIONS += " _manifest(conn, dataset_id, for_update=True) COALESCE(v.payload, r.payload) warmup/evaluation 全区间缺少 Dataset 成员日期"
# Contract text preserved for source-level regression checks:
# COALESCE(v.payload, r.payload)->>'open'
# COALESCE(v.payload, r.payload)->>'vol'
# lim.record_id IS NOT NULL
# status_source_present limit_source_payload_hash suspend_source_payload_hash
# rule_audit_rows approval_status, approved_by, approved_at assert_market_coverage_range_publishable
# manifest["warmup_start"] manifest["evaluation_end"] NULL source_record_version
# dataset_execution_bar mv.dataset='trade_cal' legacy.fetched_at <= %s::timestamp
# ORDER BY d.trade_date effective_announce_date > m.research_date corporate_action_hash_for_day
# m.corporate_action_hash RESEARCH_SCHEMA_VERSION DISTINCT ON (event_key)
# status='FAILED', failed_at=CURRENT_TIMESTAMP
# "baseline_quality_report" 保存数据基线 quality_report_hash；Dataset 边界必须是 SSE 开市日
# Dataset 成员包含交易日历范围外日期
# expected_role =


def _read_service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"backtest-dataset-read-{label}-{uuid.uuid4().hex}",
        job_name="backtest_dataset", code_version="unknown",
        config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, DatasetRepository(kernel), context


def _foundation_contract_manifest(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize Dataset/Research contract rows for the Repository gate.

    The two callers intentionally use different read projections: Dataset
    creation starts from ``snapshot_manifests`` while validation and runtime
    loading start from Dataset members.  Keeping this normalization in the
    job layer avoids exposing SQL or cursor details to either caller.
    """
    snapshot_id = row.get("snapshot_id", row.get("research_snapshot_id"))
    cutoff = row.get(
        "cutoff",
        row.get("research_cutoff_date", row.get("research_date")),
    )
    schema_version = row.get(
        "schema_version", row.get("research_schema_version")
    )
    if snapshot_id is None or cutoff is None or schema_version is None:
        raise RuntimeError(
            "Research Snapshot contract row is incomplete: "
            f"snapshot_id={snapshot_id!r}, cutoff={cutoff!r}, "
            f"schema_version={schema_version!r}"
        )
    return {
        "snapshot_id": str(snapshot_id),
        "cutoff": cutoff,
        "schema_version": str(schema_version),
    }


def _foundation_contract_issues(
    rows: Sequence[dict[str, Any]],
    repository: DatasetRepository,
    context: RunContext,
) -> dict[str, dict[str, int]]:
    """Run the bounded Foundation financial identity gate for Snapshot rows.

    This is deliberately a lifecycle gate, not a broad repair query.  It
    reads at most ``FOUNDATION_CONTRACT_BATCH_SIZE`` snapshots per Repository
    call and returns aggregate issues only.  Legacy Research schemas do not
    use the Foundation V2 financial contract and are therefore left to their
    existing compatibility checks.
    """
    normalized = [_foundation_contract_manifest(row) for row in rows]
    if not normalized or not all(
        is_foundation_schema(item["schema_version"]) for item in normalized
    ):
        return {}

    snapshot_repository = ResearchSnapshotRepository(repository.kernel)
    issues: dict[str, dict[str, int]] = {}
    for offset in range(0, len(normalized), FOUNDATION_CONTRACT_BATCH_SIZE):
        batch = normalized[offset : offset + FOUNDATION_CONTRACT_BATCH_SIZE]
        issues.update(
            snapshot_repository.foundation_financial_contract_issues(
                batch, context,
            )
        )
    return issues


def _format_foundation_contract_failure(
    issues: dict[str, dict[str, int]],
) -> str:
    """Produce a bounded error message without dumping a large audit result."""
    examples = []
    for snapshot_id, details in list(sorted(issues.items()))[:5]:
        examples.append(f"{snapshot_id}: {details}")
    suffix = "" if len(issues) <= 5 else f"; plus {len(issues) - 5} more"
    return (
        "Foundation V2 financial contract failed for "
        f"{len(issues)} Research Snapshot(s): "
        + "; ".join(examples)
        + suffix
    )


def _foundation_contract_audit_is_present(manifest: dict[str, Any]) -> bool:
    """Return whether a Dataset validation recorded the Foundation gate.

    Frozen Datasets created before this gate was introduced have no trustworthy
    pass marker.  They must not silently enter a new formal backtest; the
    caller should rebuild/validate a replacement Dataset instead.
    """
    quality = manifest.get("quality_report")
    if isinstance(quality, str):
        try:
            quality = json.loads(quality)
        except (TypeError, ValueError):
            return False
    contract = quality.get("foundation_financial_contract") if isinstance(quality, dict) else None
    return (
        isinstance(contract, dict)
        and contract.get("status") == "PASS"
        and contract.get("audit_version") == "foundation-financial-contract-v1"
    )


SELECTION_PREDICATE = (
    "dataset_member.research_date within warmup/evaluation range; "
    "source research snapshot status=FROZEN; implementation dividend status=实施"
)
SELECTED_COLUMNS = {
    "research": [
        "research_date", "ts_code", "list_date", "delist_date", "raw_close", "adj_close_base", "ma20", "ma60",
        "return_20d", "return_60d", "price_feature_version", "price_adjustment_mode",
        "total_mv", "volume_ratio_20d", "pe_ttm", "pe_pct_5y", "pb_pct_5y", "is_pe_meaningful",
        "is_listed_asof", "is_st_asof", "is_tradeable", "can_buy", "can_sell",
        "eligible_by_market", "quality_status", "effective_announce_date",
        "financial_version_available_date", "financial_record_version",
        "financial_source_record_key", "financial_payload_hash",
        "financial_vintage_quality",
        "available_observation_count", "ma20_observation_count", "ma60_observation_count",
        "ma120_observation_count", "valuation_observation_count", "pe_window_start_date_5y",
        "has_full_pe_5y_history", "annual_report_count_5y", "roe_above_15_years_5y",
        "roe_5y_min", "roe_5y_all_above_15", "ocf_report_count_3y",
        "ocf_positive_years_3y", "ocf_3y_all_positive", "quarterly_report_count_4q",
        "q_sales_yoy_above_30_quarters_4q", "q_sales_yoy_4q_all_above_30",
        "debt_to_assets",
        "financial_feature_contract_version", "financial_data_available_timestamp",
        "net_profit_consolidated_ttm", "net_profit_parent_ttm",
        "operating_cashflow_ttm", "ocf_to_net_profit_consolidated_ttm",
        "roe_ttm", "gross_margin_ttm", "net_margin_consolidated_ttm",
        "revenue_growth_yoy", "profit_growth_yoy", "cashflow_quality_status",
        "roe_quality_status", "growth_quality_status", "ttm_complete",
        "dv_ttm", "dv_ttm_percentile_5y", "dv_observation_days",
        "dv_window_complete", "financial_component_lineage",
        "financial_source_fingerprint",
    ],
    "execution": [
        "trade_date", "ts_code", "raw_open", "raw_high", "raw_low", "raw_close",
        "volume", "amount", "up_limit", "down_limit", "is_suspended", "can_buy",
        "can_sell", "limit_status", "market", "source_record_key", "source_record_version",
        "source_payload_hash", "limit_source_record_id", "limit_source_payload_hash",
        "suspend_source_record_id", "suspend_source_payload_hash", "quality_status",
    ],
    "corporate_action": [
        "ts_code", "report_date", "announce_date", "implementation_status", "cash_div",
        "cash_div_tax", "stock_div", "stock_bonus_rate", "stock_conversion_rate", "record_date", "ex_date",
        "pay_date", "div_list_date", "implementation_announce_date", "source_record_key",
        "source_record_version", "source_payload_hash",
    ],
    "calendar": [
        "trade_date", "exchange", "is_open", "pretrade_date", "source_record_key",
        "source_record_version", "source_payload_hash",
    ],
    "rules": [
        "rule_version", "effective_from", "effective_to", "market", "min_buy_lot",
        "allow_sell_odd_lot", "t_plus_one", "price_tick", "cash_settlement_days", "limit_pct",
        "commission_rate", "min_commission", "stamp_duty_sell_rate", "transfer_fee_rate", "slippage_bps",
        "parameters", "rule_hash",
    ],
    "benchmark": [
        "trade_date", "ts_code", "close", "source_record_key",
        "source_record_version", "source_payload_hash",
    ],
}
AVAILABILITY_RULES = {
    "research": "snapshot research_date is the decision date; effective_announce_date <= research_date",
    "execution": {
            "visibility": "one complete latest raw payload bounded by the release source manifest",
        "selection": (
            "tushare_market_raw_version ordered by fetched_at DESC, record_id DESC "
            "for each daily business key; only when no version exists does the query use "
            "tushare_daily_raw.payload"
        ),
        "frozen_fields": (
            "execution OHLC are parsed from one selected Raw payload; Tushare vol/amount are normalized "
            "from shares-in-hands/thousand-yuan to shares/yuan; limits, tradeability "
            "and source version/hash are copied into Dataset; "
            "a version payload is never field-by-field mixed with legacy Raw; legacy rows may have "
            "NULL source_record_version but must have a deterministic payload hash"
        ),
    },
    "corporate_action": "implementation status only; effective announcement <= ex_date; ex_date drives entitlement",
    "price_adjustment": "adj_close_base only for indicators; raw OHLC only for execution and stop logic",
    "liquidity": (
        "opening and stop orders use the previous completed daily bar's volume/amount; "
        "the current day's full volume is not available at the open"
    ),
    "market_coverage": {
        "policy": "validated_baseline_quality_report_equivalent_for_dates_without_gate",
        "with_gate_record": "latest gate per dataset/date must be PASS; BLOCKED or missing PASS blocks",
        "without_gate_record": "validated data baseline quality report covers historical dates",
    },
}


def _execution_limit_safety(
    up_limit: float | None,
    down_limit: float | None,
    is_suspended: bool | None,
    can_buy: bool | None,
    can_sell: bool | None,
) -> tuple[float | None, float | None, bool, bool, bool]:
    """把缺失/哨兵涨跌停价转换为不可撮合的安全状态。

    Tushare 某些无涨跌幅或历史缺失记录会返回空值或 100000 哨兵。不能把哨兵
    当真实价格，也不能在限制价未知时允许回测成交；价格仍可用于持仓估值。
    返回规范化上下限、可买、可卖和是否触发 WARNING。
    """
    # 这里只改变 Dataset 物化结果，不改写 Raw/Standard 源数据。
    up = normalize_execution_limit(up_limit)
    down = normalize_execution_limit(down_limit)
    buy = bool(can_buy)
    sell = bool(can_sell)
    unknown = (
        not bool(is_suspended)
        and (buy or sell)
        and (up is None or down is None)
    )
    if unknown:
        return up, down, False, False, True
    return up, down, buy, sell, False


def _canonical(value: Any):
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        return None
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _canonical(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    return value


def hash_rows(rows: Iterable[Iterable[Any]]) -> str:
    hasher = _IncrementalRowHasher()
    for row in rows:
        hasher.update(row)
    return hasher.hexdigest()


class _IncrementalRowHasher:
    """Hash rows with the same canonical representation as ``hash_rows``."""

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self.count = 0

    def update(self, row: Iterable[Any]) -> None:
        self._digest.update(
            (
                json.dumps(
                    [_canonical(item) for item in row],
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                + "\n"
            ).encode("utf-8")
        )
        self.count += 1

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def _git_metadata() -> dict[str, Any]:
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        status = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=all"], text=True
        )
        dirty = bool(status.strip())
        diff_hash = None
        if dirty:
            # status 文本只能描述哪些文件变了，不能还原跟踪文件差异，也不能覆盖
            # 未跟踪源码。脏工作区只允许显式归档，因此把差异和未跟踪文件内容纳入哈希。
            digest = hashlib.sha256()
            digest.update(subprocess.check_output(["git", "diff", "--binary", "HEAD"]))
            untracked = subprocess.check_output(
                ["git", "ls-files", "--others", "--exclude-standard", "-z"]
            )
            for raw_path in untracked.split(b"\0"):
                if not raw_path:
                    continue
                path = Path(os.fsdecode(raw_path))
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
    except Exception:
        return {"commit": None, "dirty": True, "diff_hash": "unavailable", "code_version": "unknown+dirty"}


def _manifest(dataset_id: str, *, for_update: bool = False) -> dict[str, Any]:
    """读取 Dataset manifest；锁语义由状态转换 Gateway 负责。"""
    kernel, repository, context = _read_service("manifest")
    try:
        return repository.manifest(dataset_id, context)
    finally:
        kernel.close()


_BASELINE_HASH_UNSET = object()


def _check_baseline(
    baseline_tag: str,
    evaluation_end: date,
    expected_quality_report_hash: str | None | object = _BASELINE_HASH_UNSET,
    expected_source_manifest_hash: str | None | object = _BASELINE_HASH_UNSET,
) -> dict[str, Any]:
    """校验基线，并返回 Dataset 必须绑定的不可变 Raw 边界。"""
    kernel, repository, context = _read_service("baseline")
    try:
        row = repository.baseline(baseline_tag, context)
    finally:
        kernel.close()
    if not row or row["status"] != "validated":
        raise ValueError(f"数据基线不存在或未 validated: {baseline_tag}")
    if row["baseline_cutoff_date"] < evaluation_end:
        raise ValueError(f"数据基线截止日 {row['baseline_cutoff_date']} 早于回测结束日 {evaluation_end}")
    actual_quality_report_hash = str(row["quality_report_hash"]).strip() if row["quality_report_hash"] else ""
    if not actual_quality_report_hash:
        raise ValueError(f"数据基线缺少 quality_report_hash，不能作为历史覆盖门禁等价物: {baseline_tag}")
    actual_source_manifest_hash = str(row["source_manifest_hash"]).strip() if row["source_manifest_hash"] else ""
    source_manifest = row["source_manifest"]
    if not actual_source_manifest_hash or not source_manifest:
        raise ValueError(f"数据基线缺少 Raw 来源 Manifest，不能冻结执行数据版本: {baseline_tag}")
    canonical_source_manifest = json.dumps(
        _canonical(source_manifest), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )
    stored_manifest_hash = hashlib.sha256(
        canonical_source_manifest.encode("utf-8")
    ).hexdigest()
    if stored_manifest_hash != actual_source_manifest_hash:
        raise ValueError(f"数据基线 Raw 来源 Manifest 内容哈希不一致: {baseline_tag}")
    if expected_quality_report_hash is not _BASELINE_HASH_UNSET:
        expected = str(expected_quality_report_hash).strip() if expected_quality_report_hash else ""
        if not expected:
            raise ValueError(
                f"Dataset 未绑定数据基线 quality_report_hash，必须重新创建 Dataset: {baseline_tag}"
            )
        if expected != actual_quality_report_hash:
            raise ValueError(
                f"数据基线 quality_report_hash 已变化，Dataset 必须重新 build: {baseline_tag}"
            )
    if expected_source_manifest_hash is not _BASELINE_HASH_UNSET:
        expected = (
            str(expected_source_manifest_hash).strip()
            if expected_source_manifest_hash else ""
        )
        if not expected:
            raise ValueError(
                f"Dataset 未绑定数据基线 source_manifest_hash，必须重新创建 Dataset: {baseline_tag}"
            )
        if expected != actual_source_manifest_hash:
            raise ValueError(
                f"数据基线 source_manifest_hash 已变化，Dataset 必须重新创建: {baseline_tag}"
            )
    return {
        "quality_report_hash": actual_quality_report_hash,
        "source_manifest_hash": actual_source_manifest_hash,
        "source_manifest": source_manifest,
        "cutoff": row["baseline_cutoff_date"],
    }


def missing_snapshot_dates(
    open_dates: Iterable[date], snapshot_dates: Iterable[date]
) -> tuple[date, ...]:
    """Return open dates without a corresponding single-day frozen snapshot."""
    required = {
        day if isinstance(day, date) else date.fromisoformat(str(day))
        for day in open_dates
    }
    present = {
        day if isinstance(day, date) else date.fromisoformat(str(day))
        for day in snapshot_dates
    }
    return tuple(sorted(required - present))


def _assert_build_code_current(
    manifest: dict[str, Any],
    metadata: dict[str, Any],
    *,
    allow_validation_only: bool = False,
) -> None:
    """确保冻结时的代码身份与物化时一致，避免错误覆盖 Dataset 血缘。"""
    fields = (
        ("code_version", "code_version"),
        ("git_commit", "commit"),
        ("git_dirty", "dirty"),
        ("diff_hash", "diff_hash"),
    )
    mismatches = []
    for manifest_field, metadata_field in fields:
        if manifest.get(manifest_field) != metadata.get(metadata_field):
            mismatches.append(manifest_field)
    if mismatches and not (
        allow_validation_only
        and mismatches == ["diff_hash"]
        and manifest.get("git_dirty") is True
        and metadata.get("dirty") is True
    ):
        raise RuntimeError(
            "Dataset build 后代码身份发生变化，必须重新 build 后才能 freeze: "
            + ", ".join(mismatches)
        )


def create_dataset(
    snapshot_ids: list[str],
    dataset_name: str,
    dataset_version: str,
    baseline_tag: str,
    warmup_start: date,
    evaluation_start: date,
    evaluation_end: date,
    out_of_sample_start: date | None = None,
    calendar_rule_version: str = "tushare-sse-v1",
    market_rule_version: str = "cn-a-share-v2",
    fee_rule_version: str = "research-fee-assumption-v3",
    benchmark_code: str = "000300.SH",
    dataset_id: str | None = None,
) -> str:
    """创建 Dataset；校验在 Repository 读边界内完成，写入走 typed gateway。"""
    if not snapshot_ids:
        raise ValueError("至少需要一个 FROZEN Research Snapshot")
    if not dataset_name or len(dataset_name) > 160:
        raise ValueError("dataset_name must be between 1 and 160 characters")
    if not dataset_version or len(dataset_version) > 40:
        raise ValueError("dataset_version must be between 1 and 40 characters")
    if not benchmark_code or "." not in benchmark_code:
        raise ValueError("benchmark_code 必须是 Tushare 指数代码")
    if not warmup_start <= evaluation_start <= evaluation_end:
        raise ValueError("warmup/evaluation 日期范围无效")
    dataset_id = dataset_id or str(uuid.uuid4())
    unique_ids = list(dict.fromkeys(snapshot_ids))
    kernel, repository, context = _read_service("create")
    try:
        baseline = _check_baseline(baseline_tag, evaluation_end)
        calendar_status = repository.calendar_status(
            [warmup_start, evaluation_start, evaluation_end], context
        )
        for required_day in (warmup_start, evaluation_start, evaluation_end):
            if calendar_status.get(required_day) is not True:
                raise ValueError(f"Dataset 边界必须是 SSE 开市日且存在交易日历: {required_day}")
        rows = repository.snapshot_manifests(unique_ids, context)
        by_id = {str(row["snapshot_id"]): row for row in rows}
        if len(by_id) != len(unique_ids):
            raise ValueError(f"Research Snapshot 不存在: {[item for item in unique_ids if item not in by_id]}")
        dates = [row["research_cutoff_date"] for row in rows]
        if len(set(dates)) != len(dates):
            raise ValueError("一个 Dataset 不能包含重复研究日期")
        snapshot_calendar = repository.calendar_status(dates, context)
        closed = [day for day in dates if snapshot_calendar.get(day) is not True]
        if closed:
            raise ValueError(f"Research Snapshot 成员日期必须是 SSE 开市日: {closed}")
        required_open_dates = repository.open_dates(warmup_start, evaluation_end, context)
        evaluation_open_dates = [day for day in required_open_dates if evaluation_start <= day <= evaluation_end]
        if not evaluation_open_dates:
            raise ValueError("evaluation 区间没有 SSE 开市日")
        if out_of_sample_start is None:
            out_of_sample_start = evaluation_open_dates[min(len(evaluation_open_dates) - 1, max(0, int(len(evaluation_open_dates) * 0.8)))]
        if out_of_sample_start not in evaluation_open_dates:
            raise ValueError("out_of_sample_start 必须是 evaluation 区间内的 SSE 开市日")
        missing = missing_snapshot_dates(required_open_dates, dates)
        if missing:
            raise ValueError(f"Dataset 缺少 {len(missing)} 个交易日 Snapshot，首日={missing[0]}")
        if any(row["status"] != "FROZEN" for row in rows):
            raise ValueError("Backtest Dataset 只能引用 FROZEN Research Snapshot")
        schema_versions = {row["schema_version"] for row in rows}
        if not schema_versions <= SUPPORTED_RESEARCH_SCHEMA_VERSIONS:
            raise ValueError(f"Research Snapshot schema_version 不兼容: {sorted(schema_versions)}")
        if len(schema_versions) > 1 and not all(
            is_foundation_schema(version) for version in schema_versions
        ):
            raise ValueError(
                "Dataset 只能混合 Foundation Research Snapshot schema；"
                f"实际={sorted(schema_versions)}"
            )
        foundation_contract_issues = _foundation_contract_issues(
            rows, repository, context,
        )
        if foundation_contract_issues:
            raise RuntimeError(
                _format_foundation_contract_failure(foundation_contract_issues)
            )
        if any(row["data_baseline_tag"] != baseline_tag for row in rows):
            raise ValueError("Dataset 基线必须与所有 Research Snapshot 的 data_baseline_tag 一致")
        if any(row["baseline_quality_report_hash"] != baseline["quality_report_hash"] for row in rows):
            raise ValueError("Research Snapshot 绑定的基线质量哈希与 Dataset 基线不一致")
        if any(row["baseline_source_manifest_hash"] != baseline["source_manifest_hash"] for row in rows):
            raise ValueError("Research Snapshot 绑定的 Raw 来源边界与 Dataset 基线不一致")
        if not any(evaluation_start <= day <= evaluation_end for day in dates):
            raise ValueError("没有覆盖 evaluation 区间的 Research Snapshot")
        availability_rules = dict(AVAILABILITY_RULES)
        availability_rules["research_snapshot_schema_versions"] = sorted(schema_versions)
        availability_rules["research_snapshot_schema_policy"] = (
            "same_schema_or_mixed_foundation_schema_versions"
        )
        manifest = {
            "dataset_id": dataset_id, "dataset_version": dataset_version, "dataset_name": dataset_name,
            "data_baseline_tag": baseline_tag, "baseline_quality_report_hash": baseline["quality_report_hash"],
            "baseline_source_manifest_hash": baseline["source_manifest_hash"], "schema_version": SCHEMA_VERSION,
            "adapter_version": ADAPTER_VERSION, "feature_version": FEATURE_VERSION,
            "warmup_start": warmup_start, "evaluation_start": evaluation_start, "evaluation_end": evaluation_end,
            "out_of_sample_start": out_of_sample_start, "calendar_rule_version": calendar_rule_version,
            "market_rule_version": market_rule_version, "fee_rule_version": fee_rule_version,
            "benchmark_code": benchmark_code, "selection_predicate": SELECTION_PREDICATE,
            "selected_columns": _json_param(SELECTED_COLUMNS), "hash_algorithm": HASH_ALGORITHM,
            "availability_rules": _json_param(availability_rules),
        }
        with repository.lifecycle_lock(dataset_id):
            repository.create_manifest(manifest, context)
            members = []
            for row in sorted(rows, key=lambda item: item["research_cutoff_date"]):
                day = row["research_cutoff_date"]
                if day < warmup_start or day > evaluation_end:
                    raise ValueError(f"Snapshot 日期 {day} 超出 Dataset 范围")
                members.append({
                    "dataset_id": dataset_id, "research_snapshot_id": row["snapshot_id"],
                    "research_date": day,
                    "member_role": "EVALUATION" if evaluation_start <= day <= evaluation_end else "WARMUP",
                    "snapshot_input_hash": row["input_manifest_hash"],
                })
            repository.create_members(members, context)
        return dataset_id
    finally:
        kernel.close()


def _json_document(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return value
    return value


def _component_from_manifest(
    manifest: dict[str, Any], effective_from: date, effective_to: date,
) -> DatasetComponent:
    baseline = manifest.get("baseline")
    input_hash = str(manifest.get("input_manifest_hash") or "").strip()
    baseline_hash = str(manifest.get("baseline_quality_report_hash") or "").strip()
    if not baseline or not input_hash or not baseline_hash:
        raise RuntimeError(
            f"FROZEN Dataset 缺少组合必需的基线或输入哈希: {manifest.get('dataset_id')}"
        )
    return DatasetComponent(
        dataset_id=str(manifest["dataset_id"]),
        effective_from=effective_from,
        effective_to=effective_to,
        input_manifest_hash=input_hash,
        manifest_hash=frozen_manifest_hash(manifest),
        baseline_tag=str(baseline),
        baseline_quality_report_hash=baseline_hash,
    )


def _assert_resumable_composite_manifest(
    existing: dict[str, Any], intended: dict[str, Any],
) -> None:
    if existing.get("status") != "DRAFT":
        raise RuntimeError("指定 dataset_id 已存在且不是可恢复的 DRAFT 组合数据集")
    for field, expected in intended.items():
        if field in {"dataset_id", "status"}:
            continue
        actual = existing.get(field)
        if field in {"selected_columns", "availability_rules"}:
            actual = _json_document(actual)
            expected = _json_document(expected)
        if actual != expected:
            raise RuntimeError(
                "指定 dataset_id 对应的 DRAFT 组合身份与本次输入不一致: "
                f"{field}"
            )


def _read_component_leaves(
    repository: DatasetRepository,
    context: RunContext,
    components: Sequence[dict[str, Any]],
) -> tuple[list[DatasetComponent], list[dict[str, Any]]]:
    if not components or len(components) > MAX_COMPONENTS:
        raise RuntimeError("组合 Dataset 组件为空或超过 128 个")
    materialized: list[DatasetComponent] = []
    manifests: list[dict[str, Any]] = []
    for expected_ordinal, row in enumerate(components):
        if int(row["ordinal"]) != expected_ordinal:
            raise RuntimeError("组合 Dataset ordinal 必须从 0 连续递增")
        child = repository.manifest(str(row["component_dataset_id"]), context)
        component = _component_from_manifest(
            child, row["effective_from"], row["effective_to"],
        )
        if child.get("status") != "FROZEN" or child.get("dataset_kind", "MATERIALIZED") != "MATERIALIZED":
            raise RuntimeError(
                f"组合 Dataset 只能引用 FROZEN MATERIALIZED 子集: {child['dataset_id']}"
            )
        proof = _json_document(child.get("quality_report"))
        if (
            not isinstance(proof, dict)
            or proof.get("status") != "VALID"
            or proof.get("issues") != []
            or not str(child.get("quality_report_hash") or "").strip()
        ):
            raise RuntimeError(
                f"FROZEN 子 Dataset 缺少有效质量报告证明: {child['dataset_id']}"
            )
        proof_counts = proof.get("validation_counts")
        required_counts = (
            "missing_execution_rows", "missing_execution_lineage",
            "missing_limit_rows", "missing_calendar_lineage",
            "future_dividend_rows", "invalid_dividend_dates",
            "missing_dividend_lineage", "inconsistent_stock_rates",
        )
        if proof_counts is not None:
            if not isinstance(proof_counts, dict) or any(
                proof_counts.get(name) != 0 for name in required_counts
            ):
                raise RuntimeError(
                    f"FROZEN 子 Dataset 的继承质量计数不完整或非零: {child['dataset_id']}"
                )
        elif any(proof.get(name) != 0 for name in (
            "missing_execution_rows", "missing_limit_rows",
        )):
            raise RuntimeError(
                f"旧版 FROZEN 子 Dataset 缺少可接受的质量计数证明: {child['dataset_id']}"
            )
        stored_input_hash = str(row["component_input_manifest_hash"] or "").strip()
        stored_manifest_hash = str(row["component_manifest_hash"] or "").strip()
        if (
            stored_input_hash != component.input_manifest_hash
            or stored_manifest_hash != component.manifest_hash
            or str(row["component_baseline_tag"]) != component.baseline_tag
            or str(row["component_baseline_quality_report_hash"] or "").strip()
            != component.baseline_quality_report_hash
        ):
            raise RuntimeError(
                f"组合 Dataset 子集身份或哈希已变化: {component.dataset_id}"
            )
        if not (
            child["warmup_start"] <= component.effective_from
            <= component.effective_to <= child["evaluation_end"]
        ):
            raise RuntimeError(
                f"组合 Dataset 日期范围超出子集冻结范围: {component.dataset_id}"
            )
        materialized.append(component)
        manifests.append(child)
    assert_compatible_manifests(manifests)
    return materialized, manifests


def create_composite_dataset(
    component_dataset_ids: Sequence[str],
    dataset_name: str,
    dataset_version: str,
    *,
    dataset_id: str | None = None,
) -> str:
    """Create a metadata-only, multi-baseline Dataset over frozen components.

    The first ID may be a prior composite, which is flattened by reusing its
    component references. Every following ID must be a newly frozen physical
    extension Dataset. The new Dataset owns no copied market or snapshot rows.
    """
    if len(component_dataset_ids) < 2:
        raise ValueError("至少需要一个历史 Dataset 和一个增量 Dataset")
    if len(component_dataset_ids) > MAX_COMPONENTS:
        raise ValueError(f"输入 Dataset 数量不能超过 {MAX_COMPONENTS}")
    if not dataset_name or len(dataset_name) > 160:
        raise ValueError("dataset_name must be between 1 and 160 characters")
    if not dataset_version or len(dataset_version) > 40:
        raise ValueError("dataset_version must be between 1 and 40 characters")
    if len(set(component_dataset_ids)) != len(component_dataset_ids):
        raise ValueError("component_dataset_ids 不能重复")

    dataset_id = dataset_id or str(uuid.uuid4())
    kernel, repository, context = _read_service("compose")
    try:
        top_level = [repository.manifest(str(item), context) for item in component_dataset_ids]
        if any(row["status"] != "FROZEN" for row in top_level):
            raise ValueError("只允许组合已 FROZEN 的 Backtest Dataset")

        components: list[DatasetComponent] = []
        leaves: list[dict[str, Any]] = []
        first = top_level[0]
        if first.get("dataset_kind", "MATERIALIZED") == "COMPOSITE":
            prior_rows = repository.composition_components(str(first["dataset_id"]), context)
            components, leaves = _read_component_leaves(repository, context, prior_rows)
            if not components or components[-1].effective_to != first["evaluation_end"]:
                raise RuntimeError("历史组合 Dataset 的组件范围与其 Manifest 截止日不一致")
        else:
            component = _component_from_manifest(
                first, first["warmup_start"], first["evaluation_end"],
            )
            components.append(component)
            leaves.append(first)

        used_ids = {component.dataset_id for component in components}
        for extension in top_level[1:]:
            if extension.get("dataset_kind", "MATERIALIZED") != "MATERIALIZED":
                raise ValueError("只有第一个输入可以是 COMPOSITE；后续输入必须是物化增量 Dataset")
            extension_id = str(extension["dataset_id"])
            if extension_id in used_ids:
                raise ValueError(f"重复的物化组件 Dataset: {extension_id}")
            prior_end = components[-1].effective_to
            extension_end = extension["evaluation_end"]
            if extension_end <= prior_end:
                raise ValueError(
                    f"增量 Dataset 未向后扩展日期范围: {extension_id}"
                )
            following_dates = repository.open_dates(prior_end, extension_end, context)
            starts = [day for day in following_dates if day > prior_end]
            if not starts:
                raise ValueError(f"增量 Dataset 没有新增开市日: {extension_id}")
            effective_from = starts[0]
            if effective_from < extension["evaluation_start"]:
                raise ValueError(
                    f"增量 Dataset 在 {effective_from} 之前没有正式 Evaluation 快照: {extension_id}"
                )
            if extension_end not in following_dates:
                raise ValueError(f"增量 Dataset 截止日不是有效 SSE 开市日: {extension_end}")
            components.append(
                _component_from_manifest(extension, effective_from, extension_end)
            )
            leaves.append(extension)
            used_ids.add(extension_id)

        if len(components) < 2:
            raise ValueError("组合数据集至少需要两个物化冻结组件")
        if len(components) > MAX_COMPONENTS:
            raise ValueError(
                f"组合组件超过安全上限 {MAX_COMPONENTS}；请按月或季度构建增量 Dataset"
            )
        assert_compatible_manifests(leaves)
        root_start = components[0].effective_from
        root_end = components[-1].effective_to
        expected_dates = repository.open_dates(root_start, root_end, context)
        validate_component_coverage(components, expected_dates)

        latest_leaf = leaves[-1]
        baseline = repository.baseline(str(latest_leaf["baseline"]), context)
        if not baseline or baseline["status"] != "validated":
            raise ValueError("最新组件的数据基线不存在或尚未 validated")
        if baseline["baseline_cutoff_date"] < root_end:
            raise ValueError("最新组件数据基线截止日早于组合 Dataset 结束日期")
        if (
            str(baseline["quality_report_hash"] or "").strip()
            != str(latest_leaf["baseline_quality_report_hash"] or "").strip()
            or str(baseline["source_manifest_hash"] or "").strip()
            != str(latest_leaf["baseline_source_manifest_hash"] or "").strip()
        ):
            raise ValueError("最新组件与已验证数据基线的质量/来源哈希不一致")

        metadata = _git_metadata()
        root = {
            "dataset_id": dataset_id,
            "dataset_name": dataset_name,
            "dataset_version": dataset_version,
            "dataset_kind": "COMPOSITE",
            "data_baseline_tag": latest_leaf["baseline"],
            "baseline_quality_report_hash": latest_leaf["baseline_quality_report_hash"],
            "baseline_source_manifest_hash": latest_leaf["baseline_source_manifest_hash"],
            "schema_version": first["schema_version"],
            "adapter_version": first["adapter_version"],
            "feature_version": first["feature_version"],
            "price_adjustment_mode": first["price_adjustment_mode"],
            "warmup_start": components[0].effective_from,
            "evaluation_start": first["evaluation_start"],
            "evaluation_end": root_end,
            "out_of_sample_start": first["out_of_sample_start"],
            "calendar_rule_version": first["calendar_rule_version"],
            "market_rule_version": first["market_rule_version"],
            "fee_rule_version": first["fee_rule_version"],
            "benchmark_code": first["benchmark_code"],
            "selection_predicate": (
                "ordered FROZEN MATERIALIZED Dataset components; each open date "
                "belongs to exactly one component; facts are read-through, never copied"
            ),
            "selected_columns": _json_param(_json_document(first["selected_columns"])),
            "hash_algorithm": first["hash_algorithm"],
            "availability_rules": _json_param(_json_document(first["availability_rules"])),
            "status": "DRAFT",
            "code_version": metadata["code_version"],
            "git_commit": metadata["commit"],
            "git_dirty": metadata["dirty"],
            "diff_hash": metadata["diff_hash"],
            "notes": "Metadata-only composition; all source components remain independently FROZEN.",
        }
        _persist_composite_manifest(
            dataset_id, root, components, repository, context,
        )
        return dataset_id
    finally:
        kernel.close()


def _persist_composite_manifest(
    dataset_id: str,
    root: dict[str, Any],
    components: Sequence[DatasetComponent],
    repository: DatasetRepository,
    context: RunContext,
) -> None:
    """Persist the composite root and its components under one lifecycle lock."""
    with repository.lifecycle_lock(dataset_id):
        try:
            existing = repository.manifest(dataset_id, context)
        except ValueError:
            existing = None
        if existing is None:
            repository.create_manifest(root, context)
        else:
            _assert_resumable_composite_manifest(existing, root)

        component_rows = [
            {
                "ordinal": ordinal,
                "component_dataset_id": component.dataset_id,
                "effective_from": component.effective_from,
                "effective_to": component.effective_to,
                "component_input_manifest_hash": component.input_manifest_hash,
                "component_manifest_hash": component.manifest_hash,
                "component_baseline_tag": component.baseline_tag,
                "component_baseline_quality_report_hash": (
                    component.baseline_quality_report_hash
                ),
            }
            for ordinal, component in enumerate(components)
        ]
        assert_resumable_component_prefix(
            repository.composition_components(
                dataset_id, context, allow_empty=True,
            ),
            component_rows,
        )
        repository.create_composition_components(dataset_id, component_rows, context)


def _validate_composite_dataset_impl(
    dataset_id: str,
    manifest: dict[str, Any],
    repository: DatasetRepository,
    context: RunContext,
) -> dict[str, Any]:
    if manifest.get("dataset_kind") != "COMPOSITE":
        raise ValueError(f"Dataset 不是 COMPOSITE: {dataset_id}")
    if manifest["status"] not in {"DRAFT", "BUILDING", "FAILED"}:
        raise RuntimeError(f"组合 Dataset 当前不可校验: {manifest['status']}")
    components, leaves = _read_component_leaves(
        repository, context, repository.composition_components(dataset_id, context),
    )
    if len(components) < 2:
        raise RuntimeError("组合 Dataset 至少需要两个有效组件")
    assert_compatible_manifests(leaves)
    expected_dates = repository.open_dates(
        manifest["warmup_start"], manifest["evaluation_end"], context,
    )
    validate_component_coverage(components, expected_dates)
    actual_component_dates = repository.dataset_open_dates(
        dataset_id, manifest["warmup_start"], manifest["evaluation_end"], context,
    )
    if tuple(actual_component_dates) != tuple(expected_dates):
        raise RuntimeError("组合 Dataset 子组件保存的交易日历存在缺失或多余日期")
    if (
        components[0].effective_from != manifest["warmup_start"]
        or components[-1].effective_to != manifest["evaluation_end"]
        or manifest["evaluation_start"] < manifest["warmup_start"]
        or manifest["evaluation_end"] < manifest["evaluation_start"]
    ):
        raise RuntimeError("组合 Dataset 根 Manifest 日期范围与组件范围不一致")
    for component, child in zip(components, leaves):
        if component.effective_from > child["evaluation_start"] and component is components[0]:
            raise RuntimeError("首个组件没有覆盖 Dataset warmup 起点")
        if component.effective_from < child["evaluation_start"] and component is not components[0]:
            raise RuntimeError("增量组件不能把 warmup 日期冒充为新 Evaluation 日期")
    last = leaves[-1]
    if (
        manifest["baseline"] != last["baseline"]
        or str(manifest["baseline_quality_report_hash"] or "").strip()
        != str(last["baseline_quality_report_hash"] or "").strip()
        or str(manifest["baseline_source_manifest_hash"] or "").strip()
        != str(last["baseline_source_manifest_hash"] or "").strip()
    ):
        raise RuntimeError("组合 Dataset 根基线身份与最后一个组件不一致")
    baseline = repository.baseline(manifest["baseline"], context)
    if not baseline or baseline["status"] != "validated":
        raise RuntimeError("组合 Dataset 绑定的数据基线不再处于 validated")
    if baseline["baseline_cutoff_date"] < manifest["evaluation_end"]:
        raise RuntimeError("组合 Dataset 的数据基线截止日覆盖不足")
    if (
        str(baseline["quality_report_hash"] or "").strip()
        != str(manifest["baseline_quality_report_hash"] or "").strip()
        or str(baseline["source_manifest_hash"] or "").strip()
        != str(manifest["baseline_source_manifest_hash"] or "").strip()
    ):
        raise RuntimeError("组合 Dataset 与当前已验证基线哈希不一致")

    contract_rows = repository.research_snapshot_contracts(dataset_id, context)
    schemas = {str(row["research_schema_version"]) for row in contract_rows}
    foundation_only = bool(contract_rows) and all(is_foundation_schema(item) for item in schemas)
    component_foundation_pass = all(
        _foundation_contract_audit_is_present(child) for child in leaves
    )
    if foundation_only and not component_foundation_pass:
        raise RuntimeError("至少一个 Foundation 组件缺少已通过的财务身份门禁证明")
    foundation_contract = {
        "status": "PASS" if foundation_only else "NOT_APPLICABLE",
        "audit_version": "foundation-financial-contract-v1",
        "snapshot_count": len(contract_rows),
        "issue_snapshot_count": 0,
        "inherited_from_frozen_components": True,
    }

    input_hash = composition_hash(components)
    component_hash_rows = [
        {
            "dataset_id": component.dataset_id,
            "effective_from": component.effective_from,
            "effective_to": component.effective_to,
            "input_manifest_hash": component.input_manifest_hash,
            "component_manifest_hash": component.manifest_hash,
        }
        for component in components
    ]
    counts = {
        "missing_execution_rows": 0,
        "missing_execution_lineage": 0,
        "missing_limit_rows": 0,
        "missing_calendar_lineage": 0,
        "future_dividend_rows": 0,
        "invalid_dividend_dates": 0,
        "missing_dividend_lineage": 0,
        "inconsistent_stock_rates": 0,
    }
    quality = {
        "status": "VALID",
        "issues": [],
        "validation_mode": "FROZEN_COMPONENT_METADATA_AND_DATE_COVERAGE",
        "materialized_fact_copy": False,
        "member_count": len(expected_dates),
        "component_count": len(components),
        "components": component_hash_rows,
        "foundation_financial_contract": foundation_contract,
        "validation_counts": counts,
    }
    root_hashes = {
        name: stable_hash({
            "kind": name,
            "components": [
                {
                    "dataset_id": component.dataset_id,
                    "effective_from": component.effective_from.isoformat(),
                    "effective_to": component.effective_to.isoformat(),
                    "component_hash": child.get(f"{name}_hash"),
                }
                for component, child in zip(components, leaves)
            ],
        })
        for name in (
            "calendar", "rules", "execution", "corporate_action", "benchmark",
        )
    }
    report_hash = _quality_report_hash(quality)
    if manifest["status"] in {"DRAFT", "FAILED"}:
        changed = repository.transition_manifest(
            context, dataset_id, (manifest["status"],), "BUILDING",
        )
        if changed != 1:
            raise RuntimeError("组合 Dataset 状态并发变化，不能进入 BUILDING")
    fields = {
        "input_manifest_hash": input_hash,
        "calendar_hash": root_hashes["calendar"],
        "rules_hash": root_hashes["rules"],
        "execution_hash": root_hashes["execution"],
        "corporate_action_hash": root_hashes["corporate_action"],
        "benchmark_hash": root_hashes["benchmark"],
        "quality_report": _json_param(quality),
        "quality_report_hash": report_hash,
    }
    changed = repository.transition_manifest(
        context, dataset_id, ("BUILDING",), "VALIDATED", fields,
    )
    if changed != 1:
        raise RuntimeError("组合 Dataset 校验完成但状态转换失败")
    return quality


def _verify_frozen_composition(
    manifest: dict[str, Any],
    repository: DatasetRepository,
    context: RunContext,
) -> None:
    """Verify only small immutable metadata/calendar ranges before a run."""
    components, leaves = _read_component_leaves(
        repository,
        context,
        repository.composition_components(str(manifest["dataset_id"]), context),
    )
    assert_compatible_manifests(leaves)
    expected_hash = composition_hash(components)
    if str(manifest.get("input_manifest_hash") or "").strip() != expected_hash:
        raise RuntimeError("FROZEN COMPOSITE Dataset composition hash mismatch")
    if (
        components[0].effective_from != manifest["warmup_start"]
        or components[-1].effective_to != manifest["evaluation_end"]
        or manifest["baseline"] != leaves[-1]["baseline"]
    ):
        raise RuntimeError("FROZEN COMPOSITE Dataset root boundaries/provenance mismatch")
    expected_dates = repository.open_dates(
        manifest["warmup_start"], manifest["evaluation_end"], context,
    )
    validate_component_coverage(components, expected_dates)
    frozen_component_calendar = repository.open_calendar(
        str(manifest["dataset_id"]), context,
    )
    if tuple(expected_dates) != tuple(frozen_component_calendar):
        raise RuntimeError("FROZEN COMPOSITE Dataset 的子集交易日历存在缺失或多余日期")


def _json_param(value: Any):
    """将结构化值规范化为稳定 JSON 文本，由写入 Gateway 负责类型适配。"""
    return json.dumps(
        _canonical(value), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )


def _quality_report_hash(value: Any) -> str:
    """对质量报告做确定性哈希，并统一处理 date/Decimal 等数据库类型。"""
    encoded = json.dumps(
        _canonical(value), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _checkpoint(dataset_id: str, stage: str) -> dict[str, Any] | None:
    """读取 Dataset 阶段检查点；检查点只用于恢复流程，不参与业务数据计算。"""
    kernel, repository, context = _read_service("checkpoint")
    try:
        return repository.checkpoint(dataset_id, stage, context)
    finally:
        kernel.close()


def _set_checkpoint(
    dataset_id: str,
    stage: str,
    status: str,
    *,
    progress_done: int = 0,
    progress_total: int | None = None,
    last_trade_date: date | None = None,
    stage_hash: str | None = None,
    details: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    """在独立连接中写入 checkpoint，避免被大事务阻塞。"""
    kernel, repository, context = _read_service("checkpoint-write")
    try:
        repository.set_checkpoint(
            dataset_id, stage, status, context,
            progress_done=progress_done, progress_total=progress_total,
            last_trade_date=last_trade_date, stage_hash=stage_hash,
            details=details or {}, error_message=error_message,
        )
    finally:
        kernel.close()


def build_dataset(dataset_id: str) -> dict[str, Any]:
    """兼容旧入口；实际执行委托给可恢复的按日期批处理构建器。"""
    # Keep the historical API name, but route all callers through the
    # resumable date-batched implementation.  The former body below used one
    # REPEATABLE READ transaction for the complete Dataset and is intentionally
    # unreachable until it is removed in the next cleanup window.
    from data_collect.jobs.backtest_dataset_batched import build_dataset_batched

    return build_dataset_batched(dataset_id, batch_days=5)

def _materialized_hashes(dataset_id: str) -> dict[str, Any]:
    """读取 Dataset 子表的最终物化行，返回全局/按日哈希。

    校验必须对账 Dataset 自己的物料，而不是再次对 mutable 上游表计算哈希。
    这样才能发现物化过程中的列顺序、类型转换、默认值或写入截断问题。
    """
    execution_hasher = _IncrementalRowHasher()
    execution_by_day: dict[date, dict[str, Any]] = {}
    validation_counts = {
        "missing_execution_rows": 0,
        "missing_execution_lineage": 0,
        "missing_limit_rows": 0,
        "missing_calendar_lineage": 0,
        "future_dividend_rows": 0,
        "invalid_dividend_dates": 0,
        "missing_dividend_lineage": 0,
        "inconsistent_stock_rates": 0,
    }
    last_key: tuple | None = None
    kernel, repository, context = _read_service("materialized-hashes")
    try:
        while True:
            page = repository.execution_page(dataset_id, context, last_key, 5_000)
            rows = [
                tuple(row[column] for column in (
                    "trade_date", "ts_code", "market", "raw_open", "raw_high", "raw_low",
                    "raw_close", "volume", "amount", "up_limit", "down_limit",
                    "is_suspended", "can_buy", "can_sell", "limit_status",
                    "source_record_key", "source_record_version", "source_payload_hash",
                    "limit_source_record_id", "limit_source_payload_hash",
                    "suspend_source_record_id", "suspend_source_payload_hash", "quality_status",
                ))
            for row in page
            ]
            if not rows:
                break
            for row in rows:
                execution_hasher.update(row)
                stats = execution_by_day.setdefault(
                    row[0], {
                        "hasher": _IncrementalRowHasher(),
                        "count": 0,
                        "quality_counts": {},
                    }
                )
                stats["hasher"].update(row)
                stats["count"] += 1
                quality = row[-1]
                quality_counts = stats["quality_counts"]
                quality_counts[quality] = quality_counts.get(quality, 0) + 1
                if quality == "MISSING_SOURCE":
                    validation_counts["missing_execution_rows"] += 1
                if (
                    (row[12] is True or row[13] is True)
                    and (
                        row[17] is None
                        or row[18] is None
                        or row[19] is None
                    )
                ) or (row[11] is True and row[20] is None):
                    validation_counts["missing_execution_lineage"] += 1
                if (
                    quality == "VALID"
                    and row[11] is not True
                    and (row[12] is True or row[13] is True)
                    and (row[9] is None or row[10] is None)
                ):
                    validation_counts["missing_limit_rows"] += 1
            last_key = tuple(rows[-1][:2])
        calendar_rows = repository.materialized_calendar_rows(dataset_id, context)
        validation_counts["missing_calendar_lineage"] = sum(
            row[6] is None for row in calendar_rows
        )
        dividend_rows = repository.materialized_dividend_rows(dataset_id, context)
        dividend_by_day: dict[date, list[tuple]] = {}
        for row in dividend_rows:
            dividend_by_day.setdefault(row[10], []).append(row)
            ex_date = row[10]
            available_date = row[13] or row[2] or row[1] or ex_date
            if ex_date is not None and available_date is not None and available_date > ex_date:
                validation_counts["future_dividend_rows"] += 1
            if (
                ex_date is not None
                and ((row[11] is not None and row[11] < ex_date) or (row[12] is not None and row[12] < ex_date))
            ):
                validation_counts["invalid_dividend_dates"] += 1
            if row[15] is None or row[16] is None:
                validation_counts["missing_dividend_lineage"] += 1
            if (
                row[4] is not None
                and (row[5] is not None or row[6] is not None)
                and abs(row[4] - ((row[5] or 0) + (row[6] or 0))) > 1e-8
            ):
                validation_counts["inconsistent_stock_rates"] += 1

        # rules_hash 必须与 build 阶段使用完全相同的审计字段集合；
        # load_frozen_dataset 另外使用不含审批元数据的 rule_rows。
        rule_audit_rows = repository.materialized_rule_rows(dataset_id, context, audit=True)
        rule_rows = repository.materialized_rule_rows(dataset_id, context)
        benchmark_rows = repository.materialized_benchmark_rows(dataset_id, context)
    finally:
        kernel.close()

    return {
        "execution_by_day": execution_by_day,
        "calendar_rows": calendar_rows,
        "dividend_rows": dividend_rows,
        "dividend_by_day": dividend_by_day,
        "rule_rows": rule_rows,
        "benchmark_rows": benchmark_rows,
        "execution_hash": execution_hasher.hexdigest(),
        "calendar_hash": hash_rows(calendar_rows),
        "corporate_action_hash": hash_rows(dividend_rows),
        "rules_hash": hash_rows(rule_audit_rows),
        "benchmark_hash": hash_rows(benchmark_rows),
        "validation_counts": validation_counts,
    }


def _validate_dataset_unlocked(dataset_id: str) -> dict[str, Any]:
    kernel, repository, context = _read_service("validate")
    try:
        manifest = repository.manifest(dataset_id, context)
        if manifest.get("dataset_kind", "MATERIALIZED") == "COMPOSITE":
            return _validate_composite_dataset_impl(
                dataset_id, manifest, repository, context,
            )
        if manifest["status"] != "BUILDING":
            raise RuntimeError(f"只有 BUILDING Dataset 可校验: {manifest['status']}")
        _check_baseline(
            manifest["baseline"], manifest["evaluation_end"],
            expected_quality_report_hash=manifest.get("baseline_quality_report_hash"),
            expected_source_manifest_hash=manifest.get("baseline_source_manifest_hash"),
        )
        issues: list[str] = []
        if manifest.get("hash_algorithm") != HASH_ALGORITHM:
            issues.append(f"hash_algorithm 不受支持: {manifest.get('hash_algorithm')}")
        if not manifest.get("selection_predicate") or not manifest.get("selected_columns"):
            issues.append("Dataset 未记录完整的切片谓词和字段集合")
        if not manifest.get("availability_rules"):
            issues.append("Dataset 未记录字段可见性规则")
        members = repository.member_validation_rows(dataset_id, context)
        if not members:
            issues.append("dataset_member 为空")
        contract_rows = repository.research_snapshot_contracts(dataset_id, context)
        foundation_contract_issues = _foundation_contract_issues(
            contract_rows, repository, context,
        )
        if foundation_contract_issues:
            issues.append(_format_foundation_contract_failure(foundation_contract_issues))
        contract_schemas = {
            str(row["research_schema_version"]) for row in contract_rows
        }
        foundation_contract_quality = {
            "status": (
                "PASS"
                if contract_rows
                and all(is_foundation_schema(schema) for schema in contract_schemas)
                and not foundation_contract_issues
                else "NOT_APPLICABLE"
            ),
            "audit_version": "foundation-financial-contract-v1",
            "snapshot_count": len(contract_rows),
            "issue_snapshot_count": len(foundation_contract_issues),
        }
        materialized = _materialized_hashes(dataset_id)
        benchmark_repair = manifest.get("benchmark_source_manifest")
        benchmark_repair_hash = manifest.get("benchmark_source_manifest_hash")
        if benchmark_repair or benchmark_repair_hash:
            if not benchmark_repair or not benchmark_repair_hash:
                issues.append("Dataset 基准修复来源清单不完整")
            else:
                encoded_repair = json.dumps(
                    _canonical(benchmark_repair), ensure_ascii=False,
                    sort_keys=True, separators=(",", ":"), allow_nan=False,
                )
                actual_repair_hash = hashlib.sha256(
                    encoded_repair.encode("utf-8")
                ).hexdigest()
                if actual_repair_hash != str(benchmark_repair_hash).strip():
                    issues.append("Dataset 基准修复来源清单哈希不一致")
        for field, label in (
            ("calendar_hash", "calendar"), ("rules_hash", "rules"),
            ("execution_hash", "execution"), ("corporate_action_hash", "corporate_action"),
            ("benchmark_hash", "benchmark"),
        ):
            if manifest.get(field) != materialized[f"{label}_hash"]:
                issues.append(f"{field} 与 Dataset 物化结果不一致")
        if not materialized["calendar_rows"] or not materialized["rule_rows"]:
            issues.append("Dataset 物化日历或规则为空")
        member_dates = [row[0] for row in members]
        if [row[0] for row in materialized["benchmark_rows"]] != member_dates:
            issues.append("Dataset 基准行情未逐日覆盖成员日期")
        if sorted(materialized["execution_by_day"]) != member_dates:
            issues.append("Dataset 成员日期与物化执行行情日期不一致")
        # _materialized_hashes already streamed every execution row to
        # compute the immutable materialized hash.  Reusing these counters
        # avoids a second set of full scans over the ~10GB execution table,
        # which previously made validation hit the governed statement timeout.
        counts = materialized["validation_counts"]
        for key, message in (
            ("missing_execution_rows", "执行行情缺失"),
            ("missing_execution_lineage", "执行行情 Raw 血缘不完整"),
            ("missing_calendar_lineage", "交易日历 Raw 血缘不完整"),
            ("missing_limit_rows", "执行行情缺少涨跌停价"),
            ("future_dividend_rows", "公司行为存在除权日前不可见记录"),
            ("invalid_dividend_dates", "公司行为支付/上市日期无效"),
            ("missing_dividend_lineage", "公司行为 Raw 血缘不完整"),
            ("inconsistent_stock_rates", "公司行为送转股分项不一致"),
        ):
            if counts[key]:
                issues.append(f"{message}: {counts[key]}")
        quality = {
            "status": "FAILED" if issues else "VALID", "issues": issues,
            "member_count": len(members), "missing_execution_rows": counts["missing_execution_rows"],
            "missing_limit_rows": counts["missing_limit_rows"], "benchmark_code": manifest.get("benchmark_code"),
            "benchmark_rows": len(materialized["benchmark_rows"]), "out_of_sample_start": manifest.get("out_of_sample_start"),
            "baseline_quality_report_hash": manifest.get("baseline_quality_report_hash"),
            "baseline_source_manifest_hash": manifest.get("baseline_source_manifest_hash"),
            "foundation_financial_contract": foundation_contract_quality,
            # Persist the exact counters produced while streaming the
            # materialized Dataset.  The database freeze trigger verifies
            # this immutable proof instead of rescanning the multi-GB child
            # tables during the status transition.
            "validation_counts": counts,
        }
        report_hash = _quality_report_hash(quality)
        if issues:
            repository.transition_manifest(
                context, dataset_id, ("BUILDING",), "FAILED",
                {"failure_reason": "; ".join(issues)[:4000], "quality_report": _json_param(quality), "quality_report_hash": report_hash},
            )
            raise RuntimeError(f"Backtest Dataset 校验失败: {issues}")
        repository.transition_manifest(
            context, dataset_id, ("BUILDING",), "VALIDATED",
            {"quality_report": _json_param(quality), "quality_report_hash": report_hash},
        )
        return quality
    finally:
        kernel.close()


def _validate_dataset_impl(
    dataset_id: str, *, resume_failed: bool = False,
) -> dict[str, Any]:
    lock_kernel, repository, _context = _read_service("validate-lifecycle-lock")
    try:
        with repository.lifecycle_lock(dataset_id):
            try:
                if resume_failed:
                    changed = repository.resume_failed(
                        dataset_id, _context,
                    )
                    if changed != 1:
                        raise RuntimeError(
                            f"只能在 FAILED Dataset 上恢复校验: {dataset_id}"
                        )
                return _validate_dataset_unlocked(dataset_id)
            except Exception as exc:
                _mark_dataset_validation_failed(dataset_id, exc)
                raise
    finally:
        lock_kernel.close()


def _mark_dataset_validation_failed(dataset_id: str, exc: Exception) -> None:
    kernel, repository, context = _read_service("validate-failed")
    try:
        repository.transition_manifest(
            context, dataset_id, ("BUILDING",), "FAILED",
            {"failure_reason": str(exc)[:4000]},
        )
    except Exception:
        pass
    finally:
        kernel.close()


def validate_dataset(
    dataset_id: str, *, resume_failed: bool = False,
) -> dict[str, Any]:
    return _validate_dataset_impl(dataset_id, resume_failed=resume_failed)


def freeze_dataset(
    dataset_id: str,
    code_archive_path: str | None = None,
    *,
    allow_validation_only: bool = False,
):
    metadata = _git_metadata()
    if metadata["dirty"] and not code_archive_path:
        raise RuntimeError("正式冻结要求 Git 工作区干净；如需保留脏工作区，请提供 code_archive_path")
    if code_archive_path and not Path(code_archive_path).exists():
        raise FileNotFoundError(f"代码归档不存在: {code_archive_path}")
    archive_hash = archive_size = None
    if code_archive_path:
        archive_hash, archive_size = _sha256_file(code_archive_path)
    kernel, repository, context = _read_service("freeze")
    try:
        with repository.lifecycle_lock(dataset_id):
            manifest = repository.manifest(dataset_id, context)
            if manifest["status"] != "VALIDATED":
                raise RuntimeError(f"只有 VALIDATED Dataset 可冻结: {manifest['status']}")
            contract_rows = repository.research_snapshot_contracts(dataset_id, context)
            contract_schemas = {
                str(row["research_schema_version"]) for row in contract_rows
            }
            if contract_rows and all(is_foundation_schema(schema) for schema in contract_schemas):
                if not _foundation_contract_audit_is_present(manifest):
                    raise RuntimeError(
                        "Foundation V2 Dataset 缺少通过财务来源身份门禁的验收记录，"
                        "必须重新 validate 后才能冻结"
                    )
            _check_baseline(manifest["baseline"], manifest["evaluation_end"], expected_quality_report_hash=manifest.get("baseline_quality_report_hash"), expected_source_manifest_hash=manifest.get("baseline_source_manifest_hash"))
            _assert_build_code_current(
                manifest,
                metadata,
                allow_validation_only=allow_validation_only,
            )
            changed = repository.transition_manifest(
                context, dataset_id, ("VALIDATED",), "FROZEN",
                {"code_archive_path": code_archive_path, "code_archive_sha256": archive_hash, "code_archive_size_bytes": archive_size, "code_version": metadata["code_version"], "git_commit": metadata["commit"], "git_dirty": metadata["dirty"], "diff_hash": metadata["diff_hash"]},
            )
            if changed != 1:
                raise RuntimeError("Backtest Dataset 状态在冻结期间发生变化，拒绝并发冻结")
    finally:
        kernel.close()


def _dividend_event_from_materialized_row(row: Sequence[Any]) -> DividendEvent:
    """把冻结 Dataset 的持久化列显式映射到领域事件。

    持久化记录包含用于审计的 ``report_date``，而撮合领域模型不消费该字段。
    禁止使用位置展开，避免数据库列新增或排序差异静默错配金额和日期。
    """
    if len(row) != 17:
        raise RuntimeError(f"冻结分红事件字段数异常: expected=17, actual={len(row)}")
    return DividendEvent(
        ts_code=row[0],
        ex_date=row[10],
        cash_div=row[7],
        stock_div=row[4],
        stock_bonus_rate=row[5],
        stock_conversion_rate=row[6],
        pay_date=row[11],
        record_date=row[9],
        implementation_status=row[3],
        source_record_key=row[14],
        source_record_version=row[15],
        source_payload_hash=row[16],
        announce_date=row[2],
        implementation_announce_date=row[13],
        div_list_date=row[12],
        cash_div_tax=row[8],
    )


_BAR_COLUMNS = (
    "trade_date", "ts_code", "raw_open", "raw_high", "raw_low", "raw_close",
    "volume", "amount", "up_limit", "down_limit", "is_suspended", "can_buy",
    "can_sell", "limit_status", "market",
)


def _market_bar_from_row(row: dict[str, Any]) -> MarketBar:
    return MarketBar(**{key: row[key] for key in _BAR_COLUMNS})


def _selected_observation_columns(
    observation_columns: Sequence[str] | None,
) -> list[str]:
    all_columns = tuple(SELECTED_COLUMNS["research"])
    selected = list(observation_columns or all_columns[2:])
    unknown = set(selected) - set(all_columns[2:])
    if unknown:
        raise ValueError(f"unknown frozen observation columns: {sorted(unknown)}")
    return ["research_date", "ts_code", *selected]


def _resolve_frozen_window(
    repository: DatasetRepository,
    dataset_id: str,
    context: RunContext,
    evaluation_config=None,
):
    """Resolve the immutable Dataset boundaries for every reader.

    只有 FROZEN Dataset 可以用于正式回测；这个检查对内存版和流式版共用。
    Evaluation 边界必须是 FROZEN Dataset 开市日。
    """
    manifest = repository.manifest(dataset_id, context)
    if manifest["status"] != "FROZEN":
        raise RuntimeError(f"鍙湁 FROZEN Dataset 鍙互鐢ㄤ簬姝ｅ紡鍥炴祴: {manifest['status']}")
    if manifest.get("dataset_kind", "MATERIALIZED") == "COMPOSITE":
        _verify_frozen_composition(manifest, repository, context)
    contract_rows = repository.research_snapshot_contracts(dataset_id, context)
    contract_schemas = {
        str(row["research_schema_version"]) for row in contract_rows
    }
    if contract_rows and all(is_foundation_schema(schema) for schema in contract_schemas):
        if not _foundation_contract_audit_is_present(manifest):
            raise RuntimeError(
                "Foundation V2 Dataset 未记录财务来源身份门禁通过结果，"
                "禁止正式回测；请创建并验收修复后的 Dataset"
            )
    effective_warmup_start = manifest["warmup_start"]
    effective_evaluation_start = manifest["evaluation_start"]
    effective_evaluation_end = manifest["evaluation_end"]
    if evaluation_config is not None:
        effective_warmup_start = evaluation_config.warmup_start
        effective_evaluation_start = evaluation_config.evaluation_start
        effective_evaluation_end = evaluation_config.evaluation_end
        if not (
            manifest["warmup_start"] <= effective_warmup_start
            <= effective_evaluation_start <= effective_evaluation_end
            <= manifest["evaluation_end"]
        ):
            raise RuntimeError(
                "Evaluation 璇勪环鍖洪棿蹇呴』瀹屽叏浣嶄簬 FROZEN Dataset 鐨勫喕缁撹寖鍥村唴"
            )
    source_calendar = repository.open_calendar(dataset_id, context)
    required_boundaries = {
        effective_warmup_start,
        effective_evaluation_start,
        effective_evaluation_end,
    }
    missing_boundaries = sorted(required_boundaries - set(source_calendar))
    if missing_boundaries:
        raise RuntimeError(
            f"Evaluation 杈圭晫蹇呴』鏄?FROZEN Dataset 寮€甯傛棩: {missing_boundaries}"
        )
    calendar = tuple(
        day for day in source_calendar
        if effective_warmup_start <= day <= effective_evaluation_end
    )
    source_oos_start = manifest["out_of_sample_start"] or effective_evaluation_start
    effective_oos_start = min(
        max(source_oos_start, effective_evaluation_start),
        effective_evaluation_end,
    )
    return (
        manifest, calendar, effective_warmup_start, effective_evaluation_start,
        effective_evaluation_end, effective_oos_start,
    )


def _rules_for_calendar(calendar: Sequence[date], rows: Iterable[Sequence[Any]]):
    rules = {}
    for row in rows:
        (*_, parameters, _rule_hash) = row
        effective_from, effective_to, market = row[3], row[4], row[5]
        market_rule = MarketRule(
            market=market, min_buy_lot=row[6], allow_sell_odd_lot=row[7],
            t_plus_one=row[8], price_tick=row[9], cash_settlement_days=row[10],
            max_volume_participation_pct=float(
                (parameters or {}).get("max_volume_participation_pct", 1.0)
            ),
            liquidity_policy=str(
                (parameters or {}).get("liquidity_policy", "reject_full_order")
            ),
        )
        fee_rule = FeeRule(
            commission_rate=row[12], min_commission=row[13],
            stamp_duty_sell_rate=row[14], transfer_fee_rate=row[15],
            slippage_bps=row[16],
        )
        for day in calendar:
            if effective_from <= day and (effective_to is None or day <= effective_to):
                rules[(day, None if market in {"ALL", "*"} else market)] = (
                    market_rule, fee_rule,
                )
    return rules


class StreamingFrozenDataset:
    """Bounded read-only Dataset view for long formal backtests.

    Bars and observations are fetched through Repository page methods once
    per trade date and retained only in small LRU caches.  The engine receives
    the same behavioral methods as ``BacktestDataset`` and never sees SQL or
    a database handle.  Auxiliary inputs are small enough to materialize.
    """

    __slots__ = (
        "dataset_id", "calendar", "evaluation_start", "evaluation_end",
        "warmup_start", "out_of_sample_start", "input_hash", "benchmark_code",
        "benchmark_closes", "member_roles", "snapshot_ids", "require_explicit_rules",
        "_repository", "_context", "_page_size", "_bar_cache_limit",
        "_observation_cache_limit", "_bar_cache", "_observation_cache",
        "_dividends", "_rules", "_delist_dates", "_next_by_date", "_closed",
        "_observation_columns",
    )

    def __init__(
        self, *, dataset_id: str, repository: DatasetRepository,
        context: RunContext, manifest: dict[str, Any], calendar: tuple[date, ...],
        evaluation_start: date, evaluation_end: date, warmup_start: date,
        out_of_sample_start: date, observation_columns: Sequence[str] | None = None,
        page_size: int = 10_000, bar_cache_days: int = 3,
        observation_cache_days: int = 2,
    ):
        if not 1 <= page_size <= 50_000:
            raise ValueError("streaming page_size must be between 1 and 50000")
        if bar_cache_days < 2 or observation_cache_days < 1:
            raise ValueError("streaming cache windows are too small for engine lookback")
        self.dataset_id = dataset_id
        self.calendar = tuple(calendar)
        self.evaluation_start = evaluation_start
        self.evaluation_end = evaluation_end
        self.warmup_start = warmup_start
        self.out_of_sample_start = out_of_sample_start
        self.input_hash = manifest["input_manifest_hash"]
        self.benchmark_code = manifest["benchmark_code"]
        self.require_explicit_rules = True
        self._repository = repository
        self._context = context
        self._page_size = page_size
        self._bar_cache_limit = bar_cache_days
        self._observation_cache_limit = observation_cache_days
        self._bar_cache = OrderedDict()
        self._observation_cache = OrderedDict()
        self._closed = False
        self._observation_columns = _selected_observation_columns(observation_columns)
        self._next_by_date = {
            day: self.calendar[index + 1] if index + 1 < len(self.calendar) else None
            for index, day in enumerate(self.calendar)
        }
        self._dividends = {}
        for row in repository.load_dividends(dataset_id, context):
            event = _dividend_event_from_materialized_row(row)
            self._dividends.setdefault(event.ex_date, []).append(event)
        self._dividends = {
            day: tuple(events) for day, events in self._dividends.items()
        }
        self._rules = _rules_for_calendar(
            self.calendar, repository.load_rules(dataset_id, context)
        )
        self.member_roles, self.snapshot_ids = repository.member_roles_and_snapshots(
            dataset_id, context
        )
        self.benchmark_closes = repository.load_benchmark_closes(
            dataset_id, self.benchmark_code, context
        )
        self._delist_dates = {}

    @property
    def evaluation_dates(self) -> tuple[date, ...]:
        return tuple(
            day for day in self.calendar
            if self.evaluation_start <= day <= self.evaluation_end
        )

    def _ensure_open(self):
        if self._closed:
            raise RuntimeError("streaming Frozen Dataset has already been closed")

    @staticmethod
    def _cache_put(cache, key, value, limit: int):
        cache.pop(key, None)
        cache[key] = value
        while len(cache) > limit:
            cache.popitem(last=False)

    def _load_bars(self, day: date) -> dict[str, MarketBar]:
        self._ensure_open()
        cached = self._bar_cache.get(day)
        if cached is not None:
            self._bar_cache.move_to_end(day)
            return cached
        result: dict[str, MarketBar] = {}
        last_key = None
        while True:
            page = self._repository.bar_page(
                self.dataset_id, self._context, last_key, self._page_size,
                date_from=day, date_to=day,
            )
            if not page:
                break
            page_key = (page[-1]["trade_date"], page[-1]["ts_code"])
            if last_key is not None and page_key <= last_key:
                raise RuntimeError(f"execution page did not advance for {day}")
            for row in page:
                bar = _market_bar_from_row(row)
                if bar.ts_code in result:
                    raise RuntimeError(f"duplicate execution bar in Dataset: {day}/{bar.ts_code}")
                result[bar.ts_code] = bar
            if len(page) < self._page_size:
                break
            last_key = page_key
        self._cache_put(self._bar_cache, day, result, self._bar_cache_limit)
        return result

    def _load_observations(self, day: date) -> tuple[Observation, ...]:
        self._ensure_open()
        cached = self._observation_cache.get(day)
        if cached is not None:
            self._observation_cache.move_to_end(day)
            return cached
        result: list[Observation] = []
        seen_codes: set[str] = set()
        last_key = None
        while True:
            page = self._repository.observation_page(
                self.dataset_id, self._observation_columns, self._context,
                last_key, self._page_size, date_from=day, date_to=day,
            )
            if not page:
                break
            page_key = (page[-1]["research_date"], page[-1]["ts_code"])
            if last_key is not None and page_key <= last_key:
                raise RuntimeError(f"observation page did not advance for {day}")
            for row in page:
                values = {column: row[column] for column in self._observation_columns}
                row_day, code = values.pop("research_date"), values.pop("ts_code")
                if row_day != day:
                    raise RuntimeError(
                        f"observation page escaped requested date: {row_day} != {day}"
                    )
                if code in seen_codes:
                    raise RuntimeError(f"duplicate observation in Dataset: {day}/{code}")
                seen_codes.add(code)
                delist = values.get("delist_date")
                if delist is not None:
                    existing = self._delist_dates.get(code)
                    if existing is not None and existing != delist:
                        raise RuntimeError(f"inconsistent delist_date in Dataset: {code}")
                    self._delist_dates[code] = delist
                else:
                    self._delist_dates.setdefault(code, None)
                result.append(Observation(day, code, values))
            if len(page) < self._page_size:
                break
            last_key = page_key
        value = tuple(result)
        self._cache_put(self._observation_cache, day, value, self._observation_cache_limit)
        return value

    def next_trade_date(self, day: date) -> date | None:
        self._ensure_open()
        return self._next_by_date.get(day)

    def bar(self, day: date, ts_code: str) -> MarketBar | None:
        return self._load_bars(day).get(ts_code)

    def observations_for(self, day: date) -> tuple[Observation, ...]:
        return self._load_observations(day)

    def dividend_events_for(self, day: date) -> tuple[DividendEvent, ...]:
        self._ensure_open()
        return self._dividends.get(day, ())

    def delist_date_for(self, ts_code: str) -> date | None:
        self._ensure_open()
        return self._delist_dates.get(ts_code)

    def rules_for(self, day: date, market: str | None = None):
        self._ensure_open()
        return self._rules.get((day, market)) or self._rules.get((day, None))

    def close(self):
        if not self._closed:
            self._bar_cache.clear()
            self._observation_cache.clear()
            self._closed = True
            self._repository.kernel.close()

    def __enter__(self):
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def open_streaming_frozen_dataset(
    dataset_id: str,
    observation_columns: Sequence[str] | None = None,
    *,
    evaluation_config=None,
    page_size: int = 10_000,
    bar_cache_days: int = 3,
    observation_cache_days: int = 2,
) -> StreamingFrozenDataset:
    """Open a bounded reader; the caller owns and must close it."""
    kernel, repository, context = _read_service("stream")
    try:
        (
            manifest, calendar, warmup_start, evaluation_start,
            evaluation_end, out_of_sample_start,
        ) = _resolve_frozen_window(repository, dataset_id, context, evaluation_config)
        return StreamingFrozenDataset(
            dataset_id=dataset_id, repository=repository, context=context,
            manifest=manifest, calendar=calendar,
            evaluation_start=evaluation_start, evaluation_end=evaluation_end,
            warmup_start=warmup_start, out_of_sample_start=out_of_sample_start,
            observation_columns=observation_columns, page_size=page_size,
            bar_cache_days=bar_cache_days,
            observation_cache_days=observation_cache_days,
        )
    except Exception:
        kernel.close()
        raise


def load_frozen_dataset(
    dataset_id: str,
    observation_columns: Sequence[str] | None = None,
    *,
    evaluation_config=None,
) -> BacktestDataset:
    kernel, repository, context = _read_service("load")
    try:
        (
            manifest, calendar, effective_warmup_start, effective_evaluation_start,
            effective_evaluation_end, effective_oos_start,
        ) = _resolve_frozen_window(repository, dataset_id, context, evaluation_config)
        bars: dict[date, dict[str, MarketBar]] = {}
        last_key = None
        while True:
            page = repository.bar_page(
                dataset_id, context, last_key, 10_000,
                date_from=effective_warmup_start,
                date_to=effective_evaluation_end,
            )
            if not page:
                break
            for row in page:
                bar = _market_bar_from_row(row)
                bars.setdefault(bar.trade_date, {})[bar.ts_code] = bar
            last_key = (page[-1]["trade_date"], page[-1]["ts_code"])
        selected_columns = _selected_observation_columns(observation_columns)
        observations: dict[date, list[Observation]] = {}
        delist_dates: dict[str, date | None] = {}
        last_key = None
        while True:
            page = repository.observation_page(
                dataset_id, selected_columns, context, last_key, 10_000,
                date_from=effective_warmup_start,
                date_to=effective_evaluation_end,
            )
            if not page:
                break
            for row in page:
                values = {column: row[column] for column in selected_columns}
                day, code = values.pop("research_date"), values.pop("ts_code")
                delist_dates[code] = values.get("delist_date")
                observations.setdefault(day, []).append(Observation(day, code, values))
            last_key = (page[-1]["research_date"], page[-1]["ts_code"])
        dividends: dict[date, list[DividendEvent]] = {}
        for row in repository.load_dividends(dataset_id, context):
            event = _dividend_event_from_materialized_row(row)
            dividends.setdefault(event.ex_date, []).append(event)
        rules = _rules_for_calendar(calendar, repository.load_rules(dataset_id, context))
        member_roles, snapshot_ids = repository.member_roles_and_snapshots(dataset_id, context)
        return BacktestDataset(dataset_id=dataset_id, calendar=calendar, evaluation_start=effective_evaluation_start, evaluation_end=effective_evaluation_end, warmup_start=effective_warmup_start, observations={day: tuple(rows) for day, rows in observations.items()}, bars=bars, dividends={day: tuple(rows) for day, rows in dividends.items()}, member_roles=member_roles, snapshot_ids=snapshot_ids, rules=rules, delist_dates=delist_dates, benchmark_code=manifest["benchmark_code"], benchmark_closes=repository.load_benchmark_closes(dataset_id, manifest["benchmark_code"], context), out_of_sample_start=effective_oos_start, input_hash=manifest["input_manifest_hash"], require_explicit_rules=True)
    finally:
        kernel.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Backtest Dataset 管理")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create")
    create.add_argument("--snapshot-id", action="append", required=True)
    create.add_argument("--name", required=True)
    create.add_argument("--version", required=True)
    create.add_argument("--baseline-tag", required=True)
    create.add_argument("--warmup-start", required=True)
    create.add_argument("--evaluation-start", required=True)
    create.add_argument("--evaluation-end", required=True)
    create.add_argument("--out-of-sample-start")
    create.add_argument("--market-rule-version", default="cn-a-share-v2")
    create.add_argument("--fee-rule-version", default="research-fee-assumption-v3")
    create.add_argument("--benchmark-code", default="000300.SH")
    compose = sub.add_parser("compose")
    compose.add_argument(
        "--component-dataset-id", action="append", required=True,
        help="按时间顺序传入历史 Dataset/组合 Dataset，再传入新增冻结 Dataset；可重复",
    )
    compose.add_argument("--name", required=True)
    compose.add_argument("--version", required=True)
    compose.add_argument("--dataset-id", help="可选固定 ID，便于失败后恢复创建")
    build = sub.add_parser("build")
    build.add_argument("--dataset-id", required=True)
    validate = sub.add_parser("validate")
    validate.add_argument("--dataset-id", required=True)
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--dataset-id", required=True)
    freeze.add_argument("--code-archive-path")
    args = parser.parse_args(argv)
    if args.command == "create":
        print(create_dataset(args.snapshot_id, args.name, args.version, args.baseline_tag,
                              date.fromisoformat(args.warmup_start), date.fromisoformat(args.evaluation_start),
                              date.fromisoformat(args.evaluation_end),
                              out_of_sample_start=(
                                  date.fromisoformat(args.out_of_sample_start)
                                  if args.out_of_sample_start else None
                              ),
                              market_rule_version=args.market_rule_version,
                              fee_rule_version=args.fee_rule_version,
                              benchmark_code=args.benchmark_code))
    elif args.command == "compose":
        print(create_composite_dataset(
            args.component_dataset_id, args.name, args.version,
            dataset_id=args.dataset_id,
        ))
    elif args.command == "build":
        print(json.dumps(build_dataset(args.dataset_id), ensure_ascii=False, default=str))
    elif args.command == "validate":
        print(json.dumps(validate_dataset(args.dataset_id), ensure_ascii=False, default=str))
    elif args.command == "freeze":
        freeze_dataset(args.dataset_id, args.code_archive_path)
        print(f"FROZEN: {args.dataset_id}")


if __name__ == "__main__":
    main()
