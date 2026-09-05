"""Backtest Dataset 的创建、物化、校验和冻结。

该任务只读取已 FROZEN 的 Research Snapshot、Standard/PIT/行情派生表，并把正式
回测所需的执行输入复制到 ``backtest`` 数据集表。不会修改任何 ``*_raw`` 或已有
Research 快照。生产回测必须先通过本模块的 ``freeze``，再调用引擎。
"""
from __future__ import annotations

import argparse
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
)
from data_collect.backtest.risk import FeeRule, MarketRule
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
from data_collect.data_access.research_contract import STRATEGY_NEUTRAL_SCHEMA_VERSION


SCHEMA_VERSION = "backtest-dataset-v2"
ADAPTER_VERSION = "research-to-backtest-v2"
FEATURE_VERSION = "research-foundation-features-v2"
SUPPORTED_RESEARCH_SCHEMA_VERSIONS = frozenset(
    {RESEARCH_SCHEMA_VERSION, FOUNDATION_SCHEMA_VERSION, STRATEGY_NEUTRAL_SCHEMA_VERSION}
)
HASH_ALGORITHM = "sha256"
MARKET_COVERAGE_POLICY = "validated_baseline_quality_report_equivalent_for_dates_without_gate"

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
# "baseline_quality_report" 鏁版嵁鍩虹嚎 quality_report_hash 宸插彉鍖? Dataset 杈圭晫蹇呴』鏄惧紑甯傛棩
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


SELECTION_PREDICATE = (
    "dataset_member.research_date within warmup/evaluation range; "
    "source research snapshot status=FROZEN; implementation dividend status=实施"
)
SELECTED_COLUMNS = {
    "research": [
        "research_date", "ts_code", "list_date", "delist_date", "raw_close", "adj_close_base", "ma20", "ma60",
        "volume_ratio_20d", "pe_ttm", "pe_pct_5y", "pb_pct_5y", "is_pe_meaningful",
        "is_listed_asof", "is_st_asof", "is_tradeable", "can_buy", "can_sell",
        "eligible_by_market", "quality_status", "effective_announce_date",
        "financial_version_available_date", "financial_record_version",
        "financial_source_record_key", "financial_payload_hash",
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
    def normalize(value):
        if value is None:
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        # NaN/Inf、非正值和 Tushare 的 100000 哨兵都不是可撮合的限制价。
        # 这里只改变 Dataset 物化结果，不改写 Raw/Standard 源数据。
        return None if not math.isfinite(numeric) or numeric <= 0 or numeric >= 99999 else numeric

    up = normalize(up_limit)
    down = normalize(down_limit)
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


def _assert_build_code_current(manifest: dict[str, Any], metadata: dict[str, Any]) -> None:
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
    if mismatches:
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
        if len(schema_versions) != 1 or not schema_versions <= SUPPORTED_RESEARCH_SCHEMA_VERSIONS:
            raise ValueError(f"Research Snapshot schema_version 不兼容: {sorted(schema_versions)}")
        if any(row["data_baseline_tag"] != baseline_tag for row in rows):
            raise ValueError("Dataset 基线必须与所有 Research Snapshot 的 data_baseline_tag 一致")
        if any(row["baseline_quality_report_hash"] != baseline["quality_report_hash"] for row in rows):
            raise ValueError("Research Snapshot 绑定的基线质量哈希与 Dataset 基线不一致")
        if any(row["baseline_source_manifest_hash"] != baseline["source_manifest_hash"] for row in rows):
            raise ValueError("Research Snapshot 绑定的 Raw 来源边界与 Dataset 基线不一致")
        if not any(evaluation_start <= day <= evaluation_end for day in dates):
            raise ValueError("没有覆盖 evaluation 区间的 Research Snapshot")
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
            "availability_rules": _json_param(AVAILABILITY_RULES),
        }
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
            last_key = tuple(rows[-1][:2])
        calendar_rows = repository.materialized_calendar_rows(dataset_id, context)
        dividend_rows = repository.materialized_dividend_rows(dataset_id, context)
        dividend_by_day: dict[date, list[tuple]] = {}
        for row in dividend_rows:
            dividend_by_day.setdefault(row[10], []).append(row)

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
    }


def _validate_dataset_impl(dataset_id: str) -> dict[str, Any]:
    kernel, repository, context = _read_service("validate")
    try:
        manifest = repository.manifest(dataset_id, context)
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
        counts = repository.validation_counts(dataset_id, context)
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


def validate_dataset(dataset_id: str) -> dict[str, Any]:
    try:
        return _validate_dataset_impl(dataset_id)
    except Exception as exc:
        _mark_dataset_validation_failed(dataset_id, exc)
        raise


def freeze_dataset(dataset_id: str, code_archive_path: str | None = None):
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
        manifest = repository.manifest(dataset_id, context)
        if manifest["status"] != "VALIDATED":
            raise RuntimeError(f"只有 VALIDATED Dataset 可冻结: {manifest['status']}")
        _check_baseline(manifest["baseline"], manifest["evaluation_end"], expected_quality_report_hash=manifest.get("baseline_quality_report_hash"), expected_source_manifest_hash=manifest.get("baseline_source_manifest_hash"))
        _assert_build_code_current(manifest, metadata)
        changed = repository.transition_manifest(
            context, dataset_id, ("VALIDATED",), "FROZEN",
            {"code_archive_path": code_archive_path, "code_archive_sha256": archive_hash, "code_archive_size_bytes": archive_size, "code_version": metadata["code_version"], "git_commit": metadata["commit"], "git_dirty": metadata["dirty"], "diff_hash": metadata["diff_hash"]},
        )
        if changed != 1:
            raise RuntimeError("Backtest Dataset 状态在冻结期间发生变化，拒绝并发冻结")
    finally:
        kernel.close()


def load_frozen_dataset(dataset_id: str, observation_columns: Sequence[str] | None = None) -> BacktestDataset:
    kernel, repository, context = _read_service("load")
    try:
        manifest = repository.manifest(dataset_id, context)
        if manifest["status"] != "FROZEN":
            raise RuntimeError(f"只有 FROZEN Dataset 可以用于正式回测: {manifest['status']}")
        calendar = repository.open_calendar(dataset_id, context)
        bars: dict[date, dict[str, MarketBar]] = {}
        last_key = None
        while True:
            page = repository.bar_page(dataset_id, context, last_key, 10_000)
            if not page:
                break
            for row in page:
                values = {key: row[key] for key in ("trade_date", "ts_code", "raw_open", "raw_high", "raw_low", "raw_close", "volume", "amount", "up_limit", "down_limit", "is_suspended", "can_buy", "can_sell", "limit_status", "market")}
                bar = MarketBar(**values)
                bars.setdefault(bar.trade_date, {})[bar.ts_code] = bar
            last_key = (page[-1]["trade_date"], page[-1]["ts_code"])
        all_columns = tuple(SELECTED_COLUMNS["research"])
        selected = list(observation_columns or all_columns[2:])
        if set(selected) - set(all_columns[2:]):
            raise ValueError(f"unknown frozen observation columns: {sorted(set(selected) - set(all_columns[2:]))}")
        selected_columns = ["research_date", "ts_code", *selected]
        observations: dict[date, list[Observation]] = {}
        delist_dates: dict[str, date | None] = {}
        last_key = None
        while True:
            page = repository.observation_page(dataset_id, selected_columns, context, last_key, 10_000)
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
            event = DividendEvent(*row)
            dividends.setdefault(event.ex_date, []).append(event)
        rules = {}
        for row in repository.load_rules(dataset_id, context):
            (*_, parameters, _rule_hash) = row
            effective_from, effective_to, market = row[3], row[4], row[5]
            market_rule = MarketRule(market=market, min_buy_lot=row[6], allow_sell_odd_lot=row[7], t_plus_one=row[8], price_tick=row[9], cash_settlement_days=row[10], max_volume_participation_pct=float((parameters or {}).get("max_volume_participation_pct", 1.0)), liquidity_policy=str((parameters or {}).get("liquidity_policy", "reject_full_order")))
            fee_rule = FeeRule(commission_rate=row[12], min_commission=row[13], stamp_duty_sell_rate=row[14], transfer_fee_rate=row[15], slippage_bps=row[16])
            for day in calendar:
                if effective_from <= day and (effective_to is None or day <= effective_to):
                    rules[(day, None if market in {"ALL", "*"} else market)] = (market_rule, fee_rule)
        member_roles, snapshot_ids = repository.member_roles_and_snapshots(dataset_id, context)
        return BacktestDataset(dataset_id=dataset_id, calendar=calendar, evaluation_start=manifest["evaluation_start"], evaluation_end=manifest["evaluation_end"], warmup_start=manifest["warmup_start"], observations={day: tuple(rows) for day, rows in observations.items()}, bars=bars, dividends={day: tuple(rows) for day, rows in dividends.items()}, member_roles=member_roles, snapshot_ids=snapshot_ids, rules=rules, delist_dates=delist_dates, benchmark_code=manifest["benchmark_code"], benchmark_closes=repository.load_benchmark_closes(dataset_id, manifest["benchmark_code"], context), out_of_sample_start=manifest["out_of_sample_start"], input_hash=manifest["input_manifest_hash"], require_explicit_rules=True)
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
    elif args.command == "build":
        print(json.dumps(build_dataset(args.dataset_id), ensure_ascii=False, default=str))
    elif args.command == "validate":
        print(json.dumps(validate_dataset(args.dataset_id), ensure_ascii=False, default=str))
    elif args.command == "freeze":
        freeze_dataset(args.dataset_id, args.code_archive_path)
        print(f"FROZEN: {args.dataset_id}")


if __name__ == "__main__":
    main()
