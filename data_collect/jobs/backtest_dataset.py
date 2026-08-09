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
from typing import Any, Iterable

from data_collect.backtest.models import (
    BacktestDataset,
    DividendEvent,
    MarketBar,
    Observation,
)
from data_collect.backtest.risk import FeeRule, MarketRule
from data_collect.jobs.research_snapshot import (
    SCHEMA_VERSION as RESEARCH_SCHEMA_VERSION, _sha256_file,
)
from data_collect.ops.market_coverage import assert_market_coverage_range_publishable
from data_collect.utils.db import get_connection, require_psycopg2


SCHEMA_VERSION = "backtest-dataset-v2"
ADAPTER_VERSION = "research-to-backtest-v2"
FEATURE_VERSION = "price_daily_v2+valuation_daily_v1+company_financial_v1+shareholder_return_v1"
HASH_ALGORITHM = "sha256"
MARKET_COVERAGE_POLICY = "validated_baseline_quality_report_equivalent_for_dates_without_gate"
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


def _manifest(conn, dataset_id: str, *, for_update: bool = False) -> dict[str, Any]:
    with conn.cursor() as cur:
        query = """SELECT dataset_id, dataset_version, dataset_name, data_baseline_tag,
                       baseline_quality_report_hash, baseline_source_manifest_hash,
                       schema_version, adapter_version, feature_version, warmup_start,
                        evaluation_start, evaluation_end, out_of_sample_start,
                        market_rule_version, fee_rule_version,
                        status, input_manifest_hash,
                        selection_predicate, selected_columns, hash_algorithm, availability_rules,
                        calendar_hash, rules_hash, execution_hash, corporate_action_hash,
                        benchmark_code, benchmark_hash,
                        quality_report_hash, code_version, git_commit, git_dirty, diff_hash,
                        code_archive_path, code_archive_sha256, code_archive_size_bytes
                 FROM backtest.dataset_manifest WHERE dataset_id=%s"""
        if for_update:
            query += " FOR UPDATE"
        cur.execute(query, (dataset_id,))
        row = cur.fetchone()
    if not row:
        raise ValueError(f"不存在的 Backtest Dataset: {dataset_id}")
    names = ("dataset_id", "dataset_version", "dataset_name", "baseline",
             "baseline_quality_report_hash", "baseline_source_manifest_hash",
             "schema_version", "adapter_version",
             "feature_version", "warmup_start", "evaluation_start",
              "evaluation_end", "out_of_sample_start", "market_rule_version", "fee_rule_version",
              "status", "input_manifest_hash", "selection_predicate",
              "selected_columns", "hash_algorithm", "availability_rules", "calendar_hash",
              "rules_hash", "execution_hash", "corporate_action_hash",
              "benchmark_code", "benchmark_hash", "quality_report_hash",
              "code_version", "git_commit", "git_dirty", "diff_hash", "code_archive_path",
              "code_archive_sha256", "code_archive_size_bytes")
    return dict(zip(names, row))


_BASELINE_HASH_UNSET = object()


def _check_baseline(
    conn,
    baseline_tag: str,
    evaluation_end: date,
    expected_quality_report_hash: str | None | object = _BASELINE_HASH_UNSET,
    expected_source_manifest_hash: str | None | object = _BASELINE_HASH_UNSET,
) -> dict[str, Any]:
    """校验基线，并返回 Dataset 必须绑定的不可变 Raw 边界。"""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT status, baseline_cutoff_date, quality_report_hash,
                      source_manifest_hash, source_manifest
                 FROM tushare_data_release
               WHERE release_tag=%s""", (baseline_tag,)
        )
        row = cur.fetchone()
    if not row or row[0] != "validated":
        raise ValueError(f"数据基线不存在或未 validated: {baseline_tag}")
    if row[1] < evaluation_end:
        raise ValueError(f"数据基线截止日 {row[1]} 早于回测结束日 {evaluation_end}")
    actual_quality_report_hash = str(row[2]).strip() if row[2] else ""
    if not actual_quality_report_hash:
        raise ValueError(f"数据基线缺少 quality_report_hash，不能作为历史覆盖门禁等价物: {baseline_tag}")
    actual_source_manifest_hash = str(row[3]).strip() if row[3] else ""
    source_manifest = row[4]
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
        "cutoff": row[1],
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
    """创建 DRAFT Dataset，并绑定所有单日 FROZEN Snapshot。"""
    if not snapshot_ids:
        raise ValueError("至少需要一个 FROZEN Research Snapshot")
    if not benchmark_code or "." not in benchmark_code:
        raise ValueError("benchmark_code 必须是 Tushare 指数代码")
    if not warmup_start <= evaluation_start <= evaluation_end:
        raise ValueError("warmup/evaluation 日期范围无效")
    dataset_id = dataset_id or str(uuid.uuid4())
    unique_ids = list(dict.fromkeys(snapshot_ids))
    with get_connection() as conn, conn.cursor() as cur:
        baseline = _check_baseline(conn, baseline_tag, evaluation_end)
        baseline_quality_report_hash = baseline["quality_report_hash"]
        baseline_source_manifest_hash = baseline["source_manifest_hash"]
        cur.execute(
            """SELECT trade_date, is_open
                 FROM trading_calendar
                WHERE exchange='SSE' AND trade_date = ANY(%s)""",
            ([warmup_start, evaluation_start, evaluation_end],),
        )
        calendar_status = {row[0]: row[1] for row in cur.fetchall()}
        for required_day in (warmup_start, evaluation_start, evaluation_end):
            if calendar_status.get(required_day) is not True:
                raise ValueError(
                    f"Dataset 边界必须是 SSE 开市日且存在交易日历: {required_day}"
                )
        cur.execute(
            """SELECT snapshot_id, research_cutoff_date, status, input_manifest_hash,
                              schema_version, data_baseline_tag,
                              baseline_quality_report_hash,
                              baseline_source_manifest_hash
               FROM research.snapshot_manifest WHERE snapshot_id = ANY(%s::uuid[])""",
            (unique_ids,),
        )
        rows = cur.fetchall()
        by_id = {str(row[0]): row for row in rows}
        if len(by_id) != len(unique_ids):
            missing = [item for item in unique_ids if item not in by_id]
            raise ValueError(f"Research Snapshot 不存在: {missing}")
        dates = [row[1] for row in rows]
        if len(set(dates)) != len(dates):
            raise ValueError("一个 Dataset 不能包含重复研究日期")
        cur.execute(
            """SELECT trade_date, is_open
                 FROM trading_calendar
                WHERE exchange='SSE' AND trade_date = ANY(%s)""",
            (dates,),
        )
        snapshot_calendar = {row[0]: row[1] for row in cur.fetchall()}
        closed_snapshot_dates = [day for day in dates if snapshot_calendar.get(day) is not True]
        if closed_snapshot_dates:
            raise ValueError(
                f"Research Snapshot 成员日期必须是 SSE 开市日: {closed_snapshot_dates}"
            )
        cur.execute(
            """SELECT trade_date
                 FROM trading_calendar
                WHERE exchange='SSE' AND is_open IS TRUE
                  AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date""",
            (warmup_start, evaluation_end),
        )
        required_open_dates = [row[0] for row in cur.fetchall()]
        evaluation_open_dates = [
            day for day in required_open_dates if evaluation_start <= day <= evaluation_end
        ]
        if not evaluation_open_dates:
            raise ValueError("evaluation 区间没有 SSE 开市日")
        if out_of_sample_start is None:
            split_index = min(
                len(evaluation_open_dates) - 1,
                max(0, int(len(evaluation_open_dates) * 0.8)),
            )
            out_of_sample_start = evaluation_open_dates[split_index]
        if out_of_sample_start not in evaluation_open_dates:
            raise ValueError("out_of_sample_start 必须是 evaluation 区间内的 SSE 开市日")
        missing_snapshot_days = missing_snapshot_dates(required_open_dates, dates)
        if missing_snapshot_days:
            raise ValueError(
                "Dataset requires one FROZEN Research Snapshot for every SSE open day "
                f"from warmup_start through evaluation_end; missing "
                f"{len(missing_snapshot_days)} dates, first={missing_snapshot_days[0]}"
            )
        if any(row[2] != "FROZEN" for row in rows):
            raise ValueError("Backtest Dataset 只能引用 FROZEN Research Snapshot")
        if any(row[4] != RESEARCH_SCHEMA_VERSION for row in rows):
            raise ValueError(
                f"Backtest Dataset 只接受 {RESEARCH_SCHEMA_VERSION} Research Snapshot；"
                "旧版本快照必须重新生成，不能回写 FROZEN 子表"
            )
        if any(row[5] != baseline_tag for row in rows):
            mismatched = sorted({str(row[5]) for row in rows})
            raise ValueError(
                "Dataset 基线必须与所有 Research Snapshot 的 data_baseline_tag 一致: "
                f"requested={baseline_tag}, snapshots={mismatched}"
            )
        if any(row[6] != baseline_quality_report_hash for row in rows):
            raise ValueError(
                "Research Snapshot 绑定的基线质量哈希与 Dataset 基线不一致"
            )
        if any(row[7] != baseline_source_manifest_hash for row in rows):
            raise ValueError(
                "Research Snapshot 绑定的 Raw 来源边界与 Dataset 基线不一致"
            )
        if not any(evaluation_start <= day <= evaluation_end for day in dates):
            raise ValueError("没有覆盖 evaluation 区间的 Research Snapshot")
        cur.execute(
            """INSERT INTO backtest.dataset_manifest
              (dataset_id,dataset_version,dataset_name,data_baseline_tag,
               baseline_quality_report_hash,baseline_source_manifest_hash,schema_version,
               adapter_version,feature_version,warmup_start,evaluation_start,evaluation_end,
               out_of_sample_start,
                calendar_rule_version,market_rule_version,fee_rule_version,
                benchmark_code,selection_predicate,selected_columns,hash_algorithm,availability_rules)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (dataset_id, dataset_version, dataset_name, baseline_tag,
              baseline_quality_report_hash, baseline_source_manifest_hash, SCHEMA_VERSION,
              ADAPTER_VERSION, FEATURE_VERSION, warmup_start, evaluation_start, evaluation_end,
              out_of_sample_start,
              calendar_rule_version, market_rule_version, fee_rule_version,
              benchmark_code, SELECTION_PREDICATE, _json_param(SELECTED_COLUMNS), HASH_ALGORITHM,
              _json_param(AVAILABILITY_RULES)),
        )
        member_rows = []
        for snapshot_id in unique_ids:
            row = by_id[snapshot_id]
            role = "EVALUATION" if evaluation_start <= row[1] <= evaluation_end else "WARMUP"
            if row[1] < warmup_start or row[1] > evaluation_end:
                raise ValueError(f"Snapshot 日期 {row[1]} 超出 Dataset 范围")
            member_rows.append((dataset_id, row[0], row[1], role, row[3]))
        execute_values = require_psycopg2()[1]
        execute_values(
            cur,
            """INSERT INTO backtest.dataset_member
               (dataset_id,research_snapshot_id,research_date,member_role,snapshot_input_hash)
               VALUES %s""",
            member_rows,
            page_size=500,
        )
        conn.commit()
    return dataset_id


def _insert_values(cur, sql: str, rows: list[tuple]):
    if not rows:
        return
    execute_values = require_psycopg2()[1]
    execute_values(cur, sql, rows, page_size=2000)


def _json_param(value: Any):
    """以 psycopg2 JSONB 适配器传参，避免把 JSON 当普通文本写入。"""
    psycopg2, _ = require_psycopg2()
    return psycopg2.extras.Json(value, dumps=lambda item: json.dumps(item, ensure_ascii=False))


def build_dataset(dataset_id: str) -> dict[str, Any]:
    """从冻结快照及现有标准层物化执行输入。

    函数在单个 REPEATABLE READ 事务中读取，子表先清空再重建；Dataset 未冻结时这
    是可重试的。任何异常都会将 Manifest 标记为 FAILED，不会留下可用的半成品。
    """
    import psycopg2

    try:
        with get_connection() as conn:
            conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)
            with conn.cursor() as cur:
                manifest = _manifest(conn, dataset_id, for_update=True)
                if manifest["status"] not in {"DRAFT", "FAILED"}:
                    raise RuntimeError(f"只有 DRAFT/FAILED Dataset 可构建: {manifest['status']}")
                baseline = _check_baseline(
                    conn,
                    manifest["baseline"],
                    manifest["evaluation_end"],
                    expected_quality_report_hash=manifest.get("baseline_quality_report_hash"),
                    expected_source_manifest_hash=manifest.get("baseline_source_manifest_hash"),
                )
                source_manifest = baseline["source_manifest"]
                daily_max_record_id = (
                    source_manifest.get("market_versions", {})
                    .get("daily", {})
                    .get("max_record_id")
                )
                legacy_daily_max_fetched_at = (
                    source_manifest.get("legacy_market_raw", {})
                    .get("tushare_daily_raw", {})
                    .get("max_fetched_at")
                )
                trade_cal_max_record_id = (
                    source_manifest.get("market_versions", {})
                    .get("trade_cal", {})
                    .get("max_record_id")
                )
                legacy_trade_cal_max_fetched_at = (
                    source_manifest.get("legacy_market_raw", {})
                    .get("tushare_trade_cal_raw", {})
                    .get("max_fetched_at")
                )
                append_only_sources = source_manifest.get("append_only_sources", {})
                limit_max_record_id = (
                    append_only_sources.get("tushare_stk_limit_raw", {})
                    .get("max_record_id")
                )
                suspend_max_record_id = (
                    append_only_sources.get("tushare_suspend_d_raw", {})
                    .get("max_record_id")
                )
                dividend_max_record_id = (
                    append_only_sources.get("tushare_dividend_raw", {})
                    .get("max_record_id")
                )
                required_boundaries = {
                    "daily": daily_max_record_id,
                    "trade_cal": trade_cal_max_record_id,
                    "stk_limit": limit_max_record_id,
                    "suspend_d": suspend_max_record_id,
                    "dividend": dividend_max_record_id,
                }
                missing_boundaries = [
                    name for name, value in required_boundaries.items() if value is None
                ]
                if missing_boundaries:
                    raise RuntimeError(
                        "Baseline 缺少正式 Dataset 所需 Raw 高水位: "
                        + ", ".join(missing_boundaries)
                    )
                assert_market_coverage_range_publishable(
                    manifest["warmup_start"], manifest["evaluation_end"], connection=conn
                )
                cur.execute("UPDATE backtest.dataset_manifest SET status='BUILDING', failure_reason=NULL WHERE dataset_id=%s", (dataset_id,))
                cur.execute("DELETE FROM backtest.dataset_execution_bar WHERE dataset_id=%s", (dataset_id,))
                cur.execute("DELETE FROM backtest.dataset_calendar WHERE dataset_id=%s", (dataset_id,))
                cur.execute("DELETE FROM backtest.dataset_dividend_event WHERE dataset_id=%s", (dataset_id,))
                cur.execute("DELETE FROM backtest.dataset_rule_snapshot WHERE dataset_id=%s", (dataset_id,))
                cur.execute("DELETE FROM backtest.dataset_benchmark_bar WHERE dataset_id=%s", (dataset_id,))

                stream_cur = conn.cursor(name=f"dataset_execution_{dataset_id.replace('-', '')}")
                stream_cur.itersize = 5000
                stream_cur.execute(
                    """SELECT m.research_date, s.ts_code, s.market,
                              NULLIF(COALESCE(v.payload, r.payload)->>'open', '')::double precision,
                              NULLIF(COALESCE(v.payload, r.payload)->>'high', '')::double precision,
                              NULLIF(COALESCE(v.payload, r.payload)->>'low', '')::double precision,
                              NULLIF(COALESCE(v.payload, r.payload)->>'close', '')::double precision,
                              NULLIF(COALESCE(v.payload, r.payload)->>'vol', '')::double precision * 100,
                              NULLIF(COALESCE(v.payload, r.payload)->>'amount', '')::double precision * 1000,
                              NULLIF(lim.payload->>'up_limit', '')::double precision,
                              NULLIF(lim.payload->>'down_limit', '')::double precision,
                              (COALESCE(s.is_suspended_asof, FALSE) OR sus.record_id IS NOT NULL),
                              (COALESCE(s.can_buy, FALSE)
                               AND NOT (COALESCE(s.is_suspended_asof, FALSE) OR sus.record_id IS NOT NULL)),
                              (COALESCE(s.can_sell, FALSE)
                               AND NOT (COALESCE(s.is_suspended_asof, FALSE) OR sus.record_id IS NOT NULL)),
                              (COALESCE(v.payload, r.payload) IS NOT NULL
                               AND lim.record_id IS NOT NULL
                               AND s.quality_status IN ('VALID', 'WARNING')),
                              s.limit_status, COALESCE(v.payload, r.payload),
                              v.record_id, v.payload_hash,
                              lim.record_id, lim.payload_hash,
                              sus.record_id, sus.payload_hash
                         FROM backtest.dataset_member m
                         JOIN research.stock_snapshot_daily s
                           ON s.snapshot_id=m.research_snapshot_id
                          AND s.research_date=m.research_date
                         LEFT JOIN LATERAL (
                             SELECT record_id, payload, payload_hash
                              FROM tushare_market_raw_version mv
                              WHERE mv.dataset='daily'
                                AND mv.business_key=s.ts_code || ':' || to_char(m.research_date,'YYYYMMDD')
                                AND mv.record_id <= %s
                              ORDER BY mv.fetched_at DESC, mv.record_id DESC LIMIT 1
                         ) v ON TRUE
                         LEFT JOIN LATERAL (
                             SELECT legacy.payload
                               FROM tushare_daily_raw legacy
                              WHERE v.record_id IS NULL
                                AND legacy.ts_code=s.ts_code
                                AND legacy.trade_date=m.research_date
                                AND legacy.source='tushare'
                                AND legacy.fetched_at <= %s::timestamp
                              ORDER BY legacy.fetched_at DESC
                              LIMIT 1
                         ) r ON TRUE
                         LEFT JOIN LATERAL (
                             SELECT record_id, payload, payload_hash
                               FROM tushare_stk_limit_raw raw_limit
                              WHERE raw_limit.ts_code=s.ts_code
                                AND raw_limit.trade_date=m.research_date
                                AND raw_limit.record_id <= %s
                              ORDER BY raw_limit.fetched_at DESC, raw_limit.record_id DESC
                              LIMIT 1
                         ) lim ON TRUE
                         LEFT JOIN LATERAL (
                             SELECT record_id, payload_hash
                               FROM tushare_suspend_d_raw raw_suspend
                              WHERE raw_suspend.ts_code=s.ts_code
                                AND raw_suspend.trade_date=m.research_date
                                AND raw_suspend.record_id <= %s
                              ORDER BY raw_suspend.fetched_at DESC, raw_suspend.record_id DESC
                              LIMIT 1
                         ) sus ON TRUE
                        WHERE m.dataset_id=%s
                        ORDER BY m.research_date, s.ts_code""",
                    (
                        daily_max_record_id, legacy_daily_max_fetched_at,
                        limit_max_record_id, suspend_max_record_id, dataset_id,
                    ),
                )
                bar_rows = stream_cur
                if not bar_rows:
                    raise RuntimeError("Dataset 没有可物化的 Research 股票快照")
                execution_insert = []
                execution_hasher = _IncrementalRowHasher()
                per_date_stats: dict[date, dict[str, Any]] = {}
                execution_seen = False
                for row in bar_rows:
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
                        computed_hash = hashlib.sha256(json.dumps(_canonical(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
                    has_ohlc = all(
                        value is not None and float(value) > 0
                        for value in (opn, high, low, close)
                    )
                    geometry_valid = has_ohlc and float(low) <= min(float(opn), float(close)) <= max(float(opn), float(close)) <= float(high)
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
                        # 停牌、退市或当日不可交易且没有行情时，不伪造价格；
                        # 这些行保留为 WARNING，只有本应可执行的证券才阻断 Dataset。
                        quality = (
                            "WARNING"
                            if suspended or (not can_buy and not can_sell)
                            else "MISSING_SOURCE"
                        )
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
                        # 停牌日可以保留为显式 WARNING，由引擎用最近有效价格估值；
                        # 不能把缺失价格伪造成可成交行情。
                        quality = "WARNING"
                    else:
                        quality = "MISSING_SOURCE"
                    values = (
                        dataset_id, day, code, market, opn, high, low, close, volume, amount, up, down,
                        suspended, can_buy, can_sell, limit_status, f"{code}:{day}", version,
                        computed_hash, limit_version, limit_hash, suspend_version, suspend_hash,
                        quality,
                    )
                    execution_seen = True
                    execution_insert.append(values)
                    execution_hasher.update(values[1:])
                    stats = per_date_stats.setdefault(
                        day,
                        {
                            "hasher": _IncrementalRowHasher(),
                            "count": 0,
                            "missing": 0,
                            "invalid": 0,
                            "warning": 0,
                        },
                    )
                    stats["hasher"].update(values[1:])
                    stats["count"] += 1
                    if quality == "MISSING_SOURCE":
                        stats["missing"] += 1
                    elif quality == "INVALID":
                        stats["invalid"] += 1
                    elif quality == "WARNING":
                        stats["warning"] += 1
                    if len(execution_insert) >= 5000:
                        _insert_values(
                            cur,
                            """INSERT INTO backtest.dataset_execution_bar
                              (dataset_id,trade_date,ts_code,market,raw_open,raw_high,raw_low,raw_close,
                               volume,amount,up_limit,down_limit,is_suspended,can_buy,can_sell,
                               limit_status,source_record_key,source_record_version,source_payload_hash,
                               limit_source_record_id,limit_source_payload_hash,
                               suspend_source_record_id,suspend_source_payload_hash,
                               quality_status) VALUES %s""",
                            execution_insert,
                        )
                        execution_insert.clear()
                _insert_values(
                    cur,
                    """INSERT INTO backtest.dataset_execution_bar
                      (dataset_id,trade_date,ts_code,market,raw_open,raw_high,raw_low,raw_close,
                       volume,amount,up_limit,down_limit,is_suspended,can_buy,can_sell,
                       limit_status,source_record_key,source_record_version,source_payload_hash,
                       limit_source_record_id,limit_source_payload_hash,
                       suspend_source_record_id,suspend_source_payload_hash,
                       quality_status) VALUES %s""",
                    execution_insert,
                )
                stream_cur.close()
                if not execution_seen:
                    raise RuntimeError("Dataset 没有可物化的 Research 股票快照")
                cur.execute(
                    """SELECT research_date
                         FROM backtest.dataset_member
                        WHERE dataset_id=%s
                        ORDER BY research_date""",
                    (dataset_id,),
                )
                member_dates = [row[0] for row in cur.fetchall()]
                execution_dates = sorted(per_date_stats)
                if member_dates != execution_dates:
                    raise RuntimeError(
                        "Dataset 成员日期与执行行情日期不一致: "
                        f"members={len(member_dates)}, execution={len(execution_dates)}"
                    )

                benchmark_max_record_id = (
                    source_manifest.get("append_only_sources", {})
                    .get("tushare_index_daily_raw", {})
                    .get("max_record_id")
                )
                if benchmark_max_record_id is None:
                    raise RuntimeError("基线缺少指数 Raw 高水位")
                cur.execute(
                    """WITH chosen AS (
                           SELECT DISTINCT ON (r.trade_date)
                                  r.trade_date,r.ts_code,
                                  NULLIF(r.payload->>'close','')::double precision AS close,
                                  r.record_id,r.payload_hash
                             FROM tushare_index_daily_raw r
                             JOIN backtest.dataset_member m
                               ON m.dataset_id=%s AND m.research_date=r.trade_date
                            WHERE r.ts_code=%s AND r.record_id <= %s
                            ORDER BY r.trade_date,r.fetched_at DESC,r.record_id DESC
                       )
                       SELECT trade_date,ts_code,close,record_id,payload_hash
                         FROM chosen ORDER BY trade_date""",
                    (dataset_id, manifest["benchmark_code"], benchmark_max_record_id),
                )
                benchmark_source_rows = cur.fetchall()
                if len(benchmark_source_rows) != len(member_dates):
                    raise RuntimeError(
                        "基准指数未覆盖全部 Dataset 日期: "
                        f"benchmark={manifest['benchmark_code']}, "
                        f"rows={len(benchmark_source_rows)}, expected={len(member_dates)}"
                    )
                if any(row[2] is None or float(row[2]) <= 0 for row in benchmark_source_rows):
                    raise RuntimeError("基准指数存在非正或缺失收盘价")
                _insert_values(
                    cur,
                    """INSERT INTO backtest.dataset_benchmark_bar
                       (dataset_id,trade_date,ts_code,close,source_record_key,
                        source_record_version,source_payload_hash) VALUES %s""",
                    [
                        (dataset_id, day, code, close, f"{code}:{day}", version, payload_hash)
                        for day, code, close, version, payload_hash in benchmark_source_rows
                    ],
                )
                cur.execute(
                    """SELECT trade_date,ts_code,close,source_record_key,
                              source_record_version,source_payload_hash
                         FROM backtest.dataset_benchmark_bar
                        WHERE dataset_id=%s ORDER BY trade_date""",
                    (dataset_id,),
                )
                actual_benchmark_rows = cur.fetchall()

                cur.execute(
                    """WITH days AS (
                           SELECT generate_series(%s::date, %s::date, interval '1 day')::date AS trade_date
                       )
                       SELECT d.trade_date,
                              COALESCE(v.payload->>'exchange', r.payload->>'exchange', 'SSE'),
                              COALESCE(
                                  NULLIF(v.payload->>'is_open','')::integer,
                                  NULLIF(r.payload->>'is_open','')::integer
                              ) = 1,
                              to_date(NULLIF(COALESCE(
                                  v.payload->>'pretrade_date', r.payload->>'pretrade_date'
                              ), ''), 'YYYYMMDD'),
                              v.record_id,
                              COALESCE(
                                  v.payload_hash,
                                  encode(digest(convert_to(r.payload::text,'UTF8'),'sha256'),'hex')
                              )
                         FROM days d
                         LEFT JOIN LATERAL (
                             SELECT record_id,payload,payload_hash
                               FROM tushare_market_raw_version mv
                              WHERE mv.dataset='trade_cal'
                                AND mv.exchange='SSE'
                                AND mv.cal_date=d.trade_date
                                AND mv.record_id <= %s
                              ORDER BY mv.fetched_at DESC,mv.record_id DESC LIMIT 1
                         ) v ON TRUE
                         LEFT JOIN LATERAL (
                             SELECT payload
                               FROM tushare_trade_cal_raw legacy
                              WHERE v.record_id IS NULL
                                AND legacy.exchange='SSE'
                                AND legacy.cal_date=d.trade_date
                                AND legacy.fetched_at <= %s::timestamp
                              ORDER BY legacy.fetched_at DESC LIMIT 1
                         ) r ON TRUE
                        WHERE COALESCE(v.payload,r.payload) IS NOT NULL
                        ORDER BY d.trade_date""",
                    (
                        manifest["warmup_start"], manifest["evaluation_end"],
                        trade_cal_max_record_id, legacy_trade_cal_max_fetched_at,
                    ),
                )
                calendar_rows = cur.fetchall()
                if not calendar_rows:
                    raise RuntimeError("Dataset 范围没有交易日历")
                expected_calendar_days = (
                    manifest["evaluation_end"] - manifest["warmup_start"]
                ).days + 1
                if len(calendar_rows) != expected_calendar_days:
                    raise RuntimeError(
                        "Baseline Raw 交易日历区间不完整: "
                        f"rows={len(calendar_rows)}, expected={expected_calendar_days}"
                    )
                calendar_insert = []
                for cal_date, exchange, is_open, pretrade_date, version, source_hash in calendar_rows:
                    calendar_insert.append((
                        dataset_id, cal_date, exchange, is_open, pretrade_date,
                        f"{exchange}:{cal_date}", version, source_hash,
                    ))
                _insert_values(
                    cur,
                    """INSERT INTO backtest.dataset_calendar
                      (dataset_id,trade_date,exchange,is_open,pretrade_date,
                       source_record_key,source_record_version,source_payload_hash) VALUES %s""",
                    calendar_insert,
                )
                cur.execute(
                    """SELECT trade_date, exchange, is_open, pretrade_date,
                              source_record_key, source_record_version, source_payload_hash
                         FROM backtest.dataset_calendar
                        WHERE dataset_id=%s
                        ORDER BY trade_date, exchange""",
                    (dataset_id,),
                )
                actual_calendar_rows = cur.fetchall()

                cur.execute(
                    """WITH candidates AS (
                               SELECT record_id, ts_code, end_date AS report_date,
                                      ann_date AS announce_date, div_proc AS implementation_status,
                                      NULLIF(payload->>'stk_div','')::double precision AS stock_div,
                                      NULLIF(payload->>'stk_bo_rate','')::double precision AS stock_bonus_rate,
                                      NULLIF(payload->>'stk_co_rate','')::double precision AS stock_conversion_rate,
                                      NULLIF(payload->>'cash_div','')::double precision AS cash_div,
                                      NULLIF(payload->>'cash_div_tax','')::double precision AS cash_div_tax,
                                      record_date, ex_date,
                                      to_date(NULLIF(payload->>'pay_date',''),'YYYYMMDD') AS pay_date,
                                      to_date(NULLIF(payload->>'div_listdate',''),'YYYYMMDD') AS div_list_date,
                                      to_date(NULLIF(payload->>'imp_ann_date',''),'YYYYMMDD') AS implementation_announce_date,
                                      ts_code || ':' || COALESCE(end_date::text,'') || ':' ||
                                      COALESCE(div_proc,'') || ':' || COALESCE(record_date::text,'') || ':' ||
                                      COALESCE(ex_date::text,'') || ':' ||
                                      COALESCE(payload->>'imp_ann_date','') AS event_key,
                                      payload_hash, fetched_at
                                 FROM tushare_dividend_raw
                                WHERE record_id <= %s
                                  AND div_proc='实施'
                                  AND ex_date IS NOT NULL
                                  AND ex_date <= %s
                                  AND COALESCE(
                                      to_date(NULLIF(payload->>'pay_date',''),'YYYYMMDD'), ex_date
                                  ) >= %s
                                  AND COALESCE(
                                      to_date(NULLIF(payload->>'imp_ann_date',''),'YYYYMMDD'),
                                      ann_date, end_date, ex_date
                                  ) <= ex_date
                           ), chosen AS (
                               SELECT DISTINCT ON (event_key) *
                                 FROM candidates
                                ORDER BY event_key, fetched_at DESC, record_id DESC
                           )
                           SELECT ts_code, report_date, announce_date, implementation_status,
                                  stock_div, stock_bonus_rate, stock_conversion_rate, cash_div,
                                  cash_div_tax, record_date, ex_date, pay_date, div_list_date,
                                  implementation_announce_date, event_key, record_id,
                                  payload_hash
                             FROM chosen
                            ORDER BY ex_date, ts_code, event_key, record_id""",
                    (
                        dividend_max_record_id, manifest["evaluation_end"],
                        manifest["warmup_start"],
                    ),
                )
                dividend_rows = cur.fetchall()
                dividend_insert = [(dataset_id, *row) for row in dividend_rows]
                _insert_values(
                    cur,
                    """INSERT INTO backtest.dataset_dividend_event
                      (dataset_id,ts_code,report_date,announce_date,implementation_status,
                       stock_div,stock_bonus_rate,stock_conversion_rate,cash_div,cash_div_tax,
                       record_date,ex_date,pay_date,div_list_date,implementation_announce_date,
                       source_record_key,source_record_version,source_payload_hash) VALUES %s""",
                    dividend_insert,
                )
                cur.execute(
                    """SELECT ts_code, report_date, announce_date, implementation_status,
                              stock_div, stock_bonus_rate, stock_conversion_rate, cash_div,
                              cash_div_tax, record_date, ex_date, pay_date, div_list_date,
                              implementation_announce_date, source_record_key,
                              source_record_version, source_payload_hash
                         FROM backtest.dataset_dividend_event
                        WHERE dataset_id=%s
                        ORDER BY ex_date, ts_code, source_record_key, source_record_version""",
                    (dataset_id,),
                )
                actual_dividend_rows = cur.fetchall()
                # Hash the materialized Dataset rows, not the mutable source cursor values.
                corporate_action_rows_by_day: dict[date, list[tuple]] = {}
                for actual_row in actual_dividend_rows:
                    corporate_action_rows_by_day.setdefault(actual_row[10], []).append(actual_row)

                cur.execute(
                    """SELECT rule_version, market_rule_version, fee_rule_version,
                              effective_from, effective_to, market,
                              min_buy_lot, allow_sell_odd_lot, t_plus_one, price_tick,
                              cash_settlement_days, limit_pct, commission_rate, min_commission,
                              stamp_duty_sell_rate, transfer_fee_rate, slippage_bps,
                              parameters, approval_status, approved_by,
                              approved_at, approval_hash
                         FROM backtest.market_rule_effective
                         WHERE effective_from <= %s
                           AND COALESCE(effective_to, DATE '9999-12-31') >= %s
                           AND market_rule_version = %s
                           AND fee_rule_version = %s
                           AND approval_status='APPROVED'
                           AND approved_by IS NOT NULL AND approved_at IS NOT NULL
                           AND approval_hash IS NOT NULL
                         ORDER BY effective_from, market, rule_version""",
                    (manifest["evaluation_end"], manifest["warmup_start"],
                     manifest["market_rule_version"], manifest["fee_rule_version"]),
                )
                source_rules = cur.fetchall()
                previous_by_market: dict[str, date | None] = {}
                for rule in source_rules:
                    rule_market = rule[5]
                    effective_from = rule[3]
                    if rule_market in previous_by_market:
                        previous_to = previous_by_market[rule_market]
                        if previous_to is None or effective_from <= previous_to:
                            raise RuntimeError(
                                f"市场规则有效区间重叠: market={rule_market}, effective_from={effective_from}"
                            )
                    previous_by_market[rule_market] = rule[4]
                if not source_rules:
                    raise RuntimeError("backtest.market_rule_effective 尚未配置，禁止使用隐式当前规则")
                rule_rows = []
                for rule in source_rules:
                    rule_hash = hash_rows([rule])
                    rule_rows.append((dataset_id, *rule[:17], _json_param(rule[17] or {}),
                                      *rule[18:], rule_hash))
                _insert_values(
                    cur,
                    """INSERT INTO backtest.dataset_rule_snapshot
                      (dataset_id,rule_version,market_rule_version,fee_rule_version,
                       effective_from,effective_to,market,
                       min_buy_lot,allow_sell_odd_lot,t_plus_one,price_tick,cash_settlement_days,limit_pct,
                       commission_rate,min_commission,stamp_duty_sell_rate,transfer_fee_rate,
                       slippage_bps,parameters,approval_status,approved_by,approved_at,
                       approval_hash,rule_hash) VALUES %s""",
                    rule_rows,
                )
                cur.execute(
                    """SELECT rule_version, market_rule_version, fee_rule_version,
                              effective_from, effective_to, market,
                              min_buy_lot, allow_sell_odd_lot, t_plus_one, price_tick,
                              cash_settlement_days, limit_pct, commission_rate, min_commission,
                              stamp_duty_sell_rate, transfer_fee_rate, slippage_bps,
                              parameters, approval_status, approved_by, approved_at,
                              approval_hash, rule_hash
                         FROM backtest.dataset_rule_snapshot
                        WHERE dataset_id=%s
                        ORDER BY effective_from, market, rule_version""",
                    (dataset_id,),
                )
                actual_rule_rows = cur.fetchall()

                member_stats = []
                for day in sorted(per_date_stats):
                    execution_stats = per_date_stats[day]
                    execution_hash_for_day = execution_stats["hasher"].hexdigest()
                    corporate_action_hash_for_day = hash_rows(
                        corporate_action_rows_by_day.get(day, [])
                    )
                    missing_for_day = execution_stats["missing"]
                    invalid_for_day = execution_stats["invalid"]
                    warning_for_day = execution_stats["warning"]
                    member_quality = (
                        "INVALID" if invalid_for_day else
                        "MISSING_SOURCE" if missing_for_day else
                        "WARNING" if warning_for_day else "VALID"
                    )
                    cur.execute(
                        """SELECT snapshot_input_hash, observation_row_count
                           FROM backtest.dataset_member
                          WHERE dataset_id=%s AND research_date=%s""",
                        (dataset_id, day),
                    )
                    member = cur.fetchone()
                    if not member:
                        raise RuntimeError(f"执行行情出现未绑定 Dataset 成员: {day}")
                    cur.execute(
                        """SELECT COUNT(*) FROM research.stock_snapshot_daily
                           WHERE snapshot_id=(SELECT research_snapshot_id FROM backtest.dataset_member
                                              WHERE dataset_id=%s AND research_date=%s)
                             AND research_date=%s""",
                        (dataset_id, day, day),
                    )
                    obs_count = cur.fetchone()[0]
                    cur.execute(
                        """UPDATE backtest.dataset_member
                              SET observation_row_count=%s, execution_row_count=%s,
                                  execution_hash=%s, corporate_action_hash=%s,
                                  quality_status=%s
                            WHERE dataset_id=%s AND research_date=%s""",
                        (obs_count, execution_stats["count"], execution_hash_for_day,
                         corporate_action_hash_for_day, member_quality, dataset_id, day),
                    )
                    member_stats.append((
                        day, member[0], execution_hash_for_day, corporate_action_hash_for_day,
                        execution_stats["count"],
                    ))

                calendar_hash = hash_rows(actual_calendar_rows)
                rules_hash = hash_rows(actual_rule_rows)
                execution_hash = execution_hasher.hexdigest()
                corporate_action_hash = hash_rows(actual_dividend_rows)
                benchmark_hash = hash_rows(actual_benchmark_rows)
                input_manifest_hash = hash_rows([
                    (dataset_id, day, snapshot_hash, member_exec_hash, member_corporate_hash, count)
                    for day, snapshot_hash, member_exec_hash, member_corporate_hash, count in member_stats
                ] + [
                    ("calendar", calendar_hash),
                    ("rules", rules_hash),
                    ("execution", execution_hash),
                    ("corporate_action", corporate_action_hash),
                    ("benchmark", benchmark_hash),
                    ("baseline_quality_report", manifest["baseline_quality_report_hash"]),
                    ("baseline_source_manifest", manifest["baseline_source_manifest_hash"]),
                    ("out_of_sample_start", manifest["out_of_sample_start"]),
                ])
                quality = {
                    "execution_rows": execution_hasher.count,
                    "calendar_rows": len(actual_calendar_rows),
                    "corporate_action_rows": len(actual_dividend_rows),
                    "rule_rows": len(actual_rule_rows),
                    "benchmark_rows": len(actual_benchmark_rows),
                    "member_count": len(member_stats),
                    "missing_execution_rows": sum(
                        stats["missing"] for stats in per_date_stats.values()
                    ),
                    "baseline_quality_report_hash": manifest["baseline_quality_report_hash"],
                    "baseline_source_manifest_hash": manifest["baseline_source_manifest_hash"],
                    "benchmark_code": manifest["benchmark_code"],
                    "benchmark_hash": benchmark_hash,
                    "out_of_sample_start": manifest["out_of_sample_start"].isoformat(),
                    "input_hash": input_manifest_hash,
                }
                metadata = _git_metadata()
                cur.execute(
                    """UPDATE backtest.dataset_manifest
                          SET calendar_hash=%s, rules_hash=%s, execution_hash=%s,
                              corporate_action_hash=%s, benchmark_hash=%s,
                              input_manifest_hash=%s,
                              quality_report=%s, quality_report_hash=%s,
                              code_version=%s, git_commit=%s, git_dirty=%s, diff_hash=%s
                        WHERE dataset_id=%s""",
                    (calendar_hash, rules_hash, execution_hash, corporate_action_hash,
                     benchmark_hash, input_manifest_hash,
                     _json_param(quality), hashlib.sha256(json.dumps(quality, sort_keys=True).encode()).hexdigest(),
                     metadata["code_version"], metadata["commit"], metadata["dirty"], metadata["diff_hash"], dataset_id),
                )
                conn.commit()
                return quality
    except Exception as exc:
        try:
            with get_connection() as failed_conn, failed_conn.cursor() as failed_cur:
                failed_cur.execute(
                    """UPDATE backtest.dataset_manifest
                          SET status='FAILED', failed_at=CURRENT_TIMESTAMP, failure_reason=%s
                        WHERE dataset_id=%s AND status IN ('DRAFT','BUILDING','FAILED')""",
                    (str(exc)[:4000], dataset_id),
                )
                failed_conn.commit()
        except Exception:
            pass
        raise


def _materialized_hashes(cur, dataset_id: str) -> dict[str, Any]:
    """读取 Dataset 子表的最终物化行，返回全局/按日哈希。

    校验必须对账 Dataset 自己的物料，而不是再次对 mutable 上游表计算哈希。
    这样才能发现物化过程中的列顺序、类型转换、默认值或写入截断问题。
    """
    execution_cur = cur.connection.cursor(
        name=f"dataset_materialized_{dataset_id.replace('-', '')}"
    )
    execution_cur.itersize = 5000
    execution_cur.execute(
        """SELECT trade_date, ts_code, market, raw_open, raw_high, raw_low,
                          raw_close, volume, amount, up_limit, down_limit,
                          is_suspended, can_buy, can_sell, limit_status,
                          source_record_key, source_record_version, source_payload_hash,
                          limit_source_record_id, limit_source_payload_hash,
                          suspend_source_record_id, suspend_source_payload_hash,
                          quality_status
                   FROM backtest.dataset_execution_bar
                  WHERE dataset_id=%s
                  ORDER BY trade_date, ts_code""",
        (dataset_id,),
    )
    execution_hasher = _IncrementalRowHasher()
    execution_by_day: dict[date, dict[str, Any]] = {}
    while True:
        rows = execution_cur.fetchmany(5000)
        if not rows:
            break
        for row in rows:
            execution_hasher.update(row)
            stats = execution_by_day.setdefault(
                row[0], {"hasher": _IncrementalRowHasher(), "count": 0}
            )
            stats["hasher"].update(row)
            stats["count"] += 1
    execution_cur.close()

    cur.execute(
        """SELECT trade_date, exchange, is_open, pretrade_date,
                          source_record_key, source_record_version, source_payload_hash
                   FROM backtest.dataset_calendar
                  WHERE dataset_id=%s
                  ORDER BY trade_date, exchange""",
        (dataset_id,),
    )
    calendar_rows = cur.fetchall()

    cur.execute(
        """SELECT ts_code, report_date, announce_date, implementation_status,
                          stock_div, stock_bonus_rate, stock_conversion_rate, cash_div,
                          cash_div_tax, record_date, ex_date, pay_date, div_list_date,
                          implementation_announce_date, source_record_key,
                          source_record_version, source_payload_hash
                   FROM backtest.dataset_dividend_event
                  WHERE dataset_id=%s
                  ORDER BY ex_date, ts_code, source_record_key, source_record_version""",
        (dataset_id,),
    )
    dividend_rows = cur.fetchall()
    dividend_by_day: dict[date, list[tuple]] = {}
    for row in dividend_rows:
        dividend_by_day.setdefault(row[10], []).append(row)

    cur.execute(
        """SELECT rule_version, market_rule_version, fee_rule_version,
                          effective_from, effective_to, market,
                          min_buy_lot, allow_sell_odd_lot, t_plus_one, price_tick,
                          cash_settlement_days, limit_pct, commission_rate, min_commission,
                          stamp_duty_sell_rate, transfer_fee_rate, slippage_bps,
                          parameters, rule_hash
                   FROM backtest.dataset_rule_snapshot
                  WHERE dataset_id=%s
                  ORDER BY effective_from, market, rule_version""",
        (dataset_id,),
    )
    rule_rows = cur.fetchall()

    cur.execute(
        """SELECT trade_date,ts_code,close,source_record_key,
                  source_record_version,source_payload_hash
             FROM backtest.dataset_benchmark_bar
            WHERE dataset_id=%s ORDER BY trade_date""",
        (dataset_id,),
    )
    benchmark_rows = cur.fetchall()

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
        "rules_hash": hash_rows(rule_rows),
        "benchmark_hash": hash_rows(benchmark_rows),
    }


def validate_dataset(dataset_id: str) -> dict[str, Any]:
    """校验成员覆盖、FROZEN 来源、执行输入完整性和哈希字段。"""
    with get_connection() as conn, conn.cursor() as cur:
        manifest = _manifest(conn, dataset_id, for_update=True)
        if manifest["status"] != "BUILDING":
            raise RuntimeError(f"只有 BUILDING Dataset 可校验: {manifest['status']}")
        _check_baseline(
            conn,
            manifest["baseline"],
            manifest["evaluation_end"],
            expected_quality_report_hash=manifest.get("baseline_quality_report_hash"),
            expected_source_manifest_hash=manifest.get("baseline_source_manifest_hash"),
        )
        try:
            assert_market_coverage_range_publishable(
                manifest["warmup_start"], manifest["evaluation_end"], connection=conn
            )
        except RuntimeError as exc:
            issues: list[str] = [str(exc)]
        else:
            issues = []
        if manifest.get("hash_algorithm") != HASH_ALGORITHM:
            issues.append(f"hash_algorithm 不受支持: {manifest.get('hash_algorithm')}")
        if not manifest.get("selection_predicate") or not manifest.get("selected_columns"):
            issues.append("Dataset 未记录完整的切片谓词和字段集合")
        if not manifest.get("availability_rules"):
            issues.append("Dataset 未记录字段可见性规则")
        coverage_policy = (manifest.get("availability_rules") or {}).get("market_coverage", {}).get("policy")
        if coverage_policy != MARKET_COVERAGE_POLICY:
            issues.append(
                "Dataset 未明确历史覆盖门禁等价策略；必须绑定 validated baseline quality report"
            )
        cur.execute(
            """SELECT m.research_date, m.member_role, m.snapshot_input_hash,
                      s.status, s.schema_version, m.observation_row_count, m.execution_row_count,
                      m.execution_hash, m.corporate_action_hash, m.quality_status
                 FROM backtest.dataset_member m
                 JOIN research.snapshot_manifest s ON s.snapshot_id=m.research_snapshot_id
                WHERE m.dataset_id=%s ORDER BY m.research_date""",
            (dataset_id,),
        )
        members = cur.fetchall()
        if not members:
            issues.append("dataset_member 为空")
        materialized = _materialized_hashes(cur, dataset_id)
        if manifest.get("calendar_hash") != materialized["calendar_hash"]:
            issues.append("calendar_hash 与 Dataset 物化日历不一致")
        if manifest.get("rules_hash") != materialized["rules_hash"]:
            issues.append("rules_hash 与 Dataset 物化规则不一致")
        if manifest.get("execution_hash") != materialized["execution_hash"]:
            issues.append("execution_hash 与 Dataset 物化行情不一致")
        if manifest.get("corporate_action_hash") != materialized["corporate_action_hash"]:
            issues.append("corporate_action_hash 与 Dataset 物化公司行为不一致")
        if manifest.get("benchmark_hash") != materialized["benchmark_hash"]:
            issues.append("benchmark_hash 与 Dataset 物化基准行情不一致")
        if not materialized["calendar_rows"]:
            issues.append("Dataset 物化交易日历为空")
        if not materialized["rule_rows"]:
            issues.append("Dataset 物化规则为空")
        member_dates = [row[0] for row in members]
        benchmark_dates = [row[0] for row in materialized["benchmark_rows"]]
        if benchmark_dates != member_dates:
            issues.append(
                "Dataset 基准行情未逐日覆盖成员日期: "
                f"benchmark={len(benchmark_dates)}, members={len(member_dates)}"
            )
        if any(
            row[1] != manifest.get("benchmark_code")
            or row[2] is None or float(row[2]) <= 0
            or row[4] is None or row[5] is None
            for row in materialized["benchmark_rows"]
        ):
            issues.append("Dataset 基准行情代码、价格或 Raw 血缘无效")
        execution_dates = sorted(materialized["execution_by_day"])
        if member_dates != execution_dates:
            issues.append(
                "Dataset 成员日期与物化执行行情日期不一致: "
                f"members={len(member_dates)}, execution={len(execution_dates)}"
            )

        # Validate the complete materialization range independently of the
        # create step.  DRAFT/FAILED child rows are mutable, so validate must
        # catch a warm-up date removed after Dataset creation as well as an
        # evaluation date gap.
        cur.execute(
            """SELECT trade_date
                 FROM backtest.dataset_calendar
                WHERE dataset_id=%s AND is_open IS TRUE
                  AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date""",
            (dataset_id, manifest["warmup_start"], manifest["evaluation_end"]),
        )
        all_open_dates = [row[0] for row in cur.fetchall()]
        missing_range_dates = missing_snapshot_dates(all_open_dates, member_dates)
        extra_member_dates = sorted(set(member_dates) - set(all_open_dates))
        if missing_range_dates:
            issues.append(
                "warmup/evaluation 全区间缺少 Dataset 成员日期: "
                f"count={len(missing_range_dates)}, first={missing_range_dates[0]}"
            )
        if extra_member_dates:
            issues.append(
                "Dataset 成员包含交易日历范围外日期: "
                f"count={len(extra_member_dates)}, first={extra_member_dates[0]}"
            )

        member_stats = []
        for day, role, snapshot_hash, snapshot_status, schema_version, obs_count, exec_count, exec_hash, corporate_hash, quality in members:
            if snapshot_status != "FROZEN":
                issues.append(f"{day}: source snapshot 非 FROZEN")
            if schema_version != RESEARCH_SCHEMA_VERSION:
                issues.append(f"{day}: source snapshot schema_version 不兼容: {schema_version}")
            expected_role = (
                "EVALUATION"
                if manifest["evaluation_start"] <= day <= manifest["evaluation_end"]
                else "WARMUP"
            )
            if role != expected_role:
                issues.append(f"{day}: member_role 错误: expected={expected_role}, actual={role}")
            if not snapshot_hash:
                issues.append(f"{day}: snapshot_input_hash 为空")
            if obs_count <= 0 or exec_count != obs_count:
                issues.append(f"{day}: observation/execution 行数不一致 {obs_count}/{exec_count}")
            if not exec_hash:
                issues.append(f"{day}: execution_hash 为空")
            if not corporate_hash:
                issues.append(f"{day}: corporate_action_hash 为空")
            if quality in {"INVALID", "MISSING_SOURCE"}:
                issues.append(f"{day}: Dataset 成员行情质量为 {quality}")
            actual_execution_stats = materialized["execution_by_day"].get(
                day, {"hasher": _IncrementalRowHasher(), "count": 0}
            )
            actual_corporate_for_day = materialized["dividend_by_day"].get(day, [])
            actual_execution_hash = actual_execution_stats["hasher"].hexdigest()
            actual_corporate_hash = hash_rows(actual_corporate_for_day)
            if exec_count != actual_execution_stats["count"]:
                issues.append(
                    f"{day}: execution_row_count 与物化行情不一致 "
                    f"{exec_count}/{actual_execution_stats['count']}"
                )
            if exec_hash != actual_execution_hash:
                issues.append(f"{day}: execution_hash 与物化行情不一致")
            if corporate_hash != actual_corporate_hash:
                issues.append(f"{day}: corporate_action_hash 与物化公司行为不一致")
            member_stats.append((
                day, snapshot_hash, actual_execution_hash, actual_corporate_hash,
                actual_execution_stats["count"],
            ))

        calculated_input_hash = hash_rows(
            [
                (dataset_id, day, snapshot_hash, execution_hash, corporate_hash, count)
                for day, snapshot_hash, execution_hash, corporate_hash, count in member_stats
            ]
            + [
                ("calendar", materialized["calendar_hash"]),
                ("rules", materialized["rules_hash"]),
                ("execution", materialized["execution_hash"]),
                ("corporate_action", materialized["corporate_action_hash"]),
                ("benchmark", materialized["benchmark_hash"]),
                ("baseline_quality_report", manifest["baseline_quality_report_hash"]),
                ("baseline_source_manifest", manifest["baseline_source_manifest_hash"]),
                ("out_of_sample_start", manifest["out_of_sample_start"]),
            ]
        )
        if manifest.get("input_manifest_hash") != calculated_input_hash:
            issues.append("input_manifest_hash 与成员及物化输入哈希不一致")

        cur.execute(
            """SELECT trade_date FROM backtest.dataset_calendar
                WHERE dataset_id=%s AND is_open IS TRUE
                  AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date""",
            (dataset_id, manifest["evaluation_start"], manifest["evaluation_end"]),
        )
        calendar_eval_dates = [row[0] for row in cur.fetchall()]
        open_days = len(calendar_eval_dates)
        oos_days = sum(day >= manifest["out_of_sample_start"] for day in calendar_eval_dates)
        if oos_days <= 0:
            issues.append("样本外区间没有交易日")
        cur.execute(
            """SELECT COUNT(*) FROM backtest.dataset_member
                WHERE dataset_id=%s AND member_role='EVALUATION'""",
            (dataset_id,),
        )
        eval_members = cur.fetchone()[0]
        cur.execute(
            """SELECT research_date FROM backtest.dataset_member
                WHERE dataset_id=%s AND member_role='EVALUATION'
                ORDER BY research_date""",
            (dataset_id,),
        )
        member_eval_dates = [row[0] for row in cur.fetchall()]
        if calendar_eval_dates != member_eval_dates:
            issues.append(
                "evaluation 交易日覆盖不完整: "
                f"calendar={calendar_eval_dates[:3]}.../{open_days}, "
                f"snapshots={member_eval_dates[:3]}.../{eval_members}"
            )
        if open_days != eval_members:
            issues.append(f"evaluation 交易日覆盖不完整: calendar={open_days}, snapshots={eval_members}")

        cur.execute(
            """SELECT COUNT(*) FROM backtest.dataset_execution_bar
                WHERE dataset_id=%s AND quality_status='MISSING_SOURCE'""",
            (dataset_id,),
        )
        missing_rows = cur.fetchone()[0]
        if missing_rows:
            issues.append(f"执行行情缺失 {missing_rows} 行；停牌可保留 WARNING，但不能伪造价格")

        cur.execute(
            """SELECT COUNT(*) FROM backtest.dataset_execution_bar
                WHERE dataset_id=%s
                  AND (
                    ((can_buy IS TRUE OR can_sell IS TRUE)
                     AND (source_payload_hash IS NULL
                          OR limit_source_record_id IS NULL
                          OR limit_source_payload_hash IS NULL))
                    OR (is_suspended IS TRUE AND suspend_source_record_id IS NULL)
                  )""",
            (dataset_id,),
        )
        missing_execution_lineage = cur.fetchone()[0]
        if missing_execution_lineage:
            issues.append(
                f"执行行情 Raw 血缘不完整 {missing_execution_lineage} 行"
            )

        cur.execute(
            """SELECT COUNT(*) FROM backtest.dataset_calendar
                WHERE dataset_id=%s AND source_payload_hash IS NULL""",
            (dataset_id,),
        )
        missing_calendar_lineage = cur.fetchone()[0]
        if missing_calendar_lineage:
            issues.append(f"交易日历 Raw 血缘不完整 {missing_calendar_lineage} 行")

        cur.execute(
            """SELECT COUNT(*) FROM backtest.dataset_execution_bar
                WHERE dataset_id=%s
                  AND quality_status='VALID'
                  AND is_suspended IS NOT TRUE
                  AND (can_buy IS TRUE OR can_sell IS TRUE)
                  AND (up_limit IS NULL OR down_limit IS NULL)""",
            (dataset_id,),
        )
        missing_limit_rows = cur.fetchone()[0]
        if missing_limit_rows:
            issues.append(
                f"非停牌有效执行行情缺少涨跌停价 {missing_limit_rows} 行，不能证明成交可行性"
            )

        cur.execute(
            """SELECT COUNT(*)
                 FROM backtest.dataset_dividend_event
                WHERE dataset_id=%s
                  AND COALESCE(implementation_announce_date, announce_date, report_date, ex_date) > ex_date""",
            (dataset_id,),
        )
        future_dividend_rows = cur.fetchone()[0]
        if future_dividend_rows:
            issues.append(f"公司行为存在除权日前不可见的记录: {future_dividend_rows}")
        cur.execute(
            """SELECT COUNT(*) FROM backtest.dataset_dividend_event
                WHERE dataset_id=%s
                  AND ((pay_date IS NOT NULL AND pay_date < ex_date)
                    OR (div_list_date IS NOT NULL AND div_list_date < ex_date))""",
            (dataset_id,),
        )
        invalid_dividend_dates = cur.fetchone()[0]
        if invalid_dividend_dates:
            issues.append(f"公司行为支付/上市日期早于除权日: {invalid_dividend_dates}")
        cur.execute(
            """SELECT COUNT(*) FROM (
                       SELECT ts_code, source_record_key, COUNT(*) AS n
                         FROM backtest.dataset_dividend_event
                        WHERE dataset_id=%s
                        GROUP BY ts_code, source_record_key
                       HAVING COUNT(*) > 1
                   ) duplicate_events""",
            (dataset_id,),
        )
        duplicate_dividend_events = cur.fetchone()[0]
        if duplicate_dividend_events:
            issues.append(
                f"公司行为业务键仍有重复版本，禁止进入正式 Dataset: {duplicate_dividend_events}"
            )
        cur.execute(
            """SELECT COUNT(*) FROM backtest.dataset_dividend_event
                WHERE dataset_id=%s
                  AND (source_record_version IS NULL OR source_payload_hash IS NULL)""",
            (dataset_id,),
        )
        missing_dividend_lineage = cur.fetchone()[0]
        if missing_dividend_lineage:
            issues.append(f"公司行为 Raw 血缘不完整 {missing_dividend_lineage} 行")
        cur.execute(
            """SELECT COUNT(*)
                 FROM backtest.dataset_dividend_event
                WHERE dataset_id=%s
                  AND stock_div IS NOT NULL
                  AND (stock_bonus_rate IS NOT NULL OR stock_conversion_rate IS NOT NULL)
                  AND ABS(stock_div - (COALESCE(stock_bonus_rate, 0)
                                      + COALESCE(stock_conversion_rate, 0))) > 1e-8""",
            (dataset_id,),
        )
        inconsistent_stock_rates = cur.fetchone()[0]
        if inconsistent_stock_rates:
            issues.append(
                "公司行为送转股合计与分项不一致，禁止进入正式 Dataset: "
                f"{inconsistent_stock_rates}"
            )

        cur.execute("""SELECT COUNT(*), COUNT(rule_hash),
                              COUNT(*) FILTER (WHERE approval_status='APPROVED'
                                AND approved_by IS NOT NULL AND approved_at IS NOT NULL
                                AND approval_hash IS NOT NULL)
                         FROM backtest.dataset_rule_snapshot WHERE dataset_id=%s""",
                    (dataset_id,))
        rule_count, rule_hash_count, approved_rule_count = cur.fetchone()
        if approved_rule_count != rule_count:
            issues.append("Dataset 包含未持久化审批证据的市场/费用规则")
        cur.execute(
            """SELECT COUNT(*) FROM (
                       SELECT DISTINCT trade_date, market
                         FROM backtest.dataset_execution_bar
                        WHERE dataset_id=%s
                   ) e
                  WHERE NOT EXISTS (
                        SELECT 1 FROM backtest.dataset_rule_snapshot r
                         WHERE r.dataset_id=%s
                           AND r.effective_from <= e.trade_date
                           AND COALESCE(r.effective_to, DATE '9999-12-31') >= e.trade_date
                           AND (r.market = e.market OR r.market IN ('ALL','*'))
                  )""",
            (dataset_id, dataset_id),
        )
        uncovered_rule_rows = cur.fetchone()[0]
        if uncovered_rule_rows:
            issues.append(f"执行行情存在未覆盖的市场规则日期组合: {uncovered_rule_rows}")
        if rule_count == 0 or rule_hash_count != rule_count:
            issues.append("规则/费用快照为空或哈希不完整")

        cur.execute(
            """SELECT COUNT(*)
                 FROM backtest.dataset_member m
                 JOIN research.snapshot_manifest sm ON sm.snapshot_id=m.research_snapshot_id
                WHERE m.dataset_id=%s
                  AND (sm.research_cutoff_date <> m.research_date
                       OR EXISTS (
                           SELECT 1
                             FROM research.stock_snapshot_daily s
                            WHERE s.snapshot_id=m.research_snapshot_id
                              AND s.research_date=m.research_date
                              AND (s.effective_announce_date > m.research_date
                                   OR s.financial_version_available_date > m.research_date)
                       ))""",
            (dataset_id,),
        )
        future_cutoff_rows = cur.fetchone()[0]
        if future_cutoff_rows:
            issues.append(f"Research/PIT 截止日或财务可见日穿越: {future_cutoff_rows}")

        quality = {"issues": issues, "member_count": len(members), "open_evaluation_days": open_days,
                   "evaluation_snapshot_days": eval_members, "missing_execution_rows": missing_rows,
                   "missing_limit_rows": missing_limit_rows,
                   "invalid_dividend_dates": invalid_dividend_dates,
                   "future_dividend_rows": future_dividend_rows,
                   "duplicate_dividend_business_keys": duplicate_dividend_events,
                   "inconsistent_stock_rates": inconsistent_stock_rates,
                   "benchmark_code": manifest.get("benchmark_code"),
                   "benchmark_rows": len(materialized["benchmark_rows"]),
                   "benchmark_hash": materialized["benchmark_hash"],
                   "out_of_sample_start": manifest.get("out_of_sample_start"),
                   "out_of_sample_days": oos_days,
                   "baseline_quality_report_hash": manifest.get("baseline_quality_report_hash"),
                   "baseline_source_manifest_hash": manifest.get("baseline_source_manifest_hash")}
        if issues:
            quality["status"] = "FAILED"
            failure_reason = "; ".join(issues)[:4000]
            cur.execute(
                """UPDATE backtest.dataset_manifest
                      SET status='FAILED', failed_at=CURRENT_TIMESTAMP,
                          failure_reason=%s, quality_report=%s, quality_report_hash=%s
                    WHERE dataset_id=%s""",
                (failure_reason, _json_param(quality),
                 hashlib.sha256(json.dumps(quality, ensure_ascii=False, sort_keys=True).encode()).hexdigest(),
                 dataset_id),
            )
            conn.commit()
            raise RuntimeError(f"Backtest Dataset 校验失败: {issues}")
        quality["status"] = "VALID"
        report_hash = hashlib.sha256(json.dumps(quality, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
        cur.execute(
            """UPDATE backtest.dataset_manifest
                  SET status='VALIDATED', quality_report=%s, quality_report_hash=%s
                WHERE dataset_id=%s""",
            (_json_param(quality), report_hash, dataset_id),
        )
        conn.commit()
        return quality


def freeze_dataset(dataset_id: str, code_archive_path: str | None = None):
    metadata = _git_metadata()
    if metadata["dirty"] and not code_archive_path:
        raise RuntimeError(
            "正式冻结要求 Git 工作区干净；如确需保留脏工作区，请先提供存在的 code_archive_path"
        )
    if code_archive_path and not Path(code_archive_path).exists():
        raise FileNotFoundError(f"代码归档不存在: {code_archive_path}")
    archive_hash = archive_size = None
    if code_archive_path:
        archive_hash, archive_size = _sha256_file(code_archive_path)
    with get_connection() as conn, conn.cursor() as cur:
        manifest = _manifest(conn, dataset_id, for_update=True)
        if manifest["status"] != "VALIDATED":
            raise RuntimeError(f"只有 VALIDATED Dataset 可冻结: {manifest['status']}")
        _check_baseline(
            conn,
            manifest["baseline"],
            manifest["evaluation_end"],
            expected_quality_report_hash=manifest.get("baseline_quality_report_hash"),
            expected_source_manifest_hash=manifest.get("baseline_source_manifest_hash"),
        )
        _assert_build_code_current(manifest, metadata)
        cur.execute(
            """UPDATE backtest.dataset_manifest
                  SET code_archive_path=COALESCE(%s, code_archive_path),
                      code_archive_sha256=COALESCE(%s, code_archive_sha256),
                      code_archive_size_bytes=COALESCE(%s, code_archive_size_bytes),
                      code_version=%s, git_commit=%s, git_dirty=%s, diff_hash=%s,
                      status='FROZEN'
                WHERE dataset_id=%s AND status='VALIDATED'""",
            (code_archive_path, archive_hash, archive_size,
             metadata["code_version"], metadata["commit"], metadata["dirty"],
             metadata["diff_hash"], dataset_id),
        )
        if cur.rowcount != 1:
            raise RuntimeError("Backtest Dataset 状态在冻结期间发生变化，拒绝并发冻结")
        conn.commit()


def load_frozen_dataset(dataset_id: str) -> BacktestDataset:
    """从已冻结 Dataset 加载纯内存输入；不允许读取实时上游表。"""
    with get_connection() as conn, conn.cursor() as cur:
        manifest = _manifest(conn, dataset_id)
        if manifest["status"] != "FROZEN":
            raise RuntimeError(f"只有 FROZEN Dataset 可以用于正式回测: {manifest['status']}")
        cur.execute(
            """SELECT trade_date FROM backtest.dataset_calendar
                WHERE dataset_id=%s AND is_open IS TRUE
                ORDER BY trade_date""", (dataset_id,)
        )
        calendar = tuple(row[0] for row in cur.fetchall())
        cur.execute(
            """SELECT trade_date, ts_code, raw_open, raw_high, raw_low, raw_close,
                      volume, amount, up_limit, down_limit, is_suspended, can_buy,
                      can_sell, limit_status, market
                 FROM backtest.dataset_execution_bar
                WHERE dataset_id=%s ORDER BY trade_date, ts_code""", (dataset_id,)
        )
        bars: dict[tuple[date, str], MarketBar] = {}
        for row in cur.fetchall():
            bar = MarketBar(*row)
            bars[(bar.trade_date, bar.ts_code)] = bar

        observation_columns = (
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
        )
        cur.execute(
            f"""SELECT {', '.join(observation_columns)}
                   FROM research.stock_snapshot_daily s
                  WHERE s.snapshot_id IN (SELECT research_snapshot_id FROM backtest.dataset_member WHERE dataset_id=%s)
                  ORDER BY research_date, ts_code""", (dataset_id,)
        )
        observations: dict[date, list[Observation]] = {}
        delist_dates: dict[str, date | None] = {}
        for row in cur.fetchall():
            values = dict(zip(observation_columns, row))
            day = values.pop("research_date")
            code = values.pop("ts_code")
            if values.get("delist_date") is not None:
                delist_dates[code] = values["delist_date"]
            observations.setdefault(day, []).append(Observation(day, code, values))

        cur.execute(
            """SELECT ts_code, ex_date, cash_div, stock_div, stock_bonus_rate,
                      stock_conversion_rate, pay_date, record_date, implementation_status,
                      source_record_key, source_record_version, source_payload_hash,
                      announce_date, implementation_announce_date, div_list_date,
                      cash_div_tax
                 FROM backtest.dataset_dividend_event
                WHERE dataset_id=%s ORDER BY ex_date, ts_code""", (dataset_id,)
        )
        dividends: dict[date, list[DividendEvent]] = {}
        for row in cur.fetchall():
            event = DividendEvent(*row)
            dividends.setdefault(event.ex_date, []).append(event)

        cur.execute(
        """SELECT rule_version, market_rule_version, fee_rule_version,
                          effective_from, effective_to, market,
                          min_buy_lot, allow_sell_odd_lot, t_plus_one, price_tick,
                      cash_settlement_days, limit_pct, commission_rate, min_commission,
                      stamp_duty_sell_rate, transfer_fee_rate, slippage_bps,
                      parameters, rule_hash
                 FROM backtest.dataset_rule_snapshot
                WHERE dataset_id=%s ORDER BY effective_from, market""", (dataset_id,)
        )
        rule_rows = cur.fetchall()
        cur.execute(
            """SELECT trade_date,close FROM backtest.dataset_benchmark_bar
                WHERE dataset_id=%s AND ts_code=%s ORDER BY trade_date""",
            (dataset_id, manifest["benchmark_code"]),
        )
        benchmark_closes = {row[0]: float(row[1]) for row in cur.fetchall()}

    member_roles = {}
    snapshot_ids = {}
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT research_date, member_role, research_snapshot_id FROM backtest.dataset_member WHERE dataset_id=%s", (dataset_id,))
        member_rows = cur.fetchall()
        member_roles = {row[0]: row[1] for row in member_rows}
        snapshot_ids = {row[0]: str(row[2]) for row in member_rows}
    rules = {}
    for rule in rule_rows:
        (
            _row_version, _market_rule_version, _fee_rule_version,
            effective_from, effective_to, market, min_lot, odd_lot, t1, tick,
            cash_days, _limit_pct, commission, min_commission, stamp, transfer, slippage,
            parameters, _rule_hash,
        ) = rule
        parameters = parameters or {}
        market_rule = MarketRule(market=market, min_buy_lot=min_lot, allow_sell_odd_lot=odd_lot,
                                 t_plus_one=t1, price_tick=tick, cash_settlement_days=cash_days,
                                 max_volume_participation_pct=float(
                                     parameters.get("max_volume_participation_pct", 1.0)
                                 ),
                                 liquidity_policy=str(
                                     parameters.get("liquidity_policy", "reject_full_order")
                                 ))
        fee_rule = FeeRule(commission_rate=commission, min_commission=min_commission,
                           stamp_duty_sell_rate=stamp, transfer_fee_rate=transfer,
                           slippage_bps=slippage)
        for day in calendar:
            if effective_from <= day and (effective_to is None or day <= effective_to):
                rules[(day, None if market in {"ALL", "*"} else market)] = (market_rule, fee_rule)
    return BacktestDataset(
        dataset_id=dataset_id,
        calendar=calendar,
        evaluation_start=manifest["evaluation_start"],
        evaluation_end=manifest["evaluation_end"],
        warmup_start=manifest["warmup_start"],
        observations={day: tuple(rows) for day, rows in observations.items()},
        bars=bars,
        dividends={day: tuple(rows) for day, rows in dividends.items()},
        member_roles=member_roles,
        snapshot_ids=snapshot_ids,
        rules=rules,
        delist_dates=delist_dates,
        benchmark_code=manifest["benchmark_code"],
        benchmark_closes=benchmark_closes,
        out_of_sample_start=manifest["out_of_sample_start"],
        input_hash=manifest["input_manifest_hash"],
        require_explicit_rules=True,
    )


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
