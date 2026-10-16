"""Read-only readiness audit for frozen A-share research/backtest inputs."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence
from uuid import uuid4

from data_collect.backtest.engine import FundamentalValueGrowthTrendStrategy
from data_collect.backtest.strategy.quality_trend_rotation_v1 import QualityTrendRotationV1
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.backtest_run_repository import BacktestRunRepository
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.data_access.repository.research_snapshot_repository import (
    ResearchSnapshotRepository,
)
from data_collect.data_access.research_contract import is_foundation_schema
from data_collect.jobs.backtest_diagnostics import diagnose_run


_HORIZONS = (20, 40, 60)
_CAPITALS = (10_000, 20_000)
_TARGET_POSITIONS = 3
_TARGET_POSITION_RATIO = 0.30
_MAX_ENTRY_POSITION_RATIO = 0.35
_STRATEGIES = (
    FundamentalValueGrowthTrendStrategy,
    QualityTrendRotationV1,
)


def _date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if isinstance(value, (date, datetime)) else str(value)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _selected_column_summary(value: Any) -> tuple[Any, int]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return [], 0

    def count_items(item: Any) -> int:
        if isinstance(item, Mapping):
            return sum(count_items(child) for child in item.values())
        if isinstance(item, (list, tuple, set)):
            return sum(count_items(child) for child in item)
        return int(item is not None)

    if isinstance(value, Mapping):
        return dict(value), count_items(value)
    if isinstance(value, (list, tuple, set)):
        selected = list(value)
        return selected, count_items(selected)
    return [], 0


def _selection_contract_status(selection_predicate: Any, selected_column_count: int) -> str:
    return (
        "PASS"
        if str(selection_predicate or "").strip() and selected_column_count > 0
        else "FAIL"
    )


def _dataset_quality_status(
    validation_counts: Mapping[str, int] | None,
) -> str:
    if validation_counts is None:
        return "NOT RUN"
    return "FAIL" if any(validation_counts.values()) else "PASS"


def _pit_temporal_visibility_status(
    issue_totals: Mapping[str, int],
    foundation_audit_complete: bool,
    foundation_members_present: bool,
) -> str:
    if int(issue_totals.get("future_financial_availability_rows", 0) or 0) > 0:
        return "FAIL"
    if any(
        int(issue_totals.get(key, 0) or 0) > 0
        for key in (
            "missing_snapshot_rows", "missing_lineage_rows",
            "non_v2_source_rows", "invalid_v2_rows",
        )
    ):
        return "NOT RUN"
    return "PASS" if foundation_audit_complete and foundation_members_present else "NOT RUN"


def _quality_report_evidence_status(
    quality_report: Mapping[str, Any], quality_report_hash: Any,
) -> str:
    issues = quality_report.get("issues")
    has_issues = bool(issues)
    bad_summary_count = any(
        int(quality_report.get(key) or 0) > 0
        for key in ("missing_execution_rows", "missing_limit_rows")
    )
    return (
        "PASS"
        if str(quality_report.get("status") or "").upper() == "VALID"
        and bool(quality_report_hash)
        and not has_issues
        and not bad_summary_count
        else "FAIL"
    )


def _month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Split a Dataset date span into bounded calendar-month reads."""
    if start > end:
        raise ValueError("coverage start must not exceed end")
    windows: list[tuple[date, date]] = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        next_month = (
            date(cursor.year + 1, 1, 1)
            if cursor.month == 12 else date(cursor.year, cursor.month + 1, 1)
        )
        windows.append((max(start, cursor), min(end, next_month - timedelta(days=1))))
        cursor = next_month
    return windows


def summarize_horizon_coverage(
    member_dates: Sequence[date],
    trading_dates: Sequence[date],
    horizons: Sequence[int] = _HORIZONS,
    *,
    out_of_sample_start: date | None = None,
) -> dict[str, Any]:
    """Count member dates with enough later Dataset trading-calendar days."""
    ordered_trading_dates = sorted(set(trading_dates))
    index_by_date = {trading_date: index for index, trading_date in enumerate(ordered_trading_dates)}
    eligible_dates = sorted({item for item in member_dates if item in index_by_date})

    def horizon_counts(candidates: Sequence[date]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for horizon in horizons:
            mature = [
                item for item in candidates
                if index_by_date[item] + int(horizon) < len(ordered_trading_dates)
            ]
            result[f"{horizon}d"] = {
                "mature_member_date_count": len(mature),
                "first_mature_date": _iso(mature[0]) if mature else None,
                "last_mature_date": _iso(mature[-1]) if mature else None,
            }
        return result

    out_of_sample_dates = (
        [item for item in eligible_dates if item >= out_of_sample_start]
        if out_of_sample_start else []
    )
    return {
        "calendar_date_count": len(ordered_trading_dates),
        "member_date_count": len(eligible_dates),
        "first_member_date": _iso(eligible_dates[0]) if eligible_dates else None,
        "last_member_date": _iso(eligible_dates[-1]) if eligible_dates else None,
        "all_period": horizon_counts(eligible_dates),
        "out_of_sample": {
            "start": _iso(out_of_sample_start),
            "member_date_count": len(out_of_sample_dates),
            "mature_horizons": horizon_counts(out_of_sample_dates),
        },
        "scope_note": (
            "按冻结交易日历统计每个研究日期之后是否还有足够交易日；"
            "这不单独证明每只股票都存在完整未来价格行。"
        ),
    }


def _compact_run_diagnostic(report: Mapping[str, Any]) -> dict[str, Any]:
    curve = report.get("equity_curve") or []
    last_point = curve[-1] if curve else {}
    drawdowns = [
        float(row["drawdown"])
        for row in curve
        if isinstance(row, Mapping) and row.get("drawdown") is not None
    ]
    navs = [
        float(row["nav"])
        for row in curve
        if isinstance(row, Mapping) and row.get("nav") is not None
    ]
    nav_implied_drawdowns: list[float] = []
    peak_nav = 1.0
    for nav in navs:
        peak_nav = max(peak_nav, nav)
        nav_implied_drawdowns.append(nav / peak_nav - 1.0)
    has_positive_drawdown = any(value > 0 for value in drawdowns)
    has_negative_drawdown = any(value < 0 for value in drawdowns)
    mixed_drawdown_signs = has_positive_drawdown and has_negative_drawdown
    if mixed_drawdown_signs:
        persisted_max_drawdown = None
        persisted_drawdown_representation = "mixed_signs"
    elif has_positive_drawdown:
        persisted_max_drawdown = -max(drawdowns)
        persisted_drawdown_representation = "positive_magnitude"
    elif drawdowns:
        persisted_max_drawdown = min(drawdowns)
        persisted_drawdown_representation = "negative_signed"
    else:
        persisted_max_drawdown = None
        persisted_drawdown_representation = "NOT RUN"
    nav_implied_max_drawdown = min(nav_implied_drawdowns) if nav_implied_drawdowns else None
    drawdown_consistency = (
        "FAIL"
        if mixed_drawdown_signs
        else "NOT RUN"
        if persisted_max_drawdown is None or nav_implied_max_drawdown is None
        else "PASS"
        if abs(persisted_max_drawdown - nav_implied_max_drawdown) <= 0.01
        else "FAIL"
    )
    diagnostic_status = (
        "NOT RUN" if drawdown_consistency == "NOT RUN" else drawdown_consistency
    )
    forward = report.get("forward_returns") or {}
    return {
        "status": diagnostic_status,
        "scope": report.get("scope"),
        "performance_day_count": len(curve),
        "first_nav": navs[0] if navs else None,
        "minimum_nav": min(navs) if navs else None,
        "final_nav": last_point.get("nav"),
        "final_benchmark_nav": last_point.get("benchmark_nav"),
        "maximum_drawdown": persisted_max_drawdown,
        "persisted_drawdown_representation": persisted_drawdown_representation,
        "nav_implied_maximum_drawdown": nav_implied_max_drawdown,
        "drawdown_consistency": drawdown_consistency,
        "trade_summary": report.get("trade_summary"),
        "order_rejections": {
            key: (report.get("order_rejections") or {}).get(key)
            for key in ("total_orders", "filled_orders", "rejected_orders", "by_category")
        },
        "forward_returns": {
            key: {
                "available_count": value.get("available_count"),
                "returns": value.get("returns"),
            }
            for key, value in forward.items()
            if key in {f"horizon_{horizon}d" for horizon in _HORIZONS}
        },
        "forward_return_scope": (
            "只统计真实买入成交；收益是成交价至未来执行行情收盘价的纯价格路径，"
            "不等于所有候选的固定持有收益或策略净收益。"
        ),
    }


def _run_parameter_summary(run: Mapping[str, Any]) -> dict[str, Any]:
    parameters = _json_object(run.get("parameters"))
    scenario = _json_object(parameters.get("scenario"))
    keys = (
        "max_positions", "max_single_position_pct", "reserve_cash_pct",
        "max_portfolio_risk_pct", "target_position_ratio",
        "max_entry_position_ratio", "allocation_mode", "entry_slippage_bps",
        "exit_slippage_bps",
    )
    summary = {key: parameters.get(key) for key in keys if key in parameters}
    if scenario:
        summary["scenario"] = {
            key: scenario.get(key)
            for key in (
                "initial_capital", "target_positions", "target_position_ratio",
                "max_entry_position_ratio", "forced_cash_reserve_ratio",
                "buy_lot_size", "slippage_bps_per_side", "signal_time",
                "execution_time", "leftover_cash", "rebalance_existing_positions",
                "broker_commission_rate", "broker_minimum_commission",
            )
            if key in scenario
        }
    for section, keys in (
        ("fee_rule", (
            "source", "commission_rate", "min_commission", "stamp_duty_sell_rate",
            "transfer_fee_rate", "slippage_bps", "effective_slippage_bps",
        )),
        ("market_rule", (
            "source", "min_buy_lot", "allow_sell_odd_lot", "t_plus_one", "price_tick",
            "cash_settlement_days", "max_volume_participation_pct", "effective_liquidity",
            "liquidity_policy",
        )),
    ):
        values = _json_object(parameters.get(section))
        if values:
            summary[section] = {key: values[key] for key in keys if key in values}
    rule_snapshot = _json_object(parameters.get("dataset_rule_snapshot"))
    if rule_snapshot:
        effective_rules = rule_snapshot.get("effective_rules")
        if isinstance(effective_rules, (list, tuple)):
            effective_rule_objects = [
                rule for rule in effective_rules if isinstance(rule, Mapping)
            ]
            summary["dataset_rule_snapshot"] = {
                "source": rule_snapshot.get("source"),
                "rule_count": rule_snapshot.get("rule_count"),
                "rule_hashes": rule_snapshot.get("rule_hashes"),
                "effective_rule_item_count": len(effective_rules),
                "malformed_effective_rule_count": len(effective_rules) - len(effective_rule_objects),
                "effective_rules": [
                    {
                        key: _safe_mismatch_value(rule.get(key))
                        for key in (
                            "rule_version", "market_rule_version", "fee_rule_version",
                            "effective_from", "effective_to", "market",
                            "min_buy_lot", "allow_sell_odd_lot", "t_plus_one",
                            "price_tick", "cash_settlement_days", "limit_pct", "commission_rate",
                            "min_commission", "stamp_duty_sell_rate", "transfer_fee_rate",
                            "slippage_bps",
                        )
                    }
                    | _summarize_rule_parameters(rule.get("parameters"))
                    for rule in effective_rule_objects
                ],
            }
    return summary


def _rule_parameters(value: Any) -> tuple[dict[str, Any], bool]:
    if value is None:
        return {}, True
    if isinstance(value, Mapping):
        return dict(value), True
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}, False
        return (dict(decoded), True) if isinstance(decoded, Mapping) else ({}, False)
    return {}, False


def _rule_parameters_digest(value: Any) -> tuple[str | None, bool]:
    parameters, valid = _rule_parameters(value)
    if not valid:
        return None, False
    try:
        encoded = json.dumps(
            parameters, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
    except (OverflowError, TypeError, ValueError):
        return None, False
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest(), True


def _summarize_rule_parameters(value: Any) -> dict[str, Any]:
    digest, valid = _rule_parameters_digest(value)
    return {"parameters_digest": digest, "parameters_valid": valid}


def _safe_mismatch_value(value: Any) -> Any:
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, int):
        try:
            str(value)
        except ValueError:
            return {"type": "int", "bit_length": value.bit_length()}
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else repr(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return _iso(value)
    if isinstance(value, Mapping):
        return {
            str(key): _safe_mismatch_value(child)
            for key, child in list(value.items())[:50]
        }
    if isinstance(value, (list, tuple)):
        return [_safe_mismatch_value(child) for child in value[:50]]
    return type(value).__name__


def _values_match(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        return left.keys() == right.keys() and all(
            _values_match(left[key], right[key]) for key in left
        )
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(
            _values_match(a, b) for a, b in zip(left, right)
        )
    if isinstance(left, (int, float, Decimal)) and isinstance(right, (int, float, Decimal)):
        try:
            left_number = float(left)
            right_number = float(right)
        except (OverflowError, TypeError, ValueError):
            return False
        return math.isfinite(left_number) and math.isfinite(right_number) and abs(
            left_number - right_number
        ) <= 1e-9
    return _iso(left) == _iso(right) if isinstance(left, (date, datetime)) else left == right


def _normalized_rule(rule: Mapping[str, Any]) -> dict[str, Any]:
    """Project a Dataset rule row and persisted Run rule into one comparison shape."""
    result = {
        key: rule.get(key)
        for key in (
            "rule_version", "market_rule_version", "fee_rule_version",
            "effective_from", "effective_to", "market",
            "min_buy_lot", "allow_sell_odd_lot", "t_plus_one", "price_tick",
            "cash_settlement_days", "limit_pct", "commission_rate", "min_commission",
            "stamp_duty_sell_rate", "transfer_fee_rate", "slippage_bps",
        )
    }
    result["effective_from"] = _iso(result["effective_from"])
    result["effective_to"] = _iso(result["effective_to"])
    if "parameters_digest" in rule:
        result["parameters_digest"] = rule.get("parameters_digest")
        result["parameters_valid"] = rule.get("parameters_valid") is True
    else:
        (
            result["parameters_digest"], result["parameters_valid"],
        ) = _rule_parameters_digest(rule.get("parameters"))
    return result


def _dataset_rule_alignment(
    constraints: Mapping[str, Any], frozen_rules: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    """Verify the Run's recorded rules against the authoritative frozen Dataset rows."""
    snapshot = constraints.get("dataset_rule_snapshot") or {}
    effective_rules = snapshot.get("effective_rules") or []
    rule_hashes = snapshot.get("rule_hashes") or []
    if not isinstance(effective_rules, (list, tuple)):
        effective_rules = []
    if not isinstance(rule_hashes, (list, tuple)):
        rule_hashes = []
    fee_rule = constraints.get("fee_rule") or {}
    market_rule = constraints.get("market_rule") or {}
    mismatches: list[dict[str, Any]] = []

    def record_mismatch(field: str, engine_value: Any, dataset_value: Any) -> None:
        mismatches.append({
            "field": field,
            "engine_value": _safe_mismatch_value(engine_value),
            "dataset_value": _safe_mismatch_value(dataset_value),
        })

    if snapshot.get("source") != "backtest.dataset_rule_snapshot":
        record_mismatch("dataset_rule_snapshot.source", snapshot.get("source"), "backtest.dataset_rule_snapshot")
    rule_count_value = snapshot.get("rule_count")
    rule_count = rule_count_value if isinstance(rule_count_value, int) and not isinstance(rule_count_value, bool) else 0
    malformed_count = snapshot.get("malformed_effective_rule_count", 0)
    try:
        malformed_count = int(malformed_count)
    except (TypeError, ValueError):
        malformed_count = 1
    item_count = snapshot.get("effective_rule_item_count", len(effective_rules))
    try:
        item_count = int(item_count)
    except (TypeError, ValueError):
        item_count = -1
    if (
        rule_count <= 0
        or rule_count != len(effective_rules)
        or rule_count != len(rule_hashes)
        or not all(isinstance(value, str) and value.strip() for value in rule_hashes)
        or item_count != len(effective_rules) + malformed_count
        or malformed_count != 0
    ):
        record_mismatch(
            "dataset_rule_snapshot.identity",
            {
                "rule_count": rule_count, "hash_count": len(rule_hashes),
                "effective_rule_count": len(effective_rules),
                "effective_rule_item_count": item_count,
                "malformed_effective_rule_count": malformed_count,
            },
            {"all_hashes_present": all(isinstance(value, str) and value.strip() for value in rule_hashes)},
        )

    if fee_rule.get("source") != "backtest.dataset_rule_snapshot":
        record_mismatch("fee_rule.source", fee_rule.get("source"), "backtest.dataset_rule_snapshot")
    if market_rule.get("source") != "backtest.dataset_rule_snapshot":
        record_mismatch("market_rule.source", market_rule.get("source"), "backtest.dataset_rule_snapshot")

    valid_run_rules = [rule for rule in effective_rules if isinstance(rule, Mapping)]
    if frozen_rules is None:
        record_mismatch("frozen_dataset.rules", None, "authoritative Dataset rule rows")
        frozen_rules = []
    if len(valid_run_rules) != len(frozen_rules):
        record_mismatch(
            "frozen_dataset.rule_count", len(valid_run_rules), len(frozen_rules),
        )
    if len(rule_hashes) != len(frozen_rules) or rule_hashes != [
        str(rule.get("rule_hash") or "") for rule in frozen_rules
    ]:
        record_mismatch(
            "frozen_dataset.rule_hashes", rule_hashes,
            [str(rule.get("rule_hash") or "") for rule in frozen_rules],
        )
    normalized_run_rules = [_normalized_rule(rule) for rule in valid_run_rules]
    normalized_frozen_rules = [_normalized_rule(rule) for rule in frozen_rules]
    if any(
        rule.get("parameters_valid") is not True
        for rule in (*normalized_run_rules, *normalized_frozen_rules)
    ):
        record_mismatch(
            "frozen_dataset.rule_parameters_valid", False, True,
        )
    if not _values_match(normalized_run_rules, normalized_frozen_rules):
        record_mismatch(
            "frozen_dataset.effective_rules", normalized_run_rules,
            normalized_frozen_rules,
        )

    def is_finite_number(value: Any) -> bool:
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            return False
        try:
            return math.isfinite(float(value))
        except (OverflowError, TypeError, ValueError):
            return False

    dataset_slippage_values: list[float] = []
    dataset_liquidity_values: list[float] = []
    dataset_liquidity_policies: list[str] = []
    for index, rule in enumerate(frozen_rules):
        slippage = rule.get("slippage_bps")
        if not is_finite_number(slippage) or float(slippage) < 0:
            record_mismatch(
                f"frozen_dataset.rules[{index}].slippage_bps", slippage,
                "finite non-negative number",
            )
        else:
            dataset_slippage_values.append(float(slippage))

        parameters, parameters_valid = _rule_parameters(rule.get("parameters"))
        if not parameters_valid:
            record_mismatch(
                f"frozen_dataset.rules[{index}].parameters",
                type(rule.get("parameters")).__name__, "JSON object or null",
            )
        participation = parameters.get("max_volume_participation_pct", 1.0)
        if (
            not is_finite_number(participation)
            or not 0 < float(participation) <= 1
        ):
            record_mismatch(
                f"frozen_dataset.rules[{index}].max_volume_participation_pct",
                participation, "finite number in (0,1]",
            )
        else:
            dataset_liquidity_values.append(float(participation))
        liquidity_policy = parameters.get("liquidity_policy", "reject_full_order")
        if liquidity_policy != "reject_full_order":
            record_mismatch(
                f"frozen_dataset.rules[{index}].liquidity_policy",
                liquidity_policy, "reject_full_order",
            )
        else:
            dataset_liquidity_policies.append(liquidity_policy)

    dataset_slippage = sorted(set(dataset_slippage_values))
    dataset_liquidity = sorted(set(dataset_liquidity_values))
    dataset_liquidity_policies = sorted(set(dataset_liquidity_policies))
    if not _values_match(fee_rule.get("effective_slippage_bps"), dataset_slippage):
        record_mismatch(
            "fee_rule.effective_slippage_bps",
            fee_rule.get("effective_slippage_bps"), dataset_slippage,
        )
    recorded_liquidity = market_rule.get("effective_liquidity")
    if isinstance(recorded_liquidity, (list, tuple)):
        # Older Runs persisted a missing optional field as None; the Dataset
        # loader and engine consistently interpret that omission as 100%.
        recorded_liquidity = [1.0 if value is None else value for value in recorded_liquidity]
    if not _values_match(recorded_liquidity, dataset_liquidity):
        record_mismatch(
            "market_rule.effective_liquidity",
            recorded_liquidity, dataset_liquidity,
        )
    if not _values_match(market_rule.get("liquidity_policy"), dataset_liquidity_policies):
        record_mismatch(
            "market_rule.liquidity_policy",
            market_rule.get("liquidity_policy"), dataset_liquidity_policies,
        )
    return {
        "status": "PASS" if not mismatches else "FAIL",
        "rule_count": rule_count,
        "authoritative_rule_count": len(frozen_rules),
        "mismatches": mismatches,
    }


def _run_provenance_checks(
    run: Mapping[str, Any], dataset_input_hash: Any,
) -> dict[str, bool]:
    parameters = _json_object(run.get("parameters"))
    try:
        computed_parameters_hash = hashlib.sha256(
            json.dumps(
                parameters, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
    except (OverflowError, TypeError, ValueError):
        computed_parameters_hash = ""
    git_dirty = run.get("git_dirty")
    archive_hash = bool(run.get("code_archive_sha256"))
    code_identity_present = archive_hash or (
        git_dirty is False and bool(run.get("git_commit"))
    )
    code_identity_present = code_identity_present and (
        git_dirty is not True or archive_hash
    ) and (git_dirty is not None or archive_hash)
    return {
        "dataset_input_hash_matches": (
            str(run.get("input_hash") or "").strip()
            == str(dataset_input_hash or "").strip()
            and bool(dataset_input_hash)
        ),
        "parameters_hash_matches": (
            bool(run.get("parameters_hash"))
            and computed_parameters_hash == str(run.get("parameters_hash"))
        ),
        "execution_model_version_present": bool(run.get("execution_model_version")),
        "code_identity_present": code_identity_present,
    }


def _execution_evidence(
    run: Mapping[str, Any], frozen_rules: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    if str(run.get("status") or "").upper() != "SUCCEEDED":
        return {"status": "NOT RUN", "filled_orders": 0, "checks": {}}
    constraints = run.get("runtime_constraints") or {}
    scenario = constraints.get("scenario") or {}
    diagnostics = run.get("diagnostics") or {}
    order_summary = diagnostics.get("order_rejections") or {}

    def matches_number(left: Any, right: Any, tolerance: float = 1e-9) -> bool:
        try:
            return abs(float(left) - float(right)) <= tolerance
        except (OverflowError, TypeError, ValueError):
            return False

    try:
        initial_cash = float(run.get("initial_cash"))
        scenario_cash = scenario.get("initial_capital")
        cash_matches = scenario_cash is not None and abs(
            float(scenario_cash) - initial_cash
        ) <= 0.01
    except (OverflowError, TypeError, ValueError):
        cash_matches = False
    try:
        scenario_positions = int(scenario.get("target_positions"))
    except (TypeError, ValueError):
        scenario_positions = None
    position_count_matches = (
        scenario_positions == _TARGET_POSITIONS
        and constraints.get("max_positions") == scenario_positions
    )
    position_sizing_matches = (
        matches_number(scenario.get("target_position_ratio"), _TARGET_POSITION_RATIO)
        and matches_number(
            constraints.get("target_position_ratio"),
            scenario.get("target_position_ratio"),
        )
        and matches_number(
            scenario.get("max_entry_position_ratio"),
            _MAX_ENTRY_POSITION_RATIO,
        )
        and matches_number(
            constraints.get("max_entry_position_ratio"),
            scenario.get("max_entry_position_ratio"),
        )
        and matches_number(
            constraints.get("max_single_position_pct"),
            scenario.get("max_entry_position_ratio"),
        )
    )
    cash_reserve_matches = (
        "reserve_cash_pct" in constraints
        and "forced_cash_reserve_ratio" in scenario
        and matches_number(
            constraints.get("reserve_cash_pct"),
            scenario.get("forced_cash_reserve_ratio"),
        )
    )
    slippage_matches = (
        scenario.get("slippage_bps_per_side") is not None
        and matches_number(
            constraints.get("entry_slippage_bps"),
            scenario.get("slippage_bps_per_side"),
        )
        and matches_number(
            constraints.get("exit_slippage_bps"),
            scenario.get("slippage_bps_per_side"),
        )
    )
    fee_rule = constraints.get("fee_rule") or {}
    market_rule = constraints.get("market_rule") or {}
    dataset_rule_alignment = _dataset_rule_alignment(constraints, frozen_rules)
    effective_rules = [
        rule for rule in (constraints.get("dataset_rule_snapshot") or {}).get("effective_rules", [])
        if isinstance(rule, Mapping)
    ]
    fee_rule_matches_scenario = bool(effective_rules) and (
        matches_number(
            effective_rules[0].get("commission_rate"),
            scenario.get("broker_commission_rate"),
        )
        and matches_number(
            effective_rules[0].get("min_commission"),
            scenario.get("broker_minimum_commission"),
        )
        and matches_number(
            effective_rules[0].get("slippage_bps"),
            scenario.get("slippage_bps_per_side"),
        )
    )
    effective_slippages: set[float] = set()
    for rule in frozen_rules or []:
        value = rule.get("slippage_bps")
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            continue
        try:
            numeric_value = float(value)
        except (OverflowError, TypeError, ValueError):
            continue
        if math.isfinite(numeric_value):
            effective_slippages.add(numeric_value)
    slippage_matches = slippage_matches and len(effective_slippages) == 1 and matches_number(
        scenario.get("slippage_bps_per_side"), next(iter(effective_slippages), None),
    )
    fee_rule_complete = bool(frozen_rules) and all(
        rule.get(key) is not None
        for rule in (frozen_rules or [])
        for key in (
            "commission_rate", "min_commission", "stamp_duty_sell_rate",
            "transfer_fee_rate", "slippage_bps",
        )
    )
    market_rule_complete = bool(frozen_rules) and all(
        rule.get(key) is not None
        for rule in (frozen_rules or [])
        for key in ("min_buy_lot", "allow_sell_odd_lot", "t_plus_one", "price_tick")
    ) and all(
        rule.get("min_buy_lot") == 100
        and rule.get("t_plus_one") is True
        and matches_number(rule.get("price_tick"), 0.01)
        for rule in (frozen_rules or [])
    )
    scenario_lot_matches_market_rule = (
        scenario.get("buy_lot_size") == 100
        and bool(frozen_rules)
        and all(rule.get("min_buy_lot") == scenario.get("buy_lot_size") for rule in frozen_rules)
    )
    scenario_execution_contract_matches = (
        scenario.get("signal_time") == "CLOSE"
        and scenario.get("execution_time") == "NEXT_TRADING_DAY_OPEN"
        and scenario.get("leftover_cash") == "KEEP_AS_CASH"
        and scenario.get("rebalance_existing_positions") is False
    )
    try:
        filled_orders = int(float(order_summary.get("filled_orders") or 0))
    except (OverflowError, TypeError, ValueError):
        filled_orders = 0
    checks = {
        "capital_matches_scenario": cash_matches,
        "position_count_matches_scenario": position_count_matches,
        "position_sizing_matches_scenario": position_sizing_matches,
        "cash_reserve_matches_scenario": cash_reserve_matches,
        "allocation_mode_matches_scenario": constraints.get("allocation_mode") == "target_value",
        "slippage_matches_scenario": slippage_matches,
        "fee_rule_matches_scenario": fee_rule_matches_scenario,
        "fee_rule_complete": fee_rule_complete,
        "market_rule_complete": market_rule_complete,
        "scenario_lot_matches_market_rule": scenario_lot_matches_market_rule,
        "scenario_execution_contract_matches": scenario_execution_contract_matches,
        "dataset_rule_snapshot_matches_frozen_dataset": dataset_rule_alignment["status"] == "PASS",
        "execution_model_present": bool(run.get("execution_model_version")),
        "at_least_one_order_filled": filled_orders > 0,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "filled_orders": filled_orders,
        "checks": checks,
        "dataset_rule_alignment": dataset_rule_alignment,
    }


def _service():
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"research-chain-readiness-{uuid4().hex}",
        job_name="research_chain_readiness",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    return kernel, context


def list_frozen_datasets(*, limit: int = 100) -> list[dict[str, Any]]:
    kernel, context = _service()
    try:
        rows = DatasetRepository(kernel).frozen_inventory(context, limit=limit)
        return [
            {key: _iso(value) for key, value in row.items()}
            for row in rows
        ]
    finally:
        kernel.close()


def audit_dataset(dataset_id: str, *, pit_batch_size: int = 10) -> dict[str, Any]:
    if not str(dataset_id).strip():
        raise ValueError("dataset_id must not be blank")
    if pit_batch_size < 1 or pit_batch_size > 100:
        raise ValueError("pit_batch_size must be between 1 and 100")

    kernel, context = _service()
    try:
        datasets = DatasetRepository(kernel)
        manifest = datasets.manifest(dataset_id, context)
        components = datasets.composition_components(dataset_id, context)
        contracts = datasets.research_snapshot_contracts(dataset_id, context)
        member_rows = datasets.member_validation_rows(dataset_id, context)
        member_dates = [
            value for row in contracts
            if (value := _date(row.get("research_date"))) is not None
        ]
        evaluation_start = _date(manifest.get("evaluation_start"))
        evaluation_end = _date(manifest.get("evaluation_end"))
        warmup_start = _date(manifest.get("warmup_start"))
        oos_start = _date(manifest.get("out_of_sample_start"))
        calendar_start = warmup_start or evaluation_start or (min(member_dates) if member_dates else None)
        calendar_end = evaluation_end or (max(member_dates) if member_dates else None)
        field_coverage = {
            "row_count": 0,
            "fields": {
                field: 0 for field in datasets.OBSERVATION_COVERAGE_FIELDS
            },
        }
        if member_dates:
            coverage_windows = _month_windows(min(member_dates), max(member_dates))
            progress_step = max(1, len(coverage_windows) // 10)
            for index, (date_from, date_to) in enumerate(coverage_windows, start=1):
                month_coverage = datasets.observation_field_coverage(
                    dataset_id, date_from, date_to, context,
                )
                field_coverage["row_count"] += month_coverage["row_count"]
                for field, count in month_coverage["fields"].items():
                    field_coverage["fields"][field] += count
                if index % progress_step == 0 or index == len(coverage_windows):
                    print(
                        f"research-chain field coverage {index}/{len(coverage_windows)}",
                        file=sys.stderr,
                        flush=True,
                    )
        trading_dates = (
            datasets.dataset_open_dates(dataset_id, calendar_start, calendar_end, context)
            if calendar_start and calendar_end else []
        )
        evaluation_member_dates = [
            item for item in member_dates
            if (evaluation_start is None or item >= evaluation_start)
            and (evaluation_end is None or item <= evaluation_end)
        ]
        evaluation_trading_dates = [
            item for item in trading_dates
            if (evaluation_start is None or item >= evaluation_start)
            and (evaluation_end is None or item <= evaluation_end)
        ]
        horizon_coverage = summarize_horizon_coverage(
            evaluation_member_dates,
            evaluation_trading_dates,
            out_of_sample_start=oos_start,
        )

        schema_versions = sorted({
            str(row.get("research_schema_version"))
            for row in contracts if row.get("research_schema_version")
        })
        snapshot_status_counts = dict(Counter(
            str(row.get("snapshot_status") or "UNKNOWN") for row in contracts
        ))
        member_quality_counts = dict(Counter(
            str(row[9] or "UNKNOWN") for row in member_rows
        ))
        member_row_counts = {
            "observation_rows": sum(int(row[5] or 0) for row in member_rows),
            "execution_rows": sum(int(row[6] or 0) for row in member_rows),
            "members_without_execution_rows": sum(not int(row[6] or 0) for row in member_rows),
            "members_without_execution_hash": sum(
                bool(int(row[6] or 0)) and not row[7] for row in member_rows
            ),
            "members_without_corporate_action_hash": sum(
                bool(int(row[6] or 0)) and not row[8] for row in member_rows
            ),
        }
        foundation_members = [
            row for row in contracts
            if is_foundation_schema(str(row.get("research_schema_version") or ""))
        ]
        pit_issue_totals: Counter[str] = Counter()
        pit_issue_snapshots: list[dict[str, Any]] = []
        if foundation_members:
            snapshot_repository = ResearchSnapshotRepository(kernel)
            for offset in range(0, len(foundation_members), pit_batch_size):
                batch = foundation_members[offset:offset + pit_batch_size]
                pit_manifests = [
                    {
                        "snapshot_id": str(row["research_snapshot_id"]),
                        "cutoff": row["research_date"],
                        "schema_version": str(row["research_schema_version"]),
                    }
                    for row in batch
                ]
                issues = snapshot_repository.foundation_financial_contract_issues(
                    pit_manifests, context,
                )
                by_id = {str(row["research_snapshot_id"]): row for row in batch}
                for snapshot_id, details in issues.items():
                    pit_issue_totals.update(details)
                    if len(pit_issue_snapshots) < 20:
                        pit_issue_snapshots.append({
                            "research_date": _iso(by_id[snapshot_id]["research_date"]),
                            "snapshot_id": snapshot_id,
                            **details,
                        })
                completed = min(offset + len(batch), len(foundation_members))
                if completed % 100 == 0 or completed == len(foundation_members):
                    print(
                        f"research-chain PIT identity {completed}/{len(foundation_members)}",
                        file=sys.stderr,
                        flush=True,
                    )

        all_snapshots_frozen = bool(contracts) and all(
            str(row.get("snapshot_status") or "").upper() == "FROZEN"
            for row in contracts
        )
        foundation_pit_complete = bool(contracts) and len(foundation_members) == len(contracts)
        if pit_issue_totals:
            pit_status = "FAIL"
        elif foundation_pit_complete and foundation_members:
            pit_status = "PASS"
        else:
            pit_status = "NOT RUN"
        future_financial_visibility_issues = int(
            pit_issue_totals.get("future_financial_availability_rows", 0)
        )
        pit_temporal_status = _pit_temporal_visibility_status(
            pit_issue_totals, foundation_pit_complete, bool(foundation_members),
        )

        strategy_identities = [
            (strategy.strategy_id, strategy.strategy_version)
            for strategy in _STRATEGIES
        ]
        run_repository = BacktestRunRepository(kernel)
        run_rows = run_repository.readiness_run_inventory(
            dataset_id, strategy_identities, list(_CAPITALS), context,
        )
        frozen_rule_rows = run_repository.dataset_rules(dataset_id, context)[1]
        quality_report = _json_object(manifest.get("quality_report"))
        quality_validation_counts = quality_report.get("validation_counts")
        if isinstance(quality_validation_counts, Mapping):
            quality_validation_counts = {
                str(key): int(value or 0)
                for key, value in quality_validation_counts.items()
            }
        else:
            quality_validation_counts = None
    finally:
        kernel.close()

    runs: list[dict[str, Any]] = []
    for run in run_rows:
        run_result = {
            "run_id": str(run["run_id"]),
            "dataset_id": str(run["dataset_id"]),
            "strategy_id": run.get("strategy_id"),
            "strategy_version": run.get("strategy_version"),
            "run_version": run.get("run_version"),
            "initial_cash": float(run["initial_cash"]),
            "status": run.get("status"),
            "created_at": _iso(run.get("created_at")),
            "finished_at": _iso(run.get("finished_at")),
            "parameters_hash": run.get("parameters_hash"),
            "input_hash": run.get("input_hash"),
            "code_version": run.get("code_version"),
            "git_commit": run.get("git_commit"),
            "git_dirty": run.get("git_dirty"),
            "diff_hash": run.get("diff_hash"),
            "execution_model_version": run.get("execution_model_version"),
            "code_archive_sha256": run.get("code_archive_sha256"),
            "latest_attempt": {
                "run_id": str(run["latest_attempt_run_id"])
                if run.get("latest_attempt_run_id") else None,
                "status": run.get("latest_attempt_status"),
                "created_at": _iso(run.get("latest_attempt_created_at")),
                "finished_at": _iso(run.get("latest_attempt_finished_at")),
                "failure_reason": run.get("latest_attempt_failure_reason"),
            },
            "runtime_constraints": _run_parameter_summary(run),
        }
        provenance_checks = _run_provenance_checks(
            run, manifest.get("input_manifest_hash"),
        )
        run_result["provenance_checks"] = provenance_checks
        run_result["provenance_status"] = (
            "PASS" if all(provenance_checks.values()) else "FAIL"
        )
        if str(run.get("status") or "").upper() == "SUCCEEDED":
            try:
                run_result["diagnostics"] = _compact_run_diagnostic(
                    diagnose_run(str(run["run_id"]))
                )
            except Exception as exc:  # diagnostic failure is evidence, not a fatal audit crash
                run_result["diagnostics"] = {
                    "status": "FAIL",
                    "error_type": type(exc).__name__,
                }
        else:
            run_result["diagnostics"] = {"status": "NOT RUN", "reason": "Run 未成功完成"}
        run_result["execution_evidence"] = _execution_evidence(run_result, frozen_rule_rows)
        runs.append(run_result)

    selected_columns, selected_column_count = _selected_column_summary(
        manifest.get("selected_columns")
    )
    selection_contract_status = _selection_contract_status(
        manifest.get("selection_predicate"), selected_column_count,
    )
    quality_issues = quality_report.get("issues")
    if not isinstance(quality_issues, (list, tuple)):
        quality_issues = []
    dataset_quality_report_status = _quality_report_evidence_status(
        quality_report, manifest.get("quality_report_hash"),
    )
    dataset_quality_status = _dataset_quality_status(quality_validation_counts)

    oos_mature = horizon_coverage["out_of_sample"]["mature_horizons"]
    oos_coverage_status = (
        "PASS"
        if oos_start and all(oos_mature[f"{horizon}d"]["mature_member_date_count"] > 0 for horizon in _HORIZONS)
        else "FAIL" if oos_start else "NOT RUN"
    )
    dataset_frozen = (
        str(manifest.get("status") or "").upper() == "FROZEN"
        and dataset_quality_report_status == "PASS"
    )
    runs_by_pair = {
        (str(item["strategy_id"]), str(item["strategy_version"]), int(float(item["initial_cash"])))
        for item in run_rows
        if str(item.get("status") or "").upper() == "SUCCEEDED"
    }
    reproducible_pairs = {
        (str(item["strategy_id"]), str(item["strategy_version"]), int(float(item["initial_cash"])))
        for item in runs
        if str(item.get("status") or "").upper() == "SUCCEEDED"
        and item.get("provenance_status") == "PASS"
        and (item.get("diagnostics") or {}).get("status") == "PASS"
    }
    execution_pairs = {
        (str(item["strategy_id"]), str(item["strategy_version"]), int(float(item["initial_cash"])))
        for item in runs
        if str(item.get("status") or "").upper() == "SUCCEEDED"
        and (item.get("execution_evidence") or {}).get("status") == "PASS"
    }
    expected_pair_count = len(_STRATEGIES) * len(_CAPITALS)
    execution_coverage_status = (
        "PASS" if len(execution_pairs) == expected_pair_count
        else "PARTIAL" if execution_pairs
        else "FAIL" if runs_by_pair
        else "NOT RUN"
    )
    report = {
        "report_version": "research-chain-readiness-v1",
        "business_data_read_only": True,
        "operational_log_behavior": (
            "每个 DataAccessKernel 调用会创建并完成一条 ops.run_manifest 审计记录，"
            "包括每个成功回测对应的诊断调用；"
            "不写入冻结 Snapshot、Dataset、回测结果或交易数据。"
        ),
        "dataset": {
            "dataset_id": str(manifest["dataset_id"]),
            "name": manifest.get("dataset_name"),
            "version": manifest.get("dataset_version"),
            "kind": manifest.get("dataset_kind") or "MATERIALIZED",
            "status": manifest.get("status"),
            "baseline": manifest.get("baseline"),
            "schema_version": manifest.get("schema_version"),
            "feature_version": manifest.get("feature_version"),
            "warmup_start": _iso(warmup_start),
            "evaluation_start": _iso(evaluation_start),
            "evaluation_end": _iso(evaluation_end),
            "out_of_sample_start": _iso(oos_start),
            "frozen_at": _iso(manifest.get("frozen_at")),
            "input_manifest_hash": manifest.get("input_manifest_hash"),
            "selected_column_count": selected_column_count,
            "selected_columns": selected_columns,
            "component_count": len(components),
        },
        "snapshot_inputs": {
            "member_count": len(contracts),
            "feature_quality_warning_member_count": int(
                member_quality_counts.get("WARNING", 0)
            ),
            "first_member_date": _iso(min(member_dates)) if member_dates else None,
            "last_member_date": _iso(max(member_dates)) if member_dates else None,
            "schema_versions": schema_versions,
            "snapshot_status_counts": snapshot_status_counts,
            "dataset_member_quality_counts": member_quality_counts,
            "dataset_member_row_counts": member_row_counts,
            "all_snapshots_frozen": all_snapshots_frozen,
            "pit_financial_identity": {
                "status": pit_status,
                "temporal_visibility_status": pit_temporal_status,
                "future_financial_availability_rows": future_financial_visibility_issues,
                "audited_foundation_snapshot_count": len(foundation_members),
                "all_members_use_audited_foundation_schema": foundation_pit_complete,
                "issue_totals": dict(pit_issue_totals),
                "issue_snapshot_sample": pit_issue_snapshots,
            },
            "observation_field_coverage": {
                "row_count": field_coverage["row_count"],
                "fields": {
                    field: {
                        "non_null_count": count,
                        "coverage_ratio": (
                            round(count / field_coverage["row_count"], 8)
                            if field_coverage["row_count"] else None
                        ),
                    }
                    for field, count in field_coverage["fields"].items()
                },
            },
        },
        "dataset_validation": {
            "quality_report_status": dataset_quality_report_status,
            "quality_report_declared_status": quality_report.get("status"),
            "quality_report_hash_present": bool(manifest.get("quality_report_hash")),
            "quality_report_issues": list(quality_issues),
            "validation_counts": quality_validation_counts,
            "validation_counts_status": dataset_quality_status,
            "stored_summary_counts": {
                key: quality_report[key]
                for key in ("missing_execution_rows", "missing_limit_rows", "benchmark_rows")
                if key in quality_report
            },
            "dataset_member_quality_note": (
                "dataset_member_quality_counts 汇总 Snapshot 的质量标签；"
                "执行行情、日历和公司行为完整性由 Dataset validation_counts 单独核查。"
            ),
        },
        "forward_horizon_calendar_coverage": horizon_coverage,
        "per_security_forward_bar_coverage": {
            "status": "NOT RUN",
            "scope_note": (
                "本报告检查了冻结交易日历的 20/40/60 日成熟度，"
                "没有逐证券验证未来执行行情行及标签可生成率。"
            ),
        },
        "existing_strategy_runs": {
            "strategies": [
                {"strategy_id": item.strategy_id, "strategy_version": item.strategy_version}
                for item in _STRATEGIES
            ],
            "capitals": list(_CAPITALS),
            "successful_run_pair_count": len(runs_by_pair),
            "reproducible_run_pair_count": len(reproducible_pairs),
            "execution_verified_pair_count": len(execution_pairs),
            "expected_pair_count": expected_pair_count,
            "runs": runs,
        },
        "factor_research_gap": {
            "rank_ic": "NOT IMPLEMENTED",
            "quantile_group_returns": "NOT IMPLEMENTED",
            "industry_and_market_cap_exposure": "NOT IMPLEMENTED",
            "minimum_research_interface": [
                "以 FROZEN Research Snapshot 为单位的横截面特征面板，包含研究日、证券代码、schema/baseline 身份及 PIT 来源身份。",
                "基于下一可执行交易时点与复权价格路径生成 20/40/60 交易日标签，并将标签与 PIT 特征严格隔离。",
                "按交易日计算 Rank IC、分组收益、样本数与行业/市值暴露；保存实验参数、输入哈希、时间切分和样本外结果。",
            ],
            "implementation_decision": "本验收仅报告现有缺口；未建设因子统计、实验存储或机器学习。",
        },
        "conclusions": {
            "frozen_dataset_evidence": "PASS" if dataset_frozen else "FAIL",
            "dataset_selection_contract": selection_contract_status,
            "dataset_quality_report_evidence": dataset_quality_report_status,
            "dataset_full_validation_counts": dataset_quality_status,
            "snapshot_freeze_evidence": "PASS" if all_snapshots_frozen else "FAIL",
            "snapshot_feature_quality_warnings": (
                "WARNING" if member_quality_counts.get("WARNING", 0)
                else "PASS" if member_quality_counts else "NOT RUN"
            ),
            "pit_financial_identity_evidence": pit_status,
            "pit_financial_temporal_visibility": pit_temporal_status,
            "out_of_sample_20_40_60_calendar_coverage": oos_coverage_status,
            "per_security_20_40_60_future_bar_coverage": "NOT RUN",
            "persisted_vs_nav_drawdown_consistency": (
                "FAIL"
                if any(
                    (item.get("diagnostics") or {}).get("drawdown_consistency") == "FAIL"
                    for item in runs
                )
                else "PASS"
                if any((item.get("diagnostics") or {}).get("drawdown_consistency") == "PASS" for item in runs)
                else "NOT RUN"
            ),
            "reproducible_10k_20k_strategy_runs": (
                "PASS" if len(reproducible_pairs) == expected_pair_count
                else "FAIL"
            ),
            "executable_10k_20k_strategy_scenarios": execution_coverage_status,
            "execution_limit": (
                "场景执行证据核对资金、持仓、配置模式、费用、滑点、成交单位、"
                "信号/执行时点及冻结 Dataset 规则一致性，并要求至少有一笔真实成交；"
                "同时展示最新尝试状态。"
            ),
            "forward_return_limit": (
                "已有回测诊断只评估真实买入成交；不能代替全体规则候选的固定持有研究。"
            ),
        },
    }
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only audit of frozen Dataset, PIT and backtest research readiness",
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--dataset-id", help="Audit one Backtest Dataset")
    target.add_argument("--list-datasets", action="store_true", help="List frozen Datasets")
    parser.add_argument("--limit", type=int, default=100, help="Inventory row limit (1-500)")
    parser.add_argument("--pit-batch-size", type=int, default=10)
    parser.add_argument("--output", help="Optional JSON output file")
    args = parser.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if args.list_datasets:
        payload: Any = {
            "report_version": "frozen-dataset-inventory-v1",
            "business_data_read_only": True,
            "operational_log_behavior": (
                "DataAccessKernel 会创建并完成 ops.run_manifest 操作审计记录；"
                "不修改冻结数据。"
            ),
            "datasets": list_frozen_datasets(limit=args.limit),
        }
    else:
        payload = audit_dataset(args.dataset_id, pit_batch_size=args.pit_batch_size)
    encoded = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    if args.output:
        Path(args.output).write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
