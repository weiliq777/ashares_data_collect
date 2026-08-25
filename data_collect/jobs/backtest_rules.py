"""阶段三市场/费用规则的校验和只追加导入工具。

规则文件只包含经过人工/业务确认的版本化事实。默认命令只校验，不写库；写库
必须显式传 ``--confirm``，且数据库触发器禁止后续 UPDATE/DELETE。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import yaml

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.backtest_rules_repository import BacktestRulesRepository


REQUIRED_FIELDS = {
    "rule_version", "market_rule_version", "fee_rule_version", "effective_from",
    "market", "min_buy_lot", "allow_sell_odd_lot", "t_plus_one", "price_tick",
    "commission_rate", "min_commission", "stamp_duty_sell_rate", "transfer_fee_rate",
}
OPTIONAL_DEFAULTS = {
    "effective_to": None,
    "cash_settlement_days": 0,
    "limit_pct": None,
    "slippage_bps": 0.0,
    "parameters": {},
}
RULE_CONTENT_FIELDS = (
    "market_rule_version", "fee_rule_version", "effective_to", "min_buy_lot",
    "allow_sell_odd_lot", "t_plus_one", "price_tick", "cash_settlement_days",
    "limit_pct", "commission_rate", "min_commission", "stamp_duty_sell_rate",
    "transfer_fee_rate", "slippage_bps", "parameters",
)


def _parse_date(value: Any, field: str) -> date:
    try:
        return value if isinstance(value, date) else date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"{field} 不是 YYYY-MM-DD: {value}") from exc


def normalize_rule_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """校验并规范规则记录，不访问数据库。"""
    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(records):
        if not isinstance(raw, dict):
            raise ValueError(f"第 {index + 1} 条规则不是对象")
        missing = sorted(REQUIRED_FIELDS - raw.keys())
        if missing:
            raise ValueError(f"第 {index + 1} 条规则缺少字段: {missing}")
        row = dict(OPTIONAL_DEFAULTS)
        row.update(raw)
        row["effective_from"] = _parse_date(row["effective_from"], "effective_from")
        if row["effective_to"] is not None:
            row["effective_to"] = _parse_date(row["effective_to"], "effective_to")
            if row["effective_to"] < row["effective_from"]:
                raise ValueError(f"第 {index + 1} 条规则 effective_to 早于 effective_from")
        for key in ("rule_version", "market_rule_version", "fee_rule_version", "market"):
            if not str(row[key]).strip():
                raise ValueError(f"第 {index + 1} 条规则 {key} 不能为空")
        try:
            min_buy_lot = float(row["min_buy_lot"])
            cash_settlement_days = float(row["cash_settlement_days"])
            price_tick = float(row["price_tick"])
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"第 {index + 1} 条规则的整手/价位/结算字段不是有效数值") from exc
        if (
            not math.isfinite(min_buy_lot)
            or min_buy_lot <= 0
            or min_buy_lot != int(min_buy_lot)
        ):
            raise ValueError(f"第 {index + 1} 条规则 min_buy_lot 必须大于 0")
        if not math.isfinite(price_tick) or price_tick <= 0:
            raise ValueError(f"第 {index + 1} 条规则 price_tick 必须大于 0")
        if (
            not math.isfinite(cash_settlement_days)
            or cash_settlement_days < 0
            or cash_settlement_days != int(cash_settlement_days)
        ):
            raise ValueError(f"第 {index + 1} 条规则 cash_settlement_days 不能为负数")
        for key in (
            "commission_rate", "min_commission", "stamp_duty_sell_rate",
            "transfer_fee_rate", "slippage_bps",
        ):
            if not math.isfinite(float(row[key])) or float(row[key]) < 0:
                raise ValueError(f"第 {index + 1} 条规则 {key} 不能为负数")
        if row["limit_pct"] is not None and (
            not math.isfinite(float(row["limit_pct"])) or float(row["limit_pct"]) < 0
        ):
            raise ValueError(f"第 {index + 1} 条规则 limit_pct 不能为负数")
        if not isinstance(row["allow_sell_odd_lot"], bool) or not isinstance(row["t_plus_one"], bool):
            raise ValueError(f"第 {index + 1} 条规则 allow_sell_odd_lot/t_plus_one 必须是布尔值")
        if not isinstance(row["parameters"], dict):
            raise ValueError(f"第 {index + 1} 条规则 parameters 必须是对象")
        if not str(row["parameters"].get("provenance", "")).strip():
            raise ValueError(f"第 {index + 1} 条规则必须在 parameters.provenance 记录来源/审核依据")
        participation = row["parameters"].get("max_volume_participation_pct", 1.0)
        if not math.isfinite(float(participation)) or not 0 < float(participation) <= 1:
            raise ValueError(
                f"第 {index + 1} 条规则 parameters.max_volume_participation_pct 必须位于 (0,1]"
            )
        liquidity_policy = row["parameters"].get("liquidity_policy", "reject_full_order")
        if liquidity_policy != "reject_full_order":
            raise ValueError(
                f"第 {index + 1} 条规则 parameters.liquidity_policy 仅支持 reject_full_order"
            )
        row["min_buy_lot"] = int(min_buy_lot)
        row["cash_settlement_days"] = int(cash_settlement_days)
        row["price_tick"] = price_tick
        for key in (
            "commission_rate", "min_commission", "stamp_duty_sell_rate",
            "transfer_fee_rate", "slippage_bps", "limit_pct",
        ):
            if row[key] is not None:
                row[key] = float(row[key])
        row["allow_sell_odd_lot"] = bool(row["allow_sell_odd_lot"])
        row["t_plus_one"] = bool(row["t_plus_one"])
        normalized.append(row)

    by_scope: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in normalized:
        scope = (row["market"], row["market_rule_version"], row["fee_rule_version"])
        by_scope.setdefault(scope, []).append(row)
    for scope, rows in by_scope.items():
        rows.sort(key=lambda item: (item["effective_from"], item["rule_version"]))
        previous_to: date | None = None
        has_previous = False
        for row in rows:
            if has_previous and (previous_to is None or row["effective_from"] <= previous_to):
                raise ValueError(f"规则有效区间重叠: market/rule={scope}, from={row['effective_from']}")
            previous_to = row["effective_to"]
            has_previous = True
    return sorted(normalized, key=lambda row: (
        row["effective_from"], row["market"], row["rule_version"]
    ))


def rule_content_fingerprint(row: dict[str, Any]) -> str:
    """计算同一规则主键下的内容指纹，用于检测静默冲突。"""
    payload = {key: row.get(key) for key in RULE_CONTENT_FIELDS}
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def load_rule_file(path: str | Path) -> list[dict[str, Any]]:
    return load_rule_document(path)[0]


def load_rule_document(path: str | Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """读取规则和审核元数据；返回前先完成规则规范化校验。"""
    # YAML 同时兼容 JSON，便于与项目现有 config/*.yaml 约定保持一致。
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    records = payload.get("rules") if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        raise ValueError("规则 JSON 必须是数组，或包含 rules 数组")
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    if not isinstance(metadata, dict):
        raise ValueError("规则文件 metadata 必须是对象")
    return normalize_rule_records(records), metadata


def assert_rule_document_approved(metadata: dict[str, Any]) -> None:
    """阻止 REVIEW_REQUIRED 文档被 --confirm 误写入生产库。"""
    if not metadata:
        raise RuntimeError("规则文档缺少 metadata；不能 --confirm 入库")
    if metadata.get("status") != "APPROVED":
        raise RuntimeError("规则文档仍未 APPROVED；只能校验，不能 --confirm 入库")
    if not str(metadata.get("approved_by", "")).strip():
        raise RuntimeError("已批准规则必须记录 metadata.approved_by")
    if not str(metadata.get("approved_at", "")).strip():
        raise RuntimeError("已批准规则必须记录 metadata.approved_at")


def _approval_hash(metadata: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    payload = {"metadata": metadata, "rules": rows}
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def insert_rule_records(
    records: Iterable[dict[str, Any]], *, confirm: bool = False,
    approval_metadata: dict[str, Any] | None = None,
) -> int:
    if not confirm:
        raise RuntimeError("规则入库必须显式传入 confirm=True")
    rows = normalize_rule_records(records)
    metadata = approval_metadata or {}
    assert_rule_document_approved(metadata)
    approval_hash = _approval_hash(metadata, rows)
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    repository = BacktestRulesRepository(kernel)
    context = RunContext(
        run_id=f"backtest-rules-{approval_hash[:12]}", job_name="backtest_rules",
        code_version="unknown", config_hash=approval_hash, pipeline_version="data-access-v1",
    )
    try:
        return repository.insert_approved(rows, approval_hash, metadata, RULE_CONTENT_FIELDS, rule_content_fingerprint, context)
    finally:
        kernel.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="阶段三市场/费用规则导入")
    parser.add_argument("path", help="规则 YAML/JSON 文件")
    parser.add_argument("--confirm", action="store_true", help="确认追加写入规则表")
    args = parser.parse_args()
    records, metadata = load_rule_document(args.path)
    if args.confirm:
        assert_rule_document_approved(metadata)
        print(f"inserted_or_existing={insert_rule_records(records, confirm=True, approval_metadata=metadata)}")
    else:
        print(f"validated={len(records)}; 未写入数据库，需 --confirm 才会追加")


if __name__ == "__main__":
    main()
