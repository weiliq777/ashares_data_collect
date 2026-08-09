"""严格校验 AI 反方审计 JSON；本模块不写数据库。"""
from __future__ import annotations

from datetime import date
import hashlib
import json
from typing import Any, Mapping


AI_AUDIT_SCHEMA_VERSION = "ai-audit-v1"
_ACTIONS = {"PASS_REVIEW", "NEEDS_REVIEW", "REJECT", "HOLD", "SELL_REVIEW"}
_RISK_LEVELS = {"LOW", "MEDIUM", "HIGH", "UNKNOWN"}
_REQUIRED = {
    "schema_version", "candidate_id", "ts_code", "asof_date", "action",
    "risk_level", "positive_factors", "risk_flags", "missing_evidence",
    "evidence_ids", "reason", "input_hash", "skill_name", "skill_version",
    "model_version", "prompt_version",
}


class AuditValidationError(ValueError):
    """AI 输出不符合审计契约。"""


def canonical_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _list_of_strings(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise AuditValidationError(f"{field} 必须是非空字符串列表或空列表")
    return [item.strip() for item in value]


def validate_ai_audit(payload: Mapping[str, Any], packet: Mapping[str, Any]) -> dict[str, Any]:
    """校验并归一化 AI 输出，防止 AI 改写候选事实或引用未来证据。"""
    if not isinstance(payload, Mapping):
        raise AuditValidationError("AI 输出必须是 JSON 对象")
    missing = _REQUIRED - set(payload)
    if missing:
        raise AuditValidationError(f"AI 输出缺少字段: {sorted(missing)}")
    unknown = set(payload) - _REQUIRED
    if unknown:
        raise AuditValidationError(f"AI 输出包含未声明字段: {sorted(unknown)}")
    if payload["schema_version"] != AI_AUDIT_SCHEMA_VERSION:
        raise AuditValidationError("不支持的 AI 审计 schema_version")
    for field in ("candidate_id", "ts_code", "input_hash", "skill_name", "skill_version", "model_version", "prompt_version"):
        if not isinstance(payload[field], str) or not payload[field].strip():
            raise AuditValidationError(f"{field} 必须是非空字符串")
    if str(payload["candidate_id"]) != str(packet["candidate_id"]):
        raise AuditValidationError("candidate_id 与分析包不一致")
    if payload["ts_code"] != packet["ts_code"]:
        raise AuditValidationError("ts_code 与分析包不一致")
    try:
        asof_date = date.fromisoformat(str(payload["asof_date"]))
    except ValueError as exc:
        raise AuditValidationError("asof_date 必须是 YYYY-MM-DD") from exc
    if asof_date.isoformat() != str(packet["asof_date"]):
        raise AuditValidationError("asof_date 与分析包不一致")
    if payload["input_hash"] != packet["input_hash"]:
        raise AuditValidationError("input_hash 与分析包不一致，拒绝保存过期审计")
    if payload["action"] not in _ACTIONS:
        raise AuditValidationError(f"不支持的 action: {payload['action']}")
    if payload["risk_level"] not in _RISK_LEVELS:
        raise AuditValidationError(f"不支持的 risk_level: {payload['risk_level']}")
    if not isinstance(payload["reason"], str) or not payload["reason"].strip():
        raise AuditValidationError("reason 不能为空")
    normalized = dict(payload)
    for field in ("positive_factors", "risk_flags", "missing_evidence", "evidence_ids"):
        normalized[field] = _list_of_strings(payload[field], field)
    allowed_evidence = {str(item) for item in packet.get("evidence_ids", [])}
    unknown_evidence = set(normalized["evidence_ids"]) - allowed_evidence
    if unknown_evidence:
        raise AuditValidationError(f"AI 引用了分析包之外的证据: {sorted(unknown_evidence)}")
    normalized["asof_date"] = asof_date.isoformat()
    normalized["reason"] = payload["reason"].strip()
    return normalized


def audit_output_hash(payload: Mapping[str, Any]) -> str:
    """计算已校验审计 JSON 的确定性哈希。"""
    return canonical_hash(payload)


def normalize_ai_audit(payload: Mapping[str, Any], packet: Mapping[str, Any]) -> dict[str, Any]:
    normalized = validate_ai_audit(payload, packet)
    normalized["output_hash"] = audit_output_hash(normalized)
    return normalized
