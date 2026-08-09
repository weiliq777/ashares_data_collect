from __future__ import annotations

import pytest

from data_collect.decision.audit import AuditValidationError, normalize_ai_audit


def packet():
    return {
        "candidate_id": "cand-1",
        "ts_code": "000001.SZ",
        "asof_date": "2026-08-03",
        "input_hash": "a" * 64,
        "evidence_ids": ["e-1"],
    }


def payload(**changes):
    value = {
        "schema_version": "ai-audit-v1",
        "candidate_id": "cand-1",
        "ts_code": "000001.SZ",
        "asof_date": "2026-08-03",
        "action": "PASS_REVIEW",
        "risk_level": "MEDIUM",
        "positive_factors": ["trend"],
        "risk_flags": ["gap"],
        "missing_evidence": [],
        "evidence_ids": ["e-1"],
        "reason": "需要人工确认新闻证据后再决定。",
        "input_hash": "a" * 64,
        "skill_name": "ashares-decision-audit",
        "skill_version": "1.0.0",
        "model_version": "test-model",
        "prompt_version": "test-prompt-v1",
    }
    value.update(changes)
    return value


def test_audit_normalizes_and_adds_hash():
    result = normalize_ai_audit(payload(), packet())
    assert result["output_hash"]
    assert result["asof_date"] == "2026-08-03"


@pytest.mark.parametrize("field,value", [
    ("candidate_id", "other"),
    ("input_hash", "b" * 64),
    ("evidence_ids", ["not-in-packet"]),
    ("action", "BUY"),
])
def test_audit_rejects_tampering(field, value):
    with pytest.raises(AuditValidationError):
        normalize_ai_audit(payload(**{field: value}), packet())


def test_audit_rejects_unknown_output_fields():
    with pytest.raises(AuditValidationError):
        normalize_ai_audit(payload(shares=100), packet())
