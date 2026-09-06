import json
from datetime import date

import pytest

from data_collect.jobs import repair_foundation_snapshot_financial as repair


def _audit(tmp_path):
    path = tmp_path / "audit.json"
    path.write_text(json.dumps({
        "audit_version": "foundation-financial-contract-v1",
        "dataset_id": "dataset-1",
        "status": "FAIL",
        "issue_rows": [{
            "research_date": "2026-08-21",
            "snapshot_id": "old-1",
            "non_v2_source_rows": 10,
        }],
    }), encoding="utf-8")
    return path


def test_prepare_repair_verifies_frozen_snapshot_identity(monkeypatch, tmp_path):
    monkeypatch.setattr(
        repair,
        "_read_dataset_contract",
        lambda dataset_id: (
            {"status": "FROZEN", "baseline": "baseline-1"},
            [{
                "research_date": date(2026, 8, 21),
                "research_snapshot_id": "old-1",
                "snapshot_status": "FROZEN",
                "research_schema_version": "research-v2.0",
            }],
        ),
    )
    plan = repair.prepare_repair("dataset-1", _audit(tmp_path))
    assert plan["issue_count"] == 1
    assert plan["target_schema_version"] == "research-v2.1"


def test_prepare_repair_rejects_changed_source_identity(monkeypatch, tmp_path):
    monkeypatch.setattr(
        repair,
        "_read_dataset_contract",
        lambda dataset_id: (
            {"status": "FROZEN", "baseline": "baseline-1"},
            [{
                "research_date": date(2026, 8, 21),
                "research_snapshot_id": "different-id",
                "snapshot_status": "FROZEN",
                "research_schema_version": "research-v2.0",
            }],
        ),
    )
    with pytest.raises(RuntimeError, match="identity changed"):
        repair.prepare_repair("dataset-1", _audit(tmp_path))
