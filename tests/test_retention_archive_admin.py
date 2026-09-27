from types import SimpleNamespace

import pytest

from data_collect.data_access.repository.retention_archive_repository import (
    RetentionArchiveRepository,
)
from data_collect.jobs.retention_archive_admin import _preflight_status


class _Read:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def _repository_query(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.rows


class _Write:
    def __init__(self):
        self.calls = []

    def upsert(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return len(args[3])


def _repo(rows):
    kernel = SimpleNamespace(read=_Read(rows), write=_Write())
    return RetentionArchiveRepository(kernel), kernel


def _candidate_row(state="CANDIDATE", refs=0, source_status="FROZEN"):
    return {
        "registry_id": "00000000-0000-0000-0000-000000000001",
        "object_type": "DATASET",
        "object_name": "backtest.dataset_manifest",
        "object_key": "00000000-0000-0000-0000-000000000002",
        "source_status": source_status,
        "archive_state": state,
        "candidate_reason": "test",
        "reference_summary": {},
        "source_state": {},
        "audit_fingerprint": "a" * 64,
        "candidate_since": None,
        "retention_until": None,
        "approved_by": None,
        "approved_at": None,
        "archived_at": None,
        "restored_at": None,
        "notes": None,
        "current_reference_count": refs,
    }


def test_list_candidates_is_bounded_and_read_only():
    repo, kernel = _repo([{"registry_id": "x", "archive_state": "CANDIDATE"}])
    rows = repo.list_candidates(SimpleNamespace(), limit=10)
    assert rows[0]["archive_state"] == "CANDIDATE"
    assert kernel.write.calls == []


def test_approve_refuses_new_references():
    repo, _ = _repo([_candidate_row(refs=1)])
    with pytest.raises(RuntimeError, match="gained references"):
        repo.transition(
            SimpleNamespace(), action="approve",
            registry_ids=["00000000-0000-0000-0000-000000000001"],
            actor="operator", reason="test",
        )


def test_approve_is_explicit_and_writes_only_registry_state():
    repo, kernel = _repo([_candidate_row()])
    changed = repo.transition(
        SimpleNamespace(), action="approve",
        registry_ids=["00000000-0000-0000-0000-000000000001"],
        actor="operator", reason="validated",
    )
    assert changed == 1
    assert kernel.write.calls
    rows = kernel.write.calls[0][0][3]
    assert rows[0]["archive_state"] == "APPROVED"
    assert rows[0]["reference_summary"] == "{}"
    assert rows[0]["source_state"] == "{}"
    assert "DROP" not in str(rows).upper()


def test_transition_is_bounded_and_requires_known_state():
    repo, _ = _repo([_candidate_row(state="ARCHIVED")])
    with pytest.raises(ValueError, match="invalid archive state"):
        repo.transition(
            SimpleNamespace(), action="approve",
            registry_ids=["00000000-0000-0000-0000-000000000001"],
            actor="operator", reason="test",
        )
    with pytest.raises(ValueError, match="at most"):
        repo.transition(
            SimpleNamespace(), action="approve",
            registry_ids=[f"00000000-0000-0000-0000-{value:012d}" for value in range(1001)],
            actor="operator", reason="test",
        )


def test_archive_plan_is_read_only_and_returns_fresh_reference_count():
    row = _candidate_row(state="APPROVED", refs=0)
    repo, kernel = _repo([row])
    plan = repo.list_archive_plan(SimpleNamespace(), limit=10)
    assert plan[0]["archive_state"] == "APPROVED"
    assert plan[0]["current_reference_count"] == 0
    assert kernel.write.calls == []


def test_archive_plan_blocks_unsupported_object_types():
    assert _preflight_status("ROLLBACK_BACKUP", 0) == "BLOCKED_UNSUPPORTED_OBJECT_TYPE"
    assert _preflight_status("DATASET", 0) == "READY_FOR_SEPARATE_REVIEW"
    assert _preflight_status("SNAPSHOT", 1) == "BLOCKED_DEPENDENCIES"
