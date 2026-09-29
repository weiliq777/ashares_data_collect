from datetime import date, datetime

import pytest

from data_collect.data_access.context import RunContext
from data_collect.jobs.market_activity_production_daily import (
    RELEASE_SCHEMA_VERSION,
    _assert_release_identity,
    _ensure_validated_release,
    _parse_date,
    _release_tag,
)


class FakeReleaseRepository:
    def __init__(self, release=None, current=None):
        self.release = release
        self.current = current
        self.created = []
        self.validated = []
        self.committed = []

    def get_release(self, tag, _context):
        return dict(self.release) if self.release else None

    def current_validated_release(self, _domain, _context):
        return dict(self.current) if self.current else None

    def create(self, tag, cutoff, _notes, schema, _context):
        self.created.append((tag, cutoff, schema))
        self.release = {
            "release_tag": tag,
            "baseline_domain": "MARKET_ACTIVITY",
            "baseline_cutoff_date": cutoff,
            "schema_version": schema,
            "status": "draft",
        }

    def validate(self, tag, _hash, _context):
        self.validated.append(tag)
        return {"tag": tag, "baseline_domain": "MARKET_ACTIVITY"}

    def commit_validation(self, _result, _context):
        self.committed.append(True)
        self.release["status"] = "validated"
        return 1


def _context():
    return RunContext(
        run_id="activity-release-test",
        job_name="market_activity_production_daily",
        code_version="test",
        config_hash="test",
        pipeline_version="test",
    )


def test_parse_date_and_daily_release_tag_are_deterministic():
    assert _parse_date("2026-09-25") == date(2026, 9, 25)
    assert _parse_date("20260925") == date(2026, 9, 25)
    assert _parse_date(datetime(2026, 9, 25, 19, 0)) == date(2026, 9, 25)
    assert _release_tag(date(2026, 9, 25)) == "market-activity-20260925"


def test_new_daily_release_is_created_validated_and_committed_once():
    repository = FakeReleaseRepository()
    tag = "market-activity-20260925"

    result = _ensure_validated_release(repository, _context(), tag, date(2026, 9, 25))

    assert result == {"tag": tag, "status": "validated", "reused": False}
    assert repository.created == [(tag, date(2026, 9, 25), RELEASE_SCHEMA_VERSION)]
    assert repository.validated == [tag]
    assert repository.committed == [True]


def test_existing_validated_daily_release_is_reused_without_resetting_it():
    tag = "market-activity-20260925"
    repository = FakeReleaseRepository({
        "release_tag": tag,
        "baseline_domain": "MARKET_ACTIVITY",
        "baseline_cutoff_date": date(2026, 9, 25),
        "schema_version": RELEASE_SCHEMA_VERSION,
        "status": "validated",
    })

    result = _ensure_validated_release(repository, _context(), tag, date(2026, 9, 25))

    assert result["reused"] is True
    assert repository.created == []
    assert repository.validated == []


@pytest.mark.parametrize("status", ["superseded", "revoked", "failed"])
def test_non_current_release_is_not_silently_reactivated(status):
    tag = "market-activity-20260925"
    repository = FakeReleaseRepository({
        "release_tag": tag,
        "baseline_domain": "MARKET_ACTIVITY",
        "baseline_cutoff_date": date(2026, 9, 25),
        "schema_version": RELEASE_SCHEMA_VERSION,
        "status": status,
    })
    with pytest.raises(RuntimeError, match="non-current"):
        _ensure_validated_release(repository, _context(), tag, date(2026, 9, 25))


def test_release_identity_mismatch_fails_closed():
    with pytest.raises(RuntimeError, match="identity mismatch"):
        _assert_release_identity({
            "release_tag": "market-activity-20260924",
            "baseline_domain": "MARKET_ACTIVITY",
            "baseline_cutoff_date": date(2026, 9, 24),
            "schema_version": RELEASE_SCHEMA_VERSION,
        }, "market-activity-20260925", date(2026, 9, 25))


def test_historical_rerun_cannot_move_current_release_backward():
    repository = FakeReleaseRepository(current={
        "release_tag": "market-activity-20260926",
        "baseline_domain": "MARKET_ACTIVITY",
        "baseline_cutoff_date": date(2026, 9, 26),
        "schema_version": RELEASE_SCHEMA_VERSION,
        "status": "validated",
    })
    with pytest.raises(RuntimeError, match="move the current Market Activity release backward"):
        _ensure_validated_release(
            repository,
            _context(),
            "market-activity-20260925",
            date(2026, 9, 25),
        )
    assert repository.created == []
