from contextlib import contextmanager
from datetime import date, timedelta

from data_collect.data_access.context import RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.market_activity import (
    MARKET_ACTIVITY_CONTRACT_HASH,
    MarketActivityBuilder,
    MarketActivityRequest,
)
from data_collect.market_activity.metrics import build_daily_report
from data_collect.data_access.repository.market_activity_storage_repository import (
    enforce_baseline_cutoff,
)


TARGET = date(2026, 9, 22)


def _context() -> RunContext:
    return RunContext(
        run_id="market-activity-persistence-test",
        job_name="market_activity_daily",
        code_version="test",
        config_hash="test",
        pipeline_version="data-access-v1",
    )


def _rows():
    rows = []
    for offset in range(20, 0, -1):
        day = TARGET - timedelta(days=offset)
        rows.extend([
            {"stock_code": "000001.SZ", "trade_date": day, "close": 10, "volume": 100, "amount": 1000},
            {"stock_code": "000002.SZ", "trade_date": day, "close": 20, "volume": 200, "amount": 2000},
        ])
    rows.extend([
        {"stock_code": "000001.SZ", "trade_date": TARGET, "close": 11, "volume": 300, "amount": 4000},
        {"stock_code": "000002.SZ", "trade_date": TARGET, "close": 19, "volume": 100, "amount": 1000},
    ])
    return [rows]


class _FakeStorageRepository:
    def __init__(self):
        self.snapshot = {
            "activity_snapshot_id": "activity-snapshot-1",
            "status": "DRAFT",
            "input_manifest_hash": None,
            "quality_report_hash": None,
            "validated_at": None,
        }
        self.summary_writes = 0
        self.stock_writes = 0
        self.rows = _rows()

    def ensure_snapshot(self, request, context):
        return self.snapshot

    def get_snapshot(self, activity_snapshot_id, context):
        return dict(self.snapshot)

    @contextmanager
    def snapshot_lock(self, activity_snapshot_id):
        yield

    def stream_daily_bars(self, start, end, context, *, batch_rows=10_000, limit_rows=250_001):
        return iter(self.rows)

    def transition_snapshot(self, snapshot_id, context, request, from_statuses, to_status, fields=None):
        assert self.snapshot["status"] in from_statuses
        self.snapshot["status"] = to_status
        self.snapshot.update(fields or {})
        return 1

    def write_daily_summary(self, report, snapshot_id, context):
        self.summary_writes += 1
        return 1

    def write_stock_rows(self, report, snapshot_id, context):
        self.stock_writes += len(report.rows)
        return len(report.rows)

    def validate_snapshot(self, report, snapshot_id, context):
        return {"summary_count": 1, "row_count": len(report.rows), "stock_count": len(report.rows)}


def test_persistence_identity_changes_when_calculation_parameters_change():
    base = MarketActivityRequest(trade_date=TARGET, top_n=20)
    changed = MarketActivityRequest(trade_date=TARGET, top_n=50)
    assert base.methodology_hash != changed.methodology_hash
    assert MARKET_ACTIVITY_CONTRACT_HASH


def test_builder_is_idempotent_and_can_freeze_without_rebuilding():
    repository = _FakeStorageRepository()
    builder = MarketActivityBuilder(repository)
    request = MarketActivityRequest(
        trade_date=TARGET,
        top_n=1,
        data_baseline_id="foundation_v2_20260922",
    )

    first = builder.build(request, _context())
    assert first.status == "VALIDATED"
    assert first.skipped is False
    assert repository.summary_writes == 1
    assert repository.stock_writes == 2

    second = builder.build(request, _context())
    assert second.status == "VALIDATED"
    assert second.skipped is True
    assert repository.summary_writes == 1

    frozen = builder.build(request, _context(), freeze=True)
    assert frozen.status == "FROZEN"
    assert frozen.skipped is True
    assert repository.summary_writes == 1


def test_report_has_reproducibility_hashes():
    request = MarketActivityRequest(
        trade_date=TARGET,
        data_baseline_id="foundation_v2_20260922",
    )
    report = build_daily_report(_rows(), request)
    assert len(report.methodology_hash) == 64
    assert len(report.input_manifest_hash) == 64
    assert len(report.activity_hash) == 64
    assert report.data_baseline_id == "foundation_v2_20260922"


def test_activity_baseline_cutoff_is_fail_closed():
    baseline = {"baseline_cutoff_date": date(2026, 8, 21)}
    enforce_baseline_cutoff(date(2026, 8, 21), "foundation_v2_20260821", baseline)
    try:
        enforce_baseline_cutoff(date(2026, 8, 22), "foundation_v2_20260821", baseline)
    except ValueError as exc:
        assert "after the selected data baseline cutoff" in str(exc)
    else:
        raise AssertionError("activity after the release cutoff must fail closed")


def test_activity_baseline_cutoff_requires_a_valid_date():
    try:
        enforce_baseline_cutoff(
            TARGET,
            "broken-baseline",
            {"baseline_cutoff_date": None},
        )
    except ValueError as exc:
        assert "has no cutoff date" in str(exc)
    else:
        raise AssertionError("missing cutoff must fail closed")


def test_activity_policies_use_business_identity_keys():
    registry = build_default_registry()
    assert registry.require("observation.market_activity_snapshot").primary_key == (
        "activity_date_start", "activity_date_end", "methodology_hash", "data_baseline_id"
    )
    assert registry.require("observation.market_activity_daily").primary_key == (
        "trade_date", "methodology_hash", "data_baseline_id"
    )
    assert registry.require("observation.stock_activity_daily").primary_key == (
        "trade_date", "ts_code", "methodology_hash", "data_baseline_id"
    )
