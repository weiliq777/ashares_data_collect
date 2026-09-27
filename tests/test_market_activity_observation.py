from contextlib import contextmanager
from datetime import date

from data_collect.data_access.context import RunContext
from data_collect.observation.market_activity_persistence import (
    MarketActivityObservationBuilder,
)
from data_collect.observation.market_activity_rules import (
    DEFERRED_ACTIVITY_TAGS,
    MarketActivityObservationPolicy,
    build_market_activity_observations,
)


TARGET = date(2026, 9, 22)
SOURCE_ID = "c6397da9-e398-5e76-bc13-f4673cd4314d"
BASELINE = "foundation_v2_20260821"


def _source_rows():
    return [
        {
            "activity_snapshot_id": SOURCE_ID,
            "trade_date": TARGET,
            "ts_code": "000001.SZ",
            "methodology_hash": "a" * 64,
            "data_baseline_id": BASELINE,
            "volume": 400.0,
            "amount": 4000.0,
            "amount_share": 0.010,
            "volume_share": 0.008,
            "amount_rank": 1,
            "volume_rank": 1,
            "amount_ratio_previous": 4.0,
            "volume_ratio_previous": 3.0,
            "history_observation_count": 20,
            "calculation_status": "VALID",
            "quality_status": "VALID",
            "input_manifest_hash": "b" * 64,
            "activity_hash": "c" * 64,
        },
        {
            "activity_snapshot_id": SOURCE_ID,
            "trade_date": TARGET,
            "ts_code": "000002.SZ",
            "methodology_hash": "a" * 64,
            "data_baseline_id": BASELINE,
            "volume": 200.0,
            "amount": 1000.0,
            "amount_share": 0.001,
            "volume_share": 0.002,
            "amount_rank": 2,
            "volume_rank": 2,
            "amount_ratio_previous": 1.2,
            "volume_ratio_previous": 1.9,
            "history_observation_count": 20,
            "calculation_status": "VALID",
            "quality_status": "VALID",
            "input_manifest_hash": "b" * 64,
            "activity_hash": "c" * 64,
        },
        {
            "activity_snapshot_id": SOURCE_ID,
            "trade_date": TARGET,
            "ts_code": "000003.SZ",
            "methodology_hash": "a" * 64,
            "data_baseline_id": BASELINE,
            "volume": 500.0,
            "amount": 5000.0,
            "amount_share": 0.020,
            "volume_share": 0.020,
            "amount_rank": 3,
            "volume_rank": 3,
            "amount_ratio_previous": 5.0,
            "volume_ratio_previous": 5.0,
            "history_observation_count": 19,
            "calculation_status": "INSUFFICIENT_HISTORY",
            "quality_status": "PARTIAL_HISTORY",
            "input_manifest_hash": "b" * 64,
            "activity_hash": "c" * 64,
        },
    ]


def test_activity_observation_emits_explainable_tags_and_gates_history():
    report = build_market_activity_observations(
        _source_rows(),
        policy=MarketActivityObservationPolicy(),
        observation_date=TARGET,
        source_activity_snapshot_id=SOURCE_ID,
        data_baseline_id=BASELINE,
    )

    first = next(row for row in report.rows if row.ts_code == "000001.SZ")
    assert [item["tag_code"] for item in first.tags] == [
        "ABNORMAL_VOLUME", "ABNORMAL_AMOUNT", "SUDDEN_ACTIVITY", "HIGH_AMOUNT_SHARE"
    ]
    sudden = next(item for item in first.tags if item["tag_code"] == "SUDDEN_ACTIVITY")
    assert sudden["actual_value"] == 3.0
    assert sudden["threshold"] == 2.0
    assert sudden["source_metric"] == "volume_ratio_previous+amount_ratio_previous"

    insufficient = next(row for row in report.rows if row.ts_code == "000003.SZ")
    assert insufficient.tags == ()
    assert insufficient.calculation_status == "INSUFFICIENT_HISTORY"
    assert report.quality_status == "PARTIAL_HISTORY"
    assert report.tag_count == 4
    assert set(DEFERRED_ACTIVITY_TAGS).isdisjoint(
        {tag["tag_code"] for row in report.rows for tag in row.tags}
    )


def test_activity_observation_is_deterministic_and_rejects_mixed_identity():
    kwargs = {
        "policy": MarketActivityObservationPolicy(),
        "observation_date": TARGET,
        "source_activity_snapshot_id": SOURCE_ID,
        "data_baseline_id": BASELINE,
    }
    first = build_market_activity_observations(_source_rows(), **kwargs)
    second = build_market_activity_observations(list(reversed(_source_rows())), **kwargs)
    assert first.input_manifest_hash == second.input_manifest_hash
    assert first.observation_hash == second.observation_hash
    assert [row.observation_id for row in first.rows] == [row.observation_id for row in second.rows]

    mixed = _source_rows()
    mixed[0] = {**mixed[0], "data_baseline_id": "other-baseline"}
    try:
        build_market_activity_observations(mixed, **kwargs)
    except ValueError as exc:
        assert "baseline mismatch" in str(exc)
    else:
        raise AssertionError("mixed baseline must fail closed")


def _context() -> RunContext:
    return RunContext(
        run_id="market-activity-observation-test",
        job_name="market_activity_observation",
        code_version="test",
        config_hash="test",
        pipeline_version="data-access-v1",
    )


class _FakeObservationRepository:
    def __init__(self):
        self.source = {
            "activity_snapshot_id": SOURCE_ID,
            "activity_date_start": TARGET,
            "activity_date_end": TARGET,
            "data_baseline_id": BASELINE,
            "methodology_hash": "a" * 64,
            "status": "FROZEN",
        }
        self.snapshot = {"snapshot_id": "observation-snapshot-1", "status": "DRAFT"}
        self.writes = None

    def get_activity_snapshot(self, activity_snapshot_id, context):
        assert activity_snapshot_id == SOURCE_ID
        return dict(self.source)

    def ensure_snapshot(self, source, policy, context):
        return dict(self.snapshot)

    def get_snapshot(self, snapshot_id, context):
        return dict(self.snapshot)

    @contextmanager
    def snapshot_lock(self, snapshot_id):
        yield

    def transition_snapshot(self, snapshot_id, observation_date, context, from_statuses, to_status, fields=None):
        assert self.snapshot["status"] in from_statuses
        self.snapshot["status"] = to_status
        self.snapshot.update(fields or {})
        return 1

    def activity_rows(self, source_activity_snapshot_id, observation_date, context):
        return _source_rows()

    def write_rows(self, report, snapshot_id, context):
        self.writes = (report.stock_count, report.tag_count)
        return self.writes

    def validate_snapshot(self, report, snapshot_id, context):
        return {
            "row_count": report.stock_count,
            "stock_count": report.stock_count,
            "tag_count": report.tag_count,
        }


def test_builder_has_small_lifecycle_interface_and_freezes_after_validation():
    repository = _FakeObservationRepository()
    builder = MarketActivityObservationBuilder(repository)
    policy = MarketActivityObservationPolicy()

    built = builder.build(SOURCE_ID, policy, _context())
    assert built.status == "VALIDATED"
    assert built.skipped is False
    assert repository.writes == (3, 4)

    skipped = builder.build(SOURCE_ID, policy, _context())
    assert skipped.status == "VALIDATED"
    assert skipped.skipped is True

    frozen = builder.build(SOURCE_ID, policy, _context(), freeze=True)
    assert frozen.status == "FROZEN"
    assert frozen.skipped is True
