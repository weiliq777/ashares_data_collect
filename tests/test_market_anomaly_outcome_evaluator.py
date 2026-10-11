from contextlib import contextmanager
from datetime import date, timedelta
import json
from types import SimpleNamespace

from data_collect.data_access.context import RunContext
from data_collect.observation.outcome_evaluator import OutcomeEvaluator
from data_collect.observation.outcome_metrics import OutcomePolicy


T = date(2026, 9, 1)
CALENDAR = tuple(
    T + timedelta(days=offset)
    for offset in range(0, 180)
    if (T + timedelta(days=offset)).weekday() < 5
)


def _context():
    return RunContext(
        run_id="outcome-test-run", job_name="outcome-test",
        code_version="test", config_hash="test", pipeline_version="test",
    )


def _event():
    anchor = {
        "adj_close_base": 100.0,
        "total_return_price": 100.0,
        "benchmark_close": 200.0,
        "quality_status": "VALID",
        "source_asof_date": T.isoformat(),
        "price_coordinate_contract_version": "market-price-coordinate-v1",
        "source_hash": "d" * 64,
    }
    return {
        "observation_id": "11111111-1111-4111-8111-111111111111",
        "snapshot_id": "22222222-2222-4222-8222-222222222222",
        "observation_date": T,
        "ts_code": "000001.SZ",
        "input_manifest_hash": "a" * 64,
        "observation_input_manifest_hash": "a" * 64,
        "observation_hash": "b" * 64,
        "data_baseline_id": "market-activity-test",
        "snapshot_baseline_id": "market-activity-test",
        "methodology_hash": "c" * 64,
        "snapshot_methodology_hash": "c" * 64,
        "input_provenance": json.dumps({"anchor": anchor}),
        "anchor_adj_close_base": anchor["adj_close_base"],
        "anchor_total_return_price": anchor["total_return_price"],
        "anchor_benchmark_close": anchor["benchmark_close"],
        "maturity_status": "PENDING_MATURITY",
        "outcome_revision": 1,
        "evaluation_methodology_hash": OutcomePolicy().methodology_hash,
        "next_evaluation_date": CALENDAR[5],
        "horizon_status": {
            str(horizon): {
                "status": "PENDING",
                "target_date": CALENDAR[horizon].isoformat(),
                "reason_codes": [],
            }
            for horizon in OutcomePolicy().horizons
        },
        "missing_reason_codes": [],
    }


def _stock_path():
    return [
        {
            "ts_code": "000001.SZ", "trade_date": day,
            "adj_close_base": 100.0 + index,
            "total_return_price": 100.0 + index,
            "quality_status": "VALID", "source_asof_date": day,
            "price_coordinate_contract_version": "market-price-coordinate-v1",
        }
        for index, day in enumerate(CALENDAR[1:6], start=1)
    ]


class FakeRepository:
    def __init__(self, *, due_event=None):
        self.watermark = T if due_event else None
        self.due_event = due_event
        self.writes = []
        self.init_event = _event()
        self.init_remaining = True
        self.due_snapshot_ids = []

    @contextmanager
    def exclusive(self):
        yield

    def initialization_watermark(self, policy_hash):
        return self.watermark

    def advance_initialization_watermark(self, through_date, policy_hash):
        self.watermark = through_date

    def next_frozen_observation_date(self, after_date, through_date, context):
        return T if self.watermark is None and after_date < T <= through_date else None

    def snapshot_dates(self, snapshot_ids, context):
        return [{"snapshot_id": self.init_event["snapshot_id"], "observation_date_start": T, "status": "FROZEN"}]

    def initialization_batch(self, observation_date, through_date, policy_hash, context, *, snapshot_ids=(), benchmark_code="000985.CSI", limit=100):
        if not self.init_remaining:
            return [], {}
        self.init_remaining = False
        event = dict(self.init_event)
        if observation_date == through_date:
            event.update({
                "anchor_adj_close_base": 100.0,
                "anchor_total_return_price": 100.0,
                "anchor_benchmark_close": 200.0,
                "anchor_quality_status": "VALID",
                "anchor_source_asof_date": T,
                "anchor_price_coordinate_contract_version": "market-price-coordinate-v1",
                "anchor_source_hash": "d" * 64,
            })
        else:
            for field in (
                "anchor_adj_close_base", "anchor_total_return_price",
                "anchor_benchmark_close", "anchor_quality_status",
                "anchor_source_asof_date", "anchor_price_coordinate_contract_version",
                "anchor_source_hash",
            ):
                event.pop(field, None)
        return [event], {str(event["observation_id"]): tuple(CALENDAR[:121])}

    def due_batch(self, through_date, context, *, policy_hash, benchmark_code="000985.CSI", snapshot_ids=(), limit=100):
        self.due_snapshot_ids.append(tuple(snapshot_ids))
        if self.due_event is None:
            return [], {}, {}, {}
        event = self.due_event
        self.due_event = None
        event_id = str(event["observation_id"])
        return (
            [event],
            {event_id: tuple(CALENDAR[:121])},
            {event_id: _stock_path()},
            {event_id: [
                {"ts_code": benchmark_code, "trade_date": day, "close": 200.0 + i}
                for i, day in enumerate(CALENDAR[1:6], start=1)
            ]},
        )

    def write_rows(self, rows, context):
        self.writes.extend(dict(row) for row in rows)
        return len(rows)


def test_due_event_restores_anchor_metadata_from_persisted_provenance():
    event = _event()
    repository = FakeRepository(due_event=event)
    report = OutcomeEvaluator(repository).run(
        through_date=CALENDAR[5], context=_context(),
    )

    assert report.status == "COMPLETE"
    assert report.inserted_or_updated == 1
    assert repository.writes[0]["maturity_status"] == "PENDING_MATURITY"
    assert repository.writes[0]["future_price_return_5d"] is not None
    assert repository.writes[0]["horizon_status"]["5"]["status"] == "MATURED"


def test_initialization_captures_only_same_quality_gated_observation_date():
    repository = FakeRepository()
    report = OutcomeEvaluator(repository).run(
        through_date=T, context=_context(),
    )

    assert report.status == "COMPLETE"
    assert report.inserted_or_updated == 1
    assert repository.watermark == T
    assert repository.writes[0]["maturity_status"] == "PENDING_MATURITY"
    assert repository.writes[0]["anchor_adj_close_base"] == 100.0


def test_explicit_historical_replay_without_anchor_fails_closed():
    repository = FakeRepository()
    repository.snapshot_dates = lambda *_args: [
        {"snapshot_id": repository.init_event["snapshot_id"], "observation_date_start": T, "status": "FROZEN"}
    ]
    report = OutcomeEvaluator(repository).run(
        through_date=CALENDAR[5],
        observation_snapshot_ids=(repository.init_event["snapshot_id"],),
        context=_context(),
    )

    assert report.invalid_events == 1
    assert repository.writes[0]["maturity_status"] == "INVALID_INPUT"
    assert "ANCHOR_NOT_RECONSTRUCTABLE" in repository.writes[0]["missing_reason_codes"]
    assert repository.due_snapshot_ids == [
        (repository.init_event["snapshot_id"],),
    ]


def test_automatic_initialization_caps_empty_dates_and_reports_resume_needed():
    from data_collect.observation.outcome_evaluator import (
        MAX_INITIALIZATION_DATES_PER_RUN,
    )

    class ManyEmptyDates(FakeRepository):
        def __init__(self, dates):
            super().__init__()
            self.dates = tuple(dates)

        def next_frozen_observation_date(self, after_date, through_date, context):
            return next((day for day in self.dates if after_date < day <= through_date), None)

        def initialization_batch(self, *args, **kwargs):
            return [], {}

        def due_batch(self, *args, **kwargs):
            return [], {}, {}, {}

    dates = tuple(T + timedelta(days=offset) for offset in range(1, 41))
    repository = ManyEmptyDates(dates)
    repository.watermark = T

    report = OutcomeEvaluator(repository).run(
        through_date=dates[-1], context=_context(),
    )

    assert report.status == "INCOMPLETE_RUN"
    assert report.more_work_pending is True
    assert report.initialization_dates_scanned == MAX_INITIALIZATION_DATES_PER_RUN
    assert repository.watermark == dates[MAX_INITIALIZATION_DATES_PER_RUN - 1]
