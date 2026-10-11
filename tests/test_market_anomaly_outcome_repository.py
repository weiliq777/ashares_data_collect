from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

from data_collect.data_access.context import RunContext
from data_collect.data_access.repository.market_anomaly_outcome_repository import (
    BENCHMARK_CODE,
    MarketAnomalyOutcomeRepository,
)


T = date(2026, 9, 1)
SNAPSHOT_ID = "22222222-2222-4222-8222-222222222222"
OBSERVATION_ID = "11111111-1111-4111-8111-111111111111"


def _context():
    return RunContext(
        run_id="outcome-repository-test", job_name="outcome-repository-test",
        code_version="test", config_hash="test", pipeline_version="test",
    )


class FakeRead:
    def __init__(self):
        self.calls = []

    def _repository_query(self, *, context, source, sql, params, scope, max_rows=None):
        self.calls.append((source, sql, params, scope, max_rows))
        if source == "evaluation.market_anomaly_outcome":
            return getattr(self, "outcome_rows", [])
        if source == "observation.stock_anomaly_daily":
            return [{
                "snapshot_id": SNAPSHOT_ID,
                "observation_date_start": T,
                "snapshot_baseline_id": "market-activity-test",
                "snapshot_methodology_hash": "c" * 64,
                "snapshot_input_manifest_hash": "e" * 64,
                "observation_id": OBSERVATION_ID,
                "observation_date": T,
                "ts_code": "000001.SZ",
                "methodology_hash": "c" * 64,
                "data_baseline_id": "market-activity-test",
                "input_manifest_hash": "a" * 64,
                "observation_hash": "b" * 64,
            }]
        if source == "std.trading_calendar":
            return [{"observation_id": OBSERVATION_ID, "trade_date": T}]
        if source == "feature.price_daily_v2":
            return [{
                "ts_code": "000001.SZ", "trade_date": T,
                "adj_close_base": 100.0, "total_return_price": 101.0,
                "quality_status": "VALID", "source_asof_date": T,
                "price_coordinate_contract_version": "market-price-coordinate-v1",
                "calculated_at": "2026-09-01T16:00:00+08:00",
            }]
        if source == "std.index_daily_tushare":
            return [{"ts_code": BENCHMARK_CODE, "trade_date": T, "close": 200.0}]
        raise AssertionError(f"unexpected repository source: {source}")

    def _repository_explain(self, context, source, sql, params, scope):
        self.calls.append((source, sql, params, scope, None))
        return {"source": source, "plan": "EXPLAIN ONLY"}


def test_initialization_is_batched_and_captures_quality_gated_anchor():
    reader = FakeRead()
    repository = object.__new__(MarketAnomalyOutcomeRepository)
    repository.kernel = SimpleNamespace(read=reader)

    events, paths = repository.initialization_batch(
        T, T, "f" * 64, _context(), limit=100,
    )

    assert len(events) == 1
    assert paths[OBSERVATION_ID] == (T,)
    assert events[0]["anchor_quality_status"] == "VALID"
    assert events[0]["anchor_price_coordinate_contract_version"] == "market-price-coordinate-v1"
    assert len(events[0]["anchor_source_hash"]) == 64
    assert [call[0] for call in reader.calls] == [
        "observation.stock_anomaly_daily", "std.trading_calendar",
        "feature.price_daily_v2", "std.index_daily_tushare",
    ]
    benchmark_call = reader.calls[-1]
    assert "source='tushare'" in benchmark_call[1]
    assert benchmark_call[2] == (BENCHMARK_CODE, T)
    assert all(call[4] is not None and call[4] <= 12_100 for call in reader.calls)


def test_write_uses_revision_identity_and_snapshot_scoped_policy():
    upsert = Mock(return_value=1)
    repository = object.__new__(MarketAnomalyOutcomeRepository)
    repository.kernel = SimpleNamespace(write=SimpleNamespace(upsert=upsert))

    written = repository.write_rows([{
        "outcome_id": "33333333-3333-4333-8333-333333333333",
        "observation_id": OBSERVATION_ID,
        "observation_snapshot_id": SNAPSHOT_ID,
        "observation_date": T,
        "ts_code": "000001.SZ",
        "evaluation_methodology_hash": "f" * 64,
        "outcome_revision": 1,
        "maturity_status": "PENDING_MATURITY",
        "horizon_status": {}, "input_provenance": {},
        "missing_reason_codes": [], "input_manifest_hash": "a" * 64,
        "outcome_hash": "b" * 64, "benchmark_code": BENCHMARK_CODE,
    }], _context())

    assert written == 1
    kwargs = upsert.call_args.kwargs
    assert kwargs["target"] == "evaluation.market_anomaly_outcome"
    assert kwargs["key"] == (
        "observation_id", "evaluation_methodology_hash", "outcome_revision",
    )
    assert kwargs["scope"].dimensions["snapshot_id"] == SNAPSHOT_ID
    assert kwargs["scope"].date_from == kwargs["scope"].date_to == T


def test_explicit_due_probe_is_snapshot_scoped_and_row_bounded():
    reader = FakeRead()
    reader.outcome_rows = [{"outcome_id": "test"}]
    repository = object.__new__(MarketAnomalyOutcomeRepository)
    repository.kernel = SimpleNamespace(read=reader)

    assert repository.has_due(
        T, "f" * 64, _context(), snapshot_ids=(SNAPSHOT_ID,),
    ) is True

    source, sql, params, scope, max_rows = reader.calls[-1]
    assert source == "evaluation.market_anomaly_outcome"
    assert "d.snapshot_id=ANY(%s::uuid[])" in sql
    assert params == ([SNAPSHOT_ID], T, "f" * 64)
    assert scope.dimensions["snapshot_ids"] == (SNAPSHOT_ID,)
    assert max_rows == 1


def test_explicit_due_batch_does_not_scan_other_snapshot_outcomes():
    reader = FakeRead()
    repository = object.__new__(MarketAnomalyOutcomeRepository)
    repository.kernel = SimpleNamespace(read=reader)

    events, paths, stocks, benchmark = repository.due_batch(
        T, _context(), policy_hash="f" * 64,
        snapshot_ids=(SNAPSHOT_ID,), limit=100,
    )

    assert (events, paths, stocks, benchmark) == ([], {}, {}, {})
    source, sql, params, scope, max_rows = reader.calls[-1]
    assert source == "evaluation.market_anomaly_outcome"
    assert "d.snapshot_id=ANY(%s::uuid[])" in sql
    assert params == ([SNAPSHOT_ID], T, "f" * 64, 101)
    assert scope.dimensions["snapshot_ids"] == (SNAPSHOT_ID,)
    assert max_rows == 101


def test_explain_helpers_cover_daily_and_explicit_queue_selects_without_analyze():
    reader = FakeRead()
    repository = object.__new__(MarketAnomalyOutcomeRepository)
    repository.kernel = SimpleNamespace(read=reader)
    context = _context()

    init_plans = repository.explain_initialization_queries(
        T, ["000001.SZ"], [SNAPSHOT_ID], "f" * 64, context,
    )
    due_plans = repository.explain_due_queries(
        T, ["000001.SZ"], [OBSERVATION_ID], [T], "f" * 64,
        context, sample_snapshot_ids=[SNAPSHOT_ID],
    )

    assert "daily_frozen_anomalies" in init_plans
    assert {"due_probe", "explicit_due_probe", "price_paths_and_benchmark"} <= due_plans.keys()
    assert all("ANALYZE" not in sql.upper() for _, sql, *_ in reader.calls)
