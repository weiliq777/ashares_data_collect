from datetime import date

import pytest

from data_collect.backtest.engine import BacktestConfig, BacktestEngine
from data_collect.backtest.models import BacktestDataset, MarketBar
from data_collect.jobs.backtest_dataset import StreamingFrozenDataset


DAY1 = date(2025, 1, 2)
DAY2 = date(2025, 1, 3)
DAY3 = date(2025, 1, 6)
CALENDAR = (DAY1, DAY2, DAY3)


class _Kernel:
    def __init__(self):
        self.close_calls = 0

    def close(self):
        self.close_calls += 1


class _Repository:
    def __init__(self):
        self.kernel = _Kernel()
        self.bar_rows = [self._bar_row(day, code) for day in CALENDAR for code in ("A", "B", "C")]
        self.observation_rows = [
            {
                "research_date": day,
                "ts_code": code,
                "raw_close": float(index + 10),
                "delist_date": None,
            }
            for index, (day, code) in enumerate(
                (item for item in ((day, code) for day in CALENDAR for code in ("A", "B", "C")))
            )
        ]
        self.bar_calls = []
        self.observation_calls = []

    @staticmethod
    def _bar_row(day, code):
        return {
            "trade_date": day,
            "ts_code": code,
            "raw_open": 10.0,
            "raw_high": 11.0,
            "raw_low": 9.0,
            "raw_close": 10.0,
            "volume": 100.0,
            "amount": 1000.0,
            "up_limit": 12.0,
            "down_limit": 8.0,
            "is_suspended": False,
            "can_buy": True,
            "can_sell": True,
            "limit_status": None,
            "market": "SSE",
        }

    @staticmethod
    def _page(rows, last_key, limit, key_names):
        if last_key is not None:
            rows = [row for row in rows if tuple(row[name] for name in key_names) > last_key]
        return rows[:limit]

    def bar_page(self, dataset_id, context, last_key=None, limit=10_000, date_from=None, date_to=None):
        assert date_from == date_to
        self.bar_calls.append((date_from, date_to, last_key, limit))
        rows = [row for row in self.bar_rows if date_from <= row["trade_date"] <= date_to]
        return self._page(rows, last_key, limit, ("trade_date", "ts_code"))

    def observation_page(self, dataset_id, columns, context, last_key=None, limit=10_000, date_from=None, date_to=None):
        assert date_from == date_to
        self.observation_calls.append((date_from, date_to, last_key, tuple(columns), limit))
        rows = [row for row in self.observation_rows if date_from <= row["research_date"] <= date_to]
        rows = self._page(rows, last_key, limit, ("research_date", "ts_code"))
        return [{column: row[column] for column in columns} for row in rows]

    def load_dividends(self, dataset_id, context):
        return []

    def load_rules(self, dataset_id, context):
        return []

    def member_roles_and_snapshots(self, dataset_id, context):
        return ({day: "PRIMARY" for day in CALENDAR}, {day: f"snapshot-{day}" for day in CALENDAR})

    def load_benchmark_closes(self, dataset_id, benchmark_code, context):
        return {day: 100.0 + index for index, day in enumerate(CALENDAR)}


def _stream(repository, *, page_size=2, bar_cache_days=2, observation_cache_days=1):
    return StreamingFrozenDataset(
        dataset_id="dataset-1",
        repository=repository,
        context=object(),
        manifest={
            "input_manifest_hash": "a" * 64,
            "benchmark_code": "000300.SH",
        },
        calendar=CALENDAR,
        evaluation_start=DAY1,
        evaluation_end=DAY3,
        warmup_start=DAY1,
        out_of_sample_start=DAY1,
        observation_columns=("raw_close", "delist_date"),
        page_size=page_size,
        bar_cache_days=bar_cache_days,
        observation_cache_days=observation_cache_days,
    )


def test_streaming_reader_fetches_each_day_once_and_evicts_bounded_cache():
    repository = _Repository()
    dataset = _stream(repository, page_size=2, bar_cache_days=2)

    assert dataset.bar(DAY1, "A").raw_close == 10.0
    assert dataset.bar(DAY1, "C").raw_close == 10.0
    assert len(repository.bar_calls) == 2  # three rows, page size two
    assert dataset.bar(DAY1, "B").raw_close == 10.0
    assert len(repository.bar_calls) == 2

    dataset.bar(DAY2, "A")
    dataset.bar(DAY3, "A")
    assert len(dataset._bar_cache) == 2
    dataset.bar(DAY1, "A")
    assert len(repository.bar_calls) == 8  # three pages for each of the two cold dates plus DAY1 again


def test_streaming_reader_observations_are_bounded_and_update_delist_index():
    repository = _Repository()
    repository.observation_rows[0]["delist_date"] = DAY3
    dataset = _stream(repository, page_size=2, observation_cache_days=1)

    rows = dataset.observations_for(DAY1)
    assert [row.ts_code for row in rows] == ["A", "B", "C"]
    assert dataset.delist_date_for("A") == DAY3
    first_call_count = len(repository.observation_calls)
    dataset.observations_for(DAY1)
    assert len(repository.observation_calls) == first_call_count

    dataset.observations_for(DAY2)
    dataset.observations_for(DAY1)
    assert len(repository.observation_calls) == first_call_count + 4


def test_streaming_reader_matches_materialized_engine_for_noop_strategy():
    repository = _Repository()
    stream = _stream(repository, page_size=2)
    bars = {
        day: {
            code: MarketBar(**row)
            for code in ("A", "B", "C")
            for row in repository.bar_rows
            if row["trade_date"] == day and row["ts_code"] == code
        }
        for day in CALENDAR
    }
    observations = {
        day: tuple(
            row for row in stream.observations_for(day)
        )
        for day in CALENDAR
    }
    # The previous calls intentionally exercised the cache; the engine test
    # uses a fresh reader so it verifies the same cold-start path as production.
    materialized = BacktestDataset(
        dataset_id="dataset-1", calendar=CALENDAR,
        evaluation_start=DAY1, evaluation_end=DAY3, warmup_start=DAY1,
        observations=observations, bars=bars,
        member_roles=stream.member_roles, snapshot_ids=stream.snapshot_ids,
        benchmark_code="000300.SH", benchmark_closes=stream.benchmark_closes,
        input_hash="a" * 64, require_explicit_rules=True,
    )

    class NoOpStrategy:
        strategy_id = "noop"
        strategy_version = "1"

        @staticmethod
        def on_day(trade_date, observations, dataset):
            # Exercise the same access pattern as a stateful strategy: the
            # engine may request one bar per observation, but the adapter must
            # still issue only one bounded day load.
            for observation in observations:
                dataset.bar(trade_date, observation.ts_code)

        @staticmethod
        def generate_signals(trade_date, observations, positions):
            return []

    strategy = NoOpStrategy()
    materialized_result = BacktestEngine(BacktestConfig()).run(materialized, strategy, run_id="same")
    fresh_repository = _Repository()
    streaming_result = BacktestEngine(BacktestConfig()).run(
        _stream(fresh_repository, page_size=2), strategy, run_id="same"
    )
    assert streaming_result.metrics == materialized_result.metrics
    assert streaming_result.performance_daily == materialized_result.performance_daily
    assert len(fresh_repository.bar_calls) == 6
    assert len(fresh_repository.observation_calls) == 6


def test_streaming_reader_close_is_idempotent_and_blocks_future_reads():
    repository = _Repository()
    dataset = _stream(repository)
    dataset.close()
    dataset.close()
    assert repository.kernel.close_calls == 1
    with pytest.raises(RuntimeError, match="already been closed"):
        dataset.bar(DAY1, "A")
