from data_collect.data_access.repository.quality_inspection_repository import (
    QualityInspectionRepository,
)
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.jobs.tushare_data_quality import CHECKS
from data_collect.jobs.backtest_dataset import create_dataset
from pathlib import Path
from datetime import date, timedelta
from types import SimpleNamespace
import pytest


def test_quality_repository_maps_legacy_market_labels_to_canonical_standard_tables():
    assert QualityInspectionRepository.CANONICAL_TABLES == {
        "daily_kline": "std.daily_kline",
        "valuation_daily": "std.valuation_daily",
        "adj_factor_daily": "std.adj_factor_daily",
    }


def test_quality_inventory_uses_canonical_market_relations():
    assert "std.daily_kline" in CHECKS
    assert "std.valuation_daily" in CHECKS
    assert "std.adj_factor_daily" in CHECKS
    assert "daily_kline" not in CHECKS
    assert "valuation_daily" not in CHECKS
    assert "adj_factor_daily" not in CHECKS


def test_dataset_calendar_status_batches_large_boundary_sets():
    class FakeRead:
        def __init__(self):
            self.calls = []

        def _repository_query(self, context, source, sql, params, scope, max_rows):
            self.calls.append((source, params, scope.partition, max_rows))
            return [
                {"trade_date": day, "min_is_open": True, "max_is_open": True}
                for day in params[0]
            ]

    reader = FakeRead()
    repository = DatasetRepository(SimpleNamespace(read=reader))
    start = date(2021, 1, 1)
    days = [start + timedelta(days=index) for index in range(205)]

    result = repository.calendar_status(days, object())

    assert len(result) == len(days)
    assert len(reader.calls) == 3
    assert all(call[3] <= 100 for call in reader.calls)
    assert [call[2] for call in reader.calls] == [
        "calendar:boundary:0", "calendar:boundary:1", "calendar:boundary:2"
    ]


def test_dataset_calendar_status_rejects_conflicting_source_values():
    class ConflictingRead:
        def _repository_query(self, *args, **kwargs):
            return [{
                "trade_date": date(2026, 1, 2),
                "min_is_open": False,
                "max_is_open": True,
            }]

    repository = DatasetRepository(SimpleNamespace(read=ConflictingRead()))
    with pytest.raises(RuntimeError, match="conflicting is_open"):
        repository.calendar_status([date(2026, 1, 2)], object())


def test_dataset_version_length_is_rejected_before_database_write():
    with pytest.raises(ValueError, match="dataset_version"):
        create_dataset(
            snapshot_ids=["snapshot-id"],
            dataset_name="valid-name",
            dataset_version="x" * 41,
            baseline_tag="baseline",
            warmup_start=date(2026, 1, 1),
            evaluation_start=date(2026, 1, 1),
            evaluation_end=date(2026, 1, 1),
        )


def test_dataset_release_trigger_repair_uses_canonical_ops_relation():
    sql = Path("sql/093_repair_dataset_release_schema_qualification.sql").read_text(
        encoding="utf-8"
    ).lower()
    assert sql.count("from ops.tushare_data_release") == 2
    assert "from tushare_data_release" not in sql
