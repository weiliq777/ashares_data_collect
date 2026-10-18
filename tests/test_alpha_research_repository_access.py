from datetime import date

import pytest

from data_collect.data_access.context import RunContext
from data_collect.data_access.repository.dataset_repository import DatasetRepository


class _ReadGateway:
    def __init__(self):
        self.calls = []

    def _repository_query(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return []


class _Kernel:
    def __init__(self):
        self.read = _ReadGateway()


def _context():
    return RunContext(
        run_id="run", job_name="test", code_version="test",
        config_hash="test", pipeline_version="test",
    )


def _repository():
    kernel = _Kernel()
    repository = DatasetRepository(kernel)
    repository._component_cache["dataset-a"] = []
    repository._dataset_kind_cache["dataset-a"] = "MATERIALIZED"
    return repository, kernel


def test_observation_projection_rejects_unapproved_identifiers():
    repository, kernel = _repository()
    with pytest.raises(ValueError, match="unsupported frozen observation columns"):
        repository.observation_page(
            "dataset-a", ("raw_close); DROP TABLE research.stock_snapshot_daily; --",),
            _context(),
        )
    assert kernel.read.calls == []


def test_repository_allowlist_covers_the_official_backtest_projection():
    from data_collect.jobs.backtest_dataset import SELECTED_COLUMNS

    assert set(SELECTED_COLUMNS["research"]) <= DatasetRepository.OBSERVATION_COLUMNS
    assert {
        "return_20d", "return_60d", "price_feature_version",
        "price_adjustment_mode", "total_mv", "financial_vintage_quality",
    } <= set(SELECTED_COLUMNS["research"])


def test_bar_pages_include_quality_and_source_lineage_for_label_audit():
    repository, kernel = _repository()
    repository.bar_page(
        "dataset-a", _context(), date_from=date(2024, 1, 2), date_to=date(2024, 1, 3),
    )
    sql = kernel.read.calls[0][0][2]
    assert "quality_status" in sql
    assert "source_record_key" in sql
    assert "source_payload_hash" in sql


def test_dividend_page_uses_bounded_keyset_order():
    repository, kernel = _repository()
    repository.dividend_page(
        "dataset-a", _context(),
        last_key=(date(2024, 1, 2), "000001.SZ", "event-1", 2),
        limit=100, date_from=date(2024, 1, 1), date_to=date(2024, 2, 1),
    )
    args, _kwargs = kernel.read.calls[0]
    sql, params = args[2], args[3]
    assert "(d.ex_date, d.ts_code, COALESCE(d.source_record_key, '')," in sql
    assert "ORDER BY d.ex_date, d.ts_code, COALESCE(d.source_record_key, '')" in sql
    assert params == (
        "dataset-a", date(2024, 1, 1), date(2024, 2, 1),
        date(2024, 1, 2), "000001.SZ", "event-1", 2,
    )
    assert args[5] == 100
