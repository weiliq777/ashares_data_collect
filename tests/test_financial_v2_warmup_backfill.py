from datetime import date
from pathlib import Path

from data_collect.jobs.backfill_company_financial_v2_warmup import (
    DEFAULT_OUTPUT_END,
    DEFAULT_WARMUP_START,
)


def test_financial_v2_warmup_covers_first_research_date_inputs():
    assert DEFAULT_WARMUP_START == date(2016, 8, 1)
    assert DEFAULT_OUTPUT_END == date(2021, 8, 1)
    assert DEFAULT_OUTPUT_END < date(2021, 8, 2)


def test_warmup_backfill_uses_bounded_feature_partitions_only():
    source = Path(
        "data_collect/jobs/backfill_company_financial_v2_warmup.py"
    ).read_text(encoding="utf-8")
    assert "rebuild_company_financial_v2" in source
    assert 'target_table="feature.company_financial_v2"' in source
    assert "batch_stocks=batch_stocks" in source
    assert "run_raw" not in source


def test_financial_version_backfill_preserves_raw_observation_time():
    source = Path(
        "data_collect/data_access/repository/financial_repository.py"
    ).read_text(encoding="utf-8")
    assert "payload, fetched_at FROM" in source
    assert 'record["_fetched_at"] = row["fetched_at"]' in source
