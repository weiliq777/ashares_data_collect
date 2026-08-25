from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from data_collect.data_access.dataset_policy import WriteMode
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.features.technical_daily import calculate_technical_features, to_feature_rows
from data_collect.jobs.backfill_technical_daily import bounded_windows, month_windows, stock_batches


def _prices(rows: int = 30) -> pd.DataFrame:
    start = date(2026, 1, 1)
    return pd.DataFrame(
        {
            "stock_code": ["000001.SZ"] * rows,
            "trade_date": [start + timedelta(days=index) for index in range(rows)],
            "open": np.linspace(10, 12, rows),
            "high": np.linspace(10.5, 12.5, rows),
            "low": np.linspace(9.5, 11.5, rows),
            "close": np.linspace(10.2, 12.2, rows),
            "volume": [1000] * rows,
            "amount": [10000.0] * rows,
        }
    )


def test_technical_features_preserve_indicator_contract_and_history_gate():
    result = calculate_technical_features(_prices(), pd.DataFrame())

    assert {"wr10", "wr20", "rsi9", "k", "d", "j"} <= set(result.columns)
    assert result.iloc[:19]["quality_status"].eq("INSUFFICIENT_HISTORY").all()
    assert result.iloc[20:]["quality_status"].eq("VALID").all()
    assert result["feature_contract_version"].eq("technical_daily_v1").all()
    assert len(to_feature_rows(result)) == len(result)


def test_wave3_reads_feature_output_instead_of_calculating_indicators():
    source = Path("data_collect/jobs/wave3.py").read_text(encoding="utf-8")
    assert "talib" not in source
    assert "_calculate_indicators" not in source
    assert "technical_features" in source


def test_technical_feature_policy_is_scoped_and_upsertable():
    policy = build_default_registry().require("feature.technical_daily_v1")
    assert policy.primary_key == ("stock_code", "trade_date")
    assert policy.require_scope
    assert policy.allows_write(WriteMode.UPSERT)


def test_technical_backfill_is_monthly_and_resumable_by_design():
    windows = month_windows(date(2026, 1, 15), date(2026, 3, 2))
    assert windows == [
        (date(2026, 1, 15), date(2026, 1, 31)),
        (date(2026, 2, 1), date(2026, 2, 28)),
        (date(2026, 3, 1), date(2026, 3, 2)),
    ]
    assert bounded_windows(date(2026, 1, 1), date(2026, 1, 15), max_days=7) == [
        (date(2026, 1, 1), date(2026, 1, 7)),
        (date(2026, 1, 8), date(2026, 1, 14)),
        (date(2026, 1, 15), date(2026, 1, 15)),
    ]
    source = Path("data_collect/jobs/backfill_technical_daily.py").read_text(encoding="utf-8")
    assert "kernel.checkpoint.load" in source
    assert "kernel.checkpoint.mark_done" in source
    assert "upsert_technical_features" in source
    assert list(stock_batches([str(index) for index in range(501)], 250)) == [
        [str(index) for index in range(250)],
        [str(index) for index in range(250, 500)],
        ["500"],
    ]
    assert "daily_kline_for_codes" in source
    assert "STOCK_BATCH_SIZE = 250" in Path("data_collect/jobs/technical_daily.py").read_text(encoding="utf-8")


def test_wave3_dividend_read_stays_within_repository_policy_ceiling():
    source = Path("data_collect/data_access/repository/wave3_repository.py").read_text(encoding="utf-8")
    dividend_block = source.split("def divid_factors", 1)[1].split("def stock_summary", 1)[0]
    assert "100_000," in dividend_block
    assert "500_000," not in dividend_block


def test_dividend_factor_schema_repair_is_additive():
    sql = Path("sql/056_repair_divid_factors_schema.sql").read_text(encoding="utf-8").lower()
    assert "add column if not exists dr" in sql
    assert "drop" not in sql
    assert "truncate" not in sql
