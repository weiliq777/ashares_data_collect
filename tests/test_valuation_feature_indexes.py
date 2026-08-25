from pathlib import Path


def test_valuation_feature_date_indexes_are_concurrent_and_bounded():
    sql = (
        Path(__file__).parents[1] / "sql" / "062_valuation_feature_date_indexes.sql"
    ).read_text(encoding="utf-8").lower()
    assert "create index concurrently if not exists idx_valuation_daily_v1_date" in sql
    assert "on feature.valuation_daily_v1 (trade_date, ts_code)" in sql
    assert "idx_valuation_daily_v1_quality_window" in sql
