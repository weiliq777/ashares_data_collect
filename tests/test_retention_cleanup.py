from pathlib import Path


def test_safe_retention_cleanup_only_drops_generated_smoke_tables():
    sql = Path("sql/099_safe_retention_cleanup_20260907.sql").read_text(encoding="utf-8").lower()
    assert "left join std.daily_kline" in sql
    assert "drop table if exists feature.company_financial_v2_smoke_" in sql
    assert "drop table if exists feature.valuation_daily_v1_smoke_" in sql
    for protected in (
        "research.snapshot_lineage",
        "research.stock_snapshot_daily",
        "research.lineage_source_record",
        "public.tushare_daily_raw",
        "public.tushare_daily_basic_raw",
        "public._legacy_daily_kline",
        "public.tushare_daily_raw_corrupt",
    ):
        assert f"drop table if exists {protected}" not in sql
