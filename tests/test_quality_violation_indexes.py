from pathlib import Path


def test_large_quality_invariants_use_concurrent_partial_indexes():
    sql = Path("sql/088_quality_violation_partial_indexes.sql").read_text(
        encoding="utf-8"
    ).lower()

    assert sql.count("create index concurrently if not exists") == 3
    assert "idx_price_daily_v2_immature_violation" in sql
    assert "idx_valuation_daily_v1_immature_violation" in sql
    assert "idx_financial_asof_future_violation" in sql
    assert sql.count(" where ") == 3
    assert "ma20_observation_count" in sql
    assert "pe_observation_count_5y" in sql
    assert "effective_announce_date > decision_date" in sql
