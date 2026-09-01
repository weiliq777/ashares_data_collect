from pathlib import Path


def test_price_v2_replays_only_effective_company_actions():
    from data_collect.data_access.repository.feature_repository import _PRICE_V2_ADJUSTMENT_SQL

    source = _PRICE_V2_ADJUSTMENT_SQL
    assert "implementation_status = '实施'" in source
    assert "e.ex_date = d.trade_date" in source
    assert "point_in_time_total_return" in source
    assert "FROM adj_factor_daily" not in source


def test_price_v2_binds_each_set_operation_to_one_target():
    from data_collect.data_access.repository.feature_repository import (
        _PRICE_V2_ADJUSTMENT_SQL,
        _PRICE_V2_FEATURE_SQL,
    )

    assert "feature.price_daily_v2" not in _PRICE_V2_ADJUSTMENT_SQL
    assert "DELETE FROM feature.price_adjustment_factor_asof" not in _PRICE_V2_FEATURE_SQL
    assert "INSERT INTO feature.price_adjustment_factor_asof" not in _PRICE_V2_FEATURE_SQL


def test_price_v2_keeps_raw_execution_coordinate_separate():
    source = Path("data_collect/data_access/repository/feature_repository.py").read_text(encoding="utf-8")
    assert "raw_close" in source
    assert "asof_close" in source
    assert "feature.price_daily_v2" in source


def test_bounded_price_v2_does_not_double_count_warmup_rows():
    from data_collect.data_access.repository.feature_repository import _PRICE_V2_FEATURE_BOUNDED_SQL

    assert "COUNT(asof_close) FILTER (WHERE trade_date >= %s::date)" in _PRICE_V2_FEATURE_BOUNDED_SQL
