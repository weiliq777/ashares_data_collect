import inspect

from data_collect.jobs.tushare_phase2_features import rebuild_price_v2


def test_price_v2_replays_only_effective_company_actions():
    source = inspect.getsource(rebuild_price_v2)
    assert "implementation_status = '实施'" in source
    assert "e.ex_date = d.trade_date" in source
    assert "point_in_time_total_return" in source
    assert "FROM adj_factor_daily" not in source


def test_price_v2_keeps_raw_execution_coordinate_separate():
    source = inspect.getsource(rebuild_price_v2)
    assert "raw_close" in source
    assert "asof_close" in source
    assert "feature.price_daily_v2" in source
