"""验证 tushare_phase2_standard.py 的迁移：三个重建函数已是薄包装，
走 Repository → Kernel，不再含手写 SQL。"""
from __future__ import annotations

from datetime import date


def test_scoped_rebuild_dispatches_to_repository(monkeypatch):
    import data_collect.jobs.tushare_phase2_standard as m

    calls = {}

    class FakeSecRepo:
        def rebuild_status_daily(self, scope, ctx):
            calls["status"] = (scope.date_from, scope.date_to)
            return 1

    class FakePitRepo:
        def rebuild_universe(self, scope, ctx):
            calls["universe"] = (scope.date_from, scope.date_to)
            return 2

        def rebuild_tradeability(self, scope, ctx):
            calls["tradeability"] = (scope.date_from, scope.date_to)
            return 3

    class FakeMarketRepo:
        def rebuild_index_daily(self, ctx):
            calls["index_daily"] = True
            return 4

    class FakeKernel:
        def close(self):
            pass

    monkeypatch.setattr(
        m, "_repositories",
        lambda: (FakeKernel(), FakeSecRepo(), FakePitRepo(), FakeMarketRepo()),
    )

    bounds = (date(2026, 1, 1), date(2026, 1, 31))
    assert m._scoped_rebuild(bounds, "status") == 1
    assert m._scoped_rebuild(bounds, "universe") == 2
    assert m._scoped_rebuild(bounds, "tradeability") == 3
    assert set(calls) == {"status", "universe", "tradeability"}
    assert calls["status"] == (date(2026, 1, 1), date(2026, 1, 31))


def test_rebuild_functions_are_thin_wrappers():
    """迁移后三个函数体不再内联 SQL，而是调用 _scoped_rebuild。"""
    import inspect

    import data_collect.jobs.tushare_phase2_standard as m

    for fn_name in ("rebuild_status_daily", "rebuild_universe", "rebuild_tradeability"):
        src = inspect.getsource(getattr(m, fn_name))
        assert "INSERT INTO" not in src, f"{fn_name} 仍内联 SQL"
        assert "_scoped_rebuild" in src, f"{fn_name} 未改为薄包装"
