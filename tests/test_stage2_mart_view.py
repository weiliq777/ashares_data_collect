from __future__ import annotations

from pathlib import Path


def test_mart_view_migration_is_read_only_and_bounded_by_contract():
    sql = (Path(__file__).parents[1] / "sql" / "050_mart_contract_views.sql").read_text(encoding="utf-8")
    lowered = sql.lower()
    assert "create or replace view mart.v_price_feature" in lowered
    assert "feature.price_daily_v2" in lowered
    assert "feature.valuation_daily_v1" in lowered
    assert "std.daily_kline" in lowered
    assert "amount_ma20" in lowered
    assert "grant select on mart.v_price_feature to ashares_app" in lowered
    assert "insert into" not in lowered
    assert "update " not in lowered
    assert "delete from" not in lowered
    assert "drop table" not in lowered


def test_mart_view_policy_is_read_only_and_requires_scope():
    from data_collect.data_access.policy_catalog import build_default_registry

    policy = build_default_registry().require("mart.v_price_feature")
    assert not policy.write_modes
    assert policy.require_scope is True
