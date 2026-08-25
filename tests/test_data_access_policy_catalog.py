"""DatasetPolicy 通配匹配 + 默认注册表测试。"""
from __future__ import annotations

from datetime import date

import pytest

from data_collect.data_access.dataset_policy import ScanPolicy, WriteMode
from data_collect.data_access.policy_catalog import build_default_registry


def test_wildcard_raw_matches_specific_table():
    r = build_default_registry()
    p = r.require("raw.tushare_daily")
    assert p.allows_write(WriteMode.APPEND_VERSIONED)
    assert not p.allows_write(WriteMode.UPSERT)


def test_public_snapshot_raw_input_has_an_explicit_policy_alias():
    registry = build_default_registry()
    policy = registry.require("public.tushare_stock_basic_raw_version")
    assert policy.primary_key == ("business_key", "payload_hash")
    assert policy.require_scope is True


def test_wildcard_feature_allows_upsert_and_rebuild():
    r = build_default_registry()
    p = r.require("feature.price_daily_v2")
    assert p.allows_write(WriteMode.UPSERT)
    assert p.allows_write(WriteMode.REBUILD)
    assert p.scan_policy == ScanPolicy.SCOPE_REQUIRED
    assert p.require_scope is True
    assert p.direct_read_allowed is False


def test_wildcard_contract_is_publish_only_and_immutable_after_freeze():
    r = build_default_registry()
    p = r.require("contract.stock_snapshot_daily")
    assert p.allows_write(WriteMode.PUBLISH_ONLY)
    assert not p.allows_write(WriteMode.UPSERT)
    assert p.immutable_after_freeze is True


def test_stage1_contract_metadata_is_publish_only_without_snapshot_id():
    r = build_default_registry()
    dictionary = r.require("contract.data_dictionary")
    assert dictionary.allows_write(WriteMode.PUBLISH_ONLY)
    assert dictionary.require_snapshot_id is False
    assert dictionary.update_allowed is False
    assert dictionary.delete_allowed is False


def test_stage1_application_parameter_tables_are_append_only():
    r = build_default_registry()
    for name in ("backtest.app_strategy_version", "backtest.app_execution_params"):
        policy = r.require(name)
        assert policy.allows_write(WriteMode.BULK_INSERT)
        assert policy.update_allowed is False
        assert policy.delete_allowed is False


def test_wildcard_ops_allows_full_scan():
    r = build_default_registry()
    p = r.require("ops.source_registry")
    assert p.scan_policy == ScanPolicy.FULL_SCAN_ALLOWED


def test_unknown_schema_still_fail_closed():
    from data_collect.data_access.errors import PolicyViolation

    r = build_default_registry()
    with pytest.raises(PolicyViolation):
        r.require("unknown_schema.tbl")


def test_unregistered_table_in_known_schema_cannot_inherit_write_policy():
    from data_collect.data_access import DataAccessKernel, WorkScope
    from data_collect.data_access.context import Operation, SqlPlan
    from data_collect.data_access.errors import PolicyViolation

    r = build_default_registry()
    kernel = DataAccessKernel(policy_registry=r, connection_factory=lambda: None)
    plan = SqlPlan(
        operation=Operation.UPSERT,
        target="feature.unregistered_table",
        scope=WorkScope(date_from=date(2026, 1, 1)),
    )
    with pytest.raises(PolicyViolation):
        kernel.query_guard.check(plan)


@pytest.mark.parametrize(
    "name",
    (
        "backtest.signal", "backtest.fill", "backtest.metric",
        "backtest.position_daily", "backtest.position_lot",
        "backtest.cash_ledger", "backtest.performance_daily",
        "feature.company_financial_v2", "feature.valuation_daily_v1",
        "foundation.tax_rule_effective",
    ),
)
def test_all_migrated_write_targets_have_explicit_policies(name):
    registry = build_default_registry()
    assert registry.is_explicit(name)
    assert registry.require(name).write_modes


def test_feature_policy_partition_keys_match_domain_grain():
    registry = build_default_registry()
    assert registry.require("feature.company_financial_v1").partition_key == ("report_date",)
    assert registry.require("feature.company_financial_v2").partition_key == ("report_date",)
    assert registry.require("feature.shareholder_return_v1").partition_key == ("asof_date",)


def test_legacy_stock_basic_raw_is_append_only():
    registry = build_default_registry()
    policy = registry.require("tushare_stock_basic_raw")
    assert policy.write_modes == frozenset([WriteMode.BULK_INSERT])
    assert policy.update_allowed is False
    assert policy.delete_allowed is False
