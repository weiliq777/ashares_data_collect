from __future__ import annotations

from pathlib import Path


def test_stage1_migration_is_additive_and_contains_required_contracts():
    sql = (Path(__file__).parents[1] / "sql" / "049_data_foundation_stage1_contracts.sql").read_text(encoding="utf-8")
    lowered = sql.lower()
    for required in (
        "contract.contract_version",
        "contract.data_dictionary",
        "backtest.app_strategy_version",
        "backtest.app_execution_params",
        "ops.trading_day_build",
        "reject_immutable_metadata_change",
    ):
        assert required in lowered
    assert "drop table" not in lowered
    assert "delete from" not in lowered
