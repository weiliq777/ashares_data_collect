from datetime import date, timedelta
import hashlib
import json

from data_collect.jobs.research_chain_readiness import (
    _compact_run_diagnostic,
    _dataset_rule_alignment,
    _dataset_quality_status,
    _month_windows,
    _execution_evidence,
    _pit_temporal_visibility_status,
    _run_parameter_summary,
    _run_provenance_checks,
    _selected_column_summary,
    _selection_contract_status,
    _quality_report_evidence_status,
    summarize_horizon_coverage,
)


def _execution_dataset_rules():
    return [{
        "rule_version": None,
        "market_rule_version": None,
        "fee_rule_version": None,
        "effective_from": date(2025, 1, 1),
        "effective_to": None,
        "market": "ALL",
        "rule_hash": "rule-hash",
        "min_buy_lot": 100,
        "allow_sell_odd_lot": True,
        "t_plus_one": True,
        "price_tick": 0.01,
        "cash_settlement_days": 0,
        "limit_pct": None,
        "commission_rate": 0.0003,
        "min_commission": 5,
        "stamp_duty_sell_rate": 0.0005,
        "transfer_fee_rate": 0.00001,
        "slippage_bps": 10,
        "parameters": {
            "max_volume_participation_pct": 0.05,
            "liquidity_policy": "reject_full_order",
        },
    }]


def _run_snapshot_rule(dataset_rule):
    return {
        key: value for key, value in dataset_rule.items()
        if key != "rule_hash"
    }


def test_month_windows_cover_exact_requested_date_range():
    assert _month_windows(date(2024, 1, 15), date(2024, 3, 2)) == [
        (date(2024, 1, 15), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 29)),
        (date(2024, 3, 1), date(2024, 3, 2)),
    ]


def test_horizon_coverage_counts_only_member_dates_with_full_future_calendar():
    start = date(2024, 1, 1)
    trading_dates = [start + timedelta(days=index) for index in range(70)]
    member_dates = [trading_dates[index] for index in (0, 10, 20, 30, 40, 50, 60)]

    result = summarize_horizon_coverage(
        member_dates, trading_dates, (20, 40, 60),
        out_of_sample_start=trading_dates[20],
    )

    assert result["all_period"]["20d"]["mature_member_date_count"] == 5
    assert result["all_period"]["40d"]["mature_member_date_count"] == 3
    assert result["all_period"]["60d"]["mature_member_date_count"] == 1
    assert result["out_of_sample"]["mature_horizons"]["20d"]["mature_member_date_count"] == 3
    assert result["out_of_sample"]["mature_horizons"]["60d"]["mature_member_date_count"] == 0


def test_run_parameter_summary_reports_persisted_scenario_constraints():
    result = _run_parameter_summary({
        "parameters": {
            "max_positions": 3,
            "reserve_cash_pct": 0.1,
            "scenario": {
                "initial_capital": 20_000,
                "target_positions": 3,
                "forced_cash_reserve_ratio": 0.1,
            },
        },
    })

    assert result["max_positions"] == 3
    assert result["reserve_cash_pct"] == 0.1
    assert result["scenario"] == {
        "initial_capital": 20_000,
        "target_positions": 3,
        "forced_cash_reserve_ratio": 0.1,
    }


def test_compact_diagnostic_keeps_60_day_actual_entry_summary():
    result = _compact_run_diagnostic({
        "scope": {"evaluation_date_from": "2024-01-01"},
        "equity_curve": [
            {"nav": 1.0, "benchmark_nav": 1.0, "drawdown": 0.0},
            {"nav": 1.1, "benchmark_nav": 1.05, "drawdown": -0.02},
        ],
        "trade_summary": {"trade_count": 2},
        "forward_returns": {
            "horizon_60d": {"available_count": 2, "returns": {"mean": 0.1}},
        },
    })

    assert result["status"] == "FAIL"
    assert result["performance_day_count"] == 2
    assert result["maximum_drawdown"] == -0.02
    assert result["nav_implied_maximum_drawdown"] == 0.0
    assert result["drawdown_consistency"] == "FAIL"
    assert result["forward_returns"]["horizon_60d"]["available_count"] == 2
    assert "真实买入成交" in result["forward_return_scope"]


def test_compact_diagnostic_does_not_pass_without_drawdown_evidence():
    result = _compact_run_diagnostic({"equity_curve": []})

    assert result["status"] == "NOT RUN"
    assert result["drawdown_consistency"] == "NOT RUN"


def test_compact_diagnostic_normalizes_positive_drawdown_magnitudes():
    result = _compact_run_diagnostic({
        "equity_curve": [
            {"nav": 1.0, "drawdown": 0.0},
            {"nav": 0.9, "drawdown": 0.1},
        ],
    })

    assert result["maximum_drawdown"] == -0.1
    assert result["persisted_drawdown_representation"] == "positive_magnitude"
    assert result["drawdown_consistency"] == "PASS"


def test_selected_columns_counts_mapping_jsonb_fields():
    selected, count = _selected_column_summary({
        "research": ["research_date", "ts_code"],
        "execution": ["trade_date", "raw_close"],
    })

    assert selected["research"] == ["research_date", "ts_code"]
    assert count == 4


def test_selection_contract_requires_predicate_and_columns():
    assert _selection_contract_status("research_date >= %s", 4) == "PASS"
    assert _selection_contract_status("research_date >= %s", 0) == "FAIL"
    assert _selection_contract_status(None, 4) == "FAIL"


def test_dataset_quality_report_and_full_counts_are_reported_separately():
    report = {"status": "VALID", "issues": []}
    assert _quality_report_evidence_status(report, "quality-hash") == "PASS"
    assert _quality_report_evidence_status(report, None) == "FAIL"
    assert _quality_report_evidence_status({"status": "FAILED", "issues": []}, "quality-hash") == "FAIL"
    assert _dataset_quality_status({"missing_execution_rows": 0}) == "PASS"
    assert _dataset_quality_status({"missing_execution_rows": 1}) == "FAIL"
    assert _dataset_quality_status(None) == "NOT RUN"


def test_run_provenance_requires_matching_dataset_hash_and_code_identity():
    parameters = {"risk_pct": 0.01}
    parameters_hash = hashlib.sha256(
        json.dumps(parameters, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    base = {
        "input_hash": "dataset-hash",
        "parameters": parameters,
        "parameters_hash": parameters_hash,
        "execution_model_version": "execution-v1",
        "git_dirty": False,
        "git_commit": "commit-sha",
    }

    assert all(_run_provenance_checks(base, "dataset-hash").values())
    assert not _run_provenance_checks(base, "other-dataset-hash")["dataset_input_hash_matches"]
    mismatched_parameters = dict(base, parameters_hash="wrong-hash")
    assert not _run_provenance_checks(mismatched_parameters, "dataset-hash")[
        "parameters_hash_matches"
    ]
    dirty_without_archive = dict(base, git_dirty=True, git_commit="commit-sha")
    assert not _run_provenance_checks(dirty_without_archive, "dataset-hash")["code_identity_present"]
    dirty_with_archive = dict(dirty_without_archive, code_archive_sha256="archive-sha")
    assert _run_provenance_checks(dirty_with_archive, "dataset-hash")["code_identity_present"]


def test_pit_temporal_visibility_is_explicitly_bounded():
    assert _pit_temporal_visibility_status({}, True, True) == "PASS"
    assert _pit_temporal_visibility_status(
        {"future_financial_availability_rows": 1}, True, True,
    ) == "FAIL"
    assert _pit_temporal_visibility_status({}, False, True) == "NOT RUN"
    assert _pit_temporal_visibility_status(
        {"missing_snapshot_rows": 1}, True, True,
    ) == "NOT RUN"
    for identity_issue in (
        "missing_lineage_rows", "non_v2_source_rows", "invalid_v2_rows",
    ):
        assert _pit_temporal_visibility_status(
            {identity_issue: 1}, True, True,
        ) == "NOT RUN"


def test_execution_evidence_requires_constraints_and_a_real_fill():
    run = {
        "status": "SUCCEEDED",
        "initial_cash": 10_000,
        "execution_model_version": "daily-next-open-v4",
        "runtime_constraints": {
            "max_positions": 3,
            "max_single_position_pct": 0.35,
            "target_position_ratio": 0.30,
            "max_entry_position_ratio": 0.35,
            "reserve_cash_pct": 0.0,
            "allocation_mode": "target_value",
            "entry_slippage_bps": 10.0,
            "exit_slippage_bps": 10.0,
            "fee_rule": {
                "source": "backtest.dataset_rule_snapshot",
                "commission_rate": 0.0003,
                "min_commission": 5,
                "stamp_duty_sell_rate": 0.0005,
                "transfer_fee_rate": 0.00001,
                "slippage_bps": 10,
                "effective_slippage_bps": [10.0],
            },
            "market_rule": {
                "source": "backtest.dataset_rule_snapshot",
                "min_buy_lot": 100,
                "allow_sell_odd_lot": True,
                "t_plus_one": True,
                "price_tick": 0.01,
                "cash_settlement_days": 0,
                "max_volume_participation_pct": 0.05,
                "effective_liquidity": [0.05],
                "liquidity_policy": ["reject_full_order"],
            },
            "dataset_rule_snapshot": {
                "source": "backtest.dataset_rule_snapshot",
                "rule_count": 1,
                "rule_hashes": ["rule-hash"],
                "effective_rules": [{
                    "effective_from": "2025-01-01",
                    "effective_to": None,
                    "market": "ALL",
                    "min_buy_lot": 100,
                    "allow_sell_odd_lot": True,
                    "t_plus_one": True,
                    "price_tick": 0.01,
                    "cash_settlement_days": 0,
                    "commission_rate": 0.0003,
                    "min_commission": 5,
                    "stamp_duty_sell_rate": 0.0005,
                    "transfer_fee_rate": 0.00001,
                    "slippage_bps": 10,
                    "parameters": {
                        "max_volume_participation_pct": 0.05,
                        "liquidity_policy": "reject_full_order",
                    },
                }],
            },
            "scenario": {
                "initial_capital": 10_000,
                "target_positions": 3,
                "target_position_ratio": 0.30,
                "max_entry_position_ratio": 0.35,
                "forced_cash_reserve_ratio": 0.0,
                "slippage_bps_per_side": 10.0,
                "buy_lot_size": 100,
                "signal_time": "CLOSE",
                "execution_time": "NEXT_TRADING_DAY_OPEN",
                "leftover_cash": "KEEP_AS_CASH",
                "rebalance_existing_positions": False,
                "broker_commission_rate": 0.0003,
                "broker_minimum_commission": 5,
            },
        },
        "diagnostics": {"order_rejections": {"filled_orders": 1}},
    }

    dataset_rules = _execution_dataset_rules()
    assert _execution_evidence(run, dataset_rules)["status"] == "PASS"
    zero_fill = {
        **run,
        "diagnostics": {"order_rejections": {"filled_orders": 0}},
    }
    result = _execution_evidence(zero_fill, dataset_rules)
    assert result["status"] == "FAIL"
    assert result["checks"]["at_least_one_order_filled"] is False

    invalid_position_limit = {
        **run,
        "runtime_constraints": {
            **run["runtime_constraints"],
            "max_positions": 0,
        },
    }
    assert _execution_evidence(invalid_position_limit, dataset_rules)["checks"][
        "position_count_matches_scenario"
    ] is False

    missing_scenario_cash = {
        **run,
        "runtime_constraints": {
            **run["runtime_constraints"],
            "scenario": {key: value for key, value in run["runtime_constraints"]["scenario"].items()
                         if key != "initial_capital"},
        },
    }
    assert _execution_evidence(missing_scenario_cash, dataset_rules)["checks"][
        "capital_matches_scenario"
    ] is False

    mismatched_scenario = {
        **run,
        "runtime_constraints": {
            **run["runtime_constraints"],
            "scenario": {
                **run["runtime_constraints"]["scenario"],
                "target_position_ratio": 0.25,
            },
        },
    }
    assert _execution_evidence(mismatched_scenario, dataset_rules)["checks"][
        "position_sizing_matches_scenario"
    ] is False

    mismatched_reserve = {
        **run,
        "runtime_constraints": {
            **run["runtime_constraints"],
            "reserve_cash_pct": 0.1,
        },
    }
    assert _execution_evidence(mismatched_reserve, dataset_rules)["checks"][
        "cash_reserve_matches_scenario"
    ] is False

    missing_t_plus_one = {
        **run,
        "runtime_constraints": {
            **run["runtime_constraints"],
            "market_rule": {
                "min_buy_lot": 100,
                "allow_sell_odd_lot": True,
                "price_tick": 0.01,
            },
        },
    }
    assert _execution_evidence(missing_t_plus_one, dataset_rules)["checks"][
        "market_rule_complete"
    ] is True
    missing_dataset_t_plus_one = [{
        **dataset_rules[0],
        "t_plus_one": None,
    }]
    assert _execution_evidence(run, missing_dataset_t_plus_one)["checks"][
        "market_rule_complete"
    ] is False

    risk_allocation = {
        **run,
        "runtime_constraints": {
            **run["runtime_constraints"],
            "allocation_mode": "risk_budget",
        },
    }
    assert _execution_evidence(risk_allocation, dataset_rules)["checks"][
        "allocation_mode_matches_scenario"
    ] is False

    mismatched_slippage = {
        **run,
        "runtime_constraints": {
            **run["runtime_constraints"],
            "exit_slippage_bps": 15.0,
        },
    }
    assert _execution_evidence(mismatched_slippage, dataset_rules)["checks"][
        "slippage_matches_scenario"
    ] is False

    mismatched_scenario_fee = {
        **run,
        "runtime_constraints": {
            **run["runtime_constraints"],
            "scenario": {
                **run["runtime_constraints"]["scenario"],
                "broker_commission_rate": 0.0005,
            },
        },
    }
    assert _execution_evidence(mismatched_scenario_fee, dataset_rules)["checks"][
        "fee_rule_matches_scenario"
    ] is False

    mismatched_buy_lot = {
        **run,
        "runtime_constraints": {
            **run["runtime_constraints"],
            "scenario": {
                **run["runtime_constraints"]["scenario"],
                "buy_lot_size": 200,
            },
        },
    }
    assert _execution_evidence(mismatched_buy_lot, dataset_rules)["checks"][
        "scenario_lot_matches_market_rule"
    ] is False

    mismatched_execution_contract = {
        **run,
        "runtime_constraints": {
            **run["runtime_constraints"],
            "scenario": {
                **run["runtime_constraints"]["scenario"],
                "execution_time": "SAME_DAY_CLOSE",
            },
        },
    }
    assert _execution_evidence(mismatched_execution_contract, dataset_rules)["checks"][
        "scenario_execution_contract_matches"
    ] is False

    mismatched_dataset_liquidity = {
        **run,
        "runtime_constraints": {
            **run["runtime_constraints"],
            "market_rule": {
                **run["runtime_constraints"]["market_rule"],
                "max_volume_participation_pct": 1.0,
            },
        },
    }
    result = _execution_evidence(mismatched_dataset_liquidity, dataset_rules)
    assert result["checks"]["dataset_rule_snapshot_matches_frozen_dataset"] is True

    changed_dataset_rules = [{
        **dataset_rules[0],
        "parameters": {
            **dataset_rules[0]["parameters"],
            "max_volume_participation_pct": 0.10,
        },
    }]
    mismatched_dataset_snapshot = _execution_evidence(run, changed_dataset_rules)
    assert mismatched_dataset_snapshot["checks"][
        "dataset_rule_snapshot_matches_frozen_dataset"
    ] is False


def test_rule_alignment_accepts_historical_fee_changes_but_checks_authoritative_rules():
    base_dataset_rule = _execution_dataset_rules()[0]
    frozen_rules = [
        {
            **base_dataset_rule,
            "effective_from": "2021-01-01",
            "effective_to": "2022-04-28",
            "rule_hash": "first-hash",
            "stamp_duty_sell_rate": 0.001,
            "transfer_fee_rate": 0.00002,
        },
        {
            **base_dataset_rule,
            "effective_from": "2022-04-29",
            "effective_to": None,
            "rule_hash": "second-hash",
        },
    ]
    run_rules = [_run_snapshot_rule(rule) for rule in frozen_rules]
    constraints = {
        "fee_rule": {
            "source": "backtest.dataset_rule_snapshot",
            "commission_rate": 0.0003,
            "min_commission": 5,
            "stamp_duty_sell_rate": 0.0005,
            "transfer_fee_rate": 0.00001,
            "slippage_bps": 10,
            "effective_slippage_bps": [10],
        },
        "market_rule": {
            "source": "backtest.dataset_rule_snapshot",
            "max_volume_participation_pct": 1.0,
            "effective_liquidity": [0.05],
            "liquidity_policy": ["reject_full_order"],
        },
        "dataset_rule_snapshot": {
            "source": "backtest.dataset_rule_snapshot",
            "rule_count": 2,
            "rule_hashes": ["first-hash", "second-hash"],
            "effective_rule_item_count": 2,
            "malformed_effective_rule_count": 0,
            "effective_rules": run_rules,
        },
    }
    result = _dataset_rule_alignment(constraints, frozen_rules)
    assert result["status"] == "PASS"


def test_rule_alignment_applies_dataset_defaults_and_rejects_malformed_json_values():
    frozen_rule = _execution_dataset_rules()[0]
    frozen_rule["parameters"].pop("max_volume_participation_pct")
    constraints = {
        "fee_rule": {
            "source": "backtest.dataset_rule_snapshot",
            "effective_slippage_bps": [10],
        },
        "market_rule": {
            "source": "backtest.dataset_rule_snapshot",
            # Prior Runs stored the missing value as None; new Runs store 1.0.
            "effective_liquidity": [None],
            "liquidity_policy": ["reject_full_order"],
        },
        "dataset_rule_snapshot": {
            "source": "backtest.dataset_rule_snapshot",
            "rule_count": 1,
            "rule_hashes": ["rule-hash"],
            "effective_rules": [_run_snapshot_rule(frozen_rule)],
        },
    }
    assert _dataset_rule_alignment(constraints, [frozen_rule])["status"] == "PASS"
    constraints["market_rule"]["effective_liquidity"] = [1.0]
    assert _dataset_rule_alignment(constraints, [frozen_rule])["status"] == "PASS"

    malformed_rule = {
        **frozen_rule,
        "parameters": {
            **frozen_rule["parameters"],
            "max_volume_participation_pct": [0.05],
        },
    }
    malformed_constraints = {
        **constraints,
        "market_rule": {
            **constraints["market_rule"],
            "effective_liquidity": [[0.05]],
        },
        "dataset_rule_snapshot": {
            **constraints["dataset_rule_snapshot"],
            "effective_rules": [_run_snapshot_rule(malformed_rule)],
        },
    }
    malformed_result = _dataset_rule_alignment(
        malformed_constraints, [malformed_rule],
    )
    assert malformed_result["status"] == "FAIL"
    assert any(
        item["field"].endswith("max_volume_participation_pct")
        for item in malformed_result["mismatches"]
    )

    huge_number = 10 ** 10_000
    extreme_rule = {
        **frozen_rule,
        "parameters": {
            **frozen_rule["parameters"],
            "max_volume_participation_pct": huge_number,
        },
    }
    extreme_summary = _run_parameter_summary({
        "parameters": {
            "dataset_rule_snapshot": {
                "source": "backtest.dataset_rule_snapshot",
                "rule_count": 1,
                "rule_hashes": ["rule-hash"],
                "effective_rules": [_run_snapshot_rule(extreme_rule)],
            },
        },
    })
    extreme_result = _dataset_rule_alignment(extreme_summary, [extreme_rule])
    assert extreme_result["status"] == "FAIL"
    json.dumps(extreme_result)
    assert _run_provenance_checks(
        {"parameters": {"oversized": huge_number}, "parameters_hash": "hash"},
        "dataset-hash",
    )["parameters_hash_matches"] is False


def test_rule_alignment_fails_when_malformed_snapshot_rule_was_filtered():
    effective_rule = _run_snapshot_rule(_execution_dataset_rules()[0])
    constraints = {
        "fee_rule": {"source": "backtest.dataset_rule_snapshot", "effective_slippage_bps": [10]},
        "market_rule": {
            "source": "backtest.dataset_rule_snapshot",
            "effective_liquidity": [0.05],
            "liquidity_policy": ["reject_full_order"],
        },
        "dataset_rule_snapshot": {
            "source": "backtest.dataset_rule_snapshot",
            "rule_count": 1,
            "rule_hashes": ["rule-hash"],
            "effective_rule_item_count": 2,
            "malformed_effective_rule_count": 1,
            "effective_rules": [effective_rule],
        },
    }
    assert _dataset_rule_alignment(constraints, _execution_dataset_rules())["status"] == "FAIL"

    raw_parameters = {
        "dataset_rule_snapshot": {
            "source": "backtest.dataset_rule_snapshot",
            "rule_count": 1,
            "rule_hashes": ["rule-hash"],
            "effective_rules": [effective_rule, "bad-rule"],
        },
    }
    summarized = _run_parameter_summary({"parameters": raw_parameters})
    assert summarized["dataset_rule_snapshot"]["malformed_effective_rule_count"] == 1
    assert _dataset_rule_alignment(
        summarized, _execution_dataset_rules(),
    )["status"] == "FAIL"

    malformed_parameters = {
        "dataset_rule_snapshot": {
            "source": "backtest.dataset_rule_snapshot",
            "rule_count": 1,
            "rule_hashes": ["rule-hash"],
            "effective_rules": [{
                **effective_rule,
                "parameters": ["not-an-object"],
            }],
        },
    }
    malformed_summary = _run_parameter_summary({"parameters": malformed_parameters})
    assert malformed_summary["dataset_rule_snapshot"]["effective_rules"][0][
        "parameters_valid"
    ] is False
    assert _dataset_rule_alignment(
        malformed_summary, _execution_dataset_rules(),
    )["status"] == "FAIL"
