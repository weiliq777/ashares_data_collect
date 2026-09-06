from data_collect.web import backtest_dashboard


def _report(capital, run_id):
    return {
        "run": {
            "run_id": run_id,
            "dataset_id": "dataset-1",
            "strategy_id": "strategy-1",
            "strategy_version": "1.0.0",
            "input_hash": "input-1",
            "execution_model_version": "engine-1",
            "code_version": "code-1",
            "git_commit": "commit-1",
            "code_archive_sha256": "archive-1",
            "initial_cash": capital,
            "status": "SUCCEEDED",
            "parameters": {
                "evaluation_hash": "eval-1",
                "scenario": {
                    "scenario_id": f"CAP_{int(capital)}",
                    "initial_capital": capital,
                    "target_positions": 3,
                },
            },
        },
        "scope": {"signal_date_from": "2024-01-01", "signal_date_to": "2024-01-31"},
        "persisted_metrics": {"final_equity": capital, "total_return": 0.0},
        "row_counts": {"orders": 1, "fills": 1},
        "order_rejections": {"total_orders": 1, "filled_orders": 1, "rejected_orders": 0},
        "trade_summary": {"trade_count": 0},
        "equity_curve": [{"cash_ratio": 1.0}],
    }


def test_dashboard_capital_comparison_is_cached_and_read_only(monkeypatch):
    reports = {"run-10k": _report(10_000, "run-10k"), "run-30k": _report(30_000, "run-30k")}
    monkeypatch.setattr(backtest_dashboard, "_cached_report", reports.__getitem__)
    backtest_dashboard._COMPARISON_CACHE.clear()

    result = backtest_dashboard._cached_capital_comparison(["run-10k", "run-30k", "run-10k"])

    assert result["comparable"] is True
    assert result["available_run_count"] == 2
    assert result["rows"][0]["run_id"] == "run-10k"
    assert result["rows"][1]["run_id"] == "run-30k"
    assert len(backtest_dashboard._COMPARISON_CACHE) == 1
