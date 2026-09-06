from data_collect.evaluation.markdown import (
    render_capital_sensitivity_markdown,
    render_diagnostic_markdown,
)


def _report():
    return {
        "report_version": "backtest-diagnostics-v2",
        "run": {
            "run_id": "run-1",
            "status": "SUCCEEDED",
            "strategy_id": "quality_trend_rotation_v1",
            "strategy_version": "1.0.0",
            "run_version": "capital-sensitivity-10k-v1",
            "dataset_id": "dataset-1",
            "initial_cash": 10000,
            "input_hash": "input-hash",
            "execution_model_version": "daily-next-open-v2",
            "git_commit": "commit-1",
        },
        "scope": {"signal_date_from": "2021-01-01", "signal_date_to": "2025-12-31"},
        "persisted_metrics": {
            "final_equity": 4032.39,
            "total_return": -0.5968,
            "cagr": -0.15,
            "max_drawdown": 0.6593,
            "sharpe": -1.04,
            "benchmark_total_return": -0.05,
            "excess_return": -0.5468,
        },
        "row_counts": {"signals": 10, "orders": 5, "fills": 2, "position_lots": 1},
        "order_rejections": {
            "total_orders": 5,
            "filled_orders": 2,
            "rejected_orders": 3,
            "by_category": {"FILLED": 2, "LOT_OR_CASH_LIMIT": 3},
            "examples": {"LOT_OR_CASH_LIMIT": ["不足一手"]},
        },
        "trade_summary": {
            "trade_count": 1,
            "winning_trades": 0,
            "losing_trades": 1,
            "win_rate": 0,
            "net_pnl": -200,
            "average_loser_pnl": -200,
        },
        "exit_attribution": {"MA60_EXIT": {"trade_count": 1, "net_pnl": -200, "average_net_return": -0.2}},
        "entry_diagnostics": {
            "actual_entry_count": 1,
            "ret60": {"mean": 0.25, "median": 0.25, "min": 0.25, "max": 0.25},
            "missing_signal_time_input": 0,
            "historical_st_violations": [],
        },
        "trades": [{
            "ts_code": "000001.SZ", "name": "示例", "buy_date": "2024-01-01", "sell_date": "2024-02-01",
            "holding_trading_days": 20, "buy_price": 10, "sell_price": 8, "net_pnl": -200,
            "net_return": -0.2, "exit_reason": "MA60_EXIT", "entry_ret60": 0.25,
        }],
        "forward_returns": {"horizon_20d": {"available_count": 1, "returns": {"mean": -0.1, "median": -0.1, "min": -0.1, "max": -0.1}}},
        "evaluation": {
            "calendar": {
                "month": [{"period": "2024-01", "strategy_return": -0.1, "benchmark_return": 0.01, "excess_return": -0.11}],
                "quarter": [], "year": [],
            },
            "rolling_12m": [],
            "market_regime": {"definition": "BENCHMARK_MA120_SLOPE_V1", "ma_window": 120, "summary": []},
        },
        "limitations": ["示例限制"],
    }


def test_diagnostic_markdown_contains_identity_and_audit_boundaries():
    output = render_diagnostic_markdown(_report())

    assert "# 回测策略诊断数据文档" in output
    assert "run-1" in output
    assert "quality_trend_rotation_v1 / 1.0.0" in output
    assert "订单拒绝归因" in output
    assert "000001.SZ" in output
    assert "未来价格路径（事后 Evaluation）" in output
    assert "不得把事后数据倒灌回历史信号" in output
    assert "Raw（原始层）、Standard（标准层）" in output


def test_diagnostic_markdown_escapes_table_delimiters_and_capital_appendix():
    report = _report()
    report["trades"][0]["name"] = "A|B"
    capital = {
        "comparable": True,
        "evaluation_version": "standard-evaluation-v1",
        "available_run_count": 2,
        "rows": [{
            "initial_capital": 10000,
            "status": "SUCCEEDED",
            "final_equity": 4032.39,
            "total_return": -0.5968,
            "max_drawdown": 0.6593,
            "sharpe": -1.04,
            "trade_count": 1,
            "reject_rate": 0.8,
            "average_cash_ratio": 0.5,
        }],
    }
    output = render_diagnostic_markdown(report, capital_sensitivity=capital)

    assert "A\\|B" in output
    assert "资金敏感性" in output
    assert "仅允许把初始资金作为 Scenario（场景）变量" in output


def test_capital_sensitivity_markdown_is_compact_and_read_only():
    output = render_capital_sensitivity_markdown({
        "comparable": True,
        "evaluation_version": "standard-evaluation-v1",
        "available_run_count": 1,
        "rows": [{"initial_capital": 10000, "status": "SUCCEEDED", "final_equity": 9000, "total_return": -0.1}],
    })

    assert "# 资金敏感性对比" in output
    assert "¥10,000.00" in output
    assert "不会重新运行回测" in output
