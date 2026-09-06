"""Render read-only backtest facts as an AI-friendly Markdown document.

The renderer is deliberately pure: it receives an already reconstructed
diagnostic report and never opens a database connection or executes SQL.  The
document is an export of persisted facts and derived evaluation views; it is
not a second evaluation implementation and it must not suggest parameter
changes automatically.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from typing import Any


_MISSING = "—"


def _value(value: Any) -> str:
    if value is None or value == "":
        return _MISSING
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _cell(value: Any) -> str:
    """Escape a value for a Markdown table cell."""
    return _value(value).replace("|", r"\|").replace("\r", " ").replace("\n", " ")


def _number(value: Any, digits: int = 4) -> str:
    if value is None or value == "":
        return _MISSING
    try:
        number = float(value)
    except (TypeError, ValueError):
        return _cell(value)
    return f"{number:,.{digits}f}"


def _percent(value: Any, digits: int = 2) -> str:
    if value is None or value == "":
        return _MISSING
    try:
        return f"{float(value) * 100:.{digits}f}%"
    except (TypeError, ValueError):
        return _cell(value)


def _money(value: Any) -> str:
    rendered = _number(value, 2)
    return _MISSING if rendered == _MISSING else f"¥{rendered}"


def _table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> list[str]:
    output = [
        "| " + " | ".join(_cell(header) for header in headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    output.extend(
        "| " + " | ".join(_cell(value) for value in row) + " |"
        for row in rows
    )
    return output


def _section(title: str, lines: Iterable[str]) -> list[str]:
    return [f"## {title}", "", *lines, ""]


def _metric(metrics: Mapping[str, Any], key: str, formatter=_number) -> str:
    return formatter(metrics.get(key))


def _run_identity(report: Mapping[str, Any]) -> list[str]:
    run = report.get("run") or {}
    scope = report.get("scope") or {}
    rows = [
        ("Run ID", run.get("run_id")),
        ("状态", run.get("status")),
        ("策略", f"{_value(run.get('strategy_id'))} / {_value(run.get('strategy_version'))}"),
        ("运行版本", run.get("run_version")),
        ("Dataset ID", run.get("dataset_id")),
        ("初始资金", _money(run.get("initial_cash"))),
        ("评估区间", f"{_value(scope.get('signal_date_from'))} ～ {_value(scope.get('signal_date_to'))}"),
        ("输入哈希", run.get("input_hash")),
        ("执行引擎版本", run.get("execution_model_version")),
        ("代码版本", run.get("code_version")),
        ("Git 提交", run.get("git_commit")),
        ("代码归档哈希", run.get("code_archive_sha256")),
        ("报告版本", report.get("report_version")),
    ]
    return _table(("项目", "值"), rows)


def _calendar_table(evaluation: Mapping[str, Any], period: str) -> list[str]:
    rows = (evaluation.get("calendar") or {}).get(period) or []
    if not rows:
        return ["暂无该维度数据。"]
    return _table(
        ("期间", "策略收益", "基准收益", "超额收益", "最大回撤", "交易日"),
        (
            (
                row.get("period"),
                _percent(row.get("strategy_return")),
                _percent(row.get("benchmark_return")),
                _percent(row.get("excess_return")),
                _percent(row.get("max_drawdown")),
                row.get("trading_days"),
            )
            for row in rows
        ),
    )


def _regime_table(evaluation: Mapping[str, Any]) -> list[str]:
    rows = (evaluation.get("market_regime") or {}).get("summary") or []
    if not rows:
        return ["暂无市场状态切片数据。"]
    return _table(
        ("市场状态", "观察日", "起始日", "结束日", "策略收益", "基准收益", "超额收益", "最大回撤", "平均现金占比"),
        (
            (
                row.get("regime"),
                row.get("observation_days"),
                row.get("start_date"),
                row.get("end_date"),
                _percent(row.get("strategy_return")),
                _percent(row.get("benchmark_return")),
                _percent(row.get("excess_return")),
                _percent(row.get("max_drawdown")),
                _percent(row.get("average_cash_ratio")),
            )
            for row in rows
        ),
    )


def _rolling_table(evaluation: Mapping[str, Any]) -> list[str]:
    rows = (evaluation.get("rolling_12m") or [])
    if not rows:
        return ["暂无滚动窗口数据。"]
    # A rolling series can be long.  Keep every endpoint for reproducibility,
    # while avoiding an accidentally enormous document in unusual runs.
    limit = 500
    shown = list(rows[:limit])
    lines = _table(
        ("窗口起点", "窗口终点", "交易日", "策略收益", "基准收益", "超额收益", "最大回撤"),
        (
            (
                row.get("window_start"),
                row.get("window_end"),
                row.get("trading_days"),
                _percent(row.get("strategy_return")),
                _percent(row.get("benchmark_return")),
                _percent(row.get("excess_return")),
                _percent(row.get("max_drawdown")),
            )
            for row in shown
        ),
    )
    if len(rows) > limit:
        lines.extend(["", f"> 滚动窗口共 {len(rows)} 条，仅展示前 {limit} 条；完整数据仍保留在 JSON 诊断报告中。"])
    return lines


def _forward_return_lines(forward: Mapping[str, Any]) -> list[str]:
    rows = []
    for horizon, data in sorted(forward.items()):
        summary = data.get("returns") or {}
        rows.append(
            (
                horizon,
                data.get("available_count"),
                _percent(summary.get("mean")),
                _percent(summary.get("median")),
                _percent(summary.get("min")),
                _percent(summary.get("max")),
            )
        )
    return _table(("持有窗口", "样本数", "平均价格收益", "中位数", "最小", "最大"), rows) if rows else ["暂无未来价格路径数据。"]


def render_diagnostic_markdown(
    report: Mapping[str, Any],
    *,
    capital_sensitivity: Mapping[str, Any] | None = None,
    max_trade_rows: int = 500,
) -> str:
    """Render one read-only diagnostic report as structured Markdown.

    The output intentionally labels future-return data as post-hoc evaluation
    and keeps it separate from historical signal facts.  This makes the file
    safe to provide to an AI analyst without implying that future information
    participated in the strategy decision.
    """
    metrics = report.get("persisted_metrics") or {}
    summary = report.get("trade_summary") or {}
    rejections = report.get("order_rejections") or {}
    evaluation = report.get("evaluation") or {}
    lines: list[str] = [
        "# 回测策略诊断数据文档",
        "",
        "> 本文是只读诊断导出，供人工或 AI 分析。它只整理已持久化回测事实和标准 Evaluation（评估）派生结果，不会修改策略、数据底座或回测结果。",
        "> 分析时必须区分：当时可见的信号/交易事实、事后收益统计、以及分析者提出的假设。不得把事后数据倒灌回历史信号。",
        "",
    ]
    lines += _section("1. 回测身份与复现边界", _run_identity(report))

    lines += _section(
        "2. 核心结论指标",
        _table(
            ("指标", "值", "说明"),
            (
                ("期末权益", _money(metrics.get("final_equity")), "回测结束时的账户权益"),
                ("累计收益", _percent(metrics.get("total_return")), "期末权益 / 初始资金 - 1"),
                ("CAGR", _percent(metrics.get("cagr")), "按报告 Evaluation（评估）口径计算"),
                ("最大回撤", _percent(metrics.get("max_drawdown")), "非负回撤幅度"),
                ("Sharpe", _number(metrics.get("sharpe"), 4), "日收益、年化 252、无风险利率 0"),
                ("基准收益", _percent(metrics.get("benchmark_total_return")), "同一 Evaluation（评估）区间"),
                ("超额收益", _percent(metrics.get("excess_return")), "策略收益减基准收益"),
                ("年化波动率", _percent(metrics.get("annualized_volatility")), "日收益样本标准差年化"),
            ),
        ),
    )

    counts = report.get("row_counts") or {}
    lines += _section(
        "3. 执行概况",
        _table(
            ("对象", "数量"),
            ((key, value) for key, value in counts.items()),
        )
        + [""]
        + _table(
            ("交易统计", "值"),
            (
                ("完整交易数", summary.get("trade_count")),
                ("盈利交易数", summary.get("winning_trades")),
                ("亏损交易数", summary.get("losing_trades")),
                ("胜率", _percent(summary.get("win_rate"))),
                ("净盈亏", _money(summary.get("net_pnl"))),
                ("平均盈利交易", _money(summary.get("average_winner_pnl"))),
                ("平均亏损交易", _money(summary.get("average_loser_pnl"))),
                ("Profit Factor（盈亏比）", _number(summary.get("profit_factor"), 4)),
                ("最大单笔亏损", _money(summary.get("max_single_loss"))),
            ),
        ),
    )

    rejection_rows = []
    for category, count in sorted((rejections.get("by_category") or {}).items(), key=lambda item: (-item[1], item[0])):
        examples = "; ".join((rejections.get("examples") or {}).get(category, []))
        rejection_rows.append((category, count, examples or _MISSING))
    lines += _section(
        "4. 订单拒绝归因",
        [
            f"总订单 **{_value(rejections.get('total_orders'))}**，成交订单 **{_value(rejections.get('filled_orders'))}**，拒单 **{_value(rejections.get('rejected_orders'))}**。",
            "",
            *_table(("分类", "数量", "原始原因示例"), rejection_rows),
        ],
    )

    exit_rows = []
    for reason, data in sorted((report.get("exit_attribution") or {}).items(), key=lambda item: (-(item[1].get("trade_count") or 0), item[0])):
        exit_rows.append((reason, data.get("trade_count"), _money(data.get("net_pnl")), _percent(data.get("average_net_return"))))
    lines += _section("5. 退出原因归因", _table(("退出原因", "交易数", "净盈亏", "平均净收益率"), exit_rows) if exit_rows else ["暂无完整交易。"])

    entry = report.get("entry_diagnostics") or {}
    ret60 = entry.get("ret60") or {}
    lines += _section(
        "6. 入场诊断",
        _table(
            ("项目", "值"),
            (
                ("实际买入数", entry.get("actual_entry_count")),
                ("入场 RET60 平均", _percent(ret60.get("mean"))),
                ("入场 RET60 中位数", _percent(ret60.get("median"))),
                ("入场 RET60 最小/最大", f"{_percent(ret60.get('min'))} / {_percent(ret60.get('max'))}"),
                ("缺少信号时点输入", entry.get("missing_signal_time_input")),
                ("历史 ST 违规数", len(entry.get("historical_st_violations") or [])),
            ),
        ),
    )

    trades = report.get("trades") or []
    trade_table = (
        _table(
            ("代码", "名称", "买入日", "卖出日", "持有交易日", "买入价", "卖出价", "净盈亏", "净收益率", "退出原因", "入场 RET60"),
            (
                (
                    row.get("ts_code"), row.get("name"), row.get("buy_date"), row.get("sell_date"),
                    row.get("holding_trading_days"), _money(row.get("buy_price")), _money(row.get("sell_price")),
                    _money(row.get("net_pnl")), _percent(row.get("net_return")), row.get("exit_reason"),
                    _percent(row.get("entry_ret60")),
                )
                for row in trades[:max_trade_rows]
            ),
        )
        if trades else ["暂无完整交易。"]
    )
    lines += _section(
        "7. 逐笔完整交易",
        [
            "下表用于追踪实际成交，不代表所有信号都成交。股票身份以 `ts_code` 为准，名称仅作展示。",
            "",
            *trade_table,
            *( ["", f"> 完整交易共 {len(trades)} 笔，当前最多展示 {max_trade_rows} 笔。"] if len(trades) > max_trade_rows else [] ),
        ],
    )

    lines += _section("8. 未来价格路径（事后 Evaluation）", [
        "这些数据只描述实际买入成交后未来价格如何变化，不参与历史信号生成，也不等同于策略净收益。",
        "",
        *_forward_return_lines(report.get("forward_returns") or {}),
    ])

    lines += _section("9. 日历期间表现", [
        "### 月度",
        "",
        *_calendar_table(evaluation, "month"),
        "",
        "### 季度",
        "",
        *_calendar_table(evaluation, "quarter"),
        "",
        "### 年度",
        "",
        *_calendar_table(evaluation, "year"),
    ])

    regime = evaluation.get("market_regime") or {}
    lines += _section("10. 市场状态切片（仅事后评价）", [
        f"定义：`{_value(regime.get('definition'))}`；MA 窗口：`{_value(regime.get('ma_window'))}` 个交易日。该标签不会改变 Strategy（策略）V1 的交易决策。",
        "",
        *_regime_table(evaluation),
    ])

    lines += _section("11. 滚动 12 个月表现", _rolling_table(evaluation))

    if capital_sensitivity:
        capital_rows = capital_sensitivity.get("rows") or []
        lines += _section("12. 资金敏感性", [
            f"可比性：**{_value(capital_sensitivity.get('comparable'))}**；Evaluation（评估）版本：`{_value(capital_sensitivity.get('evaluation_version'))}`。只有身份一致、仅 Scenario（场景）初始资金不同的 Run 才能比较。",
            "仅允许把初始资金作为 Scenario（场景）变量；策略、Dataset（回测数据集）、Evaluation（评估）口径和执行引擎必须保持一致。",
            "",
            *_table(
                ("初始资金", "状态", "期末权益", "累计收益", "最大回撤", "Sharpe", "完整交易", "拒单率", "平均现金占比"),
                (
                    (
                        _money(row.get("initial_capital")), row.get("status"), _money(row.get("final_equity")),
                        _percent(row.get("total_return")), _percent(row.get("max_drawdown")), _number(row.get("sharpe"), 4),
                        row.get("trade_count"), _percent(row.get("reject_rate")), _percent(row.get("average_cash_ratio")),
                    )
                    for row in capital_rows
                ),
            ),
        ])

    limitations = report.get("limitations") or []
    lines += _section("12. 数据边界与分析注意事项" if not capital_sensitivity else "13. 数据边界与分析注意事项", [
        *[f"- {item}" for item in limitations],
        "- 本文是诊断事实文档，不是自动调参建议；任何策略修改都必须创建新的 StrategyVersion（策略版本）并独立回测。",
        "- Raw（原始层）、Standard（标准层）、PIT（时点层）、Feature（特征层）和 Frozen Dataset（冻结数据集）均未被本文修改。",
    ])

    lines += [
        "## 给 AI 分析时的固定任务边界",
        "",
        "请先复述数据身份和可比性，再按以下顺序分析：",
        "1. 先判断回测执行链路是否存在数据缺失、拒单异常、ST/PIT 违规或身份不一致。",
        "2. 再区分资金约束造成的结果和策略规则本身造成的结果。",
        "3. 使用逐笔交易、退出归因、入场 RET60、日历/市场状态切片互相验证，不只看累计收益。",
        "4. 明确标注‘事实’、‘推断’和‘需要额外验证的假设’，禁止把事后收益当作历史可见信息。",
        "5. 不要自动改参数；如提出改进，只给出可检验假设和单变量对照实验设计。",
        "",
        "---",
        "",
        "生成方式：`data_collect.jobs.backtest_diagnostics` 只读重建；本文不是数据库持久化事实表。",
        "",
    ]
    return "\n".join(lines)


def render_capital_sensitivity_markdown(result: Mapping[str, Any]) -> str:
    """Render only a capital comparison response as a compact Markdown file."""
    rows = result.get("rows") or []
    lines = [
        "# 资金敏感性对比",
        "",
        "> 本文只比较身份一致的回测 Run；它不会重新运行回测，也不会修改任何数据。",
        "",
        f"- 可比性：**{_value(result.get('comparable'))}**",
        f"- Evaluation（评估）版本：`{_value(result.get('evaluation_version'))}`",
        f"- 可用 Run 数：`{_value(result.get('available_run_count'))}`",
        "",
        *_table(
            ("初始资金", "状态", "期末权益", "累计收益", "最大回撤", "Sharpe", "完整交易", "拒单率", "平均现金占比"),
            (
                (
                    _money(row.get("initial_capital")), row.get("status"), _money(row.get("final_equity")),
                    _percent(row.get("total_return")), _percent(row.get("max_drawdown")), _number(row.get("sharpe"), 4),
                    row.get("trade_count"), _percent(row.get("reject_rate")), _percent(row.get("average_cash_ratio")),
                )
                for row in rows
            ),
        ),
        "",
        "分析边界：只允许把初始资金作为 Scenario（场景）变量；策略、Dataset（回测数据集）、Evaluation（评估）和执行引擎身份必须一致。",
        "",
    ]
    return "\n".join(lines)


def render_capital_sensitivity_bundle_markdown(
    reports: Sequence[Mapping[str, Any]],
    comparison: Mapping[str, Any],
    *,
    max_trade_rows_per_run: int = 1000,
) -> str:
    """Render one AI document containing all available capital scenarios.

    The bundle uses the same already reconstructed reports as the dashboard;
    it does not run a second backtest or query a different data source.  Each
    scenario keeps its own identity and diagnostics so an AI can distinguish
    capital constraints from strategy behavior.
    """
    ordered_reports = sorted(
        reports,
        key=lambda report: float((report.get("run") or {}).get("initial_cash") or 0),
    )
    lines = [
        "# 全部资金场景 AI 分析数据文档",
        "",
        "> 本文自动汇总同一 Strategy（策略）、Dataset（回测数据集）、Evaluation（评估）和执行引擎下的多个 Scenario（交易场景）。它是只读诊断导出，不会重新运行回测，也不会修改数据库。",
        "> 资金是唯一允许变化的实验变量；分析时必须先检查身份一致性，再区分资金约束和策略规则本身的影响。",
        "",
        "## 1. 实验一致性",
        "",
        f"- 可比性：**{_value(comparison.get('comparable'))}**",
        f"- Evaluation（评估）版本：`{_value(comparison.get('evaluation_version'))}`",
        f"- 已读取 Run 数：`{_value(comparison.get('available_run_count'))}`",
        f"- 请求资金档位：`{', '.join(_money(value) for value in comparison.get('requested_capitals') or [])}`",
        "",
        *_table(
            ("初始资金", "Run ID", "状态", "策略", "Dataset ID", "输入哈希", "执行引擎", "代码提交"),
            (
                (
                    _money((report.get("run") or {}).get("initial_cash")),
                    (report.get("run") or {}).get("run_id"),
                    (report.get("run") or {}).get("status"),
                    f"{_value((report.get('run') or {}).get('strategy_id'))} / {_value((report.get('run') or {}).get('strategy_version'))}",
                    (report.get("run") or {}).get("dataset_id"),
                    (report.get("run") or {}).get("input_hash"),
                    (report.get("run") or {}).get("execution_model_version"),
                    (report.get("run") or {}).get("git_commit"),
                )
                for report in ordered_reports
            ),
        ),
        "",
        "## 2. 资金敏感性总表",
        "",
        *render_capital_sensitivity_markdown(comparison).splitlines()[7:],
        "",
        "## 3. 各资金场景详细事实",
        "",
    ]

    all_trades: list[tuple[Any, Mapping[str, Any]]] = []
    for report in ordered_reports:
        run = report.get("run") or {}
        capital = _money(run.get("initial_cash"))
        metrics = report.get("persisted_metrics") or {}
        summary = report.get("trade_summary") or {}
        rejection = report.get("order_rejections") or {}
        entry = report.get("entry_diagnostics") or {}
        ret60 = entry.get("ret60") or {}
        exit_attribution = report.get("exit_attribution") or {}
        exit_table = (
            _table(
                ("退出原因", "交易数", "净盈亏", "平均净收益率"),
                (
                    (reason, value.get("trade_count"), _money(value.get("net_pnl")), _percent(value.get("average_net_return")))
                    for reason, value in sorted(exit_attribution.items())
                ),
            )
            if exit_attribution else ["暂无完整交易。"]
        )
        lines += [
            f"### {capital}（Run `{_value(run.get('run_id'))}`）",
            "",
            *_table(
                ("指标", "值"),
                (
                    ("状态", run.get("status")),
                    ("期末权益", _money(metrics.get("final_equity"))),
                    ("累计收益", _percent(metrics.get("total_return"))),
                    ("CAGR", _percent(metrics.get("cagr"))),
                    ("最大回撤", _percent(metrics.get("max_drawdown"))),
                    ("Sharpe", _number(metrics.get("sharpe"), 4)),
                    ("基准收益", _percent(metrics.get("benchmark_total_return"))),
                    ("完整交易数", summary.get("trade_count")),
                    ("胜率", _percent(summary.get("win_rate"))),
                    ("拒单率", _percent(rejection.get("rejected_orders", 0) / rejection.get("total_orders", 0) if rejection.get("total_orders") else None)),
                    ("平均现金占比", _percent(sum(row.get("cash_ratio") or 0 for row in report.get("equity_curve") or []) / len(report.get("equity_curve") or []) if report.get("equity_curve") else None)),
                    ("入场 RET60 中位数", _percent(ret60.get("median"))),
                ),
            ),
            "",
            "**订单拒绝分类**",
            "",
            *_table(
                ("分类", "数量", "原因示例"),
                (
                    (category, count, "; ".join((rejection.get("examples") or {}).get(category, [])) or _MISSING)
                    for category, count in sorted((rejection.get("by_category") or {}).items(), key=lambda item: (-item[1], item[0]))
                ),
            ),
            "",
            "**退出原因分类**",
            "",
            *exit_table,
            "",
            "**年度表现**",
            "",
            *_calendar_table(report.get("evaluation") or {}, "year"),
            "",
            "**市场状态表现（仅事后评价）**",
            "",
            *_regime_table(report.get("evaluation") or {}),
            "",
        ]
        all_trades.extend((capital, trade) for trade in (report.get("trades") or [])[:max_trade_rows_per_run])

    trade_rows = [
        (
            capital, trade.get("ts_code"), trade.get("name"), trade.get("buy_date"), trade.get("sell_date"),
            trade.get("holding_trading_days"), _money(trade.get("buy_price")), _money(trade.get("sell_price")),
            _money(trade.get("net_pnl")), _percent(trade.get("net_return")), trade.get("exit_reason"),
        )
        for capital, trade in all_trades
    ]
    trade_table = _table(
        ("初始资金", "代码", "名称", "买入日", "卖出日", "持有交易日", "买入价", "卖出价", "净盈亏", "净收益率", "退出原因"),
        trade_rows,
    ) if trade_rows else ["暂无完整交易。"]
    lines += [
        "## 4. 全部资金场景逐笔交易",
        "",
        "股票身份以 `ts_code` 为准，名称仅作展示；每行属于对应资金 Scenario（场景）的完整成交交易。",
        "",
        *trade_table,
        *(["", f"> 各资金场景最多展示 {max_trade_rows_per_run} 笔交易；当前合并输出 {len(trade_rows)} 笔。"] if len(trade_rows) > max_trade_rows_per_run * max(1, len(ordered_reports)) else []),
        "",
        "## 5. 给 AI 分析的固定边界",
        "",
        "1. 先确认所有 Run 的 Dataset、Strategy、Evaluation、执行引擎、输入哈希和代码身份一致。",
        "2. 先分析拒单率、平均现金占比、成交数量，再判断收益差异是否来自资金规模。",
        "3. 结合逐笔交易、退出原因、年度和市场状态表现，区分执行约束与策略失败机制。",
        "4. 未来价格路径和市场状态属于事后 Evaluation（评估），不能参与历史信号重算。",
        "5. 不要自动调参；任何改进建议都必须形成新 StrategyVersion（策略版本）并进行单变量对照实验。",
        "",
    ]
    return "\n".join(lines)
