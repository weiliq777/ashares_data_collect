"""Standard Evaluation（标准评估）计算。

本模块只处理已经持久化的回测事实或诊断报告，不访问数据库、不修改
Raw（原始层）、Frozen Dataset（冻结数据集）或 Strategy（策略）。所有
结果都是可重建的派生视图，身份由输入 Run（回测运行）和本模块版本决定。
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
import json
import math
from typing import Any, Iterable, Mapping, Sequence


STANDARD_EVALUATION_VERSION = "standard-evaluation-v1"
MARKET_REGIME_DEFINITION = "BENCHMARK_MA120_SLOPE_V1"
DEFAULT_CAPITALS = (10_000.0, 30_000.0, 50_000.0, 70_000.0, 100_000.0)


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _round(value: Any, digits: int = 8) -> float | None:
    number = _number(value)
    return None if number is None else round(number, digits)


def _compound(values: Iterable[Any]) -> float | None:
    numbers = [_number(value) for value in values]
    valid = [value for value in numbers if value is not None]
    if not valid:
        return None
    return math.prod(1.0 + value for value in valid) - 1.0


def _local_max_drawdown(equities: Sequence[float]) -> float | None:
    if not equities:
        return None
    peak = equities[0]
    worst = 0.0
    for equity in equities:
        if equity > peak:
            peak = equity
        if peak > 0:
            worst = min(worst, equity / peak - 1.0)
    return worst


def _drawdown_magnitude(equities: Sequence[float]) -> float | None:
    value = _local_max_drawdown(equities)
    return None if value is None else abs(value)


def _daily_returns(performance: Sequence[Mapping[str, Any]]) -> dict[date, float]:
    rows = sorted(
        (row for row in performance if _as_date(row.get("trade_date")) is not None),
        key=lambda row: _as_date(row.get("trade_date")),
    )
    result: dict[date, float] = {}
    previous_equity: float | None = None
    for row in rows:
        trade_date = _as_date(row.get("trade_date"))
        equity = _number(row.get("total_equity"))
        if trade_date is None or equity is None:
            continue
        daily = _number(row.get("daily_return"))
        if daily is None and previous_equity not in (None, 0):
            daily = equity / previous_equity - 1.0
        if daily is not None and math.isfinite(daily):
            result[trade_date] = daily
        previous_equity = equity
    return result


def _benchmark_returns(benchmark: Sequence[Mapping[str, Any]]) -> dict[date, float]:
    rows = sorted(
        (
            (_as_date(row.get("trade_date")), _number(row.get("close")))
            for row in benchmark
        ),
        key=lambda item: item[0] or date.min,
    )
    result: dict[date, float] = {}
    previous: float | None = None
    for trade_date, close in rows:
        if trade_date is None or close is None or close <= 0:
            continue
        if previous not in (None, 0):
            result[trade_date] = close / previous - 1.0
        previous = close
    return result


def _period_key(trade_date: date, period: str) -> str:
    if period == "month":
        return trade_date.strftime("%Y-%m")
    if period == "quarter":
        return f"{trade_date.year}-Q{((trade_date.month - 1) // 3) + 1}"
    if period == "year":
        return trade_date.strftime("%Y")
    raise ValueError("period 仅支持 month、quarter 或 year")


def evaluate_calendar_periods(
    performance: Sequence[Mapping[str, Any]],
    benchmark: Sequence[Mapping[str, Any]] = (),
    period: str = "month",
) -> list[dict[str, Any]]:
    """按自然月/季/年计算策略、基准和局部回撤。

    每个分组的策略收益使用该组内的日收益复合；基准使用同一组内的
    基准日收益复合。首个可用日没有日收益时，不人为填充收益。
    """
    strategy_returns = _daily_returns(performance)
    benchmark_returns = _benchmark_returns(benchmark)
    groups: dict[str, list[tuple[date, Mapping[str, Any]]]] = defaultdict(list)
    for row in performance:
        trade_date = _as_date(row.get("trade_date"))
        if trade_date is not None and _number(row.get("total_equity")) is not None:
            groups[_period_key(trade_date, period)].append((trade_date, row))
    output: list[dict[str, Any]] = []
    for key, items in sorted(groups.items()):
        items.sort(key=lambda item: item[0])
        dates = [item[0] for item in items]
        equities = [_number(item[1].get("total_equity")) for item in items]
        equity_values = [value for value in equities if value is not None]
        position_values = [_number(item[1].get("position_value")) for item in items]
        known_position_values = [value for value in position_values if value is not None]
        invested_days = sum(value > 0 for value in known_position_values)
        activity_status = (
            "UNKNOWN"
            if len(known_position_values) != len(items)
            else "CASH_ONLY"
            if invested_days == 0
            else "INVESTED"
        )
        strategy_return = _compound(strategy_returns.get(item) for item in dates)
        benchmark_return = _compound(benchmark_returns.get(item) for item in dates)
        output.append({
            "period": key,
            "period_type": period,
            "start_date": dates[0].isoformat(),
            "end_date": dates[-1].isoformat(),
            "observation_days": len(dates),
            "strategy_return": _round(strategy_return),
            "benchmark_return": _round(benchmark_return),
            "excess_return": _round(
                strategy_return - benchmark_return
                if strategy_return is not None and benchmark_return is not None else None
            ),
            "max_drawdown": _round(_drawdown_magnitude(equity_values)),
            "ending_equity": _round(equity_values[-1], 6) if equity_values else None,
            "invested_days": invested_days if activity_status != "UNKNOWN" else None,
            "cash_only_days": (
                len(items) - invested_days if activity_status != "UNKNOWN" else None
            ),
            "activity_status": activity_status,
        })
    return output


def evaluate_rolling_windows(
    performance: Sequence[Mapping[str, Any]],
    benchmark: Sequence[Mapping[str, Any]] = (),
    window_trading_days: int = 252,
    step_trading_days: int = 21,
) -> list[dict[str, Any]]:
    """按交易日构建滚动窗口，默认是一年窗口、每月一个观察点。"""
    if window_trading_days <= 1 or step_trading_days <= 0:
        raise ValueError("rolling window 必须大于 1，step 必须为正数")
    rows = sorted(
        (
            (_as_date(row.get("trade_date")), _number(row.get("total_equity")))
            for row in performance
        ),
        key=lambda item: item[0] or date.min,
    )
    rows = [(trade_date, equity) for trade_date, equity in rows if trade_date and equity is not None]
    benchmark_by_date = {
        _as_date(row.get("trade_date")): _number(row.get("close"))
        for row in benchmark
        if _as_date(row.get("trade_date")) is not None and _number(row.get("close")) not in (None, 0)
    }
    daily = _daily_returns(performance)
    output: list[dict[str, Any]] = []
    for end_index in range(window_trading_days - 1, len(rows), step_trading_days):
        start_index = end_index - window_trading_days + 1
        window = rows[start_index:end_index + 1]
        start_date, start_equity = window[0]
        end_date, end_equity = window[-1]
        if start_equity <= 0:
            continue
        strategy_return = end_equity / start_equity - 1.0
        benchmark_start = benchmark_by_date.get(start_date)
        benchmark_end = benchmark_by_date.get(end_date)
        benchmark_return = (
            benchmark_end / benchmark_start - 1.0
            if benchmark_start and benchmark_end else None
        )
        output.append({
            "window_start": start_date.isoformat(),
            "window_end": end_date.isoformat(),
            "trading_days": len(window),
            "strategy_return": _round(strategy_return),
            "benchmark_return": _round(benchmark_return),
            "excess_return": _round(
                strategy_return - benchmark_return if benchmark_return is not None else None
            ),
            "max_drawdown": _round(_drawdown_magnitude([equity for _, equity in window])),
            "last_daily_return": _round(daily.get(end_date)),
        })
    return output


def evaluate_market_regimes(
    performance: Sequence[Mapping[str, Any]],
    benchmark: Sequence[Mapping[str, Any]],
    ma_window: int = 120,
) -> dict[str, Any]:
    """用基准指数 ``close > MA120 且 MA120 上升`` 固定划分市场状态。

    MA 未形成或没有前一日 MA 时返回 UNKNOWN（未知），不把缺数据当作
    SIDEWAYS（震荡）。该标签只用于事后切片，绝不进入 Strategy V1 决策。
    """
    if ma_window < 2:
        raise ValueError("ma_window 必须至少为 2")
    rows = sorted(
        (
            (_as_date(row.get("trade_date")), _number(row.get("close")))
            for row in benchmark
        ),
        key=lambda item: item[0] or date.min,
    )
    rows = [(trade_date, close) for trade_date, close in rows if trade_date and close and close > 0]
    regime_rows: list[dict[str, Any]] = []
    closes: list[float] = []
    previous_ma: float | None = None
    for trade_date, close in rows:
        closes.append(close)
        ma = sum(closes[-ma_window:]) / ma_window if len(closes) >= ma_window else None
        slope = ma - previous_ma if ma is not None and previous_ma is not None else None
        if ma is None or slope is None:
            regime = "UNKNOWN"
            reason = "INSUFFICIENT_BENCHMARK_HISTORY"
        elif close > ma and slope > 0:
            regime = "BULL"
            reason = "CLOSE_ABOVE_MA_AND_MA_RISING"
        elif close < ma and slope < 0:
            regime = "BEAR"
            reason = "CLOSE_BELOW_MA_AND_MA_FALLING"
        else:
            regime = "SIDEWAYS"
            reason = "OTHER_MA120_STATE"
        regime_rows.append({
            "date": trade_date.isoformat(),
            "benchmark_close": _round(close, 6),
            "ma": _round(ma, 6),
            "ma_slope": _round(slope, 6),
            "regime": regime,
            "reason": reason,
        })
        if ma is not None:
            previous_ma = ma

    performance_by_date = {
        _as_date(row.get("trade_date")): row
        for row in performance
        if _as_date(row.get("trade_date")) is not None
    }
    regime_by_date = {date.fromisoformat(row["date"]): row["regime"] for row in regime_rows}
    strategy_daily = _daily_returns(performance)
    benchmark_daily = _benchmark_returns(benchmark)
    grouped: dict[str, list[date]] = defaultdict(list)
    for trade_date in sorted(performance_by_date):
        grouped[regime_by_date.get(trade_date, "UNKNOWN")].append(trade_date)
    summary: list[dict[str, Any]] = []
    for regime, dates in sorted(grouped.items()):
        values = [
            _number(performance_by_date[item].get("total_equity"))
            for item in dates
        ]
        equities = [value for value in values if value is not None]
        strategy_return = _compound(strategy_daily.get(item) for item in dates)
        benchmark_return = _compound(benchmark_daily.get(item) for item in dates)
        cash_ratios = []
        for item in dates:
            row = performance_by_date[item]
            equity = _number(row.get("total_equity"))
            cash = _number(row.get("cash_balance"))
            if equity and cash is not None:
                cash_ratios.append(cash / equity)
        summary.append({
            "regime": regime,
            "observation_days": len(dates),
            "start_date": dates[0].isoformat(),
            "end_date": dates[-1].isoformat(),
            "strategy_return": _round(strategy_return),
            "benchmark_return": _round(benchmark_return),
            "excess_return": _round(
                strategy_return - benchmark_return
                if strategy_return is not None and benchmark_return is not None else None
            ),
            "max_drawdown": _round(_drawdown_magnitude(equities)),
            "average_cash_ratio": _round(sum(cash_ratios) / len(cash_ratios) if cash_ratios else None),
        })
    return {
        "definition": MARKET_REGIME_DEFINITION,
        "ma_window": ma_window,
        "summary": summary,
        "daily": regime_rows,
    }


def _parameters(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _comparison_identity(report: Mapping[str, Any]) -> dict[str, Any]:
    run = report.get("run") or {}
    params = _parameters(run.get("parameters"))
    scenario = _parameters(params.get("scenario"))
    scenario.pop("initial_capital", None)
    scenario.pop("scenario_id", None)
    evaluation = _parameters(params.get("evaluation"))
    # The 1 万元 baseline may have been persisted before the standard
    # evaluation dimensions were added.  Normalize omitted fields to the
    # frozen V1 defaults instead of comparing an obsolete hash string.
    evaluation.setdefault("calendar_periods", ["month", "quarter", "year"])
    evaluation.setdefault("rolling_window_trading_days", 252)
    evaluation.setdefault("rolling_step_trading_days", 21)
    evaluation.setdefault("market_regime_definition", MARKET_REGIME_DEFINITION)
    evaluation.setdefault("market_regime_ma_window", 120)
    base_parameters = dict(params)
    base_parameters.pop("scenario", None)
    base_parameters.pop("scenario_hash", None)
    base_parameters.pop("backtest_fingerprint", None)
    base_parameters.pop("evaluation", None)
    base_parameters.pop("evaluation_hash", None)
    return {
        "dataset_id": run.get("dataset_id"),
        "strategy_id": run.get("strategy_id"),
        "strategy_version": run.get("strategy_version"),
        "input_hash": run.get("input_hash"),
        "execution_model_version": run.get("execution_model_version"),
        "code_version": run.get("code_version"),
        "git_commit": run.get("git_commit"),
        "git_dirty": run.get("git_dirty"),
        "diff_hash": run.get("diff_hash"),
        "code_archive_sha256": run.get("code_archive_sha256"),
        "evaluation_definition": evaluation,
        "base_parameters": base_parameters,
        "scenario_without_capital": scenario,
    }


def build_capital_sensitivity(
    reports: Sequence[Mapping[str, Any]],
    capitals: Sequence[float] = DEFAULT_CAPITALS,
) -> dict[str, Any]:
    """汇总同一 Strategy/Dataset 的不同资金 Scenario。

    该函数拒绝混入不同策略、数据集、评价口径或交易参数的 Run，避免
    把不具可比性的结果画成“资金敏感性”。缺少某资金档位只报告缺失，
    不自动重跑回测。
    """
    requested = [float(value) for value in capitals]
    if not requested or any(value <= 0 for value in requested):
        raise ValueError("capital sensitivity 的资金档位必须为正")
    identities = [_comparison_identity(report) for report in reports]
    mismatches: list[dict[str, Any]] = []
    if identities:
        baseline = identities[0]
        for index, identity in enumerate(identities[1:], start=1):
            differences = {
                key: {"baseline": baseline.get(key), "actual": identity.get(key)}
                for key in baseline
                if baseline.get(key) != identity.get(key)
            }
            if differences:
                mismatches.append({"report_index": index, "differences": differences})
    if mismatches:
        raise ValueError(f"capital sensitivity 包含不可比较的 Run: {mismatches}")
    by_capital: dict[float, Mapping[str, Any]] = {}
    duplicate_capitals: list[float] = []
    for report in reports:
        initial = _number((report.get("run") or {}).get("initial_cash"))
        if initial is None:
            continue
        if initial in by_capital:
            duplicate_capitals.append(initial)
        by_capital[initial] = report
    rows: list[dict[str, Any]] = []
    for capital in requested:
        report = by_capital.get(capital)
        if report is None:
            rows.append({"initial_capital": capital, "status": "MISSING"})
            continue
        metrics = report.get("persisted_metrics") or {}
        rejection = report.get("order_rejections") or {}
        counts = report.get("row_counts") or {}
        curve = report.get("equity_curve") or []
        cash_ratios = [_number(row.get("cash_ratio")) for row in curve]
        cash_ratios = [value for value in cash_ratios if value is not None]
        rows.append({
            "initial_capital": capital,
            "status": str((report.get("run") or {}).get("status") or "UNKNOWN"),
            "run_id": (report.get("run") or {}).get("run_id"),
            "final_equity": _round(metrics.get("final_equity"), 6),
            "total_return": _round(metrics.get("total_return")),
            "cagr": _round(metrics.get("cagr")),
            "max_drawdown": _round(metrics.get("max_drawdown")),
            "sharpe": _round(metrics.get("sharpe")),
            "trade_count": (report.get("trade_summary") or {}).get("trade_count", 0),
            "filled_orders": rejection.get("filled_orders", counts.get("fills", 0)),
            "rejected_orders": rejection.get("rejected_orders", 0),
            "total_orders": rejection.get("total_orders", counts.get("orders", 0)),
            "reject_rate": _round(
                rejection.get("rejected_orders", 0) / rejection.get("total_orders", 0)
                if rejection.get("total_orders", 0) else None
            ),
            "average_cash_ratio": _round(sum(cash_ratios) / len(cash_ratios) if cash_ratios else None),
        })
    return {
        "evaluation_version": STANDARD_EVALUATION_VERSION,
        "requested_capitals": requested,
        "duplicate_capitals": sorted(set(duplicate_capitals)),
        "available_run_count": len(reports),
        "rows": rows,
        "comparable": not mismatches,
        "identity": identities[0] if identities else {},
    }


def build_standard_evaluation(
    performance: Sequence[Mapping[str, Any]],
    benchmark: Sequence[Mapping[str, Any]] = (),
    *,
    rolling_window_trading_days: int = 252,
    rolling_step_trading_days: int = 21,
    market_regime_ma_window: int = 120,
) -> dict[str, Any]:
    """构造单个 Run 的统一评估结果。"""
    return {
        "evaluation_version": STANDARD_EVALUATION_VERSION,
        "calendar": {
            "month": evaluate_calendar_periods(performance, benchmark, "month"),
            "quarter": evaluate_calendar_periods(performance, benchmark, "quarter"),
            "year": evaluate_calendar_periods(performance, benchmark, "year"),
        },
        "rolling_12m": evaluate_rolling_windows(
            performance, benchmark, rolling_window_trading_days, rolling_step_trading_days
        ),
        "market_regime": evaluate_market_regimes(
            performance, benchmark, market_regime_ma_window
        ),
    }
