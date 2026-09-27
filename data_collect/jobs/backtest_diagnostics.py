"""Read-only diagnostics for a persisted formal Backtest Run.

This module deliberately does not write a diagnostic table.  It reconstructs
the audit trail from the immutable run children already persisted by the
backtest: signal -> order -> fill -> position lot.  Database reads are kept in
``BacktestRunRepository`` so this job never owns a Connection/Cursor or sends
SQL directly.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime
import json
import math
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable, Mapping, Sequence
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.backtest_run_repository import BacktestRunRepository
from data_collect.evaluation.markdown import (
    render_capital_sensitivity_bundle_markdown,
    render_diagnostic_markdown,
)
from data_collect.evaluation.standard import build_capital_sensitivity, build_standard_evaluation


_FORWARD_HORIZONS = (5, 10, 20, 40)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _date_value(value: Any) -> date | None:
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


def build_report_scope(
    run: Mapping[str, Any],
    signals: Sequence[Mapping[str, Any]],
    performance: Sequence[Mapping[str, Any]],
) -> dict[str, str | None]:
    """Return the configured evaluation window separately from signal coverage.

    A strategy can stop emitting signals while the persisted equity curve keeps
    running (for example, after a data gate rejects every candidate).  The
    dashboard must not present the first/last signal date as the backtest
    evaluation window because that hides precisely this kind of coverage gap.
    """
    signal_dates = sorted(
        row.get("signal_date") for row in signals
        if isinstance(row.get("signal_date"), date)
    )
    performance_dates = sorted(
        row.get("trade_date") for row in performance
        if isinstance(row.get("trade_date"), date)
    )
    run_parameters = _json_object(run.get("parameters"))
    evaluation_parameters = _json_object(run_parameters.get("evaluation"))
    evaluation_start = _date_value(evaluation_parameters.get("evaluation_start"))
    evaluation_end = _date_value(evaluation_parameters.get("evaluation_end"))
    evaluation_start = evaluation_start or (performance_dates[0] if performance_dates else None)
    evaluation_end = evaluation_end or (performance_dates[-1] if performance_dates else None)
    return {
        "evaluation_date_from": _iso(evaluation_start),
        "evaluation_date_to": _iso(evaluation_end),
        "performance_date_from": _iso(performance_dates[0] if performance_dates else None),
        "performance_date_to": _iso(performance_dates[-1] if performance_dates else None),
        "signal_date_from": _iso(signal_dates[0] if signal_dates else None),
        "signal_date_to": _iso(signal_dates[-1] if signal_dates else None),
    }


def _round(value: Any, digits: int = 8) -> float | None:
    number = _float(value)
    return None if number is None else round(number, digits)


def _summary(values: Iterable[Any]) -> dict[str, Any]:
    numbers = [number for value in values if (number := _float(value)) is not None]
    if not numbers:
        return {"count": 0, "min": None, "max": None, "mean": None, "median": None}
    return {
        "count": len(numbers),
        "min": _round(min(numbers)),
        "max": _round(max(numbers)),
        "mean": _round(mean(numbers)),
        "median": _round(median(numbers)),
    }


def classify_rejection(order: Mapping[str, Any]) -> str:
    """Normalize a human-readable rejection into a stable diagnostic bucket."""
    if str(order.get("status", "")).upper() == "FILLED":
        return "FILLED"
    text = " ".join(
        str(order.get(key) or "")
        for key in ("reject_reason", "liquidity_reject_reason")
    )
    if any(token in text for token in ("T+1", "重算仓位", "重新定价")):
        return "EXECUTION_RECALCULATION"
    if any(token in text for token in ("最大持仓", "持仓数")):
        return "MAX_POSITIONS"
    if any(token in text for token in ("一手", "整手", "向下取整", "现金上限")):
        return "LOT_OR_CASH_LIMIT"
    if any(token in text for token in ("涨停", "跌停", "不可买入", "不可卖出")):
        return "PRICE_LIMIT_OR_NOT_TRADABLE"
    if any(token in text for token in ("停牌", "执行行情", "行情缺失")):
        return "SUSPENDED_OR_MISSING_BAR"
    if "已有持仓" in text or "重复买入" in text:
        return "ALREADY_HELD"
    if "流动性" in text or "成交量" in text or "成交额" in text:
        return "LIQUIDITY"
    return "OTHER"


def _exit_bucket(reason: Any, stop_loss_price: Any = None) -> str:
    raw = str(reason or "")
    if stop_loss_price is not None or "stop" in raw.lower() or "止损" in raw:
        return "STOP_LOSS_EXIT"
    if raw == "close_below_ma60" or "ma60" in raw.lower():
        return "MA60_EXIT"
    if raw == "rank_outside_hold_threshold" or "rank" in raw.lower() or "排名" in raw:
        return "RANK_EXIT"
    if raw == "financial_quality_invalidated" or "quality" in raw.lower() or "财务" in raw:
        return "QUALITY_EXIT"
    if raw == "st_status_invalidated" or "st" in raw.lower():
        return "ST_EXIT"
    if "listing" in raw.lower() or "上市" in raw or "market_eligibility" in raw.lower():
        return "UNIVERSE_EXIT"
    if "missing" in raw.lower() or "缺少" in raw:
        return "DATA_EXIT"
    return raw or "UNKNOWN_EXIT"


def summarize_rejections(orders: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_category: dict[str, int] = defaultdict(int)
    examples: dict[str, list[str]] = defaultdict(list)
    rejected = 0
    for order in orders:
        category = classify_rejection(order)
        by_category[category] += 1
        if category == "FILLED":
            continue
        rejected += 1
        reason = str(order.get("reject_reason") or order.get("liquidity_reject_reason") or "未记录")
        if reason not in examples[category] and len(examples[category]) < 3:
            examples[category].append(reason)
    return {
        "total_orders": len(orders),
        "filled_orders": by_category.get("FILLED", 0),
        "rejected_orders": rejected,
        "by_category": dict(sorted(by_category.items())),
        "examples": dict(sorted(examples.items())),
    }


def _allocated_fee(fill: Mapping[str, Any], field: str, shares: int) -> float:
    total = _float(fill.get(field)) or 0.0
    fill_shares = max(_int(fill.get("fill_shares"), 1), 1)
    return total * shares / fill_shares


def _trading_day_distance(trading_dates: Sequence[date], start: Any, end: Any) -> int | None:
    start_date = start if isinstance(start, date) and not isinstance(start, datetime) else None
    end_date = end if isinstance(end, date) and not isinstance(end, datetime) else None
    if start_date is None or end_date is None or not trading_dates:
        return None
    eligible = [item for item in trading_dates if start_date <= item <= end_date]
    return max(len(eligible) - 1, 0) if eligible else None


def build_trade_records(
    lots: Sequence[Mapping[str, Any]],
    fills: Sequence[Mapping[str, Any]],
    orders: Sequence[Mapping[str, Any]],
    signals: Sequence[Mapping[str, Any]],
    performance_dates: Sequence[date] = (),
) -> list[dict[str, Any]]:
    fills_by_id = {str(row.get("fill_id")): row for row in fills}
    orders_by_id = {str(row.get("order_id")): row for row in orders}
    signals_by_id = {str(row.get("signal_id")): row for row in signals}
    result: list[dict[str, Any]] = []
    for lot in lots:
        open_fill = fills_by_id.get(str(lot.get("open_fill_id")))
        close_fill = fills_by_id.get(str(lot.get("close_fill_id")))
        if not open_fill or not close_fill:
            continue
        shares = _int(lot.get("open_shares")) - _int(lot.get("remaining_shares"))
        if shares <= 0:
            continue
        buy_price = _float(open_fill.get("fill_price"))
        sell_price = _float(close_fill.get("fill_price"))
        if not buy_price or not sell_price:
            continue
        buy_cost_fee = sum(
            _allocated_fee(open_fill, field, shares)
            for field in ("commission", "stamp_duty", "transfer_fee")
        )
        sell_fee = sum(
            _allocated_fee(close_fill, field, shares)
            for field in ("commission", "stamp_duty", "transfer_fee")
        )
        dividend = (_float(lot.get("accrued_cash_dividend_per_share")) or 0.0) * shares
        buy_cost = buy_price * shares + buy_cost_fee
        sell_proceeds = sell_price * shares - sell_fee
        gross_pnl = (sell_price - buy_price) * shares
        net_pnl = sell_proceeds - buy_cost + dividend
        order = orders_by_id.get(str(close_fill.get("order_id")), {})
        signal = signals_by_id.get(str(order.get("signal_id")), {})
        opening_order = orders_by_id.get(str(open_fill.get("order_id")), {})
        opening_signal_id = opening_order.get("signal_id")
        reason = _json_object(signal.get("reason"))
        raw_reason = reason.get("rule") or reason.get("reason") or order.get("description")
        trade = {
            "lot_id": str(lot.get("lot_id")),
            "ts_code": lot.get("ts_code"),
            "buy_date": _iso(open_fill.get("fill_date")),
            "sell_date": _iso(close_fill.get("fill_date")),
            "shares": shares,
            "buy_price": _round(buy_price, 6),
            "sell_price": _round(sell_price, 6),
            "gross_pnl": _round(gross_pnl, 6),
            "gross_return": _round(sell_price / buy_price - 1.0),
            "buy_fees": _round(buy_cost_fee, 6),
            "sell_fees": _round(sell_fee, 6),
            "cash_dividend_income": _round(dividend, 6),
            "net_pnl": _round(net_pnl, 6),
            "net_return": _round(net_pnl / buy_cost if buy_cost else None),
            "exit_reason": _exit_bucket(raw_reason, order.get("stop_loss_price_raw")),
            "exit_rule": raw_reason or "UNKNOWN",
            "buy_signal_id": str(opening_signal_id) if opening_signal_id else None,
            "sell_signal_id": str(order.get("signal_id")) if order.get("signal_id") else None,
            "holding_calendar_days": None,
            "holding_trading_days": None,
        }
        buy_date = open_fill.get("fill_date")
        sell_date = close_fill.get("fill_date")
        if isinstance(buy_date, date) and isinstance(sell_date, date):
            trade["holding_calendar_days"] = (sell_date - buy_date).days
            trade["holding_trading_days"] = _trading_day_distance(performance_dates, buy_date, sell_date)
        entry_reason = _json_object(signals_by_id.get(str(opening_signal_id), {}).get("reason"))
        trade["entry_ret60"] = _round(entry_reason.get("ret60"))
        trade["entry_rank"] = entry_reason.get("rank")
        result.append(trade)
    return sorted(result, key=lambda row: (row.get("buy_date") or "", row.get("ts_code") or ""))


def summarize_trades(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    wins = [row for row in trades if (_float(row.get("net_pnl")) or 0.0) > 0]
    losses = [row for row in trades if (_float(row.get("net_pnl")) or 0.0) <= 0]
    return {
        "trade_count": len(trades),
        "winning_trades": len(wins),
        "losing_trades": len(losses),
        "win_rate": _round(len(wins) / len(trades) if trades else None),
        "net_pnl": _round(sum(_float(row.get("net_pnl")) or 0.0 for row in trades), 6),
        "average_net_pnl": _round(mean([_float(row.get("net_pnl")) or 0.0 for row in trades]) if trades else None, 6),
        "average_winner_pnl": _round(mean([_float(row.get("net_pnl")) or 0.0 for row in wins]) if wins else None, 6),
        "average_loser_pnl": _round(mean([_float(row.get("net_pnl")) or 0.0 for row in losses]) if losses else None, 6),
        "profit_factor": _round(
            sum(max(_float(row.get("net_pnl")) or 0.0, 0.0) for row in trades)
            / abs(sum(min(_float(row.get("net_pnl")) or 0.0, 0.0) for row in trades))
            if losses and sum(min(_float(row.get("net_pnl")) or 0.0, 0.0) for row in trades) < 0 else None
        ),
        "max_single_loss": _round(min([_float(row.get("net_pnl")) for row in trades if _float(row.get("net_pnl")) is not None], default=None), 6),
        "holding_trading_days": _summary(row.get("holding_trading_days") for row in trades),
    }


def summarize_exit_attribution(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for trade in trades:
        groups[str(trade.get("exit_reason") or "UNKNOWN_EXIT")].append(trade)
    return {
        reason: {
            "trade_count": len(rows),
            "net_pnl": _round(sum(_float(row.get("net_pnl")) or 0.0 for row in rows), 6),
            "average_net_return": _round(mean([_float(row.get("net_return")) or 0.0 for row in rows]) if rows else None),
        }
        for reason, rows in sorted(groups.items())
    }


def build_entry_diagnostics(
    trades: Sequence[Mapping[str, Any]],
    signal_by_id: Mapping[str, Mapping[str, Any]],
    entry_inputs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    inputs_by_signal = {str(row.get("signal_id")): row for row in entry_inputs}
    rows: list[dict[str, Any]] = []
    st_violations: list[dict[str, Any]] = []
    for trade in trades:
        signal_id = trade.get("buy_signal_id")
        order_signal = signal_by_id.get(str(signal_id), {})
        # buy_signal_id is the order id for legacy persisted runs; recover the
        # signal id from the normalized report field when available.
        if not order_signal and signal_id in signal_by_id:
            order_signal = signal_by_id[signal_id]
        reason = _json_object(order_signal.get("reason"))
        source = inputs_by_signal.get(str(order_signal.get("signal_id")), {})
        row = {
            "ts_code": trade.get("ts_code"),
            "buy_date": trade.get("buy_date"),
            "name": source.get("name"),
            "entry_ret60": _round(reason.get("ret60", trade.get("entry_ret60"))),
            "entry_rank": reason.get("rank", trade.get("entry_rank")),
            "avg_amount_20": _round(reason.get("avg_amount_20"), 2),
            "is_st_asof": source.get("is_st_asof"),
            "quality_status": source.get("quality_status"),
            "snapshot_id": source.get("snapshot_id") or order_signal.get("research_snapshot_id"),
            "input_row_found": bool(source),
        }
        rows.append(row)
        if row["is_st_asof"] is True:
            st_violations.append(row)
    return {
        "actual_entry_count": len(rows),
        "ret60": _summary(row.get("entry_ret60") for row in rows),
        "historical_st_violations": st_violations,
        "missing_signal_time_input": sum(not row["input_row_found"] for row in rows),
        "entries": rows,
    }


def summarize_forward_returns(
    trades: Sequence[Mapping[str, Any]],
    future_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    by_entry: dict[tuple[str, str], dict[int, Mapping[str, Any]]] = defaultdict(dict)
    for row in future_rows:
        key = (str(row.get("ts_code")), _iso(row.get("buy_date")) or "")
        horizon = _int(row.get("horizon"))
        if horizon in _FORWARD_HORIZONS:
            by_entry[key][horizon] = row
    result: dict[str, Any] = {}
    for horizon in _FORWARD_HORIZONS:
        observations: list[dict[str, Any]] = []
        for trade in trades:
            key = (str(trade.get("ts_code")), str(trade.get("buy_date")))
            future = by_entry.get(key, {}).get(horizon)
            future_close = _float(future.get("raw_close")) if future else None
            buy_price = _float(trade.get("buy_price"))
            if future_close is None or not buy_price:
                continue
            observations.append({
                "ts_code": trade.get("ts_code"),
                "buy_date": trade.get("buy_date"),
                "trade_date": _iso(future.get("trade_date")),
                "raw_close": _round(future_close, 6),
                "price_return_from_fill": _round(future_close / buy_price - 1.0),
                "sold_before_horizon": bool(
                    trade.get("sell_date") and str(trade.get("sell_date")) <= str(future.get("trade_date"))
                ),
            })
        result[f"horizon_{horizon}d"] = {
            "available_count": len(observations),
            "returns": _summary(row["price_return_from_fill"] for row in observations),
            "observations": observations,
        }
    return result


def build_equity_curve(
    performance: Sequence[Mapping[str, Any]],
    positions: Sequence[Mapping[str, Any]],
    benchmark: Sequence[Mapping[str, Any]],
    initial_cash: Any,
) -> list[dict[str, Any]]:
    """Build the chart-ready daily series without changing persisted facts."""
    cash = _float(initial_cash) or 0.0
    benchmark_by_date = {
        row.get("trade_date"): _float(row.get("close"))
        for row in benchmark
        if row.get("trade_date") is not None and _float(row.get("close")) is not None
    }
    first_benchmark = next(
        (benchmark_by_date.get(row.get("trade_date")) for row in performance
         if benchmark_by_date.get(row.get("trade_date")) not in (None, 0)),
        None,
    )
    position_count: dict[Any, int] = defaultdict(int)
    for row in positions:
        if _int(row.get("hold_shares")) > 0:
            position_count[row.get("trade_date")] += 1
    curve: list[dict[str, Any]] = []
    for row in performance:
        equity = _float(row.get("total_equity"))
        if equity is None:
            continue
        trade_date = row.get("trade_date")
        strategy_nav = equity / cash if cash else None
        benchmark_close = benchmark_by_date.get(trade_date)
        benchmark_nav = benchmark_close / first_benchmark if first_benchmark and benchmark_close else None
        curve.append({
            "date": _iso(trade_date),
            "nav": _round(strategy_nav),
            "benchmark_nav": _round(benchmark_nav),
            "excess_nav": _round(strategy_nav / benchmark_nav if strategy_nav and benchmark_nav else None),
            "drawdown": _round(row.get("drawdown")),
            "daily_return": _round(row.get("daily_return")),
            "cash": _round(row.get("cash_balance"), 6),
            "market_value": _round(row.get("position_value"), 6),
            "cash_ratio": _round((_float(row.get("cash_balance")) or 0.0) / equity if equity else None),
            "position_count": position_count.get(trade_date, 0),
            "valuation_status": row.get("valuation_status"),
            "decision_reason": _json_object(row.get("decision_reason")),
        })
    return curve


def build_decision_log(performance: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Expose the bounded daily decision explanations and their compact summary."""
    rows: list[dict[str, Any]] = []
    reason_counts: dict[str, int] = defaultdict(int)
    dominant_failure_counts: dict[str, int] = defaultdict(int)
    for row in performance:
        reason = _json_object(row.get("decision_reason"))
        if not reason:
            continue
        trade_date = _iso(row.get("trade_date"))
        rows.append({"trade_date": trade_date, "reason": reason})
        code = reason.get("no_action_reason_code") or reason.get("reason_code")
        if code:
            reason_counts[str(code)] += 1
        dominant = reason.get("dominant_failure_code")
        if dominant:
            dominant_failure_counts[str(dominant)] += 1
    return {
        "rows": rows,
        "summary": {
            "days": len(rows),
            "no_action_days": sum(
                count for code, count in reason_counts.items()
                if code.startswith("NO_") or code in {
                    "NOT_SCHEDULED_SIGNAL_DAY", "EVALUATION_END_NO_NEXT_TRADING_DAY",
                }
            ),
            "reason_counts": dict(sorted(reason_counts.items())),
            "dominant_failure_counts": dict(sorted(dominant_failure_counts.items())),
        },
    }


def aggregate_period_returns(performance: Sequence[Mapping[str, Any]], period: str) -> list[dict[str, Any]]:
    """Compound persisted daily returns into calendar-month/year bars."""
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in performance:
        trade_date = row.get("trade_date")
        daily_return = _float(row.get("daily_return"))
        if not isinstance(trade_date, date) or daily_return is None:
            continue
        key = trade_date.strftime("%Y-%m" if period == "month" else "%Y")
        groups[key].append(row)
    output = []
    for key, rows in sorted(groups.items()):
        position_values = [_float(row.get("position_value")) for row in rows]
        known_position_values = [value for value in position_values if value is not None]
        invested_days = sum(value > 0 for value in known_position_values)
        activity_status = (
            "UNKNOWN"
            if len(known_position_values) != len(rows)
            else "CASH_ONLY"
            if invested_days == 0
            else "INVESTED"
        )
        output.append({
            "period": key,
            "return": _round(math.prod(
                1.0 + _float(row["daily_return"]) for row in rows
            ) - 1.0),
            "observation_days": len(rows),
            "invested_days": invested_days if activity_status != "UNKNOWN" else None,
            "cash_only_days": len(rows) - invested_days if activity_status != "UNKNOWN" else None,
            "activity_status": activity_status,
        })
    return output


def build_report(
    run: Mapping[str, Any],
    signals: Sequence[Mapping[str, Any]],
    orders: Sequence[Mapping[str, Any]],
    fills: Sequence[Mapping[str, Any]],
    lots: Sequence[Mapping[str, Any]],
    performance: Sequence[Mapping[str, Any]],
    metrics: Sequence[Mapping[str, Any]],
    entry_inputs: Sequence[Mapping[str, Any]],
    future_rows: Sequence[Mapping[str, Any]],
    positions: Sequence[Mapping[str, Any]] = (),
    benchmark: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    signal_by_id = {str(row.get("signal_id")): row for row in signals}
    performance_dates = sorted({
        row.get("trade_date") for row in performance
        if isinstance(row.get("trade_date"), date) and not isinstance(row.get("trade_date"), datetime)
    })
    trades = build_trade_records(lots, fills, orders, signals, performance_dates)
    metrics_map = {
        str(row.get("metric_name")): _float(row.get("metric_value"))
        for row in metrics
        if row.get("metric_name")
    }
    entry_inputs = list(entry_inputs)
    entry = build_entry_diagnostics(trades, signal_by_id, entry_inputs)
    names_by_entry = {
        (row.get("ts_code"), row.get("buy_date")): row.get("name")
        for row in entry.get("entries", [])
    }
    for trade in trades:
        trade["name"] = names_by_entry.get((trade.get("ts_code"), trade.get("buy_date")))
    initial_cash = run.get("initial_cash")
    run_parameters = _json_object(run.get("parameters"))
    evaluation_parameters = _json_object(run_parameters.get("evaluation"))
    try:
        rolling_window = int(evaluation_parameters.get("rolling_window_trading_days", 252))
        rolling_step = int(evaluation_parameters.get("rolling_step_trading_days", 21))
        regime_window = int(evaluation_parameters.get("market_regime_ma_window", 120))
    except (TypeError, ValueError):
        rolling_window, rolling_step, regime_window = 252, 21, 120
    benchmark_series = [
        {"trade_date": _iso(row.get("trade_date")), "ts_code": row.get("ts_code"), "close": _round(row.get("close"), 6)}
        for row in benchmark
    ]
    return {
        "report_version": "backtest-diagnostics-v2",
        "read_only": True,
        "run": {
            key: _iso(value) if key.endswith("_at") else value
            for key, value in run.items()
        },
        "scope": build_report_scope(run, signals, performance),
        "row_counts": {
            "signals": len(signals), "orders": len(orders), "fills": len(fills),
            "position_lots": len(lots), "performance_daily": len(performance),
        },
        "equity_curve": build_equity_curve(performance, positions, benchmark, initial_cash),
        "decision_log": build_decision_log(performance),
        "monthly_returns": aggregate_period_returns(performance, "month"),
        "annual_returns": aggregate_period_returns(performance, "year"),
        "benchmark_series": benchmark_series,
        "evaluation": build_standard_evaluation(
            performance,
            benchmark,
            rolling_window_trading_days=rolling_window,
            rolling_step_trading_days=rolling_step,
            market_regime_ma_window=regime_window,
        ),
        "order_rejections": summarize_rejections(orders),
        "trade_summary": summarize_trades(trades),
        "exit_attribution": summarize_exit_attribution(trades),
        "entry_diagnostics": entry,
        "forward_returns": summarize_forward_returns(trades, future_rows),
        "trades": trades,
        "persisted_metrics": metrics_map,
        "limitations": [
            "交易归因由 position_lot 的 open_fill_id/close_fill_id 重建；部分成交或一笔成交对应多个 lot 时按 lot 分摊费用并保留原始 fill 引用。",
            "forward_returns 使用执行成交价到未来 execution bar raw_close 的纯价格路径，不代表策略净收益，也不使用未来信息生成历史信号。",
            "当前回测没有持久化独立 realized_trade 表，因此报告不新增持久化副本。",
        ],
    }


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"backtest-diagnostics-{label}-{uuid4().hex}",
        job_name="backtest_diagnostics",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    return kernel, BacktestRunRepository(kernel), context


def diagnose_run(run_id: str) -> dict[str, Any]:
    kernel, repository, context = _service("read")
    try:
        run = repository.diagnostic_run(run_id, context)
        if not run:
            raise RuntimeError(f"未找到 Backtest Run（回测运行）: {run_id}")
        signals = repository.diagnostic_signals(run_id, context)
        orders = repository.diagnostic_orders(run_id, context)
        fills = repository.diagnostic_fills(run_id, context)
        lots = repository.diagnostic_lots(run_id, context)
        performance = repository.diagnostic_performance(run_id, context)
        positions = repository.diagnostic_positions(run_id, context)
        metrics = repository.diagnostic_metrics(run_id, context)
        signal_dates = [row.get("signal_date") for row in signals if isinstance(row.get("signal_date"), date)]
        entry_inputs = (
            repository.diagnostic_entry_inputs(
                run_id, min(signal_dates), max(signal_dates), context
            )
            if signal_dates else []
        )
        actual_entries = []
        orders_by_id = {str(row.get("order_id")): row for row in orders}
        fills_by_order: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for fill in fills:
            fills_by_order[str(fill.get("order_id"))].append(fill)
        for fill_list in fills_by_order.values():
            for fill in fill_list:
                if str(fill.get("direction")).upper() == "BUY":
                    actual_entries.append((str(fill.get("ts_code")), fill.get("fill_date")))
        actual_entries = [row for row in actual_entries if isinstance(row[1], date)]
        future_rows = repository.diagnostic_future_closes(
            str(run["dataset_id"]), actual_entries,
            min(row[1] for row in actual_entries) if actual_entries else date.today(),
            context,
        )
        performance_dates = [
            row.get("trade_date") for row in performance
            if isinstance(row.get("trade_date"), date)
        ]
        benchmark_start = min(performance_dates) if performance_dates else date.today()
        evaluation_parameters = _json_object(_json_object(run.get("parameters")).get("evaluation"))
        configured_warmup = _date_value(evaluation_parameters.get("warmup_start"))
        if configured_warmup is not None:
            benchmark_start = min(benchmark_start, configured_warmup)
        benchmark = (
            repository.diagnostic_benchmark(
                str(run["dataset_id"]), benchmark_start, max(performance_dates), context
            )
            if performance_dates else []
        )
        return build_report(
            run, signals, orders, fills, lots, performance, metrics, entry_inputs,
            future_rows, positions, benchmark,
        )
    finally:
        kernel.close()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Read-only Backtest Run diagnostic report")
    parser.add_argument("--run-id", required=True, help="Backtest Run UUID")
    parser.add_argument(
        "--format", choices=("json", "markdown"), default="json",
        help="Report format; JSON remains the default for backward compatibility",
    )
    parser.add_argument(
        "--output",
        help="Optional output file. Markdown is written as UTF-8; otherwise stdout is used",
    )
    parser.add_argument(
        "--capital-run-ids",
        help="Optional comma-separated capital-sensitivity Run IDs to append to Markdown",
    )
    args = parser.parse_args(argv)
    report = diagnose_run(args.run_id)
    capital_sensitivity = None
    capital_reports = [report]
    if args.capital_run_ids:
        known_ids = {str((report.get("run") or {}).get("run_id"))}
        for run_id in args.capital_run_ids.split(","):
            normalized = run_id.strip()
            if normalized and normalized not in known_ids:
                capital_reports.append(diagnose_run(normalized))
                known_ids.add(normalized)
        capital_sensitivity = build_capital_sensitivity(capital_reports)
    if args.format == "markdown":
        output = (
            render_capital_sensitivity_bundle_markdown(capital_reports, capital_sensitivity)
            if capital_sensitivity is not None
            else render_diagnostic_markdown(report)
        )
    else:
        payload: Any = report
        if capital_sensitivity is not None:
            payload = {"report": report, "capital_sensitivity": capital_sensitivity}
        output = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    if args.output:
        Path(args.output).write_text(output + "\n", encoding="utf-8")
        return
    print(output)


if __name__ == "__main__":
    main()
