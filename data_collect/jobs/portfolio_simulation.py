"""Portfolio v1：人工批准的模拟组合持仓、成交和每日估值。

本模块不生成买卖信号、不访问券商、不发送订单。它只接受人工批准的
``order_intent``，然后在数据库事务内写入模拟成交和账本；估值输入必须
明确绑定一个 FROZEN Research Snapshot。
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping
import uuid

from data_collect.backtest.risk import FeeRule
from data_collect.jobs.backtest_dataset import _git_metadata, _json_param
from data_collect.portfolio.models import PortfolioConfig
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.context import WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.portfolio_repository import PortfolioRepository

# Historical source contracts retained as comments; runtime DB access is Repository -> Kernel only.
# SELECT status, research_cutoff_date; INSERT INTO portfolio.account_manifest; portfolio.snapshot_binding
# portfolio.cash_ledger; portfolio.audit_event; SELECT MAX(fill_date); SELECT MAX(trade_date)
# 卖出资金结算日必须显式提供
# 'PENDING_APPROVAL'
# available_cash - equity_base * float(reserve_pct)
# 必须严格对应 FROZEN Snapshot 截止日
# FROZEN Snapshot 原始收盘价不一致
# FROM portfolio.performance_daily


def _round_price(price: float, tick: float) -> float:
    if price <= 0:
        raise ValueError("价格必须为正")
    rounded = round(round(price / tick) * tick, 8)
    if abs(price - rounded) > 1e-8:
        raise ValueError(f"价格 {price} 不符合最小价位 {tick}")
    return rounded


def _fee_rule(row) -> FeeRule:
    return FeeRule(
        commission_rate=float(row[10]),
        min_commission=float(row[11]),
        stamp_duty_sell_rate=float(row[12]),
        transfer_fee_rate=float(row[13]),
        slippage_bps=float(row[14]),
    )


def _valuation_input_hash(
    snapshot_id: str,
    valuation_date: date,
    cash: float,
    lots,
    prices: Mapping[str, float],
) -> str:
    """Hash the exact portfolio state and frozen prices used for one valuation day."""
    payload = {
        "snapshot_id": str(snapshot_id),
        "valuation_date": valuation_date.isoformat(),
        "cash": round(float(cash), 8),
        "positions": sorted(
            (
                str(row[3]),
                str(row[0]),
                int(row[1]),
                round(float(row[2]), 8),
            )
            for row in lots
        ),
        "prices": sorted((str(code), round(float(price), 8)) for code, price in prices.items()),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(run_id=f"portfolio-{label}-{uuid.uuid4().hex}", job_name="portfolio_simulation", code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1")
    return kernel, PortfolioRepository(kernel), context


def _account(portfolio_id: str, repository: PortfolioRepository, context: RunContext):
    return repository.account(portfolio_id, context)


def create_portfolio(portfolio_name: str, portfolio_version: str, source_snapshot_id: str, effective_date: date, config: PortfolioConfig | None = None, source_watchlist_id: str | None = None, code_archive_path: str | None = None) -> str:
    config = config or PortfolioConfig()
    metadata = _git_metadata()
    if metadata["dirty"] and not code_archive_path:
        raise RuntimeError("创建正式模拟组合需要干净 Git，或提供存在的 code_archive_path")
    if code_archive_path and not Path(code_archive_path).exists():
        raise FileNotFoundError(code_archive_path)
    portfolio_id = str(uuid.uuid4())
    kernel, repository, context = _service("create")
    try:
        snapshot = repository.snapshot(source_snapshot_id, context)
        if not snapshot or snapshot["status"] != "FROZEN":
            raise ValueError(f"Portfolio 只能引用 FROZEN Research Snapshot: {source_snapshot_id}")
        if effective_date > snapshot["research_cutoff_date"]:
            raise ValueError("Portfolio effective_date 不能晚于绑定 Snapshot 截止日")
        if source_watchlist_id:
            watchlist = repository.watchlist(source_watchlist_id, context)
            if not watchlist or str(watchlist["snapshot_id"]) != str(source_snapshot_id) or watchlist["status"] != "ACTIVE":
                raise ValueError("Portfolio 必须引用同一 FROZEN Snapshot 下的 ACTIVE 观察池")
        scope = repository._scope(portfolio_id, date_from=effective_date, date_to=effective_date)
        def persist_portfolio():
            repository.create_portfolio({"portfolio_id": portfolio_id, "portfolio_name": portfolio_name, "portfolio_version": portfolio_version, "source_snapshot_id": source_snapshot_id, "source_watchlist_id": source_watchlist_id, "initial_cash": config.initial_cash, "min_buy_lot": config.min_buy_lot, "allow_sell_odd_lot": config.allow_sell_odd_lot, "t_plus_one": config.t_plus_one, "reserve_cash_pct": config.reserve_cash_pct, "price_tick": config.price_tick, "commission_rate": config.fee_rule.commission_rate, "min_commission": config.fee_rule.min_commission, "stamp_duty_sell_rate": config.fee_rule.stamp_duty_sell_rate, "transfer_fee_rate": config.fee_rule.transfer_fee_rate, "slippage_bps": config.fee_rule.slippage_bps, "cash_settlement_days": config.cash_settlement_days, "status": "DRAFT", "code_version": metadata["code_version"], "git_commit": metadata["commit"], "code_archive_path": code_archive_path}, context)
            repository.activate_portfolio(portfolio_id, context)
            repository.bind_snapshot(portfolio_id, effective_date, source_snapshot_id, context)
            repository.append_cash({"portfolio_id": portfolio_id, "trade_date": effective_date, "event_type": "INIT", "amount": config.initial_cash, "balance": config.initial_cash, "available_date": effective_date, "description": "模拟组合初始现金"}, context)
            repository.append_audit({"portfolio_id": portfolio_id, "event_date": effective_date, "event_type": "PORTFOLIO_ACTIVATED", "payload": json.dumps({"snapshot_id": source_snapshot_id, "watchlist_id": source_watchlist_id, "code_version": metadata["code_version"]}, ensure_ascii=False)}, context)
        repository.atomic(context, scope, persist_portfolio, estimated_rows=5)
        return portfolio_id
    finally:
        kernel.close()


def create_intent(portfolio_id: str, snapshot_id: str, ts_code: str, direction: str, decision_date: date, intended_execution_date: date, requested_shares: int, reference_price: float | None = None, stop_loss_price_raw: float | None = None, watchlist_id: str | None = None, source_type: str = "MANUAL", reason: Mapping[str, Any] | None = None) -> str:
    if direction not in {"BUY", "SELL"} or requested_shares <= 0 or intended_execution_date < decision_date:
        raise ValueError("Intent 参数不满足 BUY/SELL、正股数和日期顺序约束")
    intent_id = str(uuid.uuid4())
    kernel, repository, context = _service("intent")
    try:
        repository.account(portfolio_id, context)
        def persist_intent():
            repository.create_intent({"intent_id": intent_id, "portfolio_id": portfolio_id, "snapshot_id": snapshot_id, "watchlist_id": watchlist_id, "ts_code": ts_code, "decision_date": decision_date, "intended_execution_date": intended_execution_date, "direction": direction, "requested_shares": requested_shares, "reference_price": reference_price, "stop_loss_price_raw": stop_loss_price_raw, "source_type": source_type, "reason": json.dumps(dict(reason or {}), ensure_ascii=False)}, context)
            repository.append_audit({"portfolio_id": portfolio_id, "event_date": decision_date, "event_type": "INTENT_CREATED", "intent_id": intent_id, "payload": json.dumps({"source_type": source_type}, ensure_ascii=False)}, context)
        repository.atomic(context, repository._scope(portfolio_id), persist_intent, estimated_rows=2)
        return intent_id
    finally:
        kernel.close()


def import_backtest_order(portfolio_id: str, backtest_order_id: str, *, actor: str = "backtest-import") -> str:
    kernel, repository, context = _service("import")
    try:
        account = repository.account(portfolio_id, context)
        if account["status"] != "ACTIVE":
            raise ValueError("只有 ACTIVE Portfolio 才能导入 Backtest 订单")
        order = repository.backtest_order(backtest_order_id, context)
        if not order:
            raise ValueError(f"不存在的 Backtest 订单: {backtest_order_id}")
        if order["run_status"] != "SUCCEEDED" or order["dataset_status"] != "FROZEN" or order["status"] != "FILLED":
            raise ValueError("只能导入 SUCCEEDED/FROZEN 数据集中的 FILLED Backtest 订单")
        source_snapshot = account["source_snapshot_id"]
        snapshot = repository.snapshot(str(source_snapshot), context)
        if not snapshot or snapshot["status"] != "FROZEN" or order["decision_date"] != snapshot["research_cutoff_date"]:
            raise ValueError("Backtest 订单决策日必须与 Portfolio 绑定 Snapshot cutoff 相同")
        if repository.dataset_member_snapshot(str(order["dataset_id"]), order["decision_date"], context) != str(source_snapshot):
            raise ValueError("Backtest 订单决策日没有绑定 Portfolio Snapshot")
        if account["source_watchlist_id"] and not repository.watchlist_contains(str(account["source_watchlist_id"]), str(source_snapshot), order["ts_code"], context):
            raise ValueError("Backtest 订单标的不存在于 Portfolio ACTIVE 观察池")
        source_hash = str(order["order_audit_hash"] or "").strip()
        if len(source_hash) != 64:
            raise ValueError("Backtest order 缺少 order_audit_hash")
        existing = repository.existing_import(portfolio_id, backtest_order_id, context)
        if existing:
            if str(existing["source_order_hash"]) != source_hash:
                raise ValueError("同一 Backtest 订单 source hash 已变化，禁止覆盖原 Intent")
            return str(existing["intent_id"])
        requested = int(order["actual_shares"] or order["order_shares"])
        if requested <= 0:
            raise ValueError("FILLED Backtest 订单缺少有效实际成交股数")
        intent_id = str(uuid.uuid4())
        def persist_import():
            repository.create_intent({"intent_id": intent_id, "portfolio_id": portfolio_id, "snapshot_id": source_snapshot, "watchlist_id": account["source_watchlist_id"], "ts_code": order["ts_code"], "decision_date": order["decision_date"], "intended_execution_date": order["order_date"], "direction": order["direction"], "requested_shares": requested, "reference_price": order["reference_price"], "stop_loss_price_raw": order["stop_loss_price_raw"], "source_type": "BACKTEST", "status": "PENDING_APPROVAL", "reason": json.dumps({"air_gap": "BACKTEST_TO_PENDING_APPROVAL", "actor": actor, "source_order_hash": source_hash}, ensure_ascii=False), "source_backtest_run_id": order["run_id"], "source_backtest_order_id": order["order_id"], "source_dataset_id": order["dataset_id"], "source_signal_id": order["signal_id"], "source_order_hash": source_hash}, context)
            repository.append_audit({"portfolio_id": portfolio_id, "event_date": order["decision_date"], "event_type": "BACKTEST_ORDER_IMPORTED_PENDING_APPROVAL", "intent_id": intent_id, "payload": json.dumps({"source_backtest_order_id": str(order["order_id"]), "source_order_hash": source_hash}, ensure_ascii=False)}, context)
        repository.atomic(context, repository._scope(portfolio_id), persist_import, estimated_rows=2)
        return intent_id
    finally:
        kernel.close()


def set_intent_status(portfolio_id: str, intent_id: str, status: str, actor: str | None = None, reason: str | None = None):
    if status not in {"APPROVED", "REJECTED", "CANCELLED", "EXPIRED"}:
        raise ValueError(f"不支持的人工意图状态: {status}")
    kernel, repository, context = _service("status")
    try:
        repository.account(portfolio_id, context)
        def persist_status():
            row = repository.update_intent_status(portfolio_id, intent_id, status, actor, context)
            if not row:
                raise ValueError("Intent 不存在")
            repository.append_audit({"portfolio_id": portfolio_id, "event_date": row["decision_date"], "event_type": f"INTENT_{status}", "intent_id": intent_id, "payload": json.dumps({"actor": actor, "reason": reason}, ensure_ascii=False)}, context)
        repository.atomic(context, repository._scope(portfolio_id), persist_status, estimated_rows=2)
    finally:
        kernel.close()


def record_manual_fill(portfolio_id: str, intent_id: str, fill_date: date, fill_price: float, next_trade_date: date | None = None, cash_available_date: date | None = None, reference_price: float | None = None) -> str:
    kernel, repository, context = _service("fill")
    try:
        account = repository.account(portfolio_id, context)
        if account["status"] != "ACTIVE":
            raise ValueError("Portfolio 不是 ACTIVE 状态")
        intent = repository.intent(portfolio_id, intent_id, context)
        if not intent or intent["status"] != "APPROVED":
            raise ValueError("只有 APPROVED intent 可以模拟成交")
        if fill_date < intent["intended_execution_date"] or fill_price <= 0:
            raise ValueError("成交日期或价格无效")
        fill_price = _round_price(float(fill_price), float(account["price_tick"]))
        requested = int(intent["requested_shares"])
        if intent["direction"] == "BUY" and requested % int(account["min_buy_lot"]) != 0:
            raise ValueError("Portfolio BUY 必须按整手")
        if intent["direction"] == "BUY" and account["t_plus_one"] and next_trade_date is None:
            raise ValueError("T+1 买入必须提供 next_trade_date")
        lots = repository.lots(portfolio_id, intent["ts_code"], context)
        total_cash, available_cash = repository.cash_totals(portfolio_id, fill_date, context)
        equity_base = repository.latest_equity(portfolio_id, fill_date, context) or float(account["initial_cash"])
        fee = FeeRule(commission_rate=float(account["commission_rate"]), min_commission=float(account["min_commission"]), stamp_duty_sell_rate=float(account["stamp_duty_sell_rate"]), transfer_fee_rate=float(account["transfer_fee_rate"]), slippage_bps=float(account["slippage_bps"]))
        value = fill_price * requested
        commission, transfer = fee.commission(value), value * fee.transfer_fee_rate
        stamp = value * fee.stamp_duty_sell_rate if intent["direction"] == "SELL" else 0.0
        if intent["direction"] == "BUY":
            if lots:
                raise ValueError("Portfolio v1 禁止已有持仓加仓")
            total_cost = value + commission + transfer
            if total_cost > max(0.0, available_cash - equity_base * float(account["reserve_cash_pct"])) + 1e-9:
                raise ValueError("买入后现金不足或突破预留现金")
            available_date = next_trade_date if account["t_plus_one"] else fill_date
            cash_amount = -total_cost
        else:
            if sum(int(row["remaining_shares"]) for row in lots) < requested:
                raise ValueError("可卖持仓不足")
            if any(row["available_sell_date"] > fill_date for row in lots):
                raise ValueError("存在尚不可卖的 T+1 持仓")
            available_date = cash_available_date or fill_date
            if account["cash_settlement_days"] and available_date <= fill_date:
                raise ValueError("递延结算必须提供晚于成交日的 cash_available_date")
            cash_amount = value - commission - stamp - transfer
        fill_id = str(uuid.uuid4())
        def persist_fill():
            repository.append_fill({"fill_id": fill_id, "portfolio_id": portfolio_id, "intent_id": intent_id, "ts_code": intent["ts_code"], "fill_date": fill_date, "direction": intent["direction"], "fill_price": fill_price, "fill_shares": requested, "commission": commission, "stamp_duty": stamp, "transfer_fee": transfer, "slippage_cost": abs(fill_price - float(reference_price)) * requested if reference_price else 0.0}, context)
            if intent["direction"] == "BUY":
                repository.append_lot({"lot_id": str(uuid.uuid4()), "portfolio_id": portfolio_id, "ts_code": intent["ts_code"], "open_fill_id": fill_id, "open_date": fill_date, "available_sell_date": available_date, "open_shares": requested, "remaining_shares": requested, "cost_price": fill_price, "stop_loss_price_raw": intent["stop_loss_price_raw"]}, context)
            else:
                remaining = requested
                for lot in lots:
                    if remaining <= 0:
                        break
                    sold = min(remaining, int(lot["remaining_shares"]))
                    remaining -= sold
                    repository.update_lot(str(lot["lot_id"]), {"remaining_shares": int(lot["remaining_shares"]) - sold, "close_date": fill_date if int(lot["remaining_shares"]) == sold else None, "close_fill_id": fill_id if int(lot["remaining_shares"]) == sold else None}, portfolio_id, context)
            total_balance = total_cash + cash_amount
            if total_balance < -1e-8:
                raise ValueError("成交后现金余额不能为负")
            repository.append_cash({"portfolio_id": portfolio_id, "trade_date": fill_date, "event_type": intent["direction"], "amount": cash_amount, "balance": total_balance, "available_date": available_date, "reference_id": fill_id, "description": "人工批准的模拟成交"}, context)
            repository.update_intent_executed(intent_id, portfolio_id, context)
            repository.append_audit({"portfolio_id": portfolio_id, "event_date": fill_date, "event_type": "FILL_RECORDED", "intent_id": intent_id, "fill_id": fill_id, "payload": json.dumps({"direction": intent["direction"], "shares": requested, "paper_only": True}, ensure_ascii=False)}, context)
        repository.atomic(context, repository._scope(portfolio_id), persist_fill, estimated_rows=8)
        return fill_id
    finally:
        kernel.close()


def mark_to_market(portfolio_id: str, valuation_date: date, snapshot_id: str, prices: Mapping[str, float]) -> dict[str, Any]:
    kernel, repository, context = _service("valuation")
    try:
        account = repository.account(portfolio_id, context)
        if account["status"] not in {"ACTIVE", "ARCHIVED"}:
            raise ValueError("只有 ACTIVE/ARCHIVED Portfolio 可以估值")
        snapshot = repository.snapshot(snapshot_id, context)
        if not snapshot or snapshot["status"] != "FROZEN" or snapshot["research_cutoff_date"] != valuation_date:
            raise ValueError("估值日必须严格对应 FROZEN Snapshot cutoff")
        if not repository.binding_exists(portfolio_id, valuation_date, snapshot_id, context):
            raise ValueError("估值 Snapshot 尚未绑定到 Portfolio 当日")
        state = repository.valuation_state(portfolio_id, valuation_date, context)
        if state["latest_fill"] is not None and state["latest_fill"] > valuation_date:
            raise ValueError("不能在未来成交已写入后回补过去估值")
        if state["latest_valuation"] is not None and state["latest_valuation"] > valuation_date:
            raise ValueError("估值必须单调前进")
        lots = state["lots"]
        codes = sorted({row["ts_code"] for row in lots})
        snapshot_prices = repository.snapshot_prices(snapshot_id, valuation_date, codes, context)
        for code in codes:
            if code not in snapshot_prices or code not in prices or prices[code] <= 0 or abs(float(prices[code]) - snapshot_prices[code]) > 1e-8:
                raise ValueError(f"估值价格与 FROZEN Snapshot 不一致: {code}")
        prices = {code: snapshot_prices[code] for code in codes}
        cash, position_value, position_rows = state["cash"], 0.0, []
        for code in codes:
            code_lots = [row for row in lots if row["ts_code"] == code]
            shares = sum(int(row["remaining_shares"]) for row in code_lots)
            avg = sum(int(row["remaining_shares"]) * float(row["cost_price"]) for row in code_lots) / shares
            value = shares * prices[code]
            position_value += value
            position_rows.append({"portfolio_id": portfolio_id, "source_snapshot_id": snapshot_id, "trade_date": valuation_date, "ts_code": code, "hold_shares": shares, "average_cost": avg, "raw_close": prices[code], "market_value": value, "unrealized_pnl": value - shares * avg, "valuation_input_hash": "", "valuation_status": "VALID"})
        total_equity = cash + position_value
        input_hash = _valuation_input_hash(snapshot_id, valuation_date, cash, [(row["ts_code"], row["remaining_shares"], row["cost_price"], row["lot_id"]) for row in lots], prices)
        existing = repository.existing_valuation(portfolio_id, valuation_date, context)
        if existing:
            if str(existing["source_snapshot_id"]) != str(snapshot_id) or existing["valuation_input_hash"] != input_hash or repository.position_count(portfolio_id, valuation_date, context) != len(position_rows):
                raise ValueError("同一组合/日期已有不同估值输入，禁止覆盖")
            return {"portfolio_id": portfolio_id, "trade_date": valuation_date, "cash_balance": float(existing["cash_balance"]), "position_value": float(existing["position_value"]), "total_equity": float(existing["total_equity"]), "daily_return": existing["daily_return"], "drawdown": existing["drawdown"], "valuation_input_hash": input_hash, "idempotent": True}
        for row in position_rows:
            row["valuation_input_hash"] = input_hash
        previous = repository.previous_equity(portfolio_id, valuation_date, float(account["initial_cash"]), context)
        peak = repository.peak_equity(portfolio_id, valuation_date, float(account["initial_cash"]), context)
        daily_return = total_equity / previous - 1 if previous else 0.0
        drawdown = (peak - total_equity) / peak if peak else 0.0
        def persist_valuation():
            repository.append_position_daily(position_rows, context)
            repository.append_performance({"portfolio_id": portfolio_id, "source_snapshot_id": snapshot_id, "trade_date": valuation_date, "cash_balance": cash, "position_value": position_value, "total_equity": total_equity, "daily_return": daily_return, "cumulative_return": total_equity / float(account["initial_cash"]) - 1, "drawdown": drawdown, "turnover": 0.0, "valuation_input_hash": input_hash, "valuation_status": "VALID"}, context)
            repository.append_audit({"portfolio_id": portfolio_id, "event_date": valuation_date, "event_type": "MARK_TO_MARKET", "payload": json.dumps({"snapshot_id": snapshot_id, "valuation_input_hash": input_hash, "position_count": len(position_rows)}, ensure_ascii=False)}, context)
        repository.atomic(context, repository._scope(portfolio_id, date_from=valuation_date, date_to=valuation_date), persist_valuation, estimated_rows=len(position_rows) + 2)
        return {"portfolio_id": portfolio_id, "trade_date": valuation_date, "cash_balance": cash, "position_value": position_value, "total_equity": total_equity, "daily_return": daily_return, "drawdown": drawdown, "valuation_input_hash": input_hash, "idempotent": False}
    finally:
        kernel.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="人工确认 Portfolio 模拟组合")
    parser.add_argument("--portfolio-id", required=True)
    parser.add_argument("--snapshot-id")
    parser.add_argument("--valuation-date")
    parser.add_argument("--import-backtest-order-id", help="只导入为 BACKTEST/PENDING_APPROVAL，不批准、不成交")
    parser.add_argument("--actor", default="backtest-import")
    parser.add_argument("--price", action="append", default=[], help="TS_CODE=PRICE，可重复")
    args = parser.parse_args(argv)
    if args.import_backtest_order_id:
        print(json.dumps(import_backtest_order(
            args.portfolio_id, args.import_backtest_order_id, actor=args.actor
        ), ensure_ascii=False, default=str))
        return
    if not args.snapshot_id or not args.valuation_date:
        parser.error("估值模式必须同时提供 --snapshot-id 和 --valuation-date")
    prices = {}
    for item in args.price:
        code, value = item.split("=", 1)
        prices[code] = float(value)
    print(json.dumps(mark_to_market(
        args.portfolio_id, date.fromisoformat(args.valuation_date), args.snapshot_id, prices
    ), ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
