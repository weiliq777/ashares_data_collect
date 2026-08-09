"""人工确认的纸面组合引擎。

它不生成研究信号、不连接券商、不发送订单。只有调用方明确批准的
``PaperIntent`` 才能进入模拟成交；所有成交、现金和持仓变化都保留为事件/投影。
"""
from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import date
from typing import Any, Mapping

from data_collect.backtest.risk import FeeRule

from .models import PaperFill, PaperIntent, PaperLot, PortfolioConfig, PortfolioResult


class PaperPortfolioEngine:
    """只做人工确认成交的确定性 Portfolio 模拟器。"""

    def __init__(self, config: PortfolioConfig | None = None):
        self.config = config or PortfolioConfig()
        self.cash = float(self.config.initial_cash)
        self.available_cash = float(self.config.initial_cash)
        self._pending_cash: list[tuple[date, float]] = []
        self._lots: dict[str, list[PaperLot]] = {}
        self._intents: dict[str, PaperIntent] = {}
        self._fills: list[PaperFill] = []
        self._cash_ledger: list[dict[str, Any]] = [{
            "trade_date": None,
            "event_type": "INIT",
            "amount": float(self.config.initial_cash),
            "balance": float(self.config.initial_cash),
            "available_date": None,
            "reference_id": None,
            "description": "模拟组合初始现金",
        }]
        self._performance_daily: list[dict[str, Any]] = []
        self._warnings: list[str] = []

    @staticmethod
    def _id(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4()}"

    def register_intent(self, intent: PaperIntent) -> PaperIntent:
        if intent.intent_id in self._intents:
            raise ValueError(f"重复的 Portfolio intent: {intent.intent_id}")
        self._intents[intent.intent_id] = intent
        return intent

    def approve_intent(self, intent_id: str, approved_by: str) -> PaperIntent:
        if not approved_by or not approved_by.strip():
            raise ValueError("模拟成交必须记录人工批准人")
        intent = self._get_intent(intent_id)
        if intent.status != "PENDING_APPROVAL":
            raise ValueError(f"只有待批准 intent 可以批准: {intent_id} ({intent.status})")
        updated = replace(intent, status="APPROVED")
        self._intents[intent_id] = updated
        return updated

    def reject_intent(self, intent_id: str, reason: str) -> PaperIntent:
        intent = self._get_intent(intent_id)
        if intent.status != "PENDING_APPROVAL":
            raise ValueError(f"只有待批准 intent 可以拒绝: {intent_id} ({intent.status})")
        updated = replace(intent, status="REJECTED", reason={**dict(intent.reason), "rejection": reason})
        self._intents[intent_id] = updated
        return updated

    def execute_approved_fill(
        self,
        intent_id: str,
        fill_date: date,
        fill_price: float,
        next_trade_date: date | None = None,
        cash_available_date: date | None = None,
        fill_shares: int | None = None,
        reference_price: float | None = None,
    ) -> PaperFill:
        intent = self._get_intent(intent_id)
        if intent.status != "APPROVED":
            raise ValueError(f"只有人工批准的 intent 可以模拟成交: {intent_id} ({intent.status})")
        if fill_date < intent.intended_execution_date:
            raise ValueError("成交日期不能早于计划成交日期")
        if fill_price <= 0:
            raise ValueError("成交价必须为正")
        shares = int(fill_shares if fill_shares is not None else intent.requested_shares)
        if shares != intent.requested_shares:
            raise ValueError("Portfolio v1 不模拟部分成交，成交股数必须等于请求股数")
        if shares <= 0:
            raise ValueError("成交股数必须为正")
        self._release_cash(fill_date)

        if intent.direction == "BUY":
            fill = self._execute_buy(intent, fill_date, fill_price, shares, next_trade_date, reference_price)
        else:
            fill = self._execute_sell(intent, fill_date, fill_price, shares, cash_available_date, reference_price)
        self._intents[intent_id] = replace(intent, status="EXECUTED")
        return fill

    def _execute_buy(
        self,
        intent: PaperIntent,
        fill_date: date,
        fill_price: float,
        shares: int,
        next_trade_date: date | None,
        reference_price: float | None,
    ) -> PaperFill:
        if shares % self.config.min_buy_lot != 0:
            raise ValueError(f"BUY 必须按 {self.config.min_buy_lot} 股整手")
        if self._active_shares(intent.ts_code) > 0:
            raise ValueError("Portfolio v1 禁止对已有持仓加仓，避免向下摊平/隐式金字塔加仓")
        if intent.stop_loss_price_raw is not None and fill_price <= intent.stop_loss_price_raw:
            raise ValueError("成交价不高于止损价，拒绝模拟买入")
        if self.config.t_plus_one and next_trade_date is None:
            raise ValueError("T+1 组合必须显式提供下一个交易日")
        if self.config.t_plus_one and next_trade_date is not None and next_trade_date <= fill_date:
            raise ValueError("next_trade_date must be later than fill_date for T+1")

        fee_rule = self.config.fee_rule
        value = fill_price * shares
        commission = fee_rule.commission(value)
        transfer = value * fee_rule.transfer_fee_rate
        total = value + commission + transfer
        equity_base = (
            float(self._performance_daily[-1]["total_equity"])
            if self._performance_daily
            else float(self.config.initial_cash)
        )
        cash_budget = max(
            0.0,
            self.available_cash - equity_base * self.config.reserve_cash_pct,
        )
        if total > cash_budget + 1e-9:
            raise ValueError("买入后会突破可用现金/预留现金约束")

        fill = PaperFill(
            fill_id=self._id("pf"), intent_id=intent.intent_id, ts_code=intent.ts_code,
            fill_date=fill_date, direction="BUY", fill_price=fill_price,
            fill_shares=shares, commission=commission, stamp_duty=0.0,
            transfer_fee=transfer,
            slippage_cost=abs(fill_price - reference_price) * shares if reference_price else 0.0,
        )
        self._fills.append(fill)
        self.cash -= total
        self.available_cash -= total
        self._append_cash(fill_date, -total, "BUY", fill.fill_id, "人工批准的模拟买入")
        sell_date = next_trade_date if self.config.t_plus_one else fill_date
        lot = PaperLot(
            lot_id=self._id("lot"), ts_code=intent.ts_code, open_date=fill_date,
            available_sell_date=sell_date or fill_date, open_shares=shares,
            remaining_shares=shares, cost_price=fill_price,
            stop_loss_price_raw=intent.stop_loss_price_raw,
        )
        self._lots.setdefault(intent.ts_code, []).append(lot)
        return fill

    def _execute_sell(
        self,
        intent: PaperIntent,
        fill_date: date,
        fill_price: float,
        shares: int,
        cash_available_date: date | None,
        reference_price: float | None,
    ) -> PaperFill:
        if not self.config.allow_sell_odd_lot and shares % self.config.min_buy_lot != 0:
            raise ValueError("当前组合规则不允许卖出零股")
        active = [lot for lot in self._lots.get(intent.ts_code, ()) if lot.remaining_shares > 0]
        if sum(lot.remaining_shares for lot in active) < shares:
            raise ValueError("可卖持仓不足")
        if any(lot.available_sell_date > fill_date for lot in active if lot.remaining_shares > 0):
            raise ValueError("存在尚未满足 T+1 的持仓")

        fee_rule = self.config.fee_rule
        value = fill_price * shares
        commission = fee_rule.commission(value)
        stamp = value * fee_rule.stamp_duty_sell_rate
        transfer = value * fee_rule.transfer_fee_rate
        proceeds = value - commission - stamp - transfer
        if self.config.cash_settlement_days:
            if cash_available_date is None:
                raise ValueError("卖出资金结算日必须显式提供")
            if cash_available_date <= fill_date:
                raise ValueError("cash_available_date must be later than fill_date when settlement is deferred")
        available_date = cash_available_date or fill_date
        if available_date < fill_date:
            raise ValueError("卖出资金可用日不能早于成交日")

        fill = PaperFill(
            fill_id=self._id("pf"), intent_id=intent.intent_id, ts_code=intent.ts_code,
            fill_date=fill_date, direction="SELL", fill_price=fill_price,
            fill_shares=shares, commission=commission, stamp_duty=stamp,
            transfer_fee=transfer,
            slippage_cost=abs(fill_price - reference_price) * shares if reference_price else 0.0,
        )
        self._fills.append(fill)
        self.cash += proceeds
        if available_date <= fill_date:
            self.available_cash += proceeds
        else:
            self._pending_cash.append((available_date, proceeds))
        self._append_cash(fill_date, proceeds, "SELL", fill.fill_id, "人工批准的模拟卖出", available_date)

        remaining = shares
        for lot in sorted(active, key=lambda item: (item.open_date, item.lot_id)):
            if remaining <= 0:
                break
            sold = min(lot.remaining_shares, remaining)
            lot.remaining_shares -= sold
            remaining -= sold
            if lot.remaining_shares == 0:
                lot.close_date = fill_date
                lot.close_fill_id = fill.fill_id
        return fill

    def mark_to_market(self, trade_date: date, prices: Mapping[str, float]) -> dict[str, Any]:
        self._release_cash(trade_date)
        position_value = 0.0
        position_rows = []
        status = "VALID"
        for code, lots in sorted(self._lots.items()):
            active = [lot for lot in lots if lot.remaining_shares > 0]
            if not active:
                continue
            price = prices.get(code)
            shares = sum(lot.remaining_shares for lot in active)
            average_cost = sum(lot.remaining_shares * lot.cost_price for lot in active) / shares
            if price is None or price <= 0:
                status = "MISSING_PRICE"
                position_rows.append({
                    "trade_date": trade_date, "ts_code": code, "hold_shares": shares,
                    "average_cost": average_cost, "raw_close": None, "market_value": None,
                    "unrealized_pnl": None, "valuation_status": "MISSING_PRICE",
                })
                continue
            market_value = shares * float(price)
            position_value += market_value
            position_rows.append({
                "trade_date": trade_date, "ts_code": code, "hold_shares": shares,
                "average_cost": average_cost, "raw_close": float(price),
                "market_value": market_value, "unrealized_pnl": market_value - shares * average_cost,
                "valuation_status": "VALID",
            })
        equity = self.cash + position_value
        previous = self._performance_daily[-1]["total_equity"] if self._performance_daily else self.config.initial_cash
        daily_return = equity / previous - 1 if previous else 0.0
        peak = max([row["total_equity"] for row in self._performance_daily] + [self.config.initial_cash])
        row = {
            "trade_date": trade_date, "cash_balance": self.cash,
            "position_value": position_value, "total_equity": equity,
            "daily_return": daily_return, "cumulative_return": equity / self.config.initial_cash - 1,
            "drawdown": (peak - equity) / peak if peak else 0.0,
            "turnover": sum(f.fill_price * f.fill_shares for f in self._fills if f.fill_date == trade_date),
            "valuation_status": status,
            "positions": tuple(position_rows),
        }
        self._performance_daily.append(row)
        return row

    def result(self) -> PortfolioResult:
        positions = tuple({
            "lot_id": lot.lot_id, "ts_code": lot.ts_code, "open_date": lot.open_date,
            "available_sell_date": lot.available_sell_date, "open_shares": lot.open_shares,
            "remaining_shares": lot.remaining_shares, "cost_price": lot.cost_price,
            "stop_loss_price_raw": lot.stop_loss_price_raw, "close_date": lot.close_date,
            "close_fill_id": lot.close_fill_id,
        } for lots in self._lots.values() for lot in lots)
        return PortfolioResult(
            status="SUCCESS", cash=self.cash, available_cash=self.available_cash,
            positions=positions,
            fills=tuple(fill.__dict__ for fill in self._fills),
            cash_ledger=tuple(self._cash_ledger),
            performance_daily=tuple(self._performance_daily),
            warnings=tuple(self._warnings),
        )

    def _get_intent(self, intent_id: str) -> PaperIntent:
        try:
            return self._intents[intent_id]
        except KeyError as exc:
            raise ValueError(f"不存在的 Portfolio intent: {intent_id}") from exc

    def _active_shares(self, ts_code: str) -> int:
        return sum(lot.remaining_shares for lot in self._lots.get(ts_code, ()) if lot.remaining_shares > 0)

    def _release_cash(self, day: date):
        pending = []
        for available_date, amount in self._pending_cash:
            if available_date <= day:
                self.available_cash += amount
            else:
                pending.append((available_date, amount))
        self._pending_cash = pending

    def _append_cash(
        self, day: date, amount: float, event_type: str, reference_id: str,
        description: str, available_date: date | None = None,
    ):
        self._cash_ledger.append({
            "trade_date": day, "event_type": event_type, "amount": float(amount),
            "balance": float(self.cash), "available_date": available_date or day,
            "reference_id": reference_id, "description": description,
        })
