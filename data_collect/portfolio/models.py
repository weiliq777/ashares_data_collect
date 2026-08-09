"""Portfolio 模拟组合的纯数据模型，不访问数据库和网络。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping

from data_collect.backtest.risk import FeeRule


@dataclass(frozen=True)
class PortfolioConfig:
    initial_cash: float = 10000.0
    min_buy_lot: int = 100
    allow_sell_odd_lot: bool = True
    t_plus_one: bool = True
    reserve_cash_pct: float = 0.10
    price_tick: float = 0.01
    cash_settlement_days: int = 0
    fee_rule: FeeRule = field(default_factory=FeeRule)

    def __post_init__(self):
        if self.initial_cash <= 0:
            raise ValueError("initial_cash 必须为正")
        if self.min_buy_lot <= 0:
            raise ValueError("min_buy_lot 必须为正")
        if not 0 <= self.reserve_cash_pct < 1:
            raise ValueError("reserve_cash_pct 必须在 [0,1) 内")
        if self.price_tick <= 0:
            raise ValueError("price_tick must be positive")
        if self.cash_settlement_days < 0:
            raise ValueError("cash_settlement_days 不能为负")


@dataclass(frozen=True)
class PaperIntent:
    intent_id: str
    ts_code: str
    direction: str
    decision_date: date
    intended_execution_date: date
    requested_shares: int
    reference_price: float | None = None
    stop_loss_price_raw: float | None = None
    reason: Mapping[str, Any] = field(default_factory=dict)
    status: str = "PENDING_APPROVAL"

    def __post_init__(self):
        if self.intended_execution_date < self.decision_date:
            raise ValueError("intended_execution_date cannot precede decision_date")
        if self.direction not in {"BUY", "SELL"}:
            raise ValueError("Portfolio 只允许 BUY/SELL")
        if self.requested_shares <= 0:
            raise ValueError("requested_shares 必须为正")


@dataclass
class PaperLot:
    lot_id: str
    ts_code: str
    open_date: date
    available_sell_date: date
    open_shares: int
    remaining_shares: int
    cost_price: float
    stop_loss_price_raw: float | None = None
    close_date: date | None = None
    close_fill_id: str | None = None


@dataclass(frozen=True)
class PaperFill:
    fill_id: str
    intent_id: str
    ts_code: str
    fill_date: date
    direction: str
    fill_price: float
    fill_shares: int
    commission: float
    stamp_duty: float
    transfer_fee: float
    slippage_cost: float = 0.0


@dataclass(frozen=True)
class PortfolioResult:
    status: str
    cash: float
    available_cash: float
    positions: tuple[Mapping[str, Any], ...]
    fills: tuple[Mapping[str, Any], ...]
    cash_ledger: tuple[Mapping[str, Any], ...]
    performance_daily: tuple[Mapping[str, Any], ...]
    warnings: tuple[str, ...] = ()
