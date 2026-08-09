"""阶段三风控和市场规则纯函数。"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np


@dataclass(frozen=True)
class FeeRule:
    commission_rate: float = 0.0003
    min_commission: float = 5.0
    stamp_duty_sell_rate: float = 0.0005
    transfer_fee_rate: float = 0.00001
    slippage_bps: float = 0.0

    def __post_init__(self):
        if any(value < 0 for value in (
            self.commission_rate, self.min_commission,
            self.stamp_duty_sell_rate, self.transfer_fee_rate, self.slippage_bps,
        )):
            raise ValueError("费用和滑点参数不能为负")

    def commission(self, value: float) -> float:
        return max(float(value) * self.commission_rate, self.min_commission)

    def buy_cost(self, value: float) -> float:
        return self.commission(value) + float(value) * self.transfer_fee_rate

    def sell_cost(self, value: float) -> float:
        return (
            self.commission(value)
            + float(value) * self.stamp_duty_sell_rate
            + float(value) * self.transfer_fee_rate
        )


@dataclass(frozen=True)
class RiskPolicy:
    """版本化的压力风险口径。"""

    risk_policy_version: str = "risk-gap-v1"
    entry_slippage_bps: float = 0.0
    exit_slippage_bps: float = 0.0
    gap_stress_pct: float = 0.0

    def __post_init__(self):
        if not self.risk_policy_version.strip():
            raise ValueError("risk_policy_version 不能为空")
        values = (self.entry_slippage_bps, self.exit_slippage_bps, self.gap_stress_pct)
        if any(not math.isfinite(float(value)) or float(value) < 0 for value in values):
            raise ValueError("风险压力参数必须是有限非负数")
        if self.gap_stress_pct > 1:
            raise ValueError("gap_stress_pct 必须位于 [0,1] 内")


@dataclass(frozen=True)
class MarketRule:
    market: str = "主板"
    min_buy_lot: int = 100
    allow_sell_odd_lot: bool = True
    t_plus_one: bool = True
    price_tick: float = 0.01

    cash_settlement_days: int = 0
    max_volume_participation_pct: float = 1.0
    liquidity_policy: str = "reject_full_order"

    def __post_init__(self):
        if self.min_buy_lot <= 0 or self.price_tick <= 0 or self.cash_settlement_days < 0:
            raise ValueError("市场规则的交易单位、最小价位或资金结算日无效")
        if not math.isfinite(float(self.max_volume_participation_pct)) or not 0 < self.max_volume_participation_pct <= 1:
            raise ValueError("max_volume_participation_pct 必须位于 (0,1] 内")
        if self.liquidity_policy != "reject_full_order":
            raise ValueError("v1 只支持 reject_full_order 流动性策略")

    def round_price(self, price: float) -> float:
        if price <= 0:
            return price
        return round(round(price / self.price_tick) * self.price_tick, 8)

    def round_adverse(self, price: float, direction: str) -> float:
        """Round a stressed/fill price in the adverse direction."""
        if price <= 0:
            return price
        units = float(price) / float(self.price_tick)
        if direction == "BUY":
            rounded_units = math.ceil(units - 1e-12)
        elif direction == "SELL":
            rounded_units = math.floor(units + 1e-12)
        else:
            raise ValueError("direction 必须是 BUY 或 SELL")
        return round(rounded_units * float(self.price_tick), 8)


@dataclass(frozen=True)
class MarketTemperatureScaler:
    """按全市场估值分位数调整风险预算。

    这是 Gemini 阶段三建议的纯函数适配器，第一版回测不会隐式启用它；正式
    启用前必须把市场温度输入作为版本化 Dataset 字段冻结。
    """

    extreme_hot_threshold: float = 0.85
    hot_threshold: float = 0.70
    neutral_threshold: float = 0.30
    cold_threshold: float = 0.15
    extreme_hot_multiplier: float = 0.25
    hot_multiplier: float = 0.50
    neutral_multiplier: float = 1.00
    cold_multiplier: float = 1.25
    extreme_cold_multiplier: float = 1.50

    def __post_init__(self):
        thresholds = (
            self.cold_threshold, self.neutral_threshold,
            self.hot_threshold, self.extreme_hot_threshold,
        )
        if any(value < 0 or value > 1 for value in thresholds):
            raise ValueError("市场温度阈值必须位于 [0, 1]")
        if not (
            self.cold_threshold <= self.neutral_threshold
            <= self.hot_threshold <= self.extreme_hot_threshold
        ):
            raise ValueError("市场温度阈值必须按冷到热递增")
        if any(value < 0 for value in (
            self.extreme_hot_multiplier, self.hot_multiplier,
            self.neutral_multiplier, self.cold_multiplier,
            self.extreme_cold_multiplier,
        )):
            raise ValueError("市场温度倍率不能为负数")

    def multiplier(self, pe_percentile_5y: float) -> float:
        percentile = float(pe_percentile_5y)
        if not 0 <= percentile <= 1:
            raise ValueError("PE 历史分位数必须位于 [0, 1]")
        if percentile >= self.extreme_hot_threshold:
            return self.extreme_hot_multiplier
        if percentile >= self.hot_threshold:
            return self.hot_multiplier
        if percentile >= self.neutral_threshold:
            return self.neutral_multiplier
        if percentile >= self.cold_threshold:
            return self.cold_multiplier
        return self.extreme_cold_multiplier

    def adjust_risk_percent(
        self,
        base_risk_pct: float,
        pe_percentile_5y: float | None,
        *,
        missing_policy: str = "reject",
    ) -> float:
        if base_risk_pct < 0:
            raise ValueError("base_risk_pct 不能为负数")
        if pe_percentile_5y is None:
            if missing_policy == "unchanged":
                return float(base_risk_pct)
            if missing_policy != "reject":
                raise ValueError("missing_policy 只能是 reject 或 unchanged")
            raise ValueError("市场温度缺失，拒绝隐式使用未调整风险预算")
        if missing_policy not in {"reject", "unchanged"}:
            raise ValueError("missing_policy 只能是 reject 或 unchanged")
        return float(base_risk_pct) * self.multiplier(pe_percentile_5y)


@dataclass(frozen=True)
class PositionSizingResult:
    eligible: bool
    theoretical_shares: float
    planned_shares: int
    actual_shares: int
    required_capital: float
    max_loss_at_stop: float
    gap_stress_loss: float
    buy_fees: float
    estimated_sell_fees: float
    estimated_slippage: float
    actual_risk_pct: float
    risk_policy_version: str
    reject_reason: str

    @property
    def shares(self) -> int:
        """兼容旧调用方；正式审计使用 planned/actual_shares。"""
        return self.actual_shares

    @property
    def reason(self) -> str:
        return self.reject_reason

    def as_dict(self) -> dict[str, Any]:
        return {
            "theoretical_shares": self.theoretical_shares,
            "planned_shares": self.planned_shares,
            "actual_shares": self.actual_shares,
            "required_capital": self.required_capital,
            "max_loss_at_stop": self.max_loss_at_stop,
            "gap_stress_loss": self.gap_stress_loss,
            "buy_fees": self.buy_fees,
            "estimated_sell_fees": self.estimated_sell_fees,
            "estimated_slippage": self.estimated_slippage,
            "actual_risk_pct": self.actual_risk_pct,
            "risk_policy_version": self.risk_policy_version,
            "eligible": self.eligible,
            "reject_reason": self.reject_reason,
        }


@dataclass(frozen=True)
class LiquidityCheck:
    allowed: bool
    volume_capacity_shares: int | None
    liquidity_status: str
    reject_reason: str | None = None


def check_liquidity(
    volume: float | None,
    requested_shares: int,
    market_rule: MarketRule,
    *,
    amount: float | None = None,
    close_price: float | None = None,
    low_price: float | None = None,
    high_price: float | None = None,
) -> LiquidityCheck:
    """按真实成交量做整笔流动性门禁；v1 不缩量成交。"""
    if requested_shares <= 0:
        return LiquidityCheck(False, 0, "INVALID_REQUEST", "请求成交股数必须为正")
    if volume is None or not math.isfinite(float(volume)):
        return LiquidityCheck(False, None, "MISSING_VOLUME", "成交量缺失或非有限")
    if volume <= 0:
        return LiquidityCheck(False, 0, "ZERO_VOLUME", "成交量为零，拒绝整笔订单")
    if amount is None:
        return LiquidityCheck(False, None, "MISSING_AMOUNT", "成交额缺失，拒绝整笔订单")
    if amount is not None and not math.isfinite(float(amount)):
        return LiquidityCheck(False, None, "MISSING_AMOUNT", "成交额缺失或非有限")
    if amount is not None and amount <= 0:
        return LiquidityCheck(False, 0, "ZERO_AMOUNT", "成交额为零，拒绝整笔订单")
    if amount is not None and close_price is not None and close_price > 0:
        average_price = float(amount) / float(volume)
        lower = float(low_price or close_price) * 0.5
        upper = float(high_price or close_price) * 1.5
        if not math.isfinite(average_price) or average_price < lower or average_price > upper:
            return LiquidityCheck(
                False, None, "UNIT_SUSPECTED",
                "成交额/成交量隐含价格异常，可能存在单位错误",
            )
    capacity = math.floor(float(volume) * market_rule.max_volume_participation_pct)
    if not market_rule.allow_sell_odd_lot:
        capacity = capacity // market_rule.min_buy_lot * market_rule.min_buy_lot
    if capacity < requested_shares:
        return LiquidityCheck(
            False, capacity, "REJECTED_VOLUME_CAPACITY",
            f"订单 {requested_shares} 股超过成交量容量 {capacity} 股，v1 不缩量成交",
        )
    return LiquidityCheck(True, capacity, "VALID", None)


def size_position(
    available_cash: float,
    risk_pct: float,
    entry_price: float,
    stop_loss_price: float,
    fee_rule: FeeRule | None = None,
    market_rule: MarketRule | None = None,
    equity_base: float | None = None,
    max_position_value: float | None = None,
    max_risk_amount: float | None = None,
    risk_policy: RiskPolicy | None = None,
) -> PositionSizingResult:
    """按风险、资金和交易费用共同约束计算 A 股买入数量。

    风险基数默认使用当前可用现金；组合引擎可以传入总权益作为 ``equity_base``。
    费用包含买入费用以及止损卖出费用，避免只控制价差而低估实际损失。
    """
    fee_rule = fee_rule or FeeRule()
    market_rule = market_rule or MarketRule()
    risk_policy = risk_policy or RiskPolicy(
        entry_slippage_bps=fee_rule.slippage_bps,
        exit_slippage_bps=fee_rule.slippage_bps,
    )
    invalid = lambda reason, theoretical=0.0: PositionSizingResult(
        False, float(theoretical), 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        risk_policy.risk_policy_version, reason,
    )
    if available_cash <= 0 or risk_pct <= 0 or entry_price <= 0:
        return invalid("资金、风险比例和买入价必须为正")
    if stop_loss_price <= 0 or entry_price <= stop_loss_price:
        return invalid("买入价必须高于有效止损价")

    risk_base = float(equity_base if equity_base is not None else available_cash)
    max_risk = risk_base * risk_pct
    if max_risk_amount is not None:
        max_risk = min(max_risk, float(max_risk_amount))
    if max_risk <= 0:
        return invalid("可用组合风险预算已耗尽")
    buy_slippage_per_share = float(entry_price) * float(risk_policy.entry_slippage_bps) / 10000.0
    sell_slippage_per_share = float(stop_loss_price) * float(risk_policy.exit_slippage_bps) / 10000.0
    gap_stress_per_share = float(stop_loss_price) * float(risk_policy.gap_stress_pct)
    pressure_buy_price = market_rule.round_adverse(
        float(entry_price) + buy_slippage_per_share, "BUY"
    )
    pressure_sell_price = market_rule.round_adverse(max(
        float(market_rule.price_tick), float(stop_loss_price) - sell_slippage_per_share
    ), "SELL")
    actual_buy_slippage_per_share = max(0.0, pressure_buy_price - float(entry_price))
    actual_sell_slippage_per_share = max(0.0, float(stop_loss_price) - pressure_sell_price)
    stress_loss_per_share = (
        max(0.0, pressure_buy_price - pressure_sell_price) + gap_stress_per_share
    )
    if stress_loss_per_share <= 0:
        return invalid("压力风险损失必须为正")
    theoretical = max_risk / stress_loss_per_share
    shares = math.floor(theoretical / market_rule.min_buy_lot) * market_rule.min_buy_lot
    if max_position_value is not None and max_position_value > 0:
        shares = min(
            shares,
            math.floor((max_position_value / pressure_buy_price) / market_rule.min_buy_lot)
            * market_rule.min_buy_lot,
        )

    while shares >= market_rule.min_buy_lot:
        buy_value = shares * pressure_buy_price
        sell_value = shares * pressure_sell_price
        buy_fees = fee_rule.buy_cost(buy_value)
        sell_fees = fee_rule.sell_cost(sell_value)
        required = buy_value + buy_fees
        estimated_slippage = shares * (
            actual_buy_slippage_per_share + actual_sell_slippage_per_share
        )
        gap_stress_loss = shares * gap_stress_per_share
        max_loss = (
            shares * max(0.0, float(entry_price) - float(stop_loss_price))
            + estimated_slippage + gap_stress_loss + buy_fees + sell_fees
        )
        if required <= available_cash and max_loss <= max_risk:
            return PositionSizingResult(
                True, float(theoretical), shares, shares, round(required, 6), round(max_loss, 6),
                round(gap_stress_loss, 6), round(buy_fees, 6), round(sell_fees, 6),
                round(estimated_slippage, 6), round(max_loss / risk_base, 8),
                risk_policy.risk_policy_version, "",
            )
        shares -= market_rule.min_buy_lot

    return invalid(
        f"向下取整后不足一手或压力费用后超过风险/现金上限（理论股数 {theoretical:.2f}）",
        theoretical,
    )


def open_position_stop_risk(
    shares: int,
    cost_price: float,
    stop_loss_price: float | None,
    fee_rule: FeeRule,
    market_rule: MarketRule,
    risk_policy: RiskPolicy,
) -> float:
    """Estimate remaining downside of an existing lot under the approved policy.

    Entry cost is already sunk.  Remaining portfolio risk therefore includes
    adverse exit slippage, the configured gap stress and estimated sell fees.
    """
    if shares <= 0 or stop_loss_price is None:
        return 0.0
    stop = float(stop_loss_price)
    if stop <= 0 or cost_price <= 0:
        return 0.0
    pressured_exit = market_rule.round_adverse(max(
        market_rule.price_tick,
        stop * (1.0 - float(risk_policy.exit_slippage_bps) / 10000.0),
    ), "SELL")
    per_share_loss = max(0.0, float(cost_price) - pressured_exit)
    per_share_loss += stop * float(risk_policy.gap_stress_pct)
    sell_value = pressured_exit * int(shares)
    return per_share_loss * int(shares) + fee_rule.sell_cost(sell_value)


class MonteCarloTester:
    """对已完成交易的 R 倍数做可复现压力测试，不替代历史回测。"""

    @staticmethod
    def run_ruin_test(
        r_multiples: Sequence[float],
        initial_capital: float = 10000.0,
        risk_pct: float = 0.02,
        num_simulations: int = 1000,
        sequence_length: int | None = None,
        ruin_threshold_pct: float = 0.50,
        random_seed: int = 42,
        method: str = "bootstrap_with_replacement",
    ) -> dict[str, Any]:
        values = [float(item) for item in r_multiples]
        input_trade_sequence_hash = hashlib.sha256(
            json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        if method not in {"bootstrap_with_replacement", "permutation_without_replacement"}:
            raise ValueError("Monte Carlo method 不受支持")
        if not values:
            parameters = {
                "initial_capital": initial_capital,
                "risk_pct": risk_pct,
                "num_simulations": num_simulations,
                "sequence_length": sequence_length,
                "ruin_threshold_pct": ruin_threshold_pct,
                "random_seed": random_seed,
                "method": method,
            }
            return {
                "status": "NO_TRADES",
                "simulations": 0,
                "method": method,
                "input_trade_sequence_hash": input_trade_sequence_hash,
                "parameters": parameters,
                "parameters_hash": hashlib.sha256(
                    json.dumps(parameters, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest(),
            }
        if initial_capital <= 0 or not 0 < risk_pct < 1 or num_simulations <= 0:
            raise ValueError("Monte Carlo 参数无效")
        length = int(sequence_length or len(values))
        if length <= 0:
            raise ValueError("sequence_length 必须为正")
        if method == "permutation_without_replacement" and length != len(values):
            raise ValueError("permutation_without_replacement 必须使用全部交易样本")

        rng = np.random.default_rng(random_seed)
        ruin_barrier = initial_capital * (1 - ruin_threshold_pct)
        max_drawdowns: list[float] = []
        finals: list[float] = []
        ruined = 0
        arr = np.asarray(values, dtype=float)
        for _ in range(num_simulations):
            if method == "bootstrap_with_replacement":
                sequence = rng.choice(arr, size=length, replace=True)
            else:
                sequence = rng.permutation(arr)
            capital = float(initial_capital)
            peak = capital
            max_dd = 0.0
            for multiple in sequence:
                capital += capital * risk_pct * float(multiple)
                peak = max(peak, capital)
                max_dd = max(max_dd, (peak - capital) / peak if peak else 1.0)
                if capital <= ruin_barrier:
                    ruined += 1
                    max_dd = max(max_dd, ruin_threshold_pct)
                    break
            max_drawdowns.append(max_dd)
            finals.append(max(0.0, capital))
        parameters = {
            "initial_capital": float(initial_capital),
            "risk_pct": float(risk_pct),
            "num_simulations": int(num_simulations),
            "sequence_length": int(length),
            "ruin_threshold_pct": float(ruin_threshold_pct),
            "random_seed": int(random_seed),
            "method": method,
        }
        return {
            "status": "OK",
            "simulations": num_simulations,
            "random_seed": random_seed,
            "method": method,
            "input_trade_sequence_hash": input_trade_sequence_hash,
            "parameters": parameters,
            "parameters_hash": hashlib.sha256(
                json.dumps(parameters, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest(),
            "ruin_probability": ruined / num_simulations,
            "expected_max_drawdown": float(np.mean(max_drawdowns)),
            "percentile_95_drawdown": float(np.percentile(max_drawdowns, 95)),
            "median_final_capital": float(np.median(finals)),
            "expectancy_r": float(np.mean(arr)),
            "win_rate_in_r": float(np.mean(arr > 0)),
        }
