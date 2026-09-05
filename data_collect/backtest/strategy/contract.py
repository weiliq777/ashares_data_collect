"""版本化的 Strategy（策略）、Scenario（交易场景）和 Evaluation（评价）契约。

这些对象只负责描述和生成稳定哈希，不访问数据库，也不保存运行中的账户状态。
三类身份分开后，修改资金规模、费用或评价口径不会改变策略规则本身。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import math
from typing import Any, Mapping


def _canonical(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): _canonical(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("哈希输入不能包含 NaN 或无穷大")
        return value
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(
        _canonical(value), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class StrategyVersionSpec:
    """策略规则身份；不包含 Scenario 或 Evaluation 参数。"""

    strategy_id: str
    strategy_version: str
    parameters: Mapping[str, Any]

    def __post_init__(self):
        if not self.strategy_id.strip() or not self.strategy_version.strip():
            raise ValueError("strategy_id 和 strategy_version 不能为空")

    def as_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "parameters": dict(self.parameters),
        }

    @property
    def strategy_hash(self) -> str:
        return canonical_hash({"kind": "strategy", **self.as_dict()})


@dataclass(frozen=True)
class ScenarioConfig:
    """交易资金、撮合和费用假设；不承载选股规则。"""

    scenario_id: str = "scen_a_share_10k_v1"
    initial_capital: float = 10000.0
    target_positions: int = 3
    target_position_ratio: float = 0.30
    max_entry_position_ratio: float = 0.35
    forced_cash_reserve_ratio: float = 0.0
    buy_lot_size: int = 100
    signal_time: str = "CLOSE"
    execution_time: str = "NEXT_TRADING_DAY_OPEN"
    rebalance_existing_positions: bool = False
    leftover_cash: str = "KEEP_AS_CASH"
    slippage_bps_per_side: float = 10.0
    broker_commission_rate: float = 0.0003
    broker_minimum_commission: float = 5.0
    fee_schedule_id: str = "cn-a-share-fee-schedule-v1"

    def __post_init__(self):
        if not self.scenario_id.strip():
            raise ValueError("scenario_id 不能为空")
        if self.initial_capital <= 0 or self.target_positions <= 0:
            raise ValueError("资金规模和目标持仓数必须为正")
        if not 0 < self.target_position_ratio <= self.max_entry_position_ratio <= 1:
            raise ValueError("目标仓位比例必须满足 0 < target <= max_entry <= 1")
        if not 0 <= self.forced_cash_reserve_ratio < 1:
            raise ValueError("forced_cash_reserve_ratio 必须位于 [0,1)")
        if self.target_positions * self.target_position_ratio > 1 - self.forced_cash_reserve_ratio:
            raise ValueError("目标持仓总比例超过扣除强制现金后的可用权益")
        if self.buy_lot_size != 100:
            raise ValueError("V1 的 A 股买入单位固定为 100 股")
        if self.slippage_bps_per_side < 0:
            raise ValueError("slippage_bps_per_side 不能为负")
        if self.broker_commission_rate < 0 or self.broker_minimum_commission < 0:
            raise ValueError("佣金参数不能为负")
        if self.signal_time != "CLOSE" or self.execution_time != "NEXT_TRADING_DAY_OPEN":
            raise ValueError("V1 只支持收盘信号、下一交易日开盘执行")
        if self.leftover_cash != "KEEP_AS_CASH":
            raise ValueError("V1 剩余资金必须保留为现金")
        if self.rebalance_existing_positions:
            raise ValueError("V1 不对未触发卖出的已有持仓做仓位再平衡")

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "initial_capital": self.initial_capital,
            "target_positions": self.target_positions,
            "target_position_ratio": self.target_position_ratio,
            "max_entry_position_ratio": self.max_entry_position_ratio,
            "forced_cash_reserve_ratio": self.forced_cash_reserve_ratio,
            "buy_lot_size": self.buy_lot_size,
            "signal_time": self.signal_time,
            "execution_time": self.execution_time,
            "rebalance_existing_positions": self.rebalance_existing_positions,
            "leftover_cash": self.leftover_cash,
            "slippage_bps_per_side": self.slippage_bps_per_side,
            "broker_commission_rate": self.broker_commission_rate,
            "broker_minimum_commission": self.broker_minimum_commission,
            "fee_schedule_id": self.fee_schedule_id,
        }

    @property
    def scenario_hash(self) -> str:
        return canonical_hash({"kind": "scenario", **self.as_dict()})


@dataclass(frozen=True)
class EvaluationConfig:
    """评价区间、基准和指标公式；不改变历史交易信号。"""

    evaluation_start: date = date(2021, 9, 1)
    evaluation_end: date = date(2026, 8, 21)
    warmup_start: date = date(2021, 8, 2)
    primary_benchmark: str = "CSI300_TOTAL_RETURN"
    fallback_benchmark: str = "CSI300_PRICE"
    return_frequency: str = "DAILY"
    annualization_factor: int = 252
    risk_free_rate: float = 0.0
    sharpe_ddof: int = 1

    def __post_init__(self):
        if not self.warmup_start <= self.evaluation_start <= self.evaluation_end:
            raise ValueError("评价区间和预热区间无效")
        if self.return_frequency != "DAILY" or self.annualization_factor != 252:
            raise ValueError("V1 Sharpe 只支持日收益率和 252 年化因子")
        if self.risk_free_rate != 0 or self.sharpe_ddof != 1:
            raise ValueError("V1 Sharpe 固定无风险利率 0 和样本标准差 ddof=1")
        if not self.primary_benchmark or not self.fallback_benchmark:
            raise ValueError("基准不能为空")
        if (
            self.primary_benchmark != "CSI300_TOTAL_RETURN"
            or self.fallback_benchmark != "CSI300_PRICE"
        ):
            raise ValueError("V1 基准固定为沪深 300 全收益，缺失时显式降级为价格指数")

    def as_dict(self) -> dict[str, Any]:
        return {
            "evaluation_start": self.evaluation_start,
            "evaluation_end": self.evaluation_end,
            "warmup_start": self.warmup_start,
            "primary_benchmark": self.primary_benchmark,
            "fallback_benchmark": self.fallback_benchmark,
            "return_frequency": self.return_frequency,
            "annualization_factor": self.annualization_factor,
            "risk_free_rate": self.risk_free_rate,
            "sharpe_ddof": self.sharpe_ddof,
        }

    @property
    def evaluation_hash(self) -> str:
        return canonical_hash({"kind": "evaluation", **self.as_dict()})


def backtest_fingerprint(
    strategy_hash: str,
    scenario_hash: str,
    dataset_hash: str,
    engine_version: str,
) -> str:
    """组合四个独立身份，形成一次回测的唯一指纹。"""
    values = (strategy_hash, scenario_hash, dataset_hash, engine_version)
    if any(not str(value).strip() for value in values):
        raise ValueError("backtest fingerprint 的组成项不能为空")
    return canonical_hash({
        "strategy_hash": strategy_hash,
        "scenario_hash": scenario_hash,
        "dataset_hash": dataset_hash,
        "engine_version": engine_version,
    })
