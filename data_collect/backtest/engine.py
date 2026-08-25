"""可复现的日线 Backtest 引擎。

引擎只接受 ``BacktestDataset``，不在运行中查询 PostgreSQL。这样可以保证输入已经
由 FROZEN Dataset 固定，避免增量任务在回测期间改变结果。
"""
from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Mapping, Protocol

from .models import (
    BacktestDataset,
    BacktestResult,
    DividendEvent,
    MarketBar,
    Observation,
    PositionLotState,
    SignalIntent,
)
from .risk import (
    FeeRule,
    MarketRule,
    MonteCarloTester,
    RiskPolicy,
    check_liquidity,
    open_position_stop_risk,
    size_position,
)


class BaseStrategy(Protocol):
    strategy_id: str
    strategy_version: str

    def generate_signals(
        self,
        trade_date: date,
        observations: tuple[Observation, ...],
        positions: Mapping[str, Mapping[str, Any]],
    ) -> list[SignalIntent]:
        ...


@dataclass(frozen=True)
class BacktestConfig:
    initial_cash: float = 10000.0
    risk_pct: float = 0.02
    max_positions: int = 10
    max_single_position_pct: float = 0.25
    reserve_cash_pct: float = 0.10
    market_rule: MarketRule = field(default_factory=MarketRule)
    fee_rule: FeeRule = field(default_factory=FeeRule)
    execution_model_version: str = "daily-next-open-v2"
    random_seed: int = 42
    force_liquidate_at_end: bool = False
    max_portfolio_risk_pct: float | None = None
    risk_policy_version: str = "risk-gap-v1"
    entry_slippage_bps: float | None = None
    exit_slippage_bps: float | None = None
    gap_stress_pct: float = 0.0
    monte_carlo_method: str = "bootstrap_with_replacement"

    def __post_init__(self):
        if self.initial_cash <= 0 or not 0 < self.risk_pct < 1:
            raise ValueError("initial_cash 和 risk_pct 无效")
        if self.max_positions <= 0 or not 0 < self.max_single_position_pct <= 1:
            raise ValueError("组合持仓约束无效")
        if self.max_portfolio_risk_pct is not None and not 0 < self.max_portfolio_risk_pct <= 1:
            raise ValueError("max_portfolio_risk_pct 必须在 (0,1] 内")
        if not 0 <= self.reserve_cash_pct < 1:
            raise ValueError("reserve_cash_pct 必须在 [0,1) 内")
        self.resolved_risk_policy()
        if self.monte_carlo_method not in {"bootstrap_with_replacement", "permutation_without_replacement"}:
            raise ValueError("monte_carlo_method 不受支持")

    def resolved_risk_policy(self, fee_rule: FeeRule | None = None) -> RiskPolicy:
        fee_rule = fee_rule or self.fee_rule
        return RiskPolicy(
            risk_policy_version=self.risk_policy_version,
            entry_slippage_bps=(
                fee_rule.slippage_bps if self.entry_slippage_bps is None else self.entry_slippage_bps
            ),
            exit_slippage_bps=(
                fee_rule.slippage_bps if self.exit_slippage_bps is None else self.exit_slippage_bps
            ),
            gap_stress_pct=self.gap_stress_pct,
        )


class ValueTrendStrategy:
    """第一版确定性示例策略：估值安全边际 + 均线趋势。

    策略只使用 Research Snapshot 当前日字段；执行价格、可成交性和仓位由引擎负责。
    这是验证链路的基准策略，不代表投资建议。
    """

    strategy_id = "value_trend_v1"
    strategy_version = "1.0.0"
    score_version = "value-trend-score-v1"

    def __init__(
        self,
        pe_percentile_max: float = 0.30,
        volume_ratio_min: float = 1.0,
        stop_loss_pct: float = 0.08,
        allow_st: bool = False,
    ):
        if not 0 <= pe_percentile_max <= 1 or volume_ratio_min < 0 or stop_loss_pct <= 0:
            raise ValueError("策略参数无效")
        self.pe_percentile_max = pe_percentile_max
        self.volume_ratio_min = volume_ratio_min
        self.stop_loss_pct = stop_loss_pct
        self.allow_st = allow_st

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "pe_percentile_max": self.pe_percentile_max,
            "volume_ratio_min": self.volume_ratio_min,
            "stop_loss_pct": self.stop_loss_pct,
            "allow_st": self.allow_st,
            "score_version": self.score_version,
        }

    def generate_signals(
        self,
        trade_date: date,
        observations: tuple[Observation, ...],
        positions: Mapping[str, Mapping[str, Any]],
    ) -> list[SignalIntent]:
        signals: list[SignalIntent] = []
        for observation in sorted(observations, key=lambda item: item.ts_code):
            code = observation.ts_code
            effective_announce_date = observation.get("effective_announce_date")
            if effective_announce_date is not None and effective_announce_date > trade_date:
                continue
            raw_close = observation.get("raw_close")
            indicator_close = observation.get("adj_close_base")
            ma20 = observation.get("ma20")
            ma60 = observation.get("ma60")
            if raw_close is None or raw_close <= 0 or indicator_close is None or indicator_close <= 0:
                continue

            # 已有仓位只依据明确的趋势破坏退出，不能重复发 BUY。
            if code in positions and positions[code].get("hold_shares", 0) > 0:
                if ma20 is not None and indicator_close < ma20:
                    signals.append(SignalIntent(
                        ts_code=code,
                        direction="SELL",
                        indicator_price=observation.get("adj_close_base"),
                        raw_reference_price=raw_close,
                        reason={"rule": "close_below_ma20", "ma20": ma20},
                    ))
                continue

            if observation.get("is_listed_asof") is False:
                continue
            if observation.get("eligible_by_market") is False:
                continue
            if observation.get("is_st_asof") and not self.allow_st:
                continue
            pe_pct = observation.get("pe_pct_5y")
            is_pe_meaningful = observation.get("is_pe_meaningful")
            if pe_pct is None or is_pe_meaningful is not True or pe_pct > self.pe_percentile_max:
                continue
            if ma20 is None or ma60 is None or indicator_close <= ma20 or ma20 <= ma60:
                continue
            volume_ratio = observation.get("volume_ratio_20d")
            # 量能条件是策略的硬过滤；缺失值不能被当成“满足条件”，否则
            # 质量不完整的股票会绕过趋势确认进入回测。
            if volume_ratio is None or volume_ratio < self.volume_ratio_min:
                continue

            stop = raw_close * (1.0 - self.stop_loss_pct)
            # 组合层使用显式、可审计的分数排序；分数只影响同日 BUY 的
            # 资金优先级，不改变策略的硬过滤条件。
            value_score = max(0.0, 1.0 - float(pe_pct))
            trend_score = max(0.0, float(indicator_close) / float(ma20) - 1.0)
            volume_score = max(0.0, min(float(volume_ratio), 5.0) - 1.0) * 0.05
            signal_score = round(value_score + trend_score + volume_score, 8)
            signals.append(SignalIntent(
                ts_code=code,
                direction="BUY",
                indicator_price=observation.get("adj_close_base"),
                raw_reference_price=raw_close,
                stop_loss_price_raw=stop,
                reason={
                    "rule": "value_trend_v1",
                    "score_version": self.score_version,
                    "signal_score": signal_score,
                    "pe_pct_5y": pe_pct,
                    "ma20": ma20,
                    "ma60": ma60,
                    "volume_ratio_20d": volume_ratio,
                },
                score=signal_score,
            ))
        return signals


class FundamentalValueGrowthTrendStrategy:
    """正式 v2 策略：成熟样本 + 基本面双轨 + 趋势触发。

    旧 ``value_trend_v1`` 保持不可变，仅用于历史兼容。该策略只读取
    research-v2.0 已冻结字段，不临时查询 PIT/Feature，也不代表投资建议。
    """

    strategy_id = "fundamental_value_growth_trend_v2"
    strategy_version = "2.0.0"
    score_version = "fundamental-value-growth-trend-score-v2"

    def __init__(
        self,
        pe_percentile_max: float = 0.30,
        roe_min: float = 15.0,
        debt_to_assets_max: float = 50.0,
        quarterly_growth_min: float = 30.0,
        volume_ratio_min: float = 1.5,
        stop_loss_pct: float = 0.08,
        min_listing_days: int = 365 * 2,
        allow_st: bool = False,
    ):
        if not 0 <= pe_percentile_max <= 1:
            raise ValueError("pe_percentile_max 必须在 [0,1]")
        if debt_to_assets_max <= 0 or volume_ratio_min < 0 or stop_loss_pct <= 0:
            raise ValueError("策略阈值无效")
        if not math.isclose(roe_min, 15.0) or not math.isclose(quarterly_growth_min, 30.0):
            raise ValueError("research-v2.0 策略规则固定为 ROE>15 与季度营收同比>30")
        self.pe_percentile_max = pe_percentile_max
        self.roe_min = roe_min
        self.debt_to_assets_max = debt_to_assets_max
        self.quarterly_growth_min = quarterly_growth_min
        self.volume_ratio_min = volume_ratio_min
        self.stop_loss_pct = stop_loss_pct
        self.min_listing_days = min_listing_days
        self.allow_st = allow_st

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "pe_percentile_max": self.pe_percentile_max,
            "roe_min": self.roe_min,
            "debt_to_assets_max": self.debt_to_assets_max,
            "quarterly_growth_min": self.quarterly_growth_min,
            "volume_ratio_min": self.volume_ratio_min,
            "stop_loss_pct": self.stop_loss_pct,
            "min_listing_days": self.min_listing_days,
            "allow_st": self.allow_st,
            "score_version": self.score_version,
        }

    @staticmethod
    def _as_date(value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value))

    def generate_signals(
        self,
        trade_date: date,
        observations: tuple[Observation, ...],
        positions: Mapping[str, Mapping[str, Any]],
    ) -> list[SignalIntent]:
        signals: list[SignalIntent] = []
        for observation in sorted(observations, key=lambda item: item.ts_code):
            code = observation.ts_code
            raw_close = observation.get("raw_close")
            indicator_close = observation.get("adj_close_base")
            ma20 = observation.get("ma20")
            ma60 = observation.get("ma60")
            if raw_close is None or raw_close <= 0 or indicator_close is None or indicator_close <= 0:
                continue

            if code in positions and positions[code].get("hold_shares", 0) > 0:
                if ma20 is not None and indicator_close < ma20:
                    signals.append(SignalIntent(
                        ts_code=code, direction="SELL",
                        indicator_price=indicator_close,
                        raw_reference_price=raw_close,
                        reason={"rule": "close_below_ma20", "strategy": self.strategy_id, "ma20": ma20},
                    ))
                continue

            if observation.get("is_listed_asof") is False or observation.get("eligible_by_market") is False:
                continue
            if observation.get("is_st_asof") and not self.allow_st:
                continue
            if (observation.get("ma20_observation_count") or 0) < 20 or (observation.get("ma60_observation_count") or 0) < 60:
                continue
            if ma20 is None or ma60 is None or indicator_close <= ma20 or ma20 <= ma60:
                continue
            volume_ratio = observation.get("volume_ratio_20d")
            if volume_ratio is None or volume_ratio < self.volume_ratio_min:
                continue
            debt = observation.get("debt_to_assets")
            if debt is None or debt >= self.debt_to_assets_max:
                continue

            traditional = bool(
                observation.get("has_full_pe_5y_history") is True
                and (observation.get("valuation_observation_count") or 0) >= 1000
                and observation.get("is_pe_meaningful") is True
                and observation.get("pe_pct_5y") is not None
                and observation.get("pe_pct_5y") <= self.pe_percentile_max
                and observation.get("annual_report_count_5y") == 5
                and observation.get("roe_5y_all_above_15") is True
                and observation.get("roe_5y_min") is not None
                and observation.get("roe_5y_min") > self.roe_min
                and observation.get("ocf_report_count_3y") == 3
                and observation.get("ocf_3y_all_positive") is True
            )
            list_date = self._as_date(observation.get("list_date"))
            growth = bool(
                list_date is not None
                and (trade_date - list_date).days >= self.min_listing_days
                and observation.get("quarterly_report_count_4q") == 4
                and observation.get("q_sales_yoy_4q_all_above_30") is True
                and observation.get("q_sales_yoy_above_30_quarters_4q") == 4
            )
            if not (traditional or growth):
                continue

            pe_pct = observation.get("pe_pct_5y")
            value_score = 0.0 if pe_pct is None else max(0.0, 1.0 - float(pe_pct))
            trend_score = max(0.0, float(indicator_close) / float(ma20) - 1.0)
            volume_score = max(0.0, min(float(volume_ratio), 5.0) - 1.0) * 0.05
            track_score = 0.20 if traditional else 0.15
            score = round(value_score + trend_score + volume_score + track_score, 8)
            stop = float(raw_close) * (1.0 - self.stop_loss_pct)
            signals.append(SignalIntent(
                ts_code=code, direction="BUY", indicator_price=indicator_close,
                raw_reference_price=raw_close, stop_loss_price_raw=stop,
                reason={
                    "rule": self.strategy_id,
                    "track": "traditional" if traditional else "growth",
                    "traditional_track": traditional,
                    "growth_track": growth,
                    "score_version": self.score_version,
                    "signal_score": score,
                    "pe_pct_5y": pe_pct,
                    "roe_5y_min": observation.get("roe_5y_min"),
                    "debt_to_assets": debt,
                    "volume_ratio_20d": volume_ratio,
                },
                score=score,
            ))
        return signals


class BacktestEngine:
    """日线、只做多、T 收盘出信号/T+1 开盘执行的确定性撮合器。"""

    def __init__(self, config: BacktestConfig | None = None):
        self.config = config or BacktestConfig()
        self._reset()

    def _reset(self):
        self.cash = self.config.initial_cash
        self.available_cash = self.config.initial_cash
        self._pending_cash: list[tuple[date, float]] = []
        self._id_counter = 0
        self._id_namespace = "unbound"
        self._positions: dict[str, list[PositionLotState]] = {}
        self._pending_orders: list[dict[str, Any]] = []
        self._entitlements: list[dict[str, Any]] = []
        self._signals: list[dict[str, Any]] = []
        self._orders: list[dict[str, Any]] = []
        self._fills: list[dict[str, Any]] = []
        self._cash_ledger: list[dict[str, Any]] = []
        self._position_daily: list[dict[str, Any]] = []
        self._performance_daily: list[dict[str, Any]] = []
        self._warnings: list[str] = []
        self._r_multiples: list[float] = []
        self._realized_trades: list[dict[str, Any]] = []
        self._last_close: dict[str, float] = {}
        self._audit_metadata: dict[str, Any] = {}
        self._initial_ledger_recorded = False

    def _new_id(self) -> str:
        """为同一 run 生成稳定且跨 run 不冲突的事件 ID。"""
        self._id_counter += 1
        return str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"ashares-backtest:{self._id_namespace}:{self._id_counter}",
        ))

    def run(
        self,
        dataset: BacktestDataset,
        strategy: BaseStrategy,
        run_id: str | None = None,
    ) -> BacktestResult:
        self._reset()
        self._id_namespace = str(run_id or dataset.dataset_id)
        self._dataset = dataset
        next_by_date = {day: dataset.next_trade_date(day) for day in dataset.calendar}
        self._previous_by_date = {
            day: (dataset.calendar[index - 1] if index > 0 else None)
            for index, day in enumerate(dataset.calendar)
        }
        for current_day in dataset.calendar:
            if current_day > dataset.evaluation_end:
                break
            self._release_cash(current_day)
            if current_day >= dataset.evaluation_start and not self._initial_ledger_recorded:
                self._append_cash(current_day, self.config.initial_cash, "INIT", None, "回测初始现金")
                self._initial_ledger_recorded = True
            before_positions = self._position_shares()
            self._capture_dividend_entitlements(current_day, before_positions, dataset)
            self._apply_due_dividend_entitlements(current_day, dataset)
            # T-1 close generated orders are decided before T intraday events.
            # Execute them at T open first; only positions still open afterwards
            # can be tested against T's low and the pre-existing stop price.
            self._execute_pending_orders(current_day, dataset)
            self._process_stop_losses(current_day, dataset)
            self._assert_no_unresolved_delist_positions(current_day, dataset)

            for observation in dataset.observations_for(current_day):
                raw_close = observation.get("raw_close")
                if raw_close is not None and raw_close > 0:
                    self._last_close[observation.ts_code] = float(raw_close)

            next_day = next_by_date.get(current_day)
            if dataset.evaluation_start <= current_day <= dataset.evaluation_end:
                positions_view = self._positions_view()
                if next_day is not None and next_day <= dataset.evaluation_end:
                    signals = strategy.generate_signals(
                        current_day, dataset.observations_for(current_day), positions_view
                    )
                    self._create_signals_and_orders(current_day, next_day, signals, dataset)
                else:
                    self._warnings.append(f"{current_day}: evaluation শেষ日没有下一交易日，信号不创建订单")
                self._record_daily(current_day, dataset)

        for order in self._pending_orders:
            if order["status"] == "PENDING":
                order["status"] = "EXPIRED"
                self._set_signal_status(order, "EXPIRED")
                order["reject_reason"] = "回测区间结束未成交"
        if self.config.force_liquidate_at_end and dataset.evaluation_dates:
            self._warnings.append(
                "force_liquidate_at_end 仅输出区间末假设清仓估值，不在区间外伪造成交"
            )

        metrics = self._calculate_metrics(dataset)
        position_lots = tuple(
            {
                "lot_id": lot.lot_id, "ts_code": lot.ts_code,
                "open_date": lot.open_date, "available_sell_date": lot.available_sell_date,
                "open_shares": lot.open_shares, "remaining_shares": lot.remaining_shares,
                "cost_price": lot.cost_price, "stop_loss_price_raw": lot.stop_loss_price_raw,
                "open_fill_id": lot.open_fill_id, "close_date": lot.close_date,
                "close_fill_id": lot.close_fill_id,
                "opening_event_type": lot.opening_event_type,
                "opening_event_key": lot.opening_event_key,
                "accrued_cash_dividend_per_share": lot.accrued_cash_dividend_per_share,
            }
            for lots in self._positions.values() for lot in lots
        )
        return BacktestResult(
            status="SUCCESS",
            initial_cash=self.config.initial_cash,
            final_equity=float(metrics["final_equity"]),
            metrics=metrics,
            signals=tuple(self._signals),
            orders=tuple(self._orders),
            fills=tuple(self._fills),
            cash_ledger=tuple(self._cash_ledger),
            performance_daily=tuple(self._performance_daily),
            position_daily=tuple(self._position_daily),
            position_lots=position_lots,
            warnings=tuple(self._warnings),
            audit_metadata=dict(self._audit_metadata),
        )

    def _position_shares(self) -> dict[str, int]:
        return {
            code: sum(max(0, lot.remaining_shares) for lot in lots)
            for code, lots in self._positions.items()
            if sum(max(0, lot.remaining_shares) for lot in lots) > 0
        }

    def _positions_view(self) -> dict[str, dict[str, Any]]:
        view: dict[str, dict[str, Any]] = {}
        for code, lots in self._positions.items():
            active = [lot for lot in lots if lot.remaining_shares > 0]
            if not active:
                continue
            shares = sum(lot.remaining_shares for lot in active)
            average = sum(lot.remaining_shares * lot.cost_price for lot in active) / shares
            view[code] = {"hold_shares": shares, "average_cost": average,
                          "lots": tuple(active)}
        return view

    def _current_risk_amount(self, day: date, dataset: BacktestDataset) -> float:
        total = 0.0
        for code, lots in self._positions.items():
            market_rule, fee_rule = self._rules_for(day, code, dataset)
            risk_policy = self.config.resolved_risk_policy(fee_rule)
            for lot in lots:
                total += open_position_stop_risk(
                    lot.remaining_shares,
                    lot.cost_price,
                    lot.stop_loss_price_raw,
                    fee_rule,
                    market_rule,
                    risk_policy,
                )
        return total

    def _rules_for(self, day: date, code: str, dataset: BacktestDataset):
        bar = dataset.bar(day, code)
        rules = dataset.rules_for(day, bar.market if bar else None)
        if rules:
            return rules
        if dataset.require_explicit_rules:
            raise RuntimeError(f"Frozen Dataset 缺少 {day}/{code} 的有效市场规则")
        return self.config.market_rule, self.config.fee_rule

    def _capture_dividend_entitlements(self, day: date, before: Mapping[str, int], dataset: BacktestDataset):
        for event in dataset.dividend_events_for(day):
            shares = int(before.get(event.ts_code, 0))
            pending_orders = [
                order for order in self._pending_orders
                if order["status"] == "PENDING"
                and order["ts_code"] == event.ts_code
                and order["order_date"] >= day
            ]
            if event.implementation_status != "实施":
                continue
            if event.effective_announce_date is not None and event.effective_announce_date > event.ex_date:
                self._warnings.append(f"{day}: 跳过除权日前不可见的公司行为 {event.source_record_key}")
                continue
            if shares <= 0 and not pending_orders:
                continue
            pay_date = event.pay_date or event.ex_date
            entitled_lots = tuple(
                (
                    lot.lot_id,
                    int(lot.remaining_shares),
                    float(lot.cost_price),
                    lot.stop_loss_price_raw,
                )
                for lot in self._positions.get(event.ts_code, [])
                if lot.remaining_shares > 0
            )
            adjusted_lots = self._adjust_corporate_action_coordinates(
                day, event, entitled_lots, pending_orders, dataset
            )
            if float(event.cash_div or 0.0) > 0:
                for lot_id, _, _, _ in adjusted_lots:
                    lot = next(
                        candidate
                        for candidate in self._positions.get(event.ts_code, ())
                        if candidate.lot_id == lot_id
                    )
                    lot.accrued_cash_dividend_per_share += float(event.cash_div)
            if shares <= 0:
                # 只有待执行订单而没有除权日前持仓时，不产生分红/送转权益，
                # 但仍必须调整订单的原始止损坐标。
                continue
            self._entitlements.append({
                "event": event,
                "shares": shares,
                "pay_date": pay_date,
                "entitled_lots": adjusted_lots,
                "stock_applied": False,
                "cash_applied": False,
            })

    def _adjust_corporate_action_coordinates(
        self,
        day: date,
        event: DividendEvent,
        entitled_lots: tuple[tuple[str, int, float, float | None], ...],
        pending_orders: list[dict[str, Any]],
        dataset: BacktestDataset,
    ) -> tuple[tuple[str, int, float, float | None], ...]:
        """在除权日把持仓/订单转换到新的原始价格坐标。

        原始执行价格在除权除息后会发生机械跳变。止损坐标按现金分红和送
        转共同调整；持仓成本只按送转股本变化调整，现金分红由现金账本入账，
        避免把同一笔现金收益同时计入成本和现金。若缺少除权日前收盘价，
        正式回测必须 fail-closed，不能继续使用旧止损价。
        """
        if not entitled_lots and not pending_orders:
            return entitled_lots
        pre_close = self._last_close.get(event.ts_code)
        if pre_close is None:
            previous_days = [candidate for candidate in dataset.calendar if candidate < day]
            if previous_days:
                previous_bar = dataset.bar(previous_days[-1], event.ts_code)
                if previous_bar and previous_bar.raw_close is not None:
                    pre_close = float(previous_bar.raw_close)
        if pre_close is None or not math.isfinite(float(pre_close)) or float(pre_close) <= 0:
            raise RuntimeError(
                f"{day}/{event.ts_code} 公司行为缺少除权日前收盘价，"
                "禁止使用未调整止损坐标"
            )
        cash_div = float(event.coordinate_cash_div)
        stock_rate = float(event.stock_rate)
        coordinate_factor = (float(pre_close) - cash_div) / (float(pre_close) * (1.0 + stock_rate))
        if not math.isfinite(coordinate_factor) or coordinate_factor <= 0:
            raise RuntimeError(
                f"{day}/{event.ts_code} 公司行为坐标调整因子无效: {coordinate_factor}"
            )
        stock_factor = 1.0 / (1.0 + stock_rate)
        adjusted: list[tuple[str, int, float, float | None]] = []
        for lot_id, shares, cost_price, stop_loss in entitled_lots:
            lot = next(
                (candidate for candidate in self._positions.get(event.ts_code, ())
                 if candidate.lot_id == lot_id),
                None,
            )
            if lot is None:
                raise RuntimeError(f"公司行为权益批次不存在: {event.ts_code}/{lot_id}")
            if stock_rate > 0:
                lot.cost_price = round(float(lot.cost_price) * stock_factor, 8)
            if lot.stop_loss_price_raw is not None:
                lot.stop_loss_price_raw = round(
                    float(lot.stop_loss_price_raw) * coordinate_factor, 8
                )
            adjusted.append((
                lot_id,
                shares,
                round(float(cost_price) * stock_factor, 8) if stock_rate > 0 else float(cost_price),
                round(float(stop_loss) * coordinate_factor, 8)
                if stop_loss is not None else None,
            ))
        for order in pending_orders:
            stop = order.get("stop_loss_price_raw")
            if stop is not None:
                order["stop_loss_price_raw"] = round(float(stop) * coordinate_factor, 8)
            order["corporate_action_adjusted"] = True
        return tuple(adjusted)

    def _apply_due_dividend_entitlements(self, day: date, dataset: BacktestDataset):
        remaining = []
        for item in self._entitlements:
            event: DividendEvent = item["event"]
            shares = item["shares"]
            if not item["stock_applied"] and event.stock_rate > 0 and day >= event.stock_available_date:
                bonus_total = 0
                # 送转股份只属于除权日前实际持有的批次。单独建立公司行为批次，
                # 避免把红股错误分配给除权日之后新买入的持仓，也保留除权日前卖出
                # 后仍应到账的股权。
                for lot_id, entitled_shares, entitled_cost, entitled_stop in item["entitled_lots"]:
                    bonus = int(math.floor(entitled_shares * event.stock_rate + 1e-9))
                    if bonus <= 0:
                        continue
                    action_lot = PositionLotState(
                        lot_id=self._new_id(),
                        ts_code=event.ts_code,
                        open_date=event.stock_available_date,
                        available_sell_date=event.stock_available_date,
                        open_shares=bonus,
                        remaining_shares=bonus,
                        # 红股/转增股与原批次共享调整后的成本基准，保证
                        # 公司行为后总成本不因“零成本红股”被错误减半。
                        cost_price=entitled_cost,
                        stop_loss_price_raw=entitled_stop,
                        open_fill_id=None,
                        opening_event_type="CORPORATE_ACTION",
                        opening_event_key=event.source_record_key,
                    )
                    self._positions.setdefault(event.ts_code, []).append(action_lot)
                    bonus_total += bonus
                if bonus_total:
                    self._append_cash(
                        day, 0.0, "CORPORATE_ACTION", None,
                        f"{event.ts_code} {event.ex_date} 送转到账 {bonus_total} 股",
                    )
                item["stock_applied"] = True
            if not item["cash_applied"] and item["pay_date"] <= day:
                cash_amount = shares * float(event.cash_div or 0.0)
                if cash_amount:
                    self.cash += cash_amount
                    self.available_cash += cash_amount
                    self._append_cash(day, cash_amount, "DIVIDEND", None,
                                      f"{event.ts_code} {event.ex_date} 现金分红")
                item["cash_applied"] = True
                self._warnings.append(f"{day}: 已处理 {event.ts_code} 公司行为，来源 {event.source_record_key}")
            stock_pending = event.stock_rate > 0 and not item["stock_applied"]
            cash_pending = not item["cash_applied"]
            if stock_pending or cash_pending:
                remaining.append(item)
        self._entitlements = remaining

    def _release_cash(self, day: date):
        remaining: list[tuple[date, float]] = []
        for available_date, amount in self._pending_cash:
            if available_date <= day:
                self.available_cash += amount
            else:
                remaining.append((available_date, amount))
        self._pending_cash = remaining

    def _pending_receivable_values(
        self, day: date, dataset: BacktestDataset, *, price_field: str = "raw_close"
    ) -> tuple[float, float]:
        """Return cash and stock receivables created on ex-date but not delivered.

        Raw prices normally drop on the ex-date.  Omitting receivables until the
        payment/listing date creates a fictitious drawdown, so daily NAV carries
        both rights as assets while keeping cash and sellable shares unchanged.
        """
        cash_receivable = 0.0
        stock_receivable_value = 0.0
        for item in self._entitlements:
            event: DividendEvent = item["event"]
            if not item["cash_applied"]:
                cash_receivable += int(item["shares"]) * float(event.cash_div or 0.0)
            if not item["stock_applied"] and event.stock_rate > 0:
                bonus_shares = sum(
                    int(math.floor(int(entitled_shares) * event.stock_rate + 1e-9))
                    for _, entitled_shares, _, _ in item["entitled_lots"]
                )
                if bonus_shares <= 0:
                    continue
                bar = dataset.bar(day, event.ts_code)
                close = (
                    getattr(bar, price_field, None)
                    if bar is not None and getattr(bar, price_field, None) is not None
                    else self._last_close.get(event.ts_code)
                )
                if close is None or float(close) <= 0:
                    raise RuntimeError(
                        f"{day}/{event.ts_code} 送转应收权益缺少估值价格"
                    )
                stock_receivable_value += bonus_shares * float(close)
        return cash_receivable, stock_receivable_value

    def _current_equity_at_open(self, day: date, dataset: BacktestDataset) -> float:
        """Value the account using only information available at the open."""
        value = float(self.cash)
        for code, lots in self._positions.items():
            shares = sum(max(0, lot.remaining_shares) for lot in lots)
            if shares <= 0:
                continue
            bar = dataset.bar(day, code)
            price = (
                bar.raw_open
                if bar is not None and bar.raw_open is not None
                else self._last_close.get(code)
            )
            if price is None or not math.isfinite(float(price)) or float(price) <= 0:
                raise RuntimeError(f"{day}/{code} 开盘权益缺少可见估值价格")
            value += shares * float(price)
        cash_receivable, stock_receivable_value = self._pending_receivable_values(
            day, dataset, price_field="raw_open"
        )
        return value + cash_receivable + stock_receivable_value

    def _investable_cash(self, equity: float) -> float:
        """Enforce reserve cash as an absolute portfolio floor."""
        reserve = max(0.0, float(equity) * float(self.config.reserve_cash_pct))
        return max(0.0, float(self.available_cash) - reserve)

    def _liquidity_reference_bar(
        self, day: date, ts_code: str, dataset: BacktestDataset
    ) -> tuple[date | None, MarketBar | None]:
        """Use the last completed daily bar; today's full volume is not open-time data."""
        previous_day = self._previous_by_date.get(day)
        return previous_day, (dataset.bar(previous_day, ts_code) if previous_day else None)

    def _cash_available_date(self, day: date, market_rule: MarketRule) -> date | None:
        candidate = day
        for _ in range(market_rule.cash_settlement_days):
            next_day = self._dataset.next_trade_date(candidate)
            if next_day is None:
                # The dataset does not contain the future settlement date. Do
                # not make unsettled sale proceeds available on the fill date;
                # a formal run must fail closed at the dataset boundary.
                return None
            candidate = next_day
        return candidate

    def _append_cash(
        self,
        day: date,
        amount: float,
        event_type: str,
        reference_id: str | None,
        description: str,
        available_date: date | None = None,
    ):
        available_date = available_date or day
        self._cash_ledger.append({
            "trade_date": day, "event_type": event_type, "amount": float(amount),
            "balance": float(self.cash), "available_date": available_date,
            "reference_id": reference_id, "description": description,
        })

    def _create_signals_and_orders(self, day: date, next_day: date,
                                   signals: list[SignalIntent], dataset: BacktestDataset):
        # 卖出优先；BUY 按显式 signal score 降序，再按代码和方向稳定排序。
        # 这使现金、最大持仓数和组合风险不足时的取舍属于正式组合规则，
        # 不再由股票代码字典序暗中决定。
        ordered = sorted(
            signals,
            key=lambda item: (
                0 if item.direction == "SELL" else 1,
                -float(item.score) if item.direction == "BUY" else 0.0,
                item.ts_code,
                item.direction,
            ),
        )
        for intent in ordered:
            signal_id = self._new_id()
            signal = {
                "signal_id": signal_id, "research_snapshot_id": dataset.snapshot_ids.get(day),
                "ts_code": intent.ts_code,
                "signal_date": day, "decision_time": None,
                "intended_execution_date": next_day, "direction": intent.direction,
                "indicator_price": intent.indicator_price,
                "raw_reference_price": intent.raw_reference_price,
                "stop_loss_price_raw": intent.stop_loss_price_raw,
                "signal_score": float(intent.score),
                "reason": dict(intent.reason), "status": "CREATED",
            }
            self._signals.append(signal)
            if intent.direction not in {"BUY", "SELL"}:
                signal["status"] = "IGNORED"
                continue
            bar = dataset.bar(next_day, intent.ts_code)
            if bar is None:
                signal["status"] = "NO_EXECUTION_BAR"
                continue
            order = {
                "order_id": self._new_id(), "signal_id": signal_id,
                "ts_code": intent.ts_code, "decision_date": day,
                "order_date": next_day, "expire_date": next_day,
                "direction": intent.direction, "planned_shares": 0, "order_shares": 0,
                "requested_shares": 0, "actual_shares": 0,
                "volume_capacity_shares": None, "liquidity_status": "NOT_CHECKED",
                "liquidity_reject_reason": None,
                "liquidity_reference_date": None,
                "theoretical_shares": 0.0, "required_capital": 0.0,
                "max_loss_at_stop": 0.0, "gap_stress_loss": 0.0,
                "buy_fees": 0.0, "estimated_sell_fees": 0.0,
                "estimated_slippage": 0.0, "actual_risk_pct": 0.0,
                "risk_policy_version": self.config.risk_policy_version,
                "signal_score": float(intent.score),
                "reference_price": intent.raw_reference_price,
                "stop_loss_price_raw": intent.stop_loss_price_raw,
                "status": "PENDING", "reject_reason": None,
            }
            if intent.direction == "SELL":
                shares = self._position_shares().get(intent.ts_code, 0)
                if shares <= 0:
                    order["status"] = "REJECTED"
                    order["reject_reason"] = "当前无可卖持仓"
                else:
                    order["planned_shares"] = shares
                    order["requested_shares"] = shares
                    # Actual shares come only from a realized Fill.
                    order["actual_shares"] = 0
                    order["order_shares"] = shares
            else:
                if intent.ts_code in self._positions_view():
                    order["status"] = "REJECTED"
                    order["reject_reason"] = "已有持仓，禁止重复买入/向下摊平"
                elif len(self._positions_view()) >= self.config.max_positions:
                    order["status"] = "REJECTED"
                    order["reject_reason"] = "达到最大持仓数"
                elif intent.raw_reference_price is None or intent.stop_loss_price_raw is None:
                    order["status"] = "REJECTED"
                    order["reject_reason"] = "缺少原始价格或原始坐标止损价"
                else:
                    # 数量按实际成交日的规则/费用预算，避免规则在 T+1 生效切换时使用旧版本。
                    market_rule, fee_rule = self._rules_for(next_day, intent.ts_code, dataset)
                    closing_equity = self._current_equity(day, dataset)
                    sizing = size_position(
                        available_cash=self._investable_cash(closing_equity),
                        risk_pct=self.config.risk_pct,
                        entry_price=float(intent.raw_reference_price),
                        stop_loss_price=float(intent.stop_loss_price_raw),
                        fee_rule=fee_rule,
                        market_rule=market_rule,
                        equity_base=closing_equity,
                        max_position_value=closing_equity * self.config.max_single_position_pct,
                        max_risk_amount=(
                            closing_equity * self.config.max_portfolio_risk_pct
                            - self._current_risk_amount(day, dataset)
                            if self.config.max_portfolio_risk_pct is not None else None
                        ),
                        risk_policy=self.config.resolved_risk_policy(fee_rule),
                    )
                    order.update(sizing.as_dict())
                    order["requested_shares"] = sizing.planned_shares
                    if not sizing.eligible:
                        order["status"] = "REJECTED"
                        order["reject_reason"] = sizing.reason
                    else:
                        order["planned_shares"] = sizing.planned_shares
                        # Sizing is a plan; no Fill exists at signal time.
                        order["actual_shares"] = 0
                        order["order_shares"] = sizing.planned_shares
            self._orders.append(order)
            if order["status"] == "PENDING":
                self._pending_orders.append(order)
            else:
                signal["status"] = "REJECTED"

    def _execute_pending_orders(self, day: date, dataset: BacktestDataset):
        pending = [order for order in self._pending_orders if order["status"] == "PENDING" and order["order_date"] == day]
        pending.sort(
            key=lambda item: (
                0 if item["direction"] == "SELL" else 1,
                -float(item.get("signal_score", 0.0)) if item["direction"] == "BUY" else 0.0,
                item["ts_code"], item["order_id"],
            )
        )
        for order in pending:
            bar = dataset.bar(day, order["ts_code"])
            if bar is None or not bar.has_execution_prices or bar.is_suspended:
                self._reject_order(order, "缺少有效执行行情或停牌")
                continue
            if order["direction"] == "BUY":
                market_rule, fee_rule = self._rules_for(day, order["ts_code"], dataset)
                # 信号生成后到 T+1 执行之间，其他订单可能已经改变持仓；再次
                # 检查可避免两个同日买单绕过“禁止加仓/最大持仓数”约束。
                if order["ts_code"] in self._positions_view():
                    self._reject_order(order, "执行时已存在持仓，禁止重复买入/向下摊平")
                    continue
                if len(self._positions_view()) >= self.config.max_positions:
                    self._reject_order(order, "执行时已达到最大持仓数")
                    continue
                if not bar.can_buy or self._limit_blocked(bar, "BUY"):
                    self._reject_order(order, "当日不可买入或涨停")
                    continue
                fill_price = self._apply_slippage(
                    float(bar.raw_open), "BUY", fee_rule, market_rule, bar=bar
                )
                stop = order.get("stop_loss_price_raw")
                if stop is None or fill_price <= float(stop):
                    self._reject_order(order, "开盘成交价不高于止损价，风险坐标失效")
                    continue
                equity = self._current_equity_at_open(day, dataset)
                actual_sizing = size_position(
                    available_cash=self._investable_cash(equity),
                    risk_pct=self.config.risk_pct,
                    # size_position applies the versioned entry slippage itself;
                    # pass the raw open to avoid charging entry slippage twice.
                    entry_price=float(bar.raw_open),
                    stop_loss_price=float(stop),
                    fee_rule=fee_rule,
                    market_rule=market_rule,
                    equity_base=equity,
                    max_position_value=equity * self.config.max_single_position_pct,
                    max_risk_amount=(
                        equity * self.config.max_portfolio_risk_pct
                        - self._current_risk_amount(day, dataset)
                        if self.config.max_portfolio_risk_pct is not None else None
                    ),
                    risk_policy=self.config.resolved_risk_policy(fee_rule),
                )
                if not actual_sizing.eligible:
                    self._reject_order(order, f"T+1 开盘按实际价格重算仓位失败: {actual_sizing.reason}")
                    continue
                order.update({
                    "theoretical_shares": actual_sizing.theoretical_shares,
                    # This is still a planned execution quantity. It becomes
                    # actual_shares only after _make_fill succeeds.
                    "actual_shares": 0,
                    "required_capital": actual_sizing.required_capital,
                    "max_loss_at_stop": actual_sizing.max_loss_at_stop,
                    "gap_stress_loss": actual_sizing.gap_stress_loss,
                    "buy_fees": actual_sizing.buy_fees,
                    "estimated_sell_fees": actual_sizing.estimated_sell_fees,
                    "estimated_slippage": actual_sizing.estimated_slippage,
                    "actual_risk_pct": actual_sizing.actual_risk_pct,
                    "risk_policy_version": actual_sizing.risk_policy_version,
                })
                if actual_sizing.actual_shares != order.get("planned_shares", 0):
                    order["resized_at_execution"] = True
                order["order_shares"] = actual_sizing.actual_shares
                liquidity_day, liquidity_bar = self._liquidity_reference_bar(
                    day, order["ts_code"], dataset
                )
                order["liquidity_reference_date"] = liquidity_day
                liquidity = check_liquidity(
                    liquidity_bar.volume if liquidity_bar else None,
                    actual_sizing.actual_shares, market_rule,
                    amount=liquidity_bar.amount if liquidity_bar else None,
                    close_price=liquidity_bar.raw_close if liquidity_bar else None,
                    low_price=liquidity_bar.raw_low if liquidity_bar else None,
                    high_price=liquidity_bar.raw_high if liquidity_bar else None,
                )
                order["volume_capacity_shares"] = liquidity.volume_capacity_shares
                order["liquidity_status"] = liquidity.liquidity_status
                order["liquidity_reject_reason"] = liquidity.reject_reason
                if not liquidity.allowed:
                    self._reject_order(order, liquidity.reject_reason or "流动性门禁拒绝")
                    continue
                value = fill_price * order["order_shares"]
                fees = fee_rule.buy_cost(value)
                cash_budget = self._investable_cash(equity)
                if value + fees > cash_budget + 1e-8:
                    self._reject_order(order, "实际成交价及费用超过预留现金后的可用预算")
                    continue
                available = dataset.next_trade_date(day) if market_rule.t_plus_one else day
                if market_rule.t_plus_one and available is None and dataset.require_explicit_rules:
                    # 没有 T+1 的数据边界时不能把当天买入错误标成当天可卖。
                    self._reject_order(order, "T+1 可卖日期超出 Dataset，拒绝边界日买入")
                    continue
                fill = self._make_fill(
                    order, day, fill_price, order["order_shares"], fees, 0.0, 0.0,
                    reference_price=float(bar.raw_open),
                )
                self.cash -= value + fees
                self.available_cash -= value + fees
                self._append_cash(day, -(value + fees), "BUY", fill["fill_id"], "买入成交")
                lot = PositionLotState(
                    lot_id=self._new_id(), ts_code=order["ts_code"], open_date=day,
                    available_sell_date=available or day, open_shares=order["order_shares"],
                    remaining_shares=order["order_shares"], cost_price=fill_price,
                    stop_loss_price_raw=order["stop_loss_price_raw"],
                    open_fill_id=fill["fill_id"],
                )
                self._positions.setdefault(order["ts_code"], []).append(lot)
                order["actual_shares"] = fill["fill_shares"]
                order["status"] = "FILLED"
                self._set_signal_status(order, "FILLED")
            else:
                market_rule, fee_rule = self._rules_for(day, order["ts_code"], dataset)
                if not bar.can_sell or self._limit_blocked(bar, "SELL"):
                    self._reject_order(order, "当日不可卖出或跌停")
                    continue
                if not self._all_shares_sellable(order["ts_code"], day, order["order_shares"]):
                    self._reject_order(order, "T+1 限制或可卖股份不足")
                    continue
                fill_price = self._apply_slippage(
                    float(bar.raw_open), "SELL", fee_rule, market_rule, bar=bar
                )
                liquidity_day, liquidity_bar = self._liquidity_reference_bar(
                    day, order["ts_code"], dataset
                )
                order["liquidity_reference_date"] = liquidity_day
                liquidity = check_liquidity(
                    liquidity_bar.volume if liquidity_bar else None,
                    order["order_shares"], market_rule,
                    amount=liquidity_bar.amount if liquidity_bar else None,
                    close_price=liquidity_bar.raw_close if liquidity_bar else None,
                    low_price=liquidity_bar.raw_low if liquidity_bar else None,
                    high_price=liquidity_bar.raw_high if liquidity_bar else None,
                )
                order["volume_capacity_shares"] = liquidity.volume_capacity_shares
                order["liquidity_status"] = liquidity.liquidity_status
                order["liquidity_reject_reason"] = liquidity.reject_reason
                if not liquidity.allowed:
                    self._reject_order(order, liquidity.reject_reason or "流动性门禁拒绝")
                    continue
                self._sell_position(
                    order["ts_code"], order["order_shares"], fill_price, day, order,
                    "STRATEGY_SELL", fee_rule, market_rule,
                    reference_price=float(bar.raw_open),
                )
                if order["status"] == "FILLED":
                    order["actual_shares"] = order["order_shares"]
                    self._set_signal_status(order, "FILLED")

    def _reject_order(self, order: dict[str, Any], reason: str):
        order["status"] = "REJECTED"
        order["reject_reason"] = reason
        # Rejected orders have no realized Fill quantity.
        order["actual_shares"] = 0
        self._set_signal_status(order, "REJECTED")

    def _set_signal_status(self, order: Mapping[str, Any], status: str):
        signal_id = order.get("signal_id")
        if signal_id is None:
            return
        for signal in self._signals:
            if signal["signal_id"] == signal_id:
                signal["status"] = status
                break

    def _apply_slippage(
        self,
        price: float,
        direction: str,
        fee_rule: FeeRule | None = None,
        market_rule: MarketRule | None = None,
        *,
        bar: MarketBar | None = None,
    ) -> float:
        fee_rule = fee_rule or self.config.fee_rule
        market_rule = market_rule or self.config.market_rule
        risk_policy = self.config.resolved_risk_policy(fee_rule)
        bps = (
            risk_policy.entry_slippage_bps
            if direction == "BUY" else risk_policy.exit_slippage_bps
        ) / 10000.0
        adjusted = market_rule.round_adverse(
            price * (1 + bps if direction == "BUY" else 1 - bps), direction
        )
        if bar is not None:
            if direction == "BUY":
                upper = min(
                    value for value in (bar.raw_high, bar.up_limit)
                    if value is not None and float(value) > 0
                )
                adjusted = min(adjusted, market_rule.round_adverse(float(upper), "SELL"))
            else:
                lower = max(
                    value for value in (bar.raw_low, bar.down_limit)
                    if value is not None and float(value) > 0
                )
                adjusted = max(adjusted, market_rule.round_adverse(float(lower), "BUY"))
        if not math.isfinite(adjusted) or adjusted <= 0:
            raise RuntimeError(f"无效成交价: {direction}/{adjusted}")
        return adjusted

    @staticmethod
    def _limit_blocked(bar: MarketBar, direction: str, intraday: bool = False) -> bool:
        # limit_status is normally derived from the closing price.  Using it to
        # decide an opening fill would leak information from later in the day.
        # Opening orders therefore compare only raw_open with the numeric limit.
        # For an intraday stop, daily bars can prove non-executability only for a
        # one-price locked limit day; merely touching/closing at the limit is not
        # enough to retroactively reject an earlier stop.
        if direction == "BUY":
            if bar.up_limit is None or bar.raw_open is None:
                return False
            if not intraday:
                return bar.raw_open >= bar.up_limit - 1e-9
            return (
                bar.raw_open >= bar.up_limit - 1e-9
                and bar.raw_low is not None
                and bar.raw_low >= bar.up_limit - 1e-9
            )
        if bar.down_limit is None or bar.raw_open is None:
            return False
        if not intraday:
            return bar.raw_open <= bar.down_limit + 1e-9
        return (
            bar.raw_open <= bar.down_limit + 1e-9
            and bar.raw_high is not None
            and bar.raw_high <= bar.down_limit + 1e-9
        )

    def _make_fill(self, order: dict[str, Any], day: date, price: float, shares: int,
                   commission: float, stamp: float, transfer: float,
                   reference_price: float | None = None) -> dict[str, Any]:
        fill = {
            "fill_id": self._new_id(), "order_id": order["order_id"],
            "ts_code": order["ts_code"], "fill_date": day,
            "direction": order["direction"], "fill_price": price,
            "fill_shares": shares, "commission": commission,
            "stamp_duty": stamp, "transfer_fee": transfer,
            "slippage_cost": (
                abs(float(price) - float(reference_price)) * shares
                if reference_price is not None else 0.0
            ),
        }
        self._fills.append(fill)
        return fill

    def _all_shares_sellable(
        self,
        code: str,
        day: date,
        shares: int,
        target_lot_ids: set[str] | None = None,
    ) -> bool:
        remaining = shares
        for lot in sorted(self._positions.get(code, []), key=lambda item: item.open_date):
            if target_lot_ids is not None and lot.lot_id not in target_lot_ids:
                continue
            if lot.remaining_shares > 0 and lot.available_sell_date <= day:
                remaining -= lot.remaining_shares
                if remaining <= 0:
                    return True
        return False

    def _sell_position(self, code: str, shares: int, price: float, day: date,
                       order: dict[str, Any], reason: str,
                       fee_rule: FeeRule | None = None,
                       market_rule: MarketRule | None = None,
                       reference_price: float | None = None,
                       target_lot_ids: set[str] | None = None):
        market_rule = market_rule or self.config.market_rule
        if not market_rule.allow_sell_odd_lot and shares % market_rule.min_buy_lot:
            self._reject_order(order, "当前市场规则不允许卖出零股")
            return
        if not self._all_shares_sellable(code, day, shares, target_lot_ids):
            self._reject_order(order, "可卖股份不足")
            return
        available_date = self._cash_available_date(day, market_rule)
        if available_date is None:
            self._reject_order(order, "鍗栧嚭璧勯噾缁撶畻鏃ユ湡瓒呭嚭 Dataset 鑼冨洿")
            return
        value = price * shares
        fee_rule = fee_rule or self.config.fee_rule
        commission = fee_rule.commission(value)
        stamp = value * fee_rule.stamp_duty_sell_rate
        transfer = value * fee_rule.transfer_fee_rate
        fill = self._make_fill(
            order, day, price, shares, commission, stamp, transfer,
            reference_price=reference_price,
        )
        proceeds = value - commission - stamp - transfer
        self.cash += proceeds
        if available_date <= day:
            self.available_cash += proceeds
        else:
            self._pending_cash.append((available_date, proceeds))
        self._append_cash(day, proceeds, "SELL", fill["fill_id"], reason, available_date)

        remaining = shares
        realized = 0.0
        dividend_income = 0.0
        risk_budget = 0.0
        for lot in sorted(self._positions.get(code, []), key=lambda item: item.open_date):
            if remaining <= 0:
                break
            if target_lot_ids is not None and lot.lot_id not in target_lot_ids:
                continue
            if lot.remaining_shares <= 0 or lot.available_sell_date > day:
                continue
            sold = min(lot.remaining_shares, remaining)
            lot.remaining_shares -= sold
            remaining -= sold
            realized += (price - lot.cost_price) * sold
            dividend_income += float(lot.accrued_cash_dividend_per_share) * sold
            if lot.stop_loss_price_raw is not None:
                risk_budget += max(0.0, lot.cost_price - lot.stop_loss_price_raw) * sold
            if lot.remaining_shares == 0:
                lot.close_date = day
                lot.close_fill_id = fill["fill_id"]
        net_pnl = realized + dividend_income - commission - stamp - transfer
        r_multiple = net_pnl / risk_budget if risk_budget > 0 else 0.0
        self._realized_trades.append({
            "ts_code": code, "exit_date": day, "reason": reason,
            "shares": shares, "exit_price": price, "pnl": net_pnl,
            "cash_dividend_income": dividend_income,
            "r_multiple": r_multiple,
        })
        if risk_budget > 0:
            self._r_multiples.append(r_multiple)
        order["status"] = "FILLED"

    def _process_stop_losses(self, day: date, dataset: BacktestDataset):
        """Execute each triggered stop against the lot that owns that stop.

        Different entry lots can carry different stop coordinates.  A combined
        FIFO sale would close the wrong lot and use the worst stop for every
        share, so equal-stop lots are grouped and filled independently.  The
        liquidity gate is evaluated once for the aggregate shares using the
        previous completed daily bar.
        """
        for code, lots in list(self._positions.items()):
            bar = dataset.bar(day, code)
            if bar is None or not bar.has_execution_prices:
                continue
            triggering = [
                lot for lot in lots
                if lot.remaining_shares > 0 and lot.stop_loss_price_raw is not None
                and lot.available_sell_date <= day and bar.raw_low is not None
                and bar.raw_low <= lot.stop_loss_price_raw
            ]
            if not triggering:
                continue
            if not bar.can_sell or bar.is_suspended or self._limit_blocked(
                bar, "SELL", intraday=True
            ):
                self._warnings.append(f"{day}: {code} 止损触发但无法卖出，保留持仓")
                continue

            market_rule, fee_rule = self._rules_for(day, code, dataset)
            total_shares = sum(lot.remaining_shares for lot in triggering)
            liquidity_day, liquidity_bar = self._liquidity_reference_bar(day, code, dataset)
            liquidity = check_liquidity(
                liquidity_bar.volume if liquidity_bar else None,
                total_shares,
                market_rule,
                amount=liquidity_bar.amount if liquidity_bar else None,
                close_price=liquidity_bar.raw_close if liquidity_bar else None,
                low_price=liquidity_bar.raw_low if liquidity_bar else None,
                high_price=liquidity_bar.raw_high if liquidity_bar else None,
            )
            groups: dict[float, list[PositionLotState]] = {}
            for lot in triggering:
                groups.setdefault(float(lot.stop_loss_price_raw), []).append(lot)

            for stop_value in sorted(groups, reverse=True):
                stop_lots = groups[stop_value]
                shares = sum(lot.remaining_shares for lot in stop_lots)
                order = {
                    "order_id": self._new_id(), "signal_id": None, "ts_code": code,
                    "decision_date": day, "order_date": day, "expire_date": day,
                    "direction": "SELL", "planned_shares": shares,
                    "order_shares": shares, "requested_shares": shares,
                    "actual_shares": 0,
                    "volume_capacity_shares": liquidity.volume_capacity_shares,
                    "liquidity_status": liquidity.liquidity_status,
                    "liquidity_reject_reason": liquidity.reject_reason,
                    "liquidity_reference_date": liquidity_day,
                    "theoretical_shares": float(shares), "required_capital": 0.0,
                    "max_loss_at_stop": 0.0, "gap_stress_loss": 0.0,
                    "buy_fees": 0.0, "estimated_sell_fees": 0.0,
                    "estimated_slippage": 0.0, "actual_risk_pct": 0.0,
                    "risk_policy_version": self.config.risk_policy_version,
                    "signal_score": 0.0, "reference_price": stop_value,
                    "stop_loss_price_raw": stop_value, "status": "PENDING",
                    "reject_reason": None, "kind": "STOP",
                    "target_lot_ids": [lot.lot_id for lot in stop_lots],
                }
                self._orders.append(order)
                if not liquidity.allowed:
                    self._reject_order(
                        order, liquidity.reject_reason or "流动性门禁拒绝"
                    )
                    continue
                reference_price = min(float(bar.raw_open), stop_value)
                self._sell_position(
                    code,
                    shares,
                    self._apply_slippage(
                        reference_price, "SELL", fee_rule, market_rule, bar=bar
                    ),
                    day,
                    order,
                    "STOP_LOSS",
                    fee_rule,
                    market_rule,
                    reference_price=reference_price,
                    target_lot_ids={lot.lot_id for lot in stop_lots},
                )
                if order["status"] == "FILLED":
                    order["actual_shares"] = order["order_shares"]

    def _process_stop_losses_legacy_unused(self, day: date, dataset: BacktestDataset):
        for code, lots in list(self._positions.items()):
            bar = dataset.bar(day, code)
            if bar is None or not bar.has_execution_prices:
                continue
            triggering = [
                lot for lot in lots
                if lot.remaining_shares > 0 and lot.stop_loss_price_raw is not None
                and lot.available_sell_date <= day and bar.raw_low is not None
                and bar.raw_low <= lot.stop_loss_price_raw
            ]
            if not triggering:
                continue
            shares = sum(lot.remaining_shares for lot in triggering)
            if not bar.can_sell or bar.is_suspended or self._limit_blocked(bar, "SELL", intraday=True):
                self._warnings.append(f"{day}: {code} 止损触发但无法卖出，保留持仓")
                continue
            order = {
                "order_id": self._new_id(), "signal_id": None, "ts_code": code,
                "decision_date": day, "order_date": day, "expire_date": day,
                "direction": "SELL", "planned_shares": shares, "order_shares": shares,
                "requested_shares": shares, "actual_shares": 0,
                "volume_capacity_shares": None, "liquidity_status": "NOT_CHECKED",
                "liquidity_reject_reason": None,
                "theoretical_shares": float(shares), "required_capital": 0.0,
                "max_loss_at_stop": 0.0, "gap_stress_loss": 0.0,
                "buy_fees": 0.0, "estimated_sell_fees": 0.0,
                "estimated_slippage": 0.0, "actual_risk_pct": 0.0,
                "risk_policy_version": self.config.risk_policy_version,
                "signal_score": 0.0,
                "reference_price": triggering[0].stop_loss_price_raw,
                "stop_loss_price_raw": triggering[0].stop_loss_price_raw,
                "status": "PENDING", "reject_reason": None, "kind": "STOP",
            }
            self._orders.append(order)
            market_rule, fee_rule = self._rules_for(day, code, dataset)
            liquidity = check_liquidity(
                bar.volume, shares, market_rule,
                amount=bar.amount, close_price=bar.raw_close,
                low_price=bar.raw_low, high_price=bar.raw_high,
            )
            order["volume_capacity_shares"] = liquidity.volume_capacity_shares
            order["liquidity_status"] = liquidity.liquidity_status
            order["liquidity_reject_reason"] = liquidity.reject_reason
            if not liquidity.allowed:
                self._reject_order(order, liquidity.reject_reason or "流动性门禁拒绝")
                continue
            open_price = float(bar.raw_open)
            stop_price = min(open_price, float(min(lot.stop_loss_price_raw for lot in triggering)))
            self._sell_position(
                code, shares, self._apply_slippage(
                    stop_price, "SELL", fee_rule, market_rule, bar=bar
                ),
                day, order, "STOP_LOSS", fee_rule, market_rule,
                reference_price=stop_price,
            )
            if order["status"] == "FILLED":
                order["actual_shares"] = order["order_shares"]

    def _assert_no_unresolved_delist_positions(self, day: date, dataset: BacktestDataset):
        """退市持仓必须有真实退出结果，禁止用最后收盘价永久估值。"""
        unresolved = []
        for code, lots in self._positions.items():
            shares = sum(max(0, lot.remaining_shares) for lot in lots)
            delist_date = dataset.delist_date_for(code)
            if shares > 0 and delist_date is not None and day >= delist_date:
                unresolved.append((code, delist_date, shares))
        if unresolved:
            raise RuntimeError(
                "存在未形成真实退出事件的退市持仓，正式回测 fail-closed: "
                f"{unresolved[:20]}"
            )

    def _current_equity(self, day: date, dataset: BacktestDataset) -> float:
        value = self.cash
        for code, lots in self._positions.items():
            if (
                sum(max(0, lot.remaining_shares) for lot in lots) > 0
                and dataset.delist_date_for(code) is not None
                and day >= dataset.delist_date_for(code)
            ):
                raise RuntimeError(f"{day}/{code} 退市持仓禁止使用最后收盘价估值")
            bar = dataset.bar(day, code)
            close = bar.raw_close if bar and bar.raw_close is not None else self._last_close.get(code, 0.0)
            value += sum(lot.remaining_shares for lot in lots) * float(close or 0.0)
        cash_receivable, stock_receivable_value = self._pending_receivable_values(day, dataset)
        value += cash_receivable + stock_receivable_value
        return value

    def _record_daily(self, day: date, dataset: BacktestDataset):
        position_value = 0.0
        status = "VALID"
        for code, lots in self._positions.items():
            active = [lot for lot in lots if lot.remaining_shares > 0]
            if not active:
                continue
            if (
                dataset.delist_date_for(code) is not None
                and day >= dataset.delist_date_for(code)
            ):
                raise RuntimeError(f"{day}/{code} 退市持仓禁止使用最后收盘价记录净值")
            bar = dataset.bar(day, code)
            close = bar.raw_close if bar and bar.raw_close is not None else self._last_close.get(code)
            if close is None:
                status = "MISSING_PRICE"
                continue
            shares = sum(lot.remaining_shares for lot in active)
            avg = sum(lot.remaining_shares * lot.cost_price for lot in active) / shares
            market_value = shares * float(close)
            position_value += market_value
            self._position_daily.append({
                "trade_date": day, "ts_code": code, "hold_shares": shares,
                "average_cost": avg, "raw_close": close, "market_value": market_value,
                "unrealized_pnl": market_value - shares * avg,
                "valuation_status": "VALID" if bar and bar.raw_close is not None else "STALE_PRICE",
            })
        cash_receivable, stock_receivable_value = self._pending_receivable_values(day, dataset)
        receivable_total = cash_receivable + stock_receivable_value
        equity = self.cash + position_value + receivable_total
        previous = self._performance_daily[-1]["total_equity"] if self._performance_daily else self.config.initial_cash
        daily_return = equity / previous - 1 if previous else 0.0
        peak = max([row["total_equity"] for row in self._performance_daily] + [self.config.initial_cash])
        turnover = sum(
            fill["fill_price"] * fill["fill_shares"]
            for fill in self._fills if fill["fill_date"] == day
        )
        self._performance_daily.append({
            "trade_date": day, "cash_balance": self.cash,
            "position_value": position_value,
            "cash_receivable": cash_receivable,
            "stock_receivable_value": stock_receivable_value,
            "receivable_total": receivable_total,
            "total_equity": equity,
            "daily_return": daily_return,
            "cumulative_return": equity / self.config.initial_cash - 1,
            "drawdown": (peak - equity) / peak if peak else 0.0,
            "turnover": turnover, "valuation_status": status,
        })

    def _forced_liquidation_value(self, day: date, dataset: BacktestDataset) -> tuple[float, int, int]:
        """按区间末最后可见原始收盘价计算假设清仓值，不伪造 Fill。

        返回值依次为：假设清仓权益、没有可执行收盘行情的持仓数、当日不可卖的持仓数。
        即使停牌、涨停或退市导致实际不能成交，也保留独立的假设估值并通过指标/警告标记，
        不把它冒充为真实成交结果。
        """
        value = float(self.cash)
        missing_price_positions = 0
        blocked_positions = 0
        for code, lots in self._positions.items():
            shares = sum(max(0, lot.remaining_shares) for lot in lots)
            if shares <= 0:
                continue
            if (
                dataset.delist_date_for(code) is not None
                and day >= dataset.delist_date_for(code)
            ):
                # 正式 run 会在当日先由 _assert_no_unresolved_delist_positions
                # 阻断；这里保留独立保护，避免调用该估值函数时复用退市前旧价。
                blocked_positions += 1
                continue
            bar = dataset.bar(day, code)
            close = bar.raw_close if bar and bar.raw_close is not None else self._last_close.get(code)
            if close is None or close <= 0:
                missing_price_positions += 1
                continue
            market_rule, fee_rule = self._rules_for(day, code, dataset)
            gross = float(close) * shares
            value += gross - fee_rule.sell_cost(gross)
            if (
                bar is None
                or bar.is_suspended
                or not bar.can_sell
                or self._limit_blocked(bar, "SELL", intraday=True)
            ):
                blocked_positions += 1
        return value, missing_price_positions, blocked_positions

    def _calculate_metrics(self, dataset: BacktestDataset) -> dict[str, float]:
        if not self._performance_daily:
            forced_value, missing_positions, blocked_positions = self._forced_liquidation_value(
                dataset.evaluation_end, dataset
            )
            self._audit_metadata["end_of_data_liquidation"] = {
                "event_type": "END_OF_DATA_LIQUIDATION",
                "trade_date": dataset.evaluation_end.isoformat(),
                "equity": forced_value,
                "missing_price_positions": missing_positions,
                "blocked_positions": blocked_positions,
                "is_hypothetical": True,
            }
            self._audit_metadata["monte_carlo"] = MonteCarloTester.run_ruin_test(
                self._r_multiples, initial_capital=self.config.initial_cash,
                risk_pct=self.config.risk_pct, random_seed=self.config.random_seed,
                method=self.config.monte_carlo_method,
            )
            metrics = {
                "final_equity": self.cash,
                "final_equity_mark_to_market": self.cash,
                "final_equity_forced_liquidation": forced_value,
                "forced_liquidation_missing_price_positions": float(missing_positions),
                "forced_liquidation_blocked_positions": float(blocked_positions),
                "total_return": 0.0,
                "max_drawdown": 0.0,
                "trade_count": 0.0,
                "win_rate": 0.0,
            }
            return self._with_benchmark_metrics(metrics, dataset, 1)
        equities = [float(row["total_equity"]) for row in self._performance_daily]
        returns = [float(row["daily_return"]) for row in self._performance_daily[1:]]
        final = equities[-1]
        peak = self.config.initial_cash
        max_dd = 0.0
        for value in equities:
            peak = max(peak, value)
            max_dd = max(max_dd, (peak - value) / peak if peak else 0.0)
        days = max(1, len(self._performance_daily))
        cagr = (final / self.config.initial_cash) ** (252 / days) - 1 if final > 0 else -1.0
        mean_return = sum(returns) / len(returns) if returns else 0.0
        variance = sum((item - mean_return) ** 2 for item in returns) / max(1, len(returns) - 1)
        volatility = math.sqrt(variance) * math.sqrt(252)
        sharpe = mean_return / math.sqrt(variance) * math.sqrt(252) if variance > 0 else 0.0
        wins = sum(1 for item in self._realized_trades if item["pnl"] > 0)
        trade_count = len(self._realized_trades)
        metrics = {
            "final_equity": final,
            "final_equity_mark_to_market": final,
            "total_return": final / self.config.initial_cash - 1,
            "cagr": cagr,
            "max_drawdown": max_dd,
            "annualized_volatility": volatility,
            "sharpe": sharpe,
            "trade_count": float(trade_count),
            "win_rate": wins / trade_count if trade_count else 0.0,
            "open_position_count": float(len(self._positions_view())),
            "turnover": sum(float(row["turnover"]) for row in self._performance_daily),
            "realized_trade_cash_dividend_income": sum(
                float(item.get("cash_dividend_income", 0.0))
                for item in self._realized_trades
            ),
        }
        metrics = self._with_benchmark_metrics(metrics, dataset, days)
        final_day = self._performance_daily[-1]["trade_date"]
        forced_value, missing_positions, blocked_positions = self._forced_liquidation_value(
            final_day, dataset
        )
        self._audit_metadata["end_of_data_liquidation"] = {
            "event_type": "END_OF_DATA_LIQUIDATION",
            "trade_date": final_day.isoformat(),
            "equity": forced_value,
            "missing_price_positions": missing_positions,
            "blocked_positions": blocked_positions,
            "is_hypothetical": True,
        }
        metrics["final_equity_forced_liquidation"] = forced_value
        metrics["forced_liquidation_missing_price_positions"] = float(missing_positions)
        metrics["forced_liquidation_blocked_positions"] = float(blocked_positions)
        if missing_positions or blocked_positions:
            self._warnings.append(
                f"{final_day}: 区间末假设清仓存在不可直接成交持仓，"
                f"missing_price={missing_positions}, blocked={blocked_positions}"
            )
        monte = MonteCarloTester.run_ruin_test(
            self._r_multiples, initial_capital=self.config.initial_cash,
            risk_pct=self.config.risk_pct, random_seed=self.config.random_seed,
            method=self.config.monte_carlo_method,
        )
        self._audit_metadata["monte_carlo"] = monte
        if monte.get("status") == "OK":
            metrics["monte_carlo_ruin_probability"] = float(monte["ruin_probability"])
            metrics["monte_carlo_expectancy_r"] = float(monte["expectancy_r"])
        return metrics

    def _with_benchmark_metrics(
        self, metrics: dict[str, float], dataset: BacktestDataset, observation_days: int
    ) -> dict[str, float]:
        if not dataset.benchmark_code:
            return metrics
        evaluation_dates = [
            day for day in dataset.evaluation_dates if day in dataset.benchmark_closes
        ]
        if len(evaluation_dates) != len(dataset.evaluation_dates):
            raise RuntimeError("FROZEN Dataset 基准行情未完整覆盖 evaluation 区间")
        first = float(dataset.benchmark_closes[evaluation_dates[0]])
        last = float(dataset.benchmark_closes[evaluation_dates[-1]])
        if first <= 0 or last <= 0:
            raise RuntimeError("FROZEN Dataset 基准收盘价无效")
        benchmark_return = last / first - 1.0
        benchmark_cagr = (
            (last / first) ** (252 / max(1, observation_days)) - 1.0
        )
        metrics["benchmark_total_return"] = benchmark_return
        metrics["benchmark_cagr"] = benchmark_cagr
        metrics["excess_total_return"] = float(metrics.get("total_return", 0.0)) - benchmark_return
        metrics["excess_cagr"] = float(metrics.get("cagr", 0.0)) - benchmark_cagr
        if not self._performance_daily:
            return metrics
        split = dataset.out_of_sample_start or dataset.evaluation_start
        performance_by_day = {
            row["trade_date"]: float(row["total_equity"])
            for row in self._performance_daily
        }
        oos_dates = [day for day in evaluation_dates if day >= split]
        in_sample_dates = [day for day in evaluation_dates if day < split]
        if oos_dates:
            prior_dates = [day for day in evaluation_dates if day < oos_dates[0]]
            strategy_oos_base = (
                performance_by_day[prior_dates[-1]]
                if prior_dates else self.config.initial_cash
            )
            benchmark_oos_base = (
                float(dataset.benchmark_closes[prior_dates[-1]])
                if prior_dates else float(dataset.benchmark_closes[oos_dates[0]])
            )
            strategy_oos_return = performance_by_day[oos_dates[-1]] / strategy_oos_base - 1.0
            benchmark_oos_return = (
                float(dataset.benchmark_closes[oos_dates[-1]]) / benchmark_oos_base - 1.0
            )
            metrics["out_of_sample_total_return"] = strategy_oos_return
            metrics["out_of_sample_benchmark_return"] = benchmark_oos_return
            metrics["out_of_sample_excess_return"] = strategy_oos_return - benchmark_oos_return
            metrics["out_of_sample_days"] = float(len(oos_dates))
        if in_sample_dates:
            strategy_is_return = (
                performance_by_day[in_sample_dates[-1]] / self.config.initial_cash - 1.0
            )
            benchmark_is_return = (
                float(dataset.benchmark_closes[in_sample_dates[-1]])
                / float(dataset.benchmark_closes[in_sample_dates[0]]) - 1.0
            )
            metrics["in_sample_total_return"] = strategy_is_return
            metrics["in_sample_benchmark_return"] = benchmark_is_return
            metrics["in_sample_excess_return"] = strategy_is_return - benchmark_is_return
            metrics["in_sample_days"] = float(len(in_sample_dates))
        return metrics
