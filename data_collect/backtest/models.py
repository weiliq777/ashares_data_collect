"""Backtest 领域模型。

这些模型不连接数据库，也不访问网络，方便用固定 fixture 验证撮合、T+1、费用和
公司行为逻辑。生产输入必须由 Frozen Dataset Materializer 生成。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
import math
from typing import Any, Mapping


def _as_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


@dataclass(frozen=True)
class MarketBar:
    trade_date: date
    ts_code: str
    raw_open: float | None
    raw_high: float | None
    raw_low: float | None
    raw_close: float | None
    volume: float | None = None
    amount: float | None = None
    up_limit: float | None = None
    down_limit: float | None = None
    is_suspended: bool = False
    can_buy: bool = True
    can_sell: bool = True
    limit_status: str | None = None
    market: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "trade_date", _as_date(self.trade_date))

    @property
    def has_execution_prices(self) -> bool:
        return all(value is not None and value > 0 for value in (
            self.raw_open, self.raw_high, self.raw_low, self.raw_close
        ))


@dataclass(frozen=True)
class Observation:
    """某个 FROZEN Research Snapshot 中的一只股票研究输入。"""

    research_date: date
    ts_code: str
    values: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        object.__setattr__(self, "research_date", _as_date(self.research_date))

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)


@dataclass(frozen=True)
class DividendEvent:
    ts_code: str
    ex_date: date
    cash_div: float = 0.0
    stock_div: float = 0.0
    stock_bonus_rate: float | None = None
    stock_conversion_rate: float | None = None
    pay_date: date | None = None
    record_date: date | None = None
    implementation_status: str = "实施"
    source_record_key: str = ""
    source_record_version: int | None = None
    source_payload_hash: str | None = None
    announce_date: date | None = None
    implementation_announce_date: date | None = None
    div_list_date: date | None = None
    cash_div_tax: float | None = None

    def __post_init__(self):
        object.__setattr__(self, "ex_date", _as_date(self.ex_date))
        if self.pay_date is not None:
            object.__setattr__(self, "pay_date", _as_date(self.pay_date))
        if self.record_date is not None:
            object.__setattr__(self, "record_date", _as_date(self.record_date))
        if self.announce_date is not None:
            object.__setattr__(self, "announce_date", _as_date(self.announce_date))
        if self.implementation_announce_date is not None:
            object.__setattr__(self, "implementation_announce_date", _as_date(self.implementation_announce_date))
        if self.div_list_date is not None:
            object.__setattr__(self, "div_list_date", _as_date(self.div_list_date))
        for field_name in ("pay_date", "div_list_date"):
            value = getattr(self, field_name)
            if value is not None and value < self.ex_date:
                raise ValueError(f"{field_name} 不能早于 ex_date")

        # Tushare 的 stk_div 是送转合计，stk_bo_rate/stk_co_rate 是分项。
        # 允许来源只提供合计或只提供分项，但两者同时存在时必须一致；否则
        # 继续运行会把股数、成本和分红收益静默放大，属于不可接受的数据错误。
        for field_name in (
            "cash_div", "cash_div_tax", "stock_div", "stock_bonus_rate",
            "stock_conversion_rate",
        ):
            value = float(getattr(self, field_name) or 0.0)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{field_name} 必须是有限非负数")
        total = float(self.stock_div or 0.0)
        components = float(self.stock_bonus_rate or 0.0) + float(self.stock_conversion_rate or 0.0)
        total_present = self.stock_div is not None
        components_present = (
            self.stock_bonus_rate is not None or self.stock_conversion_rate is not None
        )
        if total_present and components_present and not math.isclose(
            total, components, rel_tol=1e-8, abs_tol=1e-8
        ):
            raise ValueError(
                "送转股合计与送股/转增分项不一致: "
                f"stock_div={total}, components={components}"
            )

    @property
    def stock_rate(self) -> float:
        total = float(self.stock_div or 0.0)
        if total > 1e-12:
            return total
        return float(self.stock_bonus_rate or 0.0) + float(self.stock_conversion_rate or 0.0)

    @property
    def coordinate_cash_div(self) -> float:
        """Return the pre-tax cash amount used for the ex-right coordinate."""
        if self.cash_div_tax is not None:
            return float(self.cash_div_tax)
        return float(self.cash_div or 0.0)

    @property
    def effective_announce_date(self) -> date | None:
        return self.implementation_announce_date or self.announce_date

    @property
    def stock_available_date(self) -> date:
        return self.div_list_date or self.pay_date or self.ex_date


@dataclass(frozen=True)
class SignalIntent:
    ts_code: str
    direction: str
    indicator_price: float | None = None
    raw_reference_price: float | None = None
    stop_loss_price_raw: float | None = None
    reason: Mapping[str, Any] = field(default_factory=dict)
    score: float = 0.0

    def __post_init__(self):
        if self.direction not in {"BUY", "SELL", "HOLD"}:
            raise ValueError(f"unsupported signal direction: {self.direction}")
        if not math.isfinite(float(self.score)):
            raise ValueError("signal score 必须是有限数")


@dataclass
class PositionLotState:
    lot_id: str
    ts_code: str
    open_date: date
    available_sell_date: date
    open_shares: int
    remaining_shares: int
    cost_price: float
    stop_loss_price_raw: float | None
    open_fill_id: str | None = None
    close_date: date | None = None
    close_fill_id: str | None = None
    opening_event_type: str = "FILL"
    opening_event_key: str | None = None
    accrued_cash_dividend_per_share: float = 0.0


@dataclass(frozen=True)
class BacktestDataset:
    """Materialized 的不可变内存数据集。

    ``observations`` 是 Research 快照字段；``bars`` 是冻结的真实执行行情。两者
    必须由同一个 dataset manifest 产生，不能在引擎内部临时查询数据库。
    """

    dataset_id: str
    calendar: tuple[date, ...]
    evaluation_start: date
    evaluation_end: date
    warmup_start: date
    observations: Mapping[date, tuple[Observation, ...]]
    bars: Mapping[tuple[date, str], MarketBar]
    dividends: Mapping[date, tuple[DividendEvent, ...]] = field(default_factory=dict)
    member_roles: Mapping[date, str] = field(default_factory=dict)
    snapshot_ids: Mapping[date, str] = field(default_factory=dict)
    rules: Mapping[tuple[date, str | None], tuple[Any, Any]] = field(default_factory=dict)
    delist_dates: Mapping[str, date | None] = field(default_factory=dict)
    benchmark_code: str | None = None
    benchmark_closes: Mapping[date, float] = field(default_factory=dict)
    out_of_sample_start: date | None = None
    input_hash: str | None = None
    require_explicit_rules: bool = False

    def __post_init__(self):
        dates = tuple(_as_date(day) for day in self.calendar)
        if tuple(sorted(set(dates))) != dates:
            raise ValueError("dataset calendar must be strictly increasing and unique")
        start = _as_date(self.evaluation_start)
        end = _as_date(self.evaluation_end)
        warmup = _as_date(self.warmup_start)
        if warmup > start or start > end:
            raise ValueError("invalid warmup/evaluation range")
        if not dates or dates[0] > warmup or dates[-1] < end:
            raise ValueError("dataset calendar does not cover warmup/evaluation range")
        object.__setattr__(self, "calendar", dates)
        object.__setattr__(self, "evaluation_start", start)
        object.__setattr__(self, "evaluation_end", end)
        object.__setattr__(self, "warmup_start", warmup)
        oos_start = _as_date(self.out_of_sample_start or start)
        if not start <= oos_start <= end:
            raise ValueError("out_of_sample_start must be within evaluation range")
        object.__setattr__(self, "out_of_sample_start", oos_start)
        normalized_delist_dates = {
            str(code): (_as_date(value) if value is not None else None)
            for code, value in self.delist_dates.items()
        }
        # 允许 fixture 只在 Observation 中提供 delist_date；生产 loader 会同时
        # 传入显式映射，避免引擎退回到“没有行情就沿用最后收盘价”的错误路径。
        for day_rows in self.observations.values():
            for observation in day_rows:
                value = observation.get("delist_date")
                if value is not None and observation.ts_code not in normalized_delist_dates:
                    normalized_delist_dates[observation.ts_code] = _as_date(value)
        object.__setattr__(self, "delist_dates", normalized_delist_dates)

    @property
    def evaluation_dates(self) -> tuple[date, ...]:
        return tuple(day for day in self.calendar if self.evaluation_start <= day <= self.evaluation_end)

    def next_trade_date(self, day: date) -> date | None:
        day = _as_date(day)
        for candidate in self.calendar:
            if candidate > day:
                return candidate
        return None

    def bar(self, day: date, ts_code: str) -> MarketBar | None:
        return self.bars.get((_as_date(day), ts_code))

    def observations_for(self, day: date) -> tuple[Observation, ...]:
        return tuple(self.observations.get(_as_date(day), ()))

    def dividend_events_for(self, day: date) -> tuple[DividendEvent, ...]:
        return tuple(self.dividends.get(_as_date(day), ()))

    def delist_date_for(self, ts_code: str) -> date | None:
        return self.delist_dates.get(ts_code)

    def rules_for(self, day: date, market: str | None = None):
        day = _as_date(day)
        return self.rules.get((day, market)) or self.rules.get((day, None))


@dataclass(frozen=True)
class BacktestResult:
    status: str
    initial_cash: float
    final_equity: float
    metrics: Mapping[str, float]
    signals: tuple[Mapping[str, Any], ...]
    orders: tuple[Mapping[str, Any], ...]
    fills: tuple[Mapping[str, Any], ...]
    cash_ledger: tuple[Mapping[str, Any], ...]
    performance_daily: tuple[Mapping[str, Any], ...]
    position_daily: tuple[Mapping[str, Any], ...]
    position_lots: tuple[Mapping[str, Any], ...] = ()
    warnings: tuple[str, ...] = ()
    audit_metadata: Mapping[str, Any] = field(default_factory=dict)
