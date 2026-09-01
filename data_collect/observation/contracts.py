"""市场异常观察域的稳定身份和枚举契约。

本模块只保存数据契约，不导入数据库、Pandas 或 Tushare。方法论身份由
``ObservationPolicy``（观察策略）序列化后计算，避免把一个可变的随机 UUID 当成
可复现身份。
"""
from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import math
from typing import Any, Mapping


CONTRACT_VERSION = "market-anomaly-observation-2026-08-31"
METHODOLOGY_ID = "STOCK_FIRST_OPTIONAL_SECTOR"

PRIMARY_STATES = (
    "PANIC_SELLING",
    "HIGH_POSITION_DIVERGENCE",
    "TREND_EXPANSION",
    "ACCUMULATION_LIKE",
    "NEW_ANOMALY",
    "COOLING",
    "NORMAL",
)

ANOMALY_TAGS = (
    "VOLUME_EXPANSION",
    "TURNOVER_EXPANSION",
    "LOW_POSITION",
    "SECTOR_CONFIRMATION",
    "PRICE_DIVERGENCE",
    "PERSISTENT_ANOMALY",
    "MARKET_RELATIVE",
)

SECTOR_DATA_STATUSES = ("CONFIRMED", "DAILY_CAPTURED", "MISSING", "INVALID")
SECTOR_CONFIRMATION_STATUSES = ("CONFIRMED", "NOT_CONFIRMED", "UNKNOWN")

CALCULATION_STATUSES = ("VALID", "PARTIAL", "INSUFFICIENT_HISTORY", "INVALID")
QUALITY_STATUSES = ("VALID", "WARNING", "MISSING_SOURCE", "INVALID", "FAILED")


def canonicalize(value: Any) -> Any:
    """Convert values to deterministic, JSON-safe values.

    NaN and infinity are represented as ``None``. This is intentional: such
    values are missing observations, never valid numerical inputs.
    """
    if value is None:
        return None
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): canonicalize(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        values = [canonicalize(item) for item in value]
        if isinstance(value, (set, frozenset)):
            return sorted(values, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
        return values
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(
        canonicalize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


@dataclass(frozen=True)
class ObservationPolicy:
    """所有窗口、阈值、截尾和权重的单一事实来源。

    其他状态的触发条件也显式放在策略中，而不是散落在 Job（作业）里。
    修改任一字段都会改变 ``policy_hash`` 和 ``methodology_hash``，因此不会
    原地覆盖已经冻结的观察结果。
    """

    policy_id: str = "stock_first_optional_sector_v1"
    benchmark_code: str = "000985.CSI"
    industry_system: str = "SW2021"
    industry_level: int = 1

    short_window: int = 5
    anomaly_window: int = 20
    baseline_window: int = 60
    position_window: int = 252
    max_lookback: int = 252
    min_valid_fraction: float = 0.80
    min_universe_size: int = 30
    min_observation_coverage: float = 0.80
    denominator_floor: float = 1e-12

    amount_ratio_threshold: float = 1.50
    turnover_ratio_threshold: float = 1.30
    amount_zscore_threshold: float = 2.0
    amount_single_ratio_threshold: float = 1.50

    low_position_threshold: float = 0.35
    high_position_threshold: float = 0.80
    accumulation_return_min: float = -0.05
    accumulation_return_max: float = 0.12
    accumulation_drawdown_floor: float = -0.10
    accumulation_up_down_volume_ratio: float = 1.10

    panic_return_threshold: float = -0.12
    panic_drawdown_threshold: float = -0.15
    trend_return_threshold: float = 0.12
    trend_distance_ma20_threshold: float = 0.05
    high_position_return_ceiling: float = 0.02
    cooling_activity_floor: float = 1.00

    price_response_floor: float = 0.10
    score_clip_low: float = 0.0
    score_clip_high: float = 100.0
    score_min_dimensions: int = 3
    score_weights: tuple[tuple[str, float], ...] = (
        ("trading_anomaly_score", 0.30),
        ("price_structure_score", 0.20),
        ("persistence_score", 0.20),
        ("sector_confirmation_score", 0.10),
        ("risk_score", 0.20),
    )
    score_ranges: tuple[tuple[str, float, float], ...] = (
        ("amount_ratio_20_60", 1.0, 3.0),
        ("turnover_ratio_20_60", 1.0, 3.0),
        ("amount_zscore_60", 0.0, 4.0),
        ("return_20d", -0.20, 0.20),
        ("price_position_52w", 0.0, 1.0),
        ("anomaly_frequency_20", 0.0, 1.0),
        ("max_drawdown_20d", -0.40, 0.0),
        ("volatility_20d", 0.0, 0.10),
    )
    max_rows_per_tile: int = 50_000
    code_batch_size: int = 100
    date_batch_size: int = 1

    def __post_init__(self) -> None:
        if self.industry_level <= 0:
            raise ValueError("industry_level must be positive")
        for name in (
            "short_window", "anomaly_window", "baseline_window",
            "position_window", "max_lookback", "min_universe_size",
            "score_min_dimensions", "max_rows_per_tile", "code_batch_size",
            "date_batch_size",
        ):
            if int(getattr(self, name)) <= 0:
                raise ValueError(f"{name} must be positive")
        if not 0 < self.min_valid_fraction <= 1:
            raise ValueError("min_valid_fraction must be in (0, 1]")
        if not 0 < self.min_observation_coverage <= 1:
            raise ValueError("min_observation_coverage must be in (0, 1]")
        if self.max_lookback < self.position_window:
            raise ValueError("max_lookback must cover position_window")
        if self.score_clip_low >= self.score_clip_high:
            raise ValueError("score clip range must be ascending")
        if self.price_response_floor <= 0 or self.denominator_floor <= 0:
            raise ValueError("numerical floors must be positive")
        if not self.score_weights or abs(sum(weight for _, weight in self.score_weights) - 1.0) > 1e-9:
            raise ValueError("score_weights must be non-empty and sum to 1")

    def as_dict(self) -> dict[str, Any]:
        return {item.name: canonicalize(getattr(self, item.name)) for item in fields(self)}

    @property
    def policy_hash(self) -> str:
        return hashlib.sha256(canonical_json(self.as_dict()).encode("utf-8")).hexdigest()

    @property
    def methodology_hash(self) -> str:
        identity = {
            "contract_version": CONTRACT_VERSION,
            "methodology_id": METHODOLOGY_ID,
            "policy_hash": self.policy_hash,
        }
        return hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()

    @property
    def max_required_history(self) -> int:
        return max(self.position_window, self.baseline_window + self.anomaly_window, 120)

    def score_range(self, field_name: str) -> tuple[float, float] | None:
        for name, lower, upper in self.score_ranges:
            if name == field_name:
                return lower, upper
        return None


@dataclass(frozen=True)
class ObservationIdentity:
    """个股观察的四字段业务身份。"""

    observation_date: date
    ts_code: str
    methodology_hash: str
    data_baseline_id: str

    def __post_init__(self) -> None:
        if not self.ts_code.strip() or not self.methodology_hash.strip() or not self.data_baseline_id.strip():
            raise ValueError("observation identity fields must be non-empty")

    def as_string(self) -> str:
        return "|".join(
            (self.observation_date.isoformat(), self.ts_code, self.methodology_hash, self.data_baseline_id)
        )


def reason_codes(value: Any) -> tuple[str, ...]:
    """Normalize reason codes without losing deterministic ordering."""
    if value is None:
        return ()
    if isinstance(value, str):
        values = (value,)
    else:
        values = tuple(str(item) for item in value)
    return tuple(sorted({item for item in values if item}))
