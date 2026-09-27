"""Contracts for the market activity monitor（市场成交监控契约）."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from data_collect.observation.hashing import hash_object


MARKET_ACTIVITY_CONTRACT_VERSION = "market-activity-v1"
MARKET_ACTIVITY_METHODOLOGY_ID = "TRADING_ACTIVITY_CONCENTRATION"
MARKET_ACTIVITY_CONTRACT_HASH = hash_object(
    {
        "contract_version": MARKET_ACTIVITY_CONTRACT_VERSION,
        "methodology_id": MARKET_ACTIVITY_METHODOLOGY_ID,
        "source": "std.daily_kline",
        "source_columns": ("stock_code", "trade_date", "close", "volume", "amount"),
        "units": {"volume": "shares", "amount": "yuan"},
        "lookback_semantics": "preceding_available_bars_only",
        "identity": ("trade_date", "ts_code", "methodology_hash", "data_baseline_id"),
    }
)


@dataclass(frozen=True, slots=True)
class MarketActivityRequest:
    """Bounded read contract for one target trading date.

    ``lookback_trading_days`` means prior available Standard bars, not
    calendar days.  The Repository receives a bounded calendar window large
    enough to collect those bars; it never performs an unbounded scan.
    """

    trade_date: date
    lookback_trading_days: int = 20
    calendar_lookback_days: int = 45
    top_n: int = 20
    max_bars: int = 250_000
    data_baseline_id: str | None = None

    def validate(self) -> "MarketActivityRequest":
        if isinstance(self.trade_date, datetime) or not isinstance(self.trade_date, date):
            raise TypeError("trade_date must be a datetime.date")
        if not 1 <= self.lookback_trading_days <= 120:
            raise ValueError("lookback_trading_days must be in 1..120")
        if not 10 <= self.calendar_lookback_days <= 180:
            raise ValueError("calendar_lookback_days must be in 10..180")
        if self.calendar_lookback_days < self.lookback_trading_days:
            raise ValueError(
                "calendar_lookback_days must cover the requested trading-day lookback"
            )
        if not 1 <= self.top_n <= 500:
            raise ValueError("top_n must be in 1..500")
        if not 1_000 <= self.max_bars <= 1_000_000:
            raise ValueError("max_bars must be in 1000..1000000")
        if self.data_baseline_id is not None and not str(self.data_baseline_id).strip():
            raise ValueError("data_baseline_id must not be blank when supplied")
        return self

    @property
    def methodology_hash(self) -> str:
        """Stable identity of calculation parameters, excluding the data baseline."""
        return hash_object(
            {
                "methodology_id": MARKET_ACTIVITY_METHODOLOGY_ID,
                "contract_version": MARKET_ACTIVITY_CONTRACT_VERSION,
                "lookback_trading_days": self.lookback_trading_days,
                "calendar_lookback_days": self.calendar_lookback_days,
                "top_n": self.top_n,
            }
        )

    def require_persistence_identity(self) -> tuple[str, str]:
        self.validate()
        baseline = str(self.data_baseline_id or "").strip()
        if not baseline:
            raise ValueError(
                "persisted market activity requires an explicit validated data_baseline_id"
            )
        return self.methodology_hash, baseline


@dataclass(frozen=True, slots=True)
class StockActivity:
    """One stock's observed activity on the target date.

    ``amount`` is standardized成交额（元） and ``volume`` is standardized
    成交量（股）.  The rolling ratios compare the target value with the mean
    of the preceding available bars only; they never use future rows.
    """

    ts_code: str
    trade_date: date
    close: float | None
    volume: float
    amount: float
    amount_share: float | None
    volume_share: float | None
    amount_rank: int
    volume_rank: int
    amount_avg_previous: float | None
    amount_ratio_previous: float | None
    volume_avg_previous: float | None
    volume_ratio_previous: float | None
    history_observation_count: int
    reason_codes: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "ts_code": self.ts_code,
            "trade_date": self.trade_date.isoformat(),
            "close": self.close,
            "volume": self.volume,
            "amount": self.amount,
            "amount_share": self.amount_share,
            "volume_share": self.volume_share,
            "amount_rank": self.amount_rank,
            "volume_rank": self.volume_rank,
            "amount_avg_previous": self.amount_avg_previous,
            "amount_ratio_previous": self.amount_ratio_previous,
            "volume_avg_previous": self.volume_avg_previous,
            "volume_ratio_previous": self.volume_ratio_previous,
            "history_observation_count": self.history_observation_count,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, slots=True)
class MarketActivityReport:
    """Read-only result for one trading date."""

    contract_version: str
    trade_date: date
    lookback_start: date
    top_n: int
    quality_status: str
    input_row_count: int
    target_row_count: int
    invalid_target_row_count: int
    invalid_history_row_count: int
    insufficient_history_stock_count: int
    total_volume: float
    total_amount: float
    top_n_volume: float
    top_n_amount: float
    top_n_volume_share: float | None
    top_n_amount_share: float | None
    rows: tuple[StockActivity, ...]
    methodology_hash: str = ""
    data_baseline_id: str | None = None
    input_manifest_hash: str = ""
    activity_hash: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "trade_date": self.trade_date.isoformat(),
            "lookback_start": self.lookback_start.isoformat(),
            "top_n": self.top_n,
            "quality_status": self.quality_status,
            "input_row_count": self.input_row_count,
            "target_row_count": self.target_row_count,
            "invalid_target_row_count": self.invalid_target_row_count,
            "invalid_history_row_count": self.invalid_history_row_count,
            "insufficient_history_stock_count": self.insufficient_history_stock_count,
            "total_volume": self.total_volume,
            "total_amount": self.total_amount,
            "top_n_volume": self.top_n_volume,
            "top_n_amount": self.top_n_amount,
            "top_n_volume_share": self.top_n_volume_share,
            "top_n_amount_share": self.top_n_amount_share,
            "methodology_hash": self.methodology_hash,
            "data_baseline_id": self.data_baseline_id,
            "input_manifest_hash": self.input_manifest_hash,
            "activity_hash": self.activity_hash,
            "stocks": [row.as_dict() for row in self.rows],
        }

    def quality_summary(self) -> dict[str, Any]:
        """Return the bounded parent quality payload, excluding stock rows."""
        payload = self.as_dict()
        payload.pop("stocks", None)
        return payload
