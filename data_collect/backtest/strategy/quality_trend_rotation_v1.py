"""QualityTrendRotation_V1（质量趋势轮动 V1）策略。

这是只做多、低频、确定性的策略实现：基本面负责质量排雷，中期趋势负责候选排序。
策略只消费 FROZEN Backtest Dataset（冻结回测数据集），不访问数据库。
"""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import date, datetime
import math
from typing import Any, Mapping, Sequence

from ..models import Observation, SignalIntent
from .contract import StrategyVersionSpec


class QualityTrendRotationV1:
    strategy_id = "quality_trend_rotation_v1"
    strategy_version = "1.0.0"
    score_version = "quality-trend-ret60-v1"

    required_observation_columns = (
        "list_date", "delist_date", "raw_close", "adj_close_base", "ma20", "ma60",
        "is_listed_asof", "is_st_asof", "is_tradeable", "can_buy", "can_sell",
        "eligible_by_market", "quality_status", "effective_announce_date",
        "financial_data_available_timestamp", "operating_cashflow_ttm", "roe_ttm",
        "debt_to_assets", "ttm_complete", "roe_quality_status",
        "cashflow_quality_status", "available_observation_count",
        "ma20_observation_count", "ma60_observation_count",
    )

    def __init__(
        self,
        *,
        roe_ttm_min: float = 0.08,
        ocf_ttm_min: float = 0.0,
        debt_to_assets_max: float = 70.0,
        min_listing_trading_days: int = 250,
        min_avg_amount_20: float = 50_000_000.0,
        top_n: int = 3,
        rank_exit_threshold: int = 10,
        allow_st: bool = False,
    ):
        if not 0 <= roe_ttm_min <= 1:
            raise ValueError("roe_ttm_min 必须使用比例且位于 [0,1]")
        if ocf_ttm_min < 0 or not 0 < debt_to_assets_max <= 100:
            raise ValueError("质量阈值无效")
        if min_listing_trading_days <= 0 or top_n <= 0 or rank_exit_threshold < top_n:
            raise ValueError("上市天数、候选数和排名退出阈值无效")
        if min_avg_amount_20 <= 0:
            raise ValueError("20 日平均成交额阈值必须为正")
        self.roe_ttm_min = float(roe_ttm_min)
        self.ocf_ttm_min = float(ocf_ttm_min)
        self.debt_to_assets_max = float(debt_to_assets_max)
        self.min_listing_trading_days = int(min_listing_trading_days)
        self.min_avg_amount_20 = float(min_avg_amount_20)
        self.top_n = int(top_n)
        self.rank_exit_threshold = int(rank_exit_threshold)
        self.allow_st = bool(allow_st)
        self.reset_state()

    def reset_state(self) -> None:
        """清空单次回测的有限滚动状态，避免策略对象复用时串数据。"""
        self._amounts: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=20))
        self._prices: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=61))
        self._avg_amount_20: dict[str, float | None] = {}
        self._current_amount: dict[str, float | None] = {}
        self._return_60: dict[str, float | None] = {}
        self._last_state_day: date | None = None

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "roe_ttm_min": self.roe_ttm_min,
            "ocf_ttm_min": self.ocf_ttm_min,
            "debt_to_assets_max": self.debt_to_assets_max,
            "min_listing_trading_days": self.min_listing_trading_days,
            "min_avg_amount_20": self.min_avg_amount_20,
            "top_n": self.top_n,
            "rank_exit_threshold": self.rank_exit_threshold,
            "allow_st": self.allow_st,
            "entry_rule": "adj_close_base>ma20 AND ma20>ma60 AND ret60>0",
            "holding_hard_trend_exit": "adj_close_base<ma60",
            "ranking": ["ret60 DESC", "avg_amount_20 DESC", "ts_code ASC"],
            "signal_frequency": "WEEKLY_LAST_TRADING_DAY",
            "pending_sell_retry_frequency": "EVERY_SUBSEQUENT_TRADING_DAY",
            "score_version": self.score_version,
        }

    @property
    def strategy_hash(self) -> str:
        return StrategyVersionSpec(
            self.strategy_id, self.strategy_version, self.parameters,
        ).strategy_hash

    @property
    def metadata(self) -> dict[str, Any]:
        return {
            "name": self.strategy_id,
            "version": self.strategy_version,
            "description": "质量过滤 + 60 日趋势排名 + 3 股周频轮动；不代表投资建议。",
            "parameters": dict(self.parameters),
        }

    @staticmethod
    def _finite(value: Any) -> float | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    @staticmethod
    def _as_date(value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        text = str(value).strip()
        if not text:
            return None
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None

    @staticmethod
    def is_signal_day(trade_date: date, calendar: Sequence[date]) -> bool:
        """自然周最后一个交易日收盘产生信号，节假日由冻结交易日历处理。"""
        dates = tuple(calendar)
        try:
            index = dates.index(trade_date)
        except ValueError:
            return False
        if index + 1 >= len(dates):
            return False
        return trade_date.isocalendar()[:2] != dates[index + 1].isocalendar()[:2]

    def on_day(self, trade_date: date, observations: tuple[Observation, ...], dataset) -> None:
        """只用当前冻结日线推进有限滚动状态，不扫描数据库或全历史表。"""
        if self._last_state_day == trade_date:
            return
        self._last_state_day = trade_date
        for observation in sorted(observations, key=lambda item: item.ts_code):
            code = observation.ts_code
            adj_close = self._finite(observation.get("adj_close_base"))
            price_history = self._prices[code]
            if adj_close is not None and adj_close > 0:
                base = price_history[-60] if len(price_history) >= 60 else None
                price_history.append(adj_close)
                self._return_60[code] = (
                    adj_close / base - 1.0 if base is not None and base > 0 else None
                )
            else:
                self._return_60[code] = None

            bar = dataset.bar(trade_date, code)
            amount = self._finite(bar.amount) if bar is not None else None
            self._current_amount[code] = amount if amount is not None and amount > 0 else None
            amount_history = self._amounts[code]
            if amount is not None and amount > 0:
                amount_history.append(amount)
            self._avg_amount_20[code] = (
                sum(amount_history) / len(amount_history)
                if len(amount_history) == 20 else None
            )

    def _financial_quality_ok(self, observation: Observation, trade_date: date) -> bool:
        effective = self._as_date(observation.get("effective_announce_date"))
        available = self._as_date(observation.get("financial_data_available_timestamp"))
        if effective is None or effective > trade_date:
            return False
        if available is None or available > trade_date:
            return False
        if str(observation.get("quality_status") or "").upper() != "VALID":
            return False
        for key in ("roe_quality_status", "cashflow_quality_status"):
            if str(observation.get(key) or "").upper() != "VALID":
                return False
        if observation.get("ttm_complete") is not True:
            return False
        roe = self._finite(observation.get("roe_ttm"))
        ocf = self._finite(observation.get("operating_cashflow_ttm"))
        debt = self._finite(observation.get("debt_to_assets"))
        return (
            roe is not None and roe >= self.roe_ttm_min
            and ocf is not None and ocf > self.ocf_ttm_min
            and debt is not None and 0 <= debt <= self.debt_to_assets_max
        )

    def _entry_candidate(self, trade_date: date, observation: Observation) -> dict[str, Any] | None:
        raw_close = self._finite(observation.get("raw_close"))
        adj_close = self._finite(observation.get("adj_close_base"))
        ma20 = self._finite(observation.get("ma20"))
        ma60 = self._finite(observation.get("ma60"))
        ret60 = self._return_60.get(observation.ts_code)
        avg_amount = self._avg_amount_20.get(observation.ts_code)
        listed_count = self._finite(observation.get("available_observation_count"))
        if any(value is None or value <= 0 for value in (raw_close, adj_close, ma20, ma60)):
            return None
        if observation.get("is_listed_asof") is not True:
            return None
        if observation.get("eligible_by_market") is not True:
            return None
        if observation.get("is_tradeable") is not True or observation.get("can_buy") is not True:
            return None
        st_status = observation.get("is_st_asof")
        if st_status not in {True, False}:
            return None
        if st_status is True and not self.allow_st:
            return None
        if not self._financial_quality_ok(observation, trade_date):
            return None
        # available_observation_count 由冻结价格特征按证券自身有效行情累计，
        # 可精确覆盖 Dataset 预热起点之前的上市历史；不能从回测起点重新计数。
        if listed_count is None or listed_count < self.min_listing_trading_days:
            return None
        if (
            ret60 is None or ret60 <= 0
            or self._current_amount.get(observation.ts_code) is None
            or avg_amount is None or avg_amount < self.min_avg_amount_20
        ):
            return None
        if not (adj_close > ma20 > ma60):
            return None
        return {
            "observation": observation,
            "raw_close": raw_close,
            "adj_close": adj_close,
            "ma20": ma20,
            "ma60": ma60,
            "ret60": ret60,
            "avg_amount_20": avg_amount,
            "listed_trading_day_count": int(listed_count),
        }

    @staticmethod
    def _sell_intent(code: str, observation: Observation | None, rule: str, **details) -> SignalIntent:
        details = {"rule": rule, "retryable_sell": True, **details}
        raw = observation.get("raw_close") if observation else None
        adj = observation.get("adj_close_base") if observation else None
        return SignalIntent(
            ts_code=code,
            direction="SELL",
            indicator_price=adj,
            raw_reference_price=raw,
            stop_loss_price_raw=None,
            reason=details,
        )

    def generate_signals(
        self,
        trade_date: date,
        observations: tuple[Observation, ...],
        positions: Mapping[str, Mapping[str, Any]],
    ) -> list[SignalIntent]:
        by_code = {item.ts_code: item for item in observations}
        held_codes = {
            code for code, state in positions.items()
            if int(state.get("hold_shares", 0) or 0) > 0
        }
        pending_sell_codes = {
            code for code, state in positions.items() if bool(state.get("pending_exit"))
        }
        signals: list[SignalIntent] = []

        for code in sorted(held_codes):
            if code in pending_sell_codes:
                continue
            observation = by_code.get(code)
            if observation is None:
                signals.append(self._sell_intent(code, None, "missing_current_observation"))
                continue
            adj = self._finite(observation.get("adj_close_base"))
            ma60 = self._finite(observation.get("ma60"))
            if adj is None or adj <= 0 or ma60 is None or ma60 <= 0:
                signals.append(self._sell_intent(code, observation, "missing_trend_price"))
            elif observation.get("is_listed_asof") is not True:
                signals.append(self._sell_intent(code, observation, "listing_status_invalidated"))
            elif observation.get("is_st_asof") is True:
                signals.append(self._sell_intent(code, observation, "st_status_invalidated"))
            elif observation.get("is_st_asof") is not False:
                signals.append(self._sell_intent(code, observation, "missing_st_status"))
            elif observation.get("eligible_by_market") is not True:
                signals.append(self._sell_intent(code, observation, "market_eligibility_invalidated"))
            elif not self._financial_quality_ok(observation, trade_date):
                signals.append(self._sell_intent(code, observation, "financial_quality_invalidated"))
            elif adj < ma60:
                signals.append(self._sell_intent(
                    code, observation, "close_below_ma60", adj_close_base=adj, ma60=ma60,
                ))

        candidates: list[dict[str, Any]] = []
        for observation in sorted(observations, key=lambda item: item.ts_code):
            if observation.ts_code in pending_sell_codes:
                continue
            candidate = self._entry_candidate(trade_date, observation)
            if candidate is not None:
                candidates.append(candidate)
        candidates.sort(key=lambda item: (
            -float(item["ret60"]), -float(item["avg_amount_20"]),
            item["observation"].ts_code,
        ))
        ranks = {
            item["observation"].ts_code: index
            for index, item in enumerate(candidates, start=1)
        }

        for code in sorted(held_codes):
            if code in pending_sell_codes:
                continue
            rank = ranks.get(code)
            if rank is not None and rank > self.rank_exit_threshold:
                candidate = next(
                    item for item in candidates if item["observation"].ts_code == code
                )
                signals.append(self._sell_intent(
                    code, candidate["observation"], "rank_outside_hold_threshold",
                    rank=rank, rank_exit_threshold=self.rank_exit_threshold,
                    ret60=candidate["ret60"], avg_amount_20=candidate["avg_amount_20"],
                ))

        for candidate in candidates[:self.top_n]:
            observation = candidate["observation"]
            code = observation.ts_code
            if code in held_codes or code in pending_sell_codes:
                continue
            signals.append(SignalIntent(
                ts_code=code,
                direction="BUY",
                indicator_price=candidate["adj_close"],
                raw_reference_price=candidate["raw_close"],
                stop_loss_price_raw=None,
                score=round(float(candidate["ret60"]), 12),
                reason={
                    "rule": self.strategy_id,
                    "score_version": self.score_version,
                    "rank": ranks[code],
                    "ret60": candidate["ret60"],
                    "avg_amount_20": candidate["avg_amount_20"],
                    "listed_trading_day_count": candidate["listed_trading_day_count"],
                    "roe_ttm": observation.get("roe_ttm"),
                    "operating_cashflow_ttm": observation.get("operating_cashflow_ttm"),
                    "debt_to_assets": observation.get("debt_to_assets"),
                    "entry_rule": "adj_close_base>ma20 AND ma20>ma60 AND ret60>0",
                    "signal_frequency": "WEEKLY_LAST_TRADING_DAY",
                },
            ))
        return signals
