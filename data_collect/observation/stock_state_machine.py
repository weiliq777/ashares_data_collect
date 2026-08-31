"""个股异常标签、评分与主状态机（纯计算）。"""
from __future__ import annotations

from collections.abc import Mapping
from math import isfinite
from typing import Any

from data_collect.observation.contracts import ANOMALY_TAGS, ObservationPolicy, PRIMARY_STATES


ANOMALY_STATES = frozenset({
    "PANIC_SELLING", "HIGH_POSITION_DIVERGENCE", "TREND_EXPANSION",
    "ACCUMULATION_LIKE", "NEW_ANOMALY",
})


def _value(metrics: Mapping[str, Any], name: str) -> float | None:
    value = metrics.get(name)
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if isfinite(value) else None


def _all_present(metrics: Mapping[str, Any], names: tuple[str, ...]) -> bool:
    return all(_value(metrics, name) is not None for name in names)


def _linear_score(value: float | None, lower: float, upper: float) -> float | None:
    if value is None:
        return None
    if upper <= lower:
        raise ValueError("score range must be ascending")
    return max(0.0, min(100.0, (value - lower) / (upper - lower) * 100.0))


def _score(metrics: Mapping[str, Any], policy: ObservationPolicy, field: str) -> float | None:
    bounds = policy.score_range(field)
    if bounds is None:
        raise KeyError(f"no score range configured for {field}")
    return _linear_score(_value(metrics, field), *bounds)


def _average(values: list[float | None]) -> float | None:
    available = [value for value in values if value is not None]
    return sum(available) / len(available) if available else None


def calculate_scores(
    metrics: Mapping[str, Any], policy: ObservationPolicy | None = None,
    *, sector_confirmation_score: float | None = None,
) -> tuple[dict[str, float | None], tuple[str, ...], float | None]:
    """Calculate five observation-strength scores and the priority.

    Missing dimensions are omitted from the weighted average, never replaced
    by zero or a positive default. The returned missing codes make the
    renormalization auditable.
    """
    policy = policy or ObservationPolicy()
    trading = _average([
        _score(metrics, policy, "amount_ratio_20_60"),
        _score(metrics, policy, "turnover_ratio_20_60"),
        _score(metrics, policy, "amount_zscore_60"),
    ])
    price = _average([
        _linear_score(1.0 - _value(metrics, "price_position_52w") if _value(metrics, "price_position_52w") is not None else None, 0.0, 1.0),
        _linear_score(abs(_value(metrics, "return_20d")) if _value(metrics, "return_20d") is not None else None, 0.0, 0.20),
        _linear_score(abs(_value(metrics, "price_response_efficiency_20")) if _value(metrics, "price_response_efficiency_20") is not None else None, 0.0, 0.20),
    ])
    persistence = _average([
        _score(metrics, policy, "anomaly_frequency_20"),
        _linear_score(_value(metrics, "anomaly_streak"), 0.0, 10.0),
    ])
    sector = None if sector_confirmation_score is None else max(0.0, min(100.0, float(sector_confirmation_score)))
    risk = _average([
        _score(metrics, policy, "volatility_20d"),
        _score(metrics, policy, "max_drawdown_20d"),
    ])
    scores = {
        "trading_anomaly_score": trading,
        "price_structure_score": price,
        "persistence_score": persistence,
        "sector_confirmation_score": sector,
        "risk_score": risk,
    }
    missing = tuple(sorted(name.upper().replace("_SCORE", "") + "_MISSING" for name, value in scores.items() if value is None))
    weights = dict(policy.score_weights)
    weighted = [(weights[name], value) for name, value in scores.items() if value is not None and weights.get(name) is not None]
    priority = None
    if len(weighted) >= policy.score_min_dimensions:
        denominator = sum(weight for weight, _ in weighted)
        if denominator > 0:
            priority = sum(weight * value for weight, value in weighted) / denominator
    if priority is None:
        missing = tuple(sorted(set(missing) | {"PRIORITY_INSUFFICIENT_SCORE_DIMENSIONS"}))
    return scores, missing, priority


def build_tags(metrics: Mapping[str, Any], policy: ObservationPolicy | None = None) -> tuple[str, ...]:
    """Build independent tags; sector confirmation is added by its own layer."""
    policy = policy or ObservationPolicy()
    tags: list[str] = []
    amount_ratio = _value(metrics, "amount_ratio_5_60")
    turnover_ratio = _value(metrics, "turnover_ratio_5_60")
    position = _value(metrics, "price_position_52w")
    frequency = _value(metrics, "anomaly_frequency_20")
    relative_volume = _value(metrics, "relative_volume_zscore")
    response = _value(metrics, "price_response_efficiency_20")
    if amount_ratio is not None and amount_ratio >= policy.amount_ratio_threshold:
        tags.append("VOLUME_EXPANSION")
    if turnover_ratio is not None and turnover_ratio >= policy.turnover_ratio_threshold:
        tags.append("TURNOVER_EXPANSION")
    if position is not None and position < policy.low_position_threshold:
        tags.append("LOW_POSITION")
    if frequency is not None and frequency >= 0.50:
        tags.append("PERSISTENT_ANOMALY")
    if (
        amount_ratio is not None and amount_ratio >= policy.amount_ratio_threshold
        and response is not None and response <= policy.price_response_floor
    ):
        tags.append("PRICE_DIVERGENCE")
    if (relative_volume is not None and relative_volume >= 1.0) or (
        _value(metrics, "relative_return_20d") is not None and _value(metrics, "relative_return_20d") >= 0.05
    ):
        tags.append("MARKET_RELATIVE")
    return tuple(tag for tag in ANOMALY_TAGS if tag in tags)


def select_primary_state(
    metrics: Mapping[str, Any], policy: ObservationPolicy | None = None,
    *, previous_state: str | None = None,
) -> tuple[str | None, tuple[str, ...]]:
    """Choose exactly one state using the contract's priority order.

    The five states not fully parameterized in the original prose contract are
    made explicit here as policy-driven conservative rules. They use only the
    observation-day metrics and the previous observation state.
    """
    policy = policy or ObservationPolicy()
    anomaly = metrics.get("anomaly_flag")
    if anomaly is None:
        return None, ("STATE_UNDETERMINED",)

    return_20 = _value(metrics, "return_20d")
    drawdown = _value(metrics, "max_drawdown_20d")
    down = _value(metrics, "down_volume_ratio_20")
    up = _value(metrics, "up_volume_ratio_20")
    position = _value(metrics, "price_position_52w")
    distance_ma20 = _value(metrics, "distance_ma20")
    amount_ratio = _value(metrics, "amount_ratio_5_60")
    turnover_ratio = _value(metrics, "turnover_ratio_5_60")
    streak = _value(metrics, "anomaly_streak")

    conditions: dict[str, tuple[bool, tuple[str, ...]]] = {
        "PANIC_SELLING": (
            anomaly is True and return_20 is not None and drawdown is not None
            and return_20 <= policy.panic_return_threshold
            and drawdown <= policy.panic_drawdown_threshold
            and (up is None or down is None or down >= up),
            ("ANOMALY_FLAG", "NEGATIVE_RETURN_20D", "DRAWDOWN_20D", "DOWN_VOLUME_DOMINANT"),
        ),
        "HIGH_POSITION_DIVERGENCE": (
            anomaly is True and position is not None and return_20 is not None
            and position >= policy.high_position_threshold
            and return_20 <= policy.high_position_return_ceiling,
            ("ANOMALY_FLAG", "HIGH_52W_POSITION", "PRICE_NOT_EXPANDING"),
        ),
        "TREND_EXPANSION": (
            anomaly is True and return_20 is not None and distance_ma20 is not None
            and return_20 >= policy.trend_return_threshold
            and distance_ma20 >= policy.trend_distance_ma20_threshold,
            ("ANOMALY_FLAG", "POSITIVE_RETURN_20D", "ABOVE_MA20"),
        ),
        "ACCUMULATION_LIKE": (
            anomaly is True and _all_present(metrics, (
                "price_position_52w", "amount_ratio_5_60", "turnover_ratio_5_60",
                "anomaly_frequency_20", "return_20d", "max_drawdown_20d",
                "up_down_volume_ratio",
            )) and position < policy.low_position_threshold
            and amount_ratio >= policy.amount_ratio_threshold
            and turnover_ratio >= policy.turnover_ratio_threshold
            and _value(metrics, "anomaly_frequency_20") >= 0.50
            and policy.accumulation_return_min <= return_20 <= policy.accumulation_return_max
            and drawdown > policy.accumulation_drawdown_floor
            and _value(metrics, "up_down_volume_ratio") >= policy.accumulation_up_down_volume_ratio,
            ("LOW_POSITION", "AMOUNT_EXPANSION", "TURNOVER_EXPANSION", "PERSISTENT_ANOMALY", "CONTROLLED_RETURN", "CONTROLLED_DRAWDOWN", "UP_VOLUME_DOMINANT"),
        ),
        "NEW_ANOMALY": (
            anomaly is True and (streak == 1 or previous_state not in ANOMALY_STATES),
            ("ANOMALY_FLAG", "FIRST_ANOMALY_IN_SEQUENCE"),
        ),
        "COOLING": (
            anomaly is False and previous_state in ANOMALY_STATES
            and ((amount_ratio is not None and amount_ratio >= policy.cooling_activity_floor)
                 or (turnover_ratio is not None and turnover_ratio >= policy.cooling_activity_floor)),
            ("PREVIOUS_ANOMALY_STATE", "CURRENT_ANOMALY_COOLING"),
        ),
    }
    for state in PRIMARY_STATES:
        matched, reasons = conditions.get(state, (False, ()))
        if matched:
            return state, reasons
    return "NORMAL", ("NO_PRIORITY_STATE_MATCH",)


__all__ = ["ANOMALY_STATES", "calculate_scores", "build_tags", "select_primary_state"]
