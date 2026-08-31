"""Evaluation（评估域）后验路径指标的纯计算实现。

这里没有入场、成交、费用、止盈或止损语义；那些字段只属于 Scenario
Evaluation（策略场景评估）。输入序列应包含观察日 ``T`` 和其后的交易日。
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from math import isfinite, sqrt
from statistics import mean
from typing import Any

from data_collect.observation.contracts import ObservationPolicy


HORIZONS = (5, 10, 20, 60, 120)


def _positive(value: Any) -> float | None:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if isfinite(value) and value > 0 else None


def _path_metric(values: list[float], start: float) -> tuple[float | None, float | None, float | None, float | None]:
    if not values or start <= 0:
        return None, None, None, None
    peak = start
    max_drawdown = 0.0
    max_favorable = 0.0
    max_adverse = 0.0
    for value in values:
        peak = max(peak, value)
        max_drawdown = min(max_drawdown, value / peak - 1.0)
        move = value / start - 1.0
        max_favorable = max(max_favorable, move)
        max_adverse = min(max_adverse, move)
    returns = [values[index] / values[index - 1] - 1.0 for index in range(1, len(values)) if values[index - 1] > 0]
    volatility = None
    if len(returns) >= 2:
        average = mean(returns)
        volatility = sqrt(sum((item - average) ** 2 for item in returns) / (len(returns) - 1))
    return max_drawdown, max_favorable, max_adverse, volatility


def compute_outcome(
    rows: Sequence[Mapping[str, Any]], policy: ObservationPolicy | None = None,
) -> dict[str, Any]:
    """Compute mature and pending horizon fields for one observation.

    ``rows[0]`` must be the observation day. A missing future price leaves the
    corresponding horizon fields as ``None`` and sets ``PENDING_MATURITY``.
    """
    policy = policy or ObservationPolicy()
    if not rows:
        raise ValueError("outcome requires the observation-day row")
    first = rows[0]
    base_price = _positive(first.get("adj_close_base"))
    base_total = _positive(first.get("total_return_price"))
    result: dict[str, Any] = {
        "observation_date": first.get("trade_date") or first.get("observation_date"),
        "ts_code": first.get("ts_code") or first.get("stock_code"),
        "maturity_status": "MATURED",
        "missing_reason_codes": [],
    }
    price_values = [_positive(row.get("adj_close_base")) for row in rows]
    total_values = [_positive(row.get("total_return_price")) for row in rows]
    if base_price is None:
        result["maturity_status"] = "INVALID_INPUT"
        result["missing_reason_codes"].append("MISSING_ADJ_CLOSE_BASE_AT_OBSERVATION")
    if base_total is None:
        result["maturity_status"] = "INVALID_INPUT"
        result["missing_reason_codes"].append("MISSING_TOTAL_RETURN_PRICE_AT_OBSERVATION")

    for horizon in HORIZONS:
        price = price_values[horizon] if horizon < len(price_values) else None
        total = total_values[horizon] if horizon < len(total_values) else None
        if base_price is not None and price is not None:
            result[f"future_price_return_{horizon}d"] = price / base_price - 1.0
            path = price_values[:horizon + 1]
            if len(path) != horizon + 1 or any(item is None for item in path):
                drawdown = favorable = adverse = volatility = None
                result["missing_reason_codes"].append(f"MISSING_PRICE_PATH_{horizon}D")
                result["maturity_status"] = "PENDING_MATURITY" if result["maturity_status"] != "INVALID_INPUT" else result["maturity_status"]
            else:
                drawdown, favorable, adverse, volatility = _path_metric(path, base_price)
            result[f"max_drawdown_{horizon}d"] = drawdown
            result[f"max_favorable_excursion_{horizon}d"] = favorable
            result[f"max_adverse_excursion_{horizon}d"] = adverse
            result[f"volatility_{horizon}d"] = volatility
            result[f"best_return_{horizon}d"] = favorable
            result[f"worst_return_{horizon}d"] = adverse
        else:
            result[f"future_price_return_{horizon}d"] = None
            result[f"max_drawdown_{horizon}d"] = None
            result[f"max_favorable_excursion_{horizon}d"] = None
            result[f"max_adverse_excursion_{horizon}d"] = None
            result[f"volatility_{horizon}d"] = None
            result[f"best_return_{horizon}d"] = None
            result[f"worst_return_{horizon}d"] = None
        result[f"future_total_return_{horizon}d"] = (
            total / base_total - 1.0 if base_total is not None and total is not None else None
        )
        if price is None or total is None:
            result["maturity_status"] = "PENDING_MATURITY" if result["maturity_status"] != "INVALID_INPUT" else result["maturity_status"]
    result["missing_reason_codes"] = tuple(sorted(set(result["missing_reason_codes"])))
    return result


__all__ = ["HORIZONS", "compute_outcome"]
