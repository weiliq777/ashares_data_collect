"""个股 Observation（观察）指标的纯 Python 实现。

输入是一个股票按交易日升序排列的有限记录序列。该模块不访问数据库，且不
把缺失值偷偷转换为零；缺失、窗口不足和分母无效都通过 ``missing_reason_codes``
显式返回。
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from math import ceil, isfinite, log1p, sqrt
from statistics import mean
from typing import Any

from data_collect.observation.contracts import ObservationPolicy, reason_codes


def _number(value: Any, *, positive: bool = False, nonnegative: bool = False) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(result):
        return None
    if positive and result <= 0:
        return None
    if nonnegative and result < 0:
        return None
    return result


def _minimum_count(length: int, policy: ObservationPolicy) -> int:
    return max(1, ceil(length * policy.min_valid_fraction))


def _segment(rows: Sequence[Mapping[str, Any]], end: int, offset: int, length: int) -> Sequence[Mapping[str, Any]]:
    """Return a bounded segment ending ``offset`` rows before ``end``."""
    last = end - offset
    first = last - length + 1
    if first < 0 or last >= len(rows):
        return ()
    return rows[first:last + 1]


def _field_values(
    segment: Sequence[Mapping[str, Any]], field: str, *, positive: bool = False, nonnegative: bool = False,
) -> list[float]:
    values: list[float] = []
    for row in segment:
        value = _number(row.get(field), positive=positive, nonnegative=nonnegative)
        if value is not None:
            values.append(value)
    return values


def _window_mean(
    rows: Sequence[Mapping[str, Any]], end: int, offset: int, length: int, field: str,
    policy: ObservationPolicy, *, positive: bool = False, nonnegative: bool = False,
) -> float | None:
    segment = _segment(rows, end, offset, length)
    values = _field_values(segment, field, positive=positive, nonnegative=nonnegative)
    if len(values) < _minimum_count(length, policy):
        return None
    return mean(values)


def _ratio(
    rows: Sequence[Mapping[str, Any]], end: int, recent_length: int, baseline_length: int,
    field: str, policy: ObservationPolicy, *, positive: bool = False, nonnegative: bool = False,
) -> float | None:
    recent = _window_mean(rows, end, 0, recent_length, field, policy, positive=positive, nonnegative=nonnegative)
    baseline = _window_mean(rows, end, recent_length, baseline_length, field, policy, positive=positive, nonnegative=nonnegative)
    if recent is None or baseline is None or baseline <= policy.denominator_floor:
        return None
    return recent / baseline


def _amount_zscore(rows: Sequence[Mapping[str, Any]], end: int, policy: ObservationPolicy) -> float | None:
    current = _number(rows[end].get("amount"), positive=True)
    baseline_rows = _segment(rows, end, 1, policy.baseline_window)
    baseline = _field_values(baseline_rows, "amount", positive=True)
    if current is None or len(baseline) < _minimum_count(policy.baseline_window, policy):
        return None
    logged = [log1p(value) for value in baseline]
    average = mean(logged)
    if len(logged) < 2:
        return None
    variance = sum((value - average) ** 2 for value in logged) / (len(logged) - 1)
    deviation = sqrt(variance)
    if deviation <= policy.denominator_floor:
        return None
    return (log1p(current) - average) / deviation


def _single_amount_ratio(rows: Sequence[Mapping[str, Any]], end: int, policy: ObservationPolicy) -> float | None:
    current = _number(rows[end].get("amount"), positive=True)
    baseline = _window_mean(rows, end, 1, policy.baseline_window, "amount", policy, positive=True)
    if current is None or baseline is None or baseline <= policy.denominator_floor:
        return None
    return current / baseline


def _daily_return(rows: Sequence[Mapping[str, Any]], end: int, field: str = "adj_close_base") -> float | None:
    if end <= 0:
        return None
    current = _number(rows[end].get(field), positive=True)
    previous = _number(rows[end - 1].get(field), positive=True)
    if current is None or previous is None:
        return None
    return current / previous - 1.0


def _return_n(rows: Sequence[Mapping[str, Any]], end: int, n: int, field: str, policy: ObservationPolicy) -> float | None:
    if end - n < 0:
        return None
    current = _number(rows[end].get(field), positive=True)
    previous = _number(rows[end - n].get(field), positive=True)
    if current is None or previous is None or previous <= policy.denominator_floor:
        return None
    return current / previous - 1.0


def _price_position(rows: Sequence[Mapping[str, Any]], end: int, policy: ObservationPolicy) -> float | None:
    values = _field_values(_segment(rows, end, 0, policy.position_window), "adj_close_base", positive=True)
    if len(values) < _minimum_count(policy.position_window, policy):
        return None
    low, high, current = min(values), max(values), _number(rows[end].get("adj_close_base"), positive=True)
    if current is None or high - low <= policy.denominator_floor:
        return None
    return max(0.0, min(1.0, (current - low) / (high - low)))


def _distance_ma(rows: Sequence[Mapping[str, Any]], end: int, length: int, policy: ObservationPolicy) -> float | None:
    current = _number(rows[end].get("adj_close_base"), positive=True)
    average = _window_mean(rows, end, 0, length, "adj_close_base", policy, positive=True)
    if current is None or average is None or average <= policy.denominator_floor:
        return None
    return current / average - 1.0


def _max_drawdown(rows: Sequence[Mapping[str, Any]], end: int, policy: ObservationPolicy) -> float | None:
    values = _field_values(_segment(rows, end, 0, policy.anomaly_window), "adj_close_base", positive=True)
    if len(values) < _minimum_count(policy.anomaly_window, policy):
        return None
    peak = values[0]
    drawdown = 0.0
    for value in values:
        peak = max(peak, value)
        drawdown = min(drawdown, value / peak - 1.0)
    return drawdown


def _volume_structure(rows: Sequence[Mapping[str, Any]], end: int, policy: ObservationPolicy) -> tuple[float | None, float | None, float | None]:
    segment = _segment(rows, end, 0, policy.anomaly_window)
    if len(segment) != policy.anomaly_window:
        return None, None, None
    up, down, total = 0.0, 0.0, 0.0
    valid = 0
    for index in range(end - policy.anomaly_window + 1, end + 1):
        volume = _number(rows[index].get("volume"), positive=True)
        change = _daily_return(rows, index)
        if volume is None or change is None:
            continue
        valid += 1
        total += volume
        if change > 0:
            up += volume
        elif change < 0:
            down += volume
    if valid < _minimum_count(policy.anomaly_window, policy) or total <= policy.denominator_floor:
        return None, None, None
    return (
        up / total,
        down / total,
        up / down if down > policy.denominator_floor else None,
    )


def _volatility(rows: Sequence[Mapping[str, Any]], end: int, policy: ObservationPolicy) -> float | None:
    changes: list[float] = []
    for index in range(max(1, end - policy.anomaly_window + 1), end + 1):
        change = _daily_return(rows, index)
        if change is not None:
            changes.append(change)
    if len(changes) < _minimum_count(policy.anomaly_window, policy) or len(changes) < 2:
        return None
    average = mean(changes)
    return sqrt(sum((change - average) ** 2 for change in changes) / (len(changes) - 1))


def _anomaly_flag(rows: Sequence[Mapping[str, Any]], end: int, policy: ObservationPolicy) -> tuple[bool | None, float | None, float | None]:
    zscore = _amount_zscore(rows, end, policy)
    single_ratio = _single_amount_ratio(rows, end, policy)
    if zscore is None and single_ratio is None:
        return None, zscore, single_ratio
    return (
        bool(
            (zscore is not None and zscore >= policy.amount_zscore_threshold)
            or (single_ratio is not None and single_ratio >= policy.amount_single_ratio_threshold)
        ),
        zscore,
        single_ratio,
    )


def _persistence(rows: Sequence[Mapping[str, Any]], end: int, policy: ObservationPolicy) -> tuple[int | None, float | None, int | None, list[bool | None]]:
    flags: list[bool | None] = []
    # Frequency is a fixed 20-trading-day measure, while streak and the first
    # anomaly date describe the current sequence.  Keep enough bounded history
    # to avoid silently capping a long-running anomaly at 20 days.
    start = max(0, end - policy.max_lookback + 1)
    for index in range(start, end + 1):
        flags.append(_anomaly_flag(rows, index, policy)[0])
    frequency_flags = flags[-policy.anomaly_window:]
    frequency_valid = [flag for flag in frequency_flags if flag is not None]
    if len(frequency_valid) < _minimum_count(policy.anomaly_window, policy):
        return None, None, None, flags
    frequency = sum(flag is True for flag in frequency_valid) / policy.anomaly_window
    current = flags[-1]
    if current is None:
        streak = None
    elif not current:
        streak = 0
    else:
        streak = 0
        for flag in reversed(flags):
            if flag is not True:
                break
            streak += 1
    first = None
    for index, flag in enumerate(flags):
        if flag is True:
            first = start + index
            break
    days_since_first = end - first if first is not None else None
    return streak, frequency, days_since_first, flags


def _fast_persistence(
    rows: Sequence[Mapping[str, Any]],
    end: int,
    policy: ObservationPolicy,
) -> tuple[
    tuple[bool | None, float | None, float | None],
    tuple[int | None, float | None, int | None, list[bool | None]],
    list[bool | None],
]:
    """Compute the target day's anomaly state in linear time.

    The regular series API intentionally returns every row. The production
    Observation job only needs the final row, however. This implementation
    uses prefix sums for the log-amount baseline so the bounded persistence
    flags are evaluated once per date instead of recursively rescanning a
    60-day window for every earlier date.
    """
    persist_start = max(0, end - policy.max_lookback + 1)
    anomaly_window_start = max(0, end - policy.anomaly_window + 1)
    calculate_start = min(persist_start, anomaly_window_start)
    prefix_count = [0]
    prefix_sum = [0.0]
    prefix_square = [0.0]
    prefix_amount = [0.0]
    for row in rows[:end + 1]:
        amount = _number(row.get("amount"), positive=True)
        if amount is None:
            prefix_count.append(prefix_count[-1])
            prefix_sum.append(prefix_sum[-1])
            prefix_square.append(prefix_square[-1])
            prefix_amount.append(prefix_amount[-1])
            continue
        logged = log1p(amount)
        prefix_count.append(prefix_count[-1] + 1)
        prefix_sum.append(prefix_sum[-1] + logged)
        prefix_square.append(prefix_square[-1] + logged * logged)
        prefix_amount.append(prefix_amount[-1] + amount)

    minimum = _minimum_count(policy.baseline_window, policy)
    anomaly_data: dict[int, tuple[bool | None, float | None, float | None]] = {}
    for index in range(calculate_start, end + 1):
        baseline_start = index - policy.baseline_window
        current = _number(rows[index].get("amount"), positive=True)
        if baseline_start < 0 or current is None:
            anomaly_data[index] = (None, None, None)
            continue
        count = prefix_count[index] - prefix_count[baseline_start]
        if count < minimum:
            anomaly_data[index] = (None, None, None)
            continue
        total = prefix_sum[index] - prefix_sum[baseline_start]
        total_square = prefix_square[index] - prefix_square[baseline_start]
        average = total / count
        average_amount = (
            prefix_amount[index] - prefix_amount[baseline_start]
        ) / count
        single_ratio = current / average_amount if average_amount > policy.denominator_floor else None
        # The sample standard deviation uses the same log1p values as
        # _amount_zscore; prefix sums preserve its valid-value semantics.
        zscore = None
        if count >= 2:
            variance = (total_square - (total * total / count)) / (count - 1)
            deviation = sqrt(max(0.0, variance))
            if deviation > policy.denominator_floor:
                zscore = (log1p(current) - average) / deviation
        if zscore is None and single_ratio is None:
            flag = None
        else:
            flag = bool(
                (zscore is not None and zscore >= policy.amount_zscore_threshold)
                or (single_ratio is not None and single_ratio >= policy.amount_single_ratio_threshold)
            )
        anomaly_data[index] = (flag, zscore, single_ratio)

    flags = [anomaly_data[index][0] for index in range(persist_start, end + 1)]
    frequency_flags = flags[-policy.anomaly_window:]
    frequency_valid = [flag for flag in frequency_flags if flag is not None]
    if len(frequency_valid) < _minimum_count(policy.anomaly_window, policy):
        persistence = (None, None, None, flags)
    else:
        frequency = sum(flag is True for flag in frequency_valid) / policy.anomaly_window
        current_flag = flags[-1]
        if current_flag is None:
            streak = None
        elif not current_flag:
            streak = 0
        else:
            streak = 0
            for flag in reversed(flags):
                if flag is not True:
                    break
                streak += 1
        first = None
        for index, flag in enumerate(flags):
            if flag is True:
                first = persist_start + index
                break
        days_since_first = end - first if first is not None else None
        persistence = (streak, frequency, days_since_first, flags)

    window_flags = [anomaly_data[index][0] for index in range(anomaly_window_start, end + 1)]
    return anomaly_data[end], persistence, window_flags


def _compute_stock_item(
    normalized: Sequence[Mapping[str, Any]],
    end: int,
    policy: ObservationPolicy,
    *,
    anomaly_override: tuple[bool | None, float | None, float | None] | None = None,
    persistence_override: tuple[int | None, float | None, int | None, list[bool | None]] | None = None,
    anomaly_window_flags: Sequence[bool | None] | None = None,
) -> dict[str, Any]:
    row = normalized[end]
    amount_ratio_5 = _ratio(normalized, end, policy.short_window, policy.baseline_window, "amount", policy, positive=True)
    amount_ratio_20 = _ratio(normalized, end, policy.anomaly_window, policy.baseline_window, "amount", policy, positive=True)
    turnover_ratio_5 = _ratio(normalized, end, policy.short_window, policy.baseline_window, "turnover_rate_f", policy, nonnegative=True)
    turnover_ratio_20 = _ratio(normalized, end, policy.anomaly_window, policy.baseline_window, "turnover_rate_f", policy, nonnegative=True)
    if anomaly_override is None:
        anomaly_flag, zscore, single_ratio = _anomaly_flag(normalized, end, policy)
    else:
        anomaly_flag, zscore, single_ratio = anomaly_override
    if persistence_override is None:
        streak, frequency, days_since_first, _ = _persistence(normalized, end, policy)
    else:
        streak, frequency, days_since_first, _ = persistence_override
    up_ratio, down_ratio, up_down_ratio = _volume_structure(normalized, end, policy)
    return_5 = _return_n(normalized, end, 5, "adj_close_base", policy)
    return_20 = _return_n(normalized, end, 20, "adj_close_base", policy)
    position = _price_position(normalized, end, policy)
    max_drawdown = _max_drawdown(normalized, end, policy)
    volatility = _volatility(normalized, end, policy)
    response = None
    if return_20 is not None and turnover_ratio_20 is not None:
        response = abs(return_20) / max(log1p(max(0.0, turnover_ratio_20)), policy.price_response_floor)

    if anomaly_window_flags is None:
        abnormal_days_20 = sum(
            _anomaly_flag(normalized, index, policy)[0] is True
            for index in range(max(0, end - policy.anomaly_window + 1), end + 1)
        ) if frequency is not None else None
    else:
        abnormal_days_20 = sum(flag is True for flag in anomaly_window_flags) if frequency is not None else None

    missing: list[str] = []
    if amount_ratio_5 is None or amount_ratio_20 is None or zscore is None:
        missing.append("INSUFFICIENT_AMOUNT_HISTORY")
    if turnover_ratio_5 is None or turnover_ratio_20 is None:
        missing.append("MISSING_OR_INSUFFICIENT_TURNOVER_RATE_F")
    if return_5 is None or return_20 is None or position is None or max_drawdown is None:
        missing.append("INSUFFICIENT_ADJ_CLOSE_BASE_HISTORY")
    if up_ratio is None or down_ratio is None or up_down_ratio is None:
        missing.append("INSUFFICIENT_VOLUME_STRUCTURE_HISTORY")
    if streak is None or frequency is None:
        missing.append("INSUFFICIENT_ANOMALY_HISTORY")
    if _number(row.get("volume"), positive=True) is None or _number(row.get("amount"), positive=True) is None:
        missing.append("NO_VALID_ACTIVITY")

    return {
        "observation_date": row.get("trade_date"),
        "ts_code": row.get("ts_code") or row.get("stock_code"),
        "raw_close": row.get("raw_close"),
        "adj_close_base": row.get("adj_close_base"),
        "total_return_price": row.get("total_return_price"),
        "volume": row.get("volume"),
        "amount": row.get("amount"),
        "turnover_rate_f": row.get("turnover_rate_f"),
        "free_share": row.get("free_share"),
        "eligible_by_market": row.get("eligible_by_market"),
        "is_tradeable": row.get("is_tradeable"),
        "is_suspended": row.get("is_suspended") if row.get("is_suspended") is not None else row.get("is_suspended_asof"),
        "industry_system": row.get("industry_system"),
        "industry_level": row.get("industry_level"),
        "industry_code": row.get("industry_code"),
        "industry_name": row.get("industry_name"),
        "industry_source_asof_date": row.get("industry_source_asof_date"),
        "industry_data_available_timestamp": row.get("industry_data_available_timestamp"),
        "industry_quality_status": row.get("industry_quality_status"),
        "amount_ratio_5_60": amount_ratio_5,
        "amount_ratio_20_60": amount_ratio_20,
        "turnover_ratio_5_60": turnover_ratio_5,
        "turnover_ratio_20_60": turnover_ratio_20,
        "amount_zscore_60": zscore,
        "amount_single_ratio_60": single_ratio,
        "abnormal_days_20": abnormal_days_20,
        "return_1d": _daily_return(normalized, end),
        "return_5d": return_5,
        "return_20d": return_20,
        "price_position_52w": position,
        "distance_ma20": _distance_ma(normalized, end, 20, policy),
        "distance_ma60": _distance_ma(normalized, end, 60, policy),
        "max_drawdown_20d": max_drawdown,
        "up_volume_ratio_20": up_ratio,
        "down_volume_ratio_20": down_ratio,
        "up_down_volume_ratio": up_down_ratio,
        "price_response_efficiency_20": response,
        "anomaly_flag": anomaly_flag,
        "anomaly_streak": streak,
        "anomaly_frequency_20": frequency,
        "days_since_first_anomaly": days_since_first,
        "volatility_20d": volatility,
        "missing_reason_codes": reason_codes(missing),
        "calculation_status": "VALID" if not missing else "PARTIAL",
        "quality_status": "VALID" if not missing else "WARNING",
    }


def compute_stock_series(
    rows: Sequence[Mapping[str, Any]], policy: ObservationPolicy | None = None,
) -> list[dict[str, Any]]:
    """Compute all available stock metrics for every input row.

    The sequence must be one ``ts_code`` and sorted by ``trade_date``. It may
    contain warm-up rows before the requested output date; callers can select
    only the final date after computation.
    """
    policy = policy or ObservationPolicy()
    normalized = list(rows)
    if not normalized:
        return []
    codes = {str(row.get("ts_code") or row.get("stock_code") or "") for row in normalized}
    codes.discard("")
    if len(codes) > 1:
        raise ValueError("compute_stock_series accepts one stock code at a time")
    dates = [row.get("trade_date") for row in normalized]
    if dates != sorted(dates):
        raise ValueError("stock rows must be sorted by trade_date")

    return [_compute_stock_item(normalized, end, policy) for end in range(len(normalized))]


def compute_stock_point(
    rows: Sequence[Mapping[str, Any]], policy: ObservationPolicy | None = None,
) -> dict[str, Any] | None:
    """Compute only the final target-day metrics without rebuilding prior rows."""
    policy = policy or ObservationPolicy()
    normalized = list(rows)
    if not normalized:
        return None
    codes = {str(row.get("ts_code") or row.get("stock_code") or "") for row in normalized}
    codes.discard("")
    if len(codes) > 1:
        raise ValueError("compute_stock_point accepts one stock code at a time")
    dates = [row.get("trade_date") for row in normalized]
    if dates != sorted(dates):
        raise ValueError("stock rows must be sorted by trade_date")
    end = len(normalized) - 1
    anomaly, persistence, window_flags = _fast_persistence(normalized, end, policy)
    return _compute_stock_item(
        normalized,
        end,
        policy,
        anomaly_override=anomaly,
        persistence_override=persistence,
        anomaly_window_flags=window_flags,
    )


__all__ = ["compute_stock_series", "compute_stock_point"]
