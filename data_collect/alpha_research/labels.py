"""Deterministic forward total-return labels over frozen market facts."""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
import math
from typing import Any, Iterable, Mapping, Sequence


@dataclass(frozen=True, slots=True)
class ForwardReturnLabel:
    signal_date: date
    ts_code: str
    horizon: int
    target_date: date | None
    status: str
    reason: str | None
    prediction_total_return: float | None
    entry_reference_total_return: float | None
    entry_reference_status: str
    execution_status: str = "NOT_SIMULATED_BACKTEST_REQUIRED"
    relative_total_return: float | None = None
    relative_pool_mean_return: float | None = None
    relative_pool_complete_count: int = 0
    relative_pool_total_count: int = 0

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        for key in ("signal_date", "target_date"):
            date_value = value[key]
            value[key] = date_value.isoformat() if date_value is not None else None
        return value


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _field(value: Any, key: str, default: Any = None) -> Any:
    return value.get(key, default) if isinstance(value, Mapping) else getattr(value, key, default)


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _price_bar_is_usable(bar: Any) -> bool:
    quality = str(_field(bar, "quality_status", "")).strip().upper()
    if quality in {"VALID", "PASS"}:
        return True
    if quality != "WARNING":
        return False

    # Dataset WARNING also covers execution-only gaps such as unknown limit
    # prices. Keep a price-return label when the frozen price fact itself is
    # complete and internally consistent; execution eligibility is checked by
    # BacktestEngine, not inferred from this research label.
    if _boolean(_field(bar, "is_suspended")) is None:
        return False
    if not str(_field(bar, "source_payload_hash", "") or "").strip():
        return False
    prices = {
        name: _number(_field(bar, name))
        for name in ("raw_open", "raw_high", "raw_low", "raw_close", "volume", "amount")
    }
    if any(value is None or value <= 0 for value in prices.values()):
        return False
    low = prices["raw_low"]
    high = prices["raw_high"]
    open_price = prices["raw_open"]
    close = prices["raw_close"]
    if not (low <= min(open_price, close) <= max(open_price, close) <= high):
        return False
    average_price = prices["amount"] / prices["volume"]
    return low * 0.5 <= average_price <= high * 1.5


def _boolean(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"true", "t", "1", "yes"}:
        return True
    if normalized in {"false", "f", "0", "no"}:
        return False
    return None


def _action_rates(events: Iterable[Any]) -> tuple[float, float]:
    """Aggregate one implemented action per source key for an ex-date."""
    selected: dict[tuple[str, str], Any] = {}
    for event in events:
        if str(_field(event, "implementation_status", "")).strip() != "实施":
            continue
        source_key = str(_field(event, "source_record_key", "") or "")
        version = _field(event, "source_record_version")
        identity = (source_key, "" if source_key else repr(event))
        previous = selected.get(identity)
        if previous is None or int(version or 0) >= int(_field(previous, "source_record_version", 0) or 0):
            selected[identity] = event
    stock_rate = 0.0
    cash_div = 0.0
    for event in selected.values():
        raw_values = {
            name: _field(event, name)
            for name in ("stock_div", "stock_bonus_rate", "stock_conversion_rate", "cash_div")
        }
        parsed = {name: _number(value) for name, value in raw_values.items()}
        if any(raw_values[name] is not None and parsed[name] is None for name in raw_values):
            raise ValueError("frozen corporate action contains a non-finite rate")
        stock_div = parsed["stock_div"]
        bonus = parsed["stock_bonus_rate"] or 0.0
        conversion = parsed["stock_conversion_rate"] or 0.0
        cash = parsed["cash_div"] or 0.0
        rate = stock_div if stock_div is not None and stock_div > 1e-12 else bonus + conversion
        if rate < 0 or cash < 0:
            raise ValueError("frozen corporate action contains a negative rate")
        stock_rate += rate
        cash_div += cash
    return stock_rate, cash_div


def _bar_for(
    bars: Mapping[date, Mapping[str, Any]], day: date, ts_code: str,
) -> Any | None:
    row = bars.get(day)
    if row is None:
        return None
    return row.get(ts_code)


def _compound_returns_by_horizon(
    *,
    signal_date: date,
    ts_code: str,
    calendar: Sequence[date],
    calendar_index: Mapping[date, int],
    bars: Mapping[date, Mapping[str, Any]],
    actions: Mapping[date, Mapping[str, Sequence[Any]]],
    horizons: Sequence[int],
    entry_open: bool,
) -> dict[int, tuple[float | None, str, str | None]]:
    """Walk one security path once and capture every requested horizon."""
    signal_index = calendar_index[signal_date]
    target_positions = {
        signal_index + horizon: horizon
        for horizon in horizons
        if signal_index + horizon < len(calendar)
    }
    if not target_positions:
        return {}
    first_index = signal_index + 1
    first_bar = _bar_for(
        bars, calendar[first_index] if entry_open else signal_date, ts_code,
    )
    anchor_field = "raw_open" if entry_open else "raw_close"
    anchor = _number(_field(first_bar, anchor_field))
    missing_reason = "MISSING_ENTRY_OPEN" if entry_open else "MISSING_SIGNAL_CLOSE"
    quality_reason = "INVALID_ENTRY_BAR_QUALITY" if entry_open else "INVALID_SIGNAL_BAR_QUALITY"
    if first_bar is None or anchor is None or anchor <= 0:
        failure = missing_reason
    else:
        signal_suspended = _boolean(_field(first_bar, "is_suspended"))
        if signal_suspended is None:
            failure = "SUSPENSION_STATUS_UNKNOWN"
        elif not entry_open and signal_suspended:
            failure = "SUSPENDED_SIGNAL_BAR"
        elif not _price_bar_is_usable(first_bar):
            failure = quality_reason
        else:
            failure = None
    if failure is not None:
        return {
            horizon: (None, "INCOMPLETE", failure)
            for horizon in target_positions.values()
        }

    results: dict[int, tuple[float | None, str, str | None]] = {}
    compounded = 1.0
    previous_close = anchor
    for position in range(first_index, max(target_positions) + 1):
        day = calendar[position]
        bar = _bar_for(bars, day, ts_code)
        if bar is None:
            failure = "MISSING_PATH_BAR"
        else:
            suspended = _boolean(_field(bar, "is_suspended"))
            if suspended is None:
                failure = "SUSPENSION_STATUS_UNKNOWN"
            elif suspended:
                failure = "SUSPENDED_PATH"
            elif not _price_bar_is_usable(bar):
                failure = "INVALID_PATH_BAR_QUALITY"
            else:
                failure = None
        if failure is not None:
            for target_position, horizon in target_positions.items():
                if target_position >= position:
                    results[horizon] = (None, "INCOMPLETE", failure)
            break

        close = _number(_field(bar, "raw_close"))
        if close is None or close <= 0:
            failure = "MISSING_OR_INVALID_CLOSE"
        elif entry_open and position == first_index:
            open_price = _number(_field(bar, "raw_open"))
            if open_price is None or open_price <= 0:
                failure = "MISSING_ENTRY_OPEN"
            else:
                # A buyer at ex-date open is not entitled to that day's distribution.
                factor = close / open_price
                failure = None
        else:
            try:
                stock_rate, cash_div = _action_rates(
                    actions.get(day, {}).get(ts_code, ())
                )
            except ValueError:
                failure = "INVALID_CORPORATE_ACTION"
                for target_position, horizon in target_positions.items():
                    if target_position >= position:
                        results[horizon] = (None, "INCOMPLETE", failure)
                break
            factor = (close * (1.0 + stock_rate) + cash_div) / previous_close
            failure = None
        if failure is not None:
            for target_position, horizon in target_positions.items():
                if target_position >= position:
                    results[horizon] = (None, "INCOMPLETE", failure)
            break
        if not math.isfinite(factor) or factor <= 0:
            for target_position, horizon in target_positions.items():
                if target_position >= position:
                    results[horizon] = (None, "INCOMPLETE", "INVALID_TOTAL_RETURN_FACTOR")
            break
        compounded *= factor
        if not math.isfinite(compounded) or compounded <= 0:
            for target_position, horizon in target_positions.items():
                if target_position >= position:
                    results[horizon] = (None, "INCOMPLETE", "INVALID_COMPOUNDED_TOTAL_RETURN")
            break
        previous_close = close
        horizon = target_positions.get(position)
        if horizon is not None:
            results[horizon] = (compounded - 1.0, "COMPLETE", None)
    return results


def build_forward_labels(
    observations: Iterable[Mapping[str, Any]],
    *,
    calendar: Sequence[date],
    bars: Mapping[date, Mapping[str, Any]],
    actions: Mapping[date, Mapping[str, Sequence[Any]]],
    delist_dates: Mapping[str, date | str | None] | None = None,
    horizons: Sequence[int] = (20, 40, 60),
) -> list[ForwardReturnLabel]:
    """Build return labels with incomplete outcomes kept explicit.

    The prediction label starts at signal-day close. The entry reference starts
    at T+1 open and carries no fee or fill simulation; actual execution remains
    the BacktestEngine's responsibility. A known delisting within the window
    remains incomplete until the frozen Dataset provides a terminal value.
    """
    ordered_calendar = tuple(_as_date(day) for day in calendar)
    if not ordered_calendar or tuple(sorted(set(ordered_calendar))) != ordered_calendar:
        raise ValueError("calendar must be non-empty, unique, and ascending")
    calendar_index = {day: index for index, day in enumerate(ordered_calendar)}
    if not horizons or any(int(horizon) <= 0 for horizon in horizons):
        raise ValueError("horizons must be non-empty positive trading-day counts")
    if len(set(horizons)) != len(horizons):
        raise ValueError("horizons must not contain duplicates")
    normalized_delist_dates = {
        str(ts_code): _as_date(delist_date)
        for ts_code, delist_date in (delist_dates or {}).items()
        if delist_date is not None
    }
    result: list[ForwardReturnLabel] = []
    for observation in observations:
        signal_date = _as_date(observation["research_date"])
        ts_code = str(observation["ts_code"])
        signal_index = calendar_index.get(signal_date)
        if signal_index is None:
            raise ValueError(f"signal date is outside frozen calendar: {signal_date}")
        prediction_by_horizon = _compound_returns_by_horizon(
            signal_date=signal_date,
            ts_code=ts_code,
            calendar=ordered_calendar,
            calendar_index=calendar_index,
            bars=bars,
            actions=actions,
            horizons=horizons,
            entry_open=False,
        )
        entry_by_horizon = _compound_returns_by_horizon(
            signal_date=signal_date,
            ts_code=ts_code,
            calendar=ordered_calendar,
            calendar_index=calendar_index,
            bars=bars,
            actions=actions,
            horizons=horizons,
            entry_open=True,
        )
        for horizon in horizons:
            target_index = signal_index + int(horizon)
            if target_index >= len(ordered_calendar):
                result.append(ForwardReturnLabel(
                    signal_date, ts_code, int(horizon), None, "IMMATURE", "TARGET_OUTSIDE_FROZEN_CALENDAR",
                    None, None, "TARGET_OUTSIDE_FROZEN_CALENDAR",
                ))
                continue
            target_date = ordered_calendar[target_index]
            delist_date = normalized_delist_dates.get(ts_code)
            delist_reason = None
            if delist_date is not None:
                if delist_date <= signal_date:
                    delist_reason = "ALREADY_DELISTED_AT_SIGNAL"
                elif delist_date <= target_date:
                    delist_reason = "DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE"
            if delist_reason is not None:
                result.append(ForwardReturnLabel(
                    signal_date=signal_date,
                    ts_code=ts_code,
                    horizon=int(horizon),
                    target_date=target_date,
                    status="INCOMPLETE",
                    reason=delist_reason,
                    prediction_total_return=None,
                    entry_reference_total_return=None,
                    entry_reference_status=delist_reason,
                ))
                continue
            prediction, status, reason = prediction_by_horizon[int(horizon)]
            entry_reference, entry_status, entry_reason = entry_by_horizon[int(horizon)]
            result.append(ForwardReturnLabel(
                signal_date=signal_date,
                ts_code=ts_code,
                horizon=int(horizon),
                target_date=target_date,
                status=status,
                reason=reason,
                prediction_total_return=prediction,
                entry_reference_total_return=entry_reference,
                entry_reference_status=entry_status if entry_reference is not None else (entry_reason or entry_status),
            ))
    return attach_relative_returns(result)


def attach_relative_returns(
    labels: Sequence[ForwardReturnLabel],
) -> list[ForwardReturnLabel]:
    """Add same-date equal-weight excess returns without dropping incomplete rows."""
    groups: dict[tuple[date, int], list[ForwardReturnLabel]] = {}
    for label in labels:
        groups.setdefault((label.signal_date, label.horizon), []).append(label)
    completed: dict[tuple[date, int], list[ForwardReturnLabel]] = {}
    means: dict[tuple[date, int], float | None] = {}
    totals: dict[tuple[date, int], int] = {}
    for key, rows in groups.items():
        valid = [row for row in rows if row.status == "COMPLETE" and row.prediction_total_return is not None]
        completed[key] = valid
        totals[key] = len(rows)
        means[key] = (
            sum(float(row.prediction_total_return) for row in valid) / len(valid)
            if valid else None
        )
    result: list[ForwardReturnLabel] = []
    for label in labels:
        key = (label.signal_date, label.horizon)
        mean_return = means[key]
        excess = (
            float(label.prediction_total_return) - mean_return
            if label.status == "COMPLETE" and mean_return is not None
            else None
        )
        result.append(replace(
            label,
            relative_total_return=excess,
            relative_pool_mean_return=mean_return,
            relative_pool_complete_count=len(completed[key]),
            relative_pool_total_count=totals[key],
        ))
    return result


__all__ = ["ForwardReturnLabel", "attach_relative_returns", "build_forward_labels"]
