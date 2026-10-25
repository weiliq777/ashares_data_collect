"""Independent reference calculation for the real-sample label comparison.

This intentionally does not call ``build_forward_labels`` or its private
helpers. It is a compact audit oracle for the fixture's fixed horizons.
"""
from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from typing import Any


def _day(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


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


def _usable_bar(bar: Mapping[str, Any]) -> bool:
    quality = str(bar.get("quality_status") or "").strip().upper()
    if quality in {"VALID", "PASS"}:
        return True
    if quality != "WARNING" or _boolean(bar.get("is_suspended")) is None:
        return False
    values = {
        field: _number(bar.get(field))
        for field in ("raw_open", "raw_high", "raw_low", "raw_close", "volume", "amount")
    }
    if not bar.get("source_payload_hash") or any(
        value is None or value <= 0 for value in values.values()
    ):
        return False
    low, high = values["raw_low"], values["raw_high"]
    open_price, close = values["raw_open"], values["raw_close"]
    if not (low <= min(open_price, close) <= max(open_price, close) <= high):
        return False
    average = values["amount"] / values["volume"]
    return low * 0.5 <= average <= high * 1.5


def _distribution_for_day(events: Iterable[Mapping[str, Any]]) -> tuple[float, float]:
    selected = {}
    for event in events:
        if str(event.get("implementation_status") or "").strip() != "实施":
            continue
        source_key = str(event.get("source_record_key") or "")
        identity = (source_key, "" if source_key else repr(sorted(event.items())))
        previous = selected.get(identity)
        if previous is None or int(event.get("source_record_version") or 0) >= int(
            previous.get("source_record_version") or 0
        ):
            selected[identity] = event
    stock_rate = cash = 0.0
    for event in selected.values():
        values = {
            name: _number(event.get(name))
            for name in ("stock_div", "stock_bonus_rate", "stock_conversion_rate", "cash_div")
        }
        if any(event.get(name) is not None and values[name] is None for name in values):
            raise ValueError("non-finite corporate action in golden sample")
        direct = values["stock_div"]
        bonus = values["stock_bonus_rate"] or 0.0
        conversion = values["stock_conversion_rate"] or 0.0
        distribution = values["cash_div"] or 0.0
        rate = direct if direct is not None and direct > 1e-12 else bonus + conversion
        if rate < 0 or distribution < 0:
            raise ValueError("negative corporate action in golden sample")
        stock_rate += rate
        cash += distribution
    return stock_rate, cash


def _walk(
    code: str,
    days: Sequence[date],
    bars: Mapping[date, Mapping[str, Mapping[str, Any]]],
    actions: Mapping[date, Mapping[str, Sequence[Mapping[str, Any]]]],
    horizon: int,
    *,
    entry_open: bool,
) -> tuple[float | None, str, str | None]:
    signal_index = 0
    first_day = days[signal_index + 1] if entry_open else days[signal_index]
    first_bar = bars.get(first_day, {}).get(code)
    anchor_field = "raw_open" if entry_open else "raw_close"
    anchor = _number(first_bar.get(anchor_field)) if first_bar else None
    if anchor is None or anchor <= 0:
        return None, "INCOMPLETE", "MISSING_ENTRY_OPEN" if entry_open else "MISSING_SIGNAL_CLOSE"
    if first_bar is None:
        return None, "INCOMPLETE", "MISSING_ENTRY_OPEN" if entry_open else "MISSING_SIGNAL_CLOSE"
    suspended = _boolean(first_bar.get("is_suspended"))
    if suspended is None:
        return None, "INCOMPLETE", "SUSPENSION_STATUS_UNKNOWN"
    if not entry_open and suspended:
        return None, "INCOMPLETE", "SUSPENDED_SIGNAL_BAR"
    if not _usable_bar(first_bar):
        return None, "INCOMPLETE", "INVALID_ENTRY_BAR_QUALITY" if entry_open else "INVALID_SIGNAL_BAR_QUALITY"

    compound = 1.0
    previous_close = anchor
    for offset in range(1, horizon + 1):
        day = days[offset]
        bar = bars.get(day, {}).get(code)
        if bar is None:
            return None, "INCOMPLETE", "MISSING_PATH_BAR"
        is_suspended = _boolean(bar.get("is_suspended"))
        if is_suspended is None:
            return None, "INCOMPLETE", "SUSPENSION_STATUS_UNKNOWN"
        if is_suspended:
            return None, "INCOMPLETE", "SUSPENDED_PATH"
        if not _usable_bar(bar):
            return None, "INCOMPLETE", "INVALID_PATH_BAR_QUALITY"
        close = _number(bar.get("raw_close"))
        if close is None or close <= 0:
            return None, "INCOMPLETE", "MISSING_OR_INVALID_CLOSE"
        if entry_open and offset == 1:
            open_price = _number(bar.get("raw_open"))
            if open_price is None or open_price <= 0:
                return None, "INCOMPLETE", "MISSING_ENTRY_OPEN"
            factor = close / open_price
        else:
            stock_rate, cash = _distribution_for_day(actions.get(day, {}).get(code, ()))
            factor = (close * (1.0 + stock_rate) + cash) / previous_close
        if not math.isfinite(factor) or factor <= 0:
            return None, "INCOMPLETE", "INVALID_TOTAL_RETURN_FACTOR"
        compound *= factor
        if not math.isfinite(compound) or compound <= 0:
            return None, "INCOMPLETE", "INVALID_COMPOUNDED_TOTAL_RETURN"
        previous_close = close
    return compound - 1.0, "COMPLETE", None


def independent_forward_labels(
    observations: Iterable[Mapping[str, Any]],
    *,
    calendar: Sequence[Any],
    bars: Iterable[Mapping[str, Any]],
    actions: Iterable[Mapping[str, Any]],
    delist_dates: Mapping[str, Any] | None = None,
    horizons: Sequence[int] = (20, 40, 60),
) -> list[dict[str, Any]]:
    days = tuple(_day(value) for value in calendar)
    if not days or tuple(sorted(set(days))) != days:
        raise ValueError("calendar must be non-empty, unique, and ascending")
    bars_by_day: dict[date, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for bar in bars:
        bars_by_day[_day(bar["trade_date"])][str(bar["ts_code"])] = bar
    actions_by_day: dict[date, dict[str, list[Mapping[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for action in actions:
        actions_by_day[_day(action["ex_date"])][str(action["ts_code"])].append(action)
    normalized_delist = {
        str(code): _day(value)
        for code, value in (delist_dates or {}).items() if value is not None
    }

    output = []
    for observation in observations:
        signal = _day(observation["research_date"])
        code = str(observation["ts_code"])
        index = days.index(signal)
        for horizon in horizons:
            target_index = index + int(horizon)
            if target_index >= len(days):
                target = None
                reason = "TARGET_OUTSIDE_FROZEN_CALENDAR"
                prediction = entry = None
                status = "IMMATURE"
                entry_status = "TARGET_OUTSIDE_FROZEN_CALENDAR"
            else:
                target = days[target_index]
                delist = normalized_delist.get(code)
                if delist is not None and delist <= target:
                    reason = (
                        "ALREADY_DELISTED_AT_SIGNAL" if delist <= signal
                        else "DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE"
                    )
                    prediction = entry = None
                    status = "INCOMPLETE"
                    entry_status = reason
                else:
                    prediction, status, reason = _walk(
                        code, days[index:], bars_by_day, actions_by_day,
                        int(horizon), entry_open=False,
                    )
                    entry, entry_status, entry_reason = _walk(
                        code, days[index:], bars_by_day, actions_by_day,
                        int(horizon), entry_open=True,
                    )
                    if entry is None:
                        entry_status = entry_reason or entry_status
            output.append({
                "signal_date": signal.isoformat(),
                "ts_code": code,
                "horizon": int(horizon),
                "target_date": target.isoformat() if target is not None else None,
                "status": status,
                "reason": reason,
                "prediction_total_return": prediction,
                "entry_reference_total_return": entry,
                "entry_reference_status": entry_status,
                "execution_status": "NOT_SIMULATED_BACKTEST_REQUIRED",
                "relative_total_return": None,
                "relative_pool_mean_return": None,
                "relative_pool_complete_count": 0,
                "relative_pool_total_count": 0,
            })

    groups = defaultdict(list)
    for row in output:
        groups[(row["signal_date"], row["horizon"])].append(row)
    for rows in groups.values():
        completed = [
            row["prediction_total_return"] for row in rows
            if row["status"] == "COMPLETE" and row["prediction_total_return"] is not None
        ]
        mean = sum(completed) / len(completed) if completed else None
        for row in rows:
            row["relative_pool_mean_return"] = mean
            row["relative_pool_complete_count"] = len(completed)
            row["relative_pool_total_count"] = len(rows)
            if row["status"] == "COMPLETE" and mean is not None:
                row["relative_total_return"] = row["prediction_total_return"] - mean
    return output
