"""Pure calculations for observed market activity."""
from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable, Mapping
from datetime import date, datetime, timedelta
import hashlib
import math
from statistics import fmean
from typing import Any

from data_collect.market_activity.contracts import (
    MARKET_ACTIVITY_CONTRACT_VERSION,
    MarketActivityReport,
    MarketActivityRequest,
    StockActivity,
)
from data_collect.observation.contracts import canonical_json, canonicalize
from data_collect.observation.hashing import hash_rows, hash_object


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _finite_number(value: Any, *, allow_none: bool = False) -> float | None:
    if value is None and allow_none:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _valid_measure(value: Any) -> float | None:
    number = _finite_number(value)
    return number if number is not None and number >= 0 else None


def _ratio(value: float, baseline: float | None) -> float | None:
    if baseline is None or baseline <= 0:
        return None
    return value / baseline


def build_daily_report(
    batches: Iterable[Iterable[Mapping[str, Any]]],
    request: MarketActivityRequest,
) -> MarketActivityReport:
    """Build one report from ordered bounded batches.

    The input must be ordered by ``trade_date, stock_code`` as guaranteed by
    :class:`MarketActivityRepository`.  The implementation retains only the
    preceding lookback bars per stock plus the target-date rows, so memory is
    bounded by the lookback and the one-day result rather than the full query
    window.
    """
    request.validate()
    target = request.trade_date
    histories: dict[str, deque[tuple[float, float]]] = defaultdict(
        lambda: deque(maxlen=request.lookback_trading_days)
    )
    target_rows: dict[str, tuple[float | None, float, float, tuple[str, ...]]] = {}
    seen_keys: set[tuple[str, date]] = set()
    previous_date: date | None = None
    target_row_count = 0
    invalid_target_row_count = 0
    invalid_history_row_count = 0
    input_row_count = 0
    input_digest = hashlib.sha256()

    def hash_input_row(raw: Mapping[str, Any], day: date) -> None:
        payload = [
            canonicalize(raw.get("stock_code")),
            canonicalize(day),
            canonicalize(raw.get("close")),
            canonicalize(raw.get("volume")),
            canonicalize(raw.get("amount")),
        ]
        input_digest.update(canonical_json(payload).encode("utf-8"))
        input_digest.update(b"\n")

    for batch in batches:
        for raw in batch:
            input_row_count += 1
            if input_row_count > request.max_bars:
                raise ValueError(
                    f"market activity input exceeds max_bars={request.max_bars}"
                )
            day = _as_date(raw.get("trade_date"))
            hash_input_row(raw, day)
            if day > target:
                raise ValueError("market activity input contains future rows")
            if previous_date is not None and day < previous_date:
                raise ValueError("market activity input must be ordered by trade_date")
            previous_date = day
            code = str(raw.get("stock_code") or "").strip()
            if not code:
                if day == target:
                    target_row_count += 1
                    invalid_target_row_count += 1
                else:
                    invalid_history_row_count += 1
                continue
            key = (code, day)
            if key in seen_keys:
                raise ValueError(f"duplicate Standard daily bar: {code} {day}")
            seen_keys.add(key)

            volume = _valid_measure(raw.get("volume"))
            amount = _valid_measure(raw.get("amount"))
            close = _finite_number(raw.get("close"), allow_none=True)
            if volume is None or amount is None:
                if day == target:
                    target_row_count += 1
                    invalid_target_row_count += 1
                else:
                    invalid_history_row_count += 1
                continue

            if day == target:
                target_row_count += 1
                if code in target_rows:
                    raise ValueError(f"duplicate target bar: {code} {day}")
                target_rows[code] = (close, volume, amount, ())
            else:
                histories[code].append((volume, amount))

    if not target_rows:
        raise ValueError(f"no valid Standard daily bars found for {target}")

    total_volume = sum(row[1] for row in target_rows.values())
    total_amount = sum(row[2] for row in target_rows.values())
    amount_order = sorted(
        target_rows,
        key=lambda code: (-target_rows[code][2], code),
    )
    volume_order = sorted(
        target_rows,
        key=lambda code: (-target_rows[code][1], code),
    )
    amount_rank = {code: index for index, code in enumerate(amount_order, start=1)}
    volume_rank = {code: index for index, code in enumerate(volume_order, start=1)}

    activities: list[StockActivity] = []
    insufficient_history_stock_count = 0
    for code in sorted(target_rows):
        close, volume, amount, reason_codes = target_rows[code]
        history = histories[code]
        reasons = list(reason_codes)
        if len(history) < request.lookback_trading_days:
            reasons.append("INSUFFICIENT_LOOKBACK")
            insufficient_history_stock_count += 1
        volume_average = (
            fmean(item[0] for item in history)
            if len(history) >= request.lookback_trading_days
            else None
        )
        amount_average = (
            fmean(item[1] for item in history)
            if len(history) >= request.lookback_trading_days
            else None
        )
        activities.append(
            StockActivity(
                ts_code=code,
                trade_date=target,
                close=close,
                volume=volume,
                amount=amount,
                amount_share=(amount / total_amount if total_amount > 0 else None),
                volume_share=(volume / total_volume if total_volume > 0 else None),
                amount_rank=amount_rank[code],
                volume_rank=volume_rank[code],
                amount_avg_previous=amount_average,
                amount_ratio_previous=_ratio(amount, amount_average),
                volume_avg_previous=volume_average,
                volume_ratio_previous=_ratio(volume, volume_average),
                history_observation_count=len(history),
                reason_codes=tuple(sorted(set(reasons))),
            )
        )

    top_codes = amount_order[: request.top_n]
    top_n_amount = sum(target_rows[code][2] for code in top_codes)
    top_n_volume = sum(target_rows[code][1] for code in volume_order[: request.top_n])
    quality_status = "PARTIAL" if invalid_target_row_count else "VALID"
    if (invalid_history_row_count or insufficient_history_stock_count) and quality_status == "VALID":
        quality_status = "PARTIAL_HISTORY"

    input_manifest_hash = input_digest.hexdigest()
    row_hash, _ = hash_rows(
        (row.as_dict() for row in activities),
        columns=(
            "ts_code", "trade_date", "close", "volume", "amount", "amount_share",
            "volume_share", "amount_rank", "volume_rank", "amount_avg_previous",
            "amount_ratio_previous", "volume_avg_previous", "volume_ratio_previous",
            "history_observation_count", "reason_codes",
        ),
    )
    methodology_hash = request.methodology_hash
    activity_hash = hash_object(
        {
            "methodology_hash": methodology_hash,
            "data_baseline_id": request.data_baseline_id,
            "input_manifest_hash": input_manifest_hash,
            "row_hash": row_hash,
            "trade_date": target,
            "top_n": request.top_n,
            "quality_status": quality_status,
            "input_row_count": input_row_count,
            "target_row_count": target_row_count,
            "invalid_target_row_count": invalid_target_row_count,
            "invalid_history_row_count": invalid_history_row_count,
            "insufficient_history_stock_count": insufficient_history_stock_count,
            "total_volume": total_volume,
            "total_amount": total_amount,
            "top_n_volume": top_n_volume,
            "top_n_amount": top_n_amount,
            "top_n_volume_share": (top_n_volume / total_volume if total_volume > 0 else None),
            "top_n_amount_share": (top_n_amount / total_amount if total_amount > 0 else None),
        }
    )

    return MarketActivityReport(
        contract_version=MARKET_ACTIVITY_CONTRACT_VERSION,
        trade_date=target,
        lookback_start=target - timedelta(days=request.calendar_lookback_days),
        top_n=request.top_n,
        quality_status=quality_status,
        input_row_count=input_row_count,
        target_row_count=target_row_count,
        invalid_target_row_count=invalid_target_row_count,
        invalid_history_row_count=invalid_history_row_count,
        insufficient_history_stock_count=insufficient_history_stock_count,
        total_volume=total_volume,
        total_amount=total_amount,
        top_n_volume=top_n_volume,
        top_n_amount=top_n_amount,
        top_n_volume_share=(top_n_volume / total_volume if total_volume > 0 else None),
        top_n_amount_share=(top_n_amount / total_amount if total_amount > 0 else None),
        rows=tuple(activities),
        methodology_hash=methodology_hash,
        data_baseline_id=(
            str(request.data_baseline_id).strip()
            if request.data_baseline_id is not None else None
        ),
        input_manifest_hash=input_manifest_hash,
        activity_hash=activity_hash,
    )
