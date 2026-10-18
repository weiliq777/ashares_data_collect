"""Date-wise cross-sectional evaluation for frozen Alpha research samples."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime
import math
import random
from statistics import mean, median, stdev
from typing import Any, Iterable, Mapping, Sequence

from data_collect.alpha_research.contracts import EVALUATION_VERSION, FACTORS, FactorSpec


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def classify_market_stages(
    signal_dates: Sequence[date],
    *,
    calendar: Sequence[date],
    benchmark_closes: Mapping[date, Any],
    lookback: int = 60,
) -> dict[date, dict[str, Any]]:
    """Classify each signal date using only the frozen benchmark's trailing return."""
    if lookback <= 0:
        raise ValueError("market-stage lookback must be positive")
    ordered = tuple(_date(day) for day in calendar)
    if tuple(sorted(set(ordered))) != ordered:
        raise ValueError("market-stage calendar must be unique and ascending")
    positions = {day: index for index, day in enumerate(ordered)}
    results: dict[date, dict[str, Any]] = {}
    for raw_day in signal_dates:
        day = _date(raw_day)
        index = positions.get(day)
        if index is None or index < lookback:
            results[day] = {"stage": "UNKNOWN", "benchmark_return": None}
            continue
        current = _number(benchmark_closes.get(day))
        previous = _number(benchmark_closes.get(ordered[index - lookback]))
        if current is None or previous is None or current <= 0 or previous <= 0:
            results[day] = {"stage": "UNKNOWN", "benchmark_return": None}
            continue
        trailing_return = current / previous - 1.0
        stage = "UPTREND" if trailing_return > 0 else "DOWNTREND" if trailing_return < 0 else "FLAT"
        results[day] = {"stage": stage, "benchmark_return": trailing_return}
    return results


def _average_ranks(values: Sequence[float]) -> list[float]:
    ordered = sorted(enumerate(values), key=lambda item: (item[1], item[0]))
    ranks = [0.0] * len(values)
    start = 0
    while start < len(ordered):
        end = start + 1
        while end < len(ordered) and ordered[end][1] == ordered[start][1]:
            end += 1
        average = (start + 1 + end) / 2.0
        for position in range(start, end):
            ranks[ordered[position][0]] = average
        start = end
    return ranks


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean, right_mean = mean(left), mean(right)
    covariance = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right))
    left_ss = sum((x - left_mean) ** 2 for x in left)
    right_ss = sum((y - right_mean) ** 2 for y in right)
    denominator = math.sqrt(left_ss * right_ss)
    return covariance / denominator if denominator > 0 else None


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    return _pearson(_average_ranks(left), _average_ranks(right))


def _residuals(values: Sequence[float], control: Sequence[float]) -> list[float] | None:
    control_mean, value_mean = mean(control), mean(values)
    denominator = sum((value - control_mean) ** 2 for value in control)
    if denominator <= 0:
        return None
    slope = sum(
        (control_value - control_mean) * (value - value_mean)
        for value, control_value in zip(values, control)
    ) / denominator
    intercept = value_mean - slope * control_mean
    return [value - (intercept + slope * control_value) for value, control_value in zip(values, control)]


def _partial_spearman(
    factor_values: Sequence[float], returns: Sequence[float], size_values: Sequence[float],
) -> float | None:
    factor_rank = _average_ranks(factor_values)
    return_rank = _average_ranks(returns)
    size_rank = _average_ranks(size_values)
    factor_residual = _residuals(factor_rank, size_rank)
    return_residual = _residuals(return_rank, size_rank)
    if factor_residual is None or return_residual is None:
        return None
    return _pearson(factor_residual, return_residual)


def evaluate_cross_section(
    signal_date: date,
    horizon: int,
    records: Sequence[Mapping[str, Any]],
    *,
    factor_specs: Sequence[FactorSpec] = FACTORS,
    group_count: int = 5,
    size_field: str = "total_mv",
) -> list[dict[str, Any]]:
    """Evaluate each factor within a single signal date and forward horizon."""
    if group_count != 5:
        raise ValueError("group_count is fixed at five")
    signal_day = _date(signal_date)
    complete_labels = [
        row for row in records
        if row.get("label_status") == "COMPLETE"
        and _number(row.get("prediction_total_return")) is not None
    ]
    label_exclusion_reasons = Counter(
        str(row.get("label_reason"))
        for row in records
        if row.get("label_status") != "COMPLETE" and row.get("label_reason")
    )
    market_stage = (
        str(records[0].get("market_stage") or "UNKNOWN")
        if records else "UNKNOWN"
    )
    output: list[dict[str, Any]] = []
    for spec in factor_specs:
        valid: list[tuple[Mapping[str, Any], float, float]] = []
        factor_reasons: Counter[str] = Counter()
        for row in records:
            factor = (row.get("factor_values") or {}).get(spec.name)
            status = getattr(factor, "status", None) if factor is not None else None
            value = getattr(factor, "value", None) if factor is not None else None
            reason = getattr(factor, "reason", None) if factor is not None else "FACTOR_NOT_COMPUTED"
            numeric_value = _number(value)
            target = _number(row.get("prediction_total_return"))
            if status != "VALID" or numeric_value is None:
                factor_reasons[str(reason or status or "MISSING_FACTOR")] += 1
                continue
            if row.get("label_status") != "COMPLETE" or target is None:
                continue
            valid.append((row, numeric_value, target))

        n_valid = len(valid)
        factor_values = [item[1] for item in valid]
        returns = [item[2] for item in valid]
        rank_ic = _spearman(factor_values, returns)
        direction = spec.higher_is_better
        oriented_ic = rank_ic * direction if rank_ic is not None and direction else None

        grouped: list[list[float]] = [[] for _ in range(group_count)]
        if n_valid >= group_count:
            ranked = sorted(
                valid,
                key=lambda item: (item[1], str(item[0].get("ts_code") or "")),
            )
            for position, (_, _, target) in enumerate(ranked):
                group = min(group_count - 1, position * group_count // n_valid)
                grouped[group].append(target)
        group_rows = [
            {
                "group": index + 1,
                "n_stocks": len(values),
                "mean_return": mean(values) if values else None,
                "median_return": median(values) if values else None,
            }
            for index, values in enumerate(grouped)
        ]
        q_spread = (
            group_rows[-1]["mean_return"] - group_rows[0]["mean_return"]
            if all(row["mean_return"] is not None for row in group_rows)
            else None
        )

        size_pairs = [
            (item[1], item[2], _number(item[0].get(size_field)))
            for item in valid
        ]
        size_pairs = [item for item in size_pairs if item[2] is not None and item[2] > 0]
        size_exposure = _spearman(
            [item[0] for item in size_pairs],
            [math.log(item[2]) for item in size_pairs],
        ) if len(size_pairs) >= 2 else None
        size_adjusted_ic = _partial_spearman(
            [item[0] for item in size_pairs],
            [item[1] for item in size_pairs],
            [math.log(item[2]) for item in size_pairs],
        ) if len(size_pairs) >= 3 else None

        output.append({
            "signal_date": signal_day.isoformat(),
            "year": signal_day.year,
            "horizon": int(horizon),
            "market_stage": market_stage,
            "factor": spec.name,
            "category": spec.category,
            "direction": direction,
            "n_total": len(records),
            "n_label_complete": len(complete_labels),
            "n_factor_valid": sum(
                1 for row in records
                if getattr((row.get("factor_values") or {}).get(spec.name), "status", None) == "VALID"
            ),
            "n_stocks": n_valid,
            "n_missing_pairs": len(records) - n_valid,
            "missing_ratio": (len(records) - n_valid) / len(records) if records else 1.0,
            "n_incomplete_labels": len(records) - len(complete_labels),
            "label_exclusion_reasons": dict(sorted(label_exclusion_reasons.items())),
            "factor_exclusion_reasons": dict(sorted(factor_reasons.items())),
            "rank_ic": rank_ic,
            "oriented_rank_ic": oriented_ic,
            "group_returns": group_rows,
            "group_spread_q5_q1": q_spread,
            "oriented_group_spread_q5_q1": q_spread * direction if q_spread is not None and direction else None,
            "size_exposure_rank_ic": size_exposure,
            "size_adjusted_rank_ic": size_adjusted_ic,
            "size_control_status": (
                "AVAILABLE" if size_adjusted_ic is not None
                else "NOT CALCULABLE_CONSTANT_SIZE_OR_SMALL_SAMPLE"
            ),
            "industry_control_status": "NOT AVAILABLE_NO_FROZEN_PIT_INDUSTRY",
            "calculation_status": "CALCULATED" if rank_ic is not None else "NOT CALCULABLE_INSUFFICIENT_OR_CONSTANT_SAMPLE",
        })
    return output


def evaluate_delist_sensitivity(
    signal_date: date,
    horizon: int,
    records: Sequence[Mapping[str, Any]],
    *,
    assumed_total_returns: Sequence[float],
    factor_specs: Sequence[FactorSpec],
) -> list[dict[str, Any]]:
    """Measure factor-statistic changes under explicit hypothetical delist returns.

    Only labels with a known delisting inside the horizon and no frozen terminal
    value are imputed. Other incomplete labels and securities already delisted
    at the signal date remain excluded.
    """
    assumptions = tuple(float(value) for value in assumed_total_returns)
    if len(set(assumptions)) != len(assumptions):
        raise ValueError("assumed delist total returns must be unique")
    if any(not math.isfinite(value) or value < -1.0 for value in assumptions):
        raise ValueError("assumed delist total returns must be finite and at least -1")

    delist_reason = "DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE"
    if not any(
        row.get("label_status") != "COMPLETE" and row.get("label_reason") == delist_reason
        for row in records
    ):
        return []

    output: list[dict[str, Any]] = []
    for spec in factor_specs:
        valid: list[tuple[str, float, float | None]] = []
        for row in records:
            factor = (row.get("factor_values") or {}).get(spec.name)
            if getattr(factor, "status", None) != "VALID":
                continue
            factor_value = _number(getattr(factor, "value", None))
            if factor_value is None:
                continue
            if row.get("label_status") == "COMPLETE":
                target = _number(row.get("prediction_total_return"))
                if target is not None:
                    valid.append((str(row.get("ts_code") or ""), factor_value, target))
            elif row.get("label_reason") == delist_reason:
                valid.append((str(row.get("ts_code") or ""), factor_value, None))
        if not any(target is None for _, _, target in valid):
            continue
        factor_ranks = _average_ranks([factor for _, factor, _ in valid])
        factor_order = sorted(range(len(valid)), key=lambda index: (valid[index][1], valid[index][0]))
        imputed_count = sum(target is None for _, _, target in valid)
        for assumed_return in assumptions:
            target_returns = [
                assumed_return if target is None else target
                for _, _, target in valid
            ]
            rank_ic = _pearson(factor_ranks, _average_ranks(target_returns))
            groups: list[list[float]] = [[] for _ in range(5)]
            for position, index in enumerate(factor_order):
                group = min(4, position * 5 // len(factor_order))
                groups[group].append(target_returns[index])
            group_means = [mean(values) if values else None for values in groups]
            group_spread = (
                group_means[-1] - group_means[0]
                if all(value is not None for value in group_means) else None
            )
            output.append({
                "signal_date": _date(signal_date).isoformat(),
                "horizon": int(horizon),
                "factor": spec.name,
                "assumed_delist_total_return": assumed_return,
                "n_delist_imputed": imputed_count,
                "n_stocks": len(valid),
                "rank_ic": rank_ic,
                "group_spread_q5_q1": group_spread,
            })
    return output


def _moving_block_interval(
    values: Sequence[float | None], *, block_length: int, repetitions: int, seed: int,
) -> tuple[float, float] | None:
    if len(values) < 2 * block_length or block_length <= 0:
        return None
    rng = random.Random(seed)
    max_start = len(values) - block_length
    estimates: list[float] = []
    for _ in range(repetitions):
        chosen: list[float] = []
        while len(chosen) < len(values):
            start = rng.randint(0, max_start)
            chosen.extend(values[start:start + block_length])
        sampled = [value for value in chosen[:len(values)] if value is not None]
        if sampled:
            estimates.append(mean(sampled))
    if not estimates:
        return None
    estimates.sort()
    low_index = max(0, math.floor(0.025 * (len(estimates) - 1)))
    high_index = min(len(estimates) - 1, math.ceil(0.975 * (len(estimates) - 1)))
    return estimates[low_index], estimates[high_index]


def summarize_evaluations(
    daily_rows: Iterable[Mapping[str, Any]],
    *,
    horizons: Sequence[int],
    bootstrap_repetitions: int = 1_000,
    min_bootstrap_dates: int = 60,
    seed: int = 20261016,
    expected_signal_dates: Sequence[date] | None = None,
    factor_specs: Sequence[FactorSpec] = FACTORS,
    unique_stocks_by_factor: Mapping[tuple[str, int], set[str]] | None = None,
    unique_stock_counts: Mapping[tuple[str, int], int] | None = None,
    relative_pool_means: Mapping[tuple[date, int], float | None] | None = None,
    market_stages: Mapping[date, str] | None = None,
) -> dict[str, Any]:
    """Aggregate date-level results; date blocks, not stock rows, are resampled."""
    by_factor: dict[tuple[str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in daily_rows:
        by_factor[(str(row["factor"]), int(row["horizon"]))].append(row)
    expected_dates = sorted({_date(day) for day in expected_signal_dates or ()})
    if not expected_dates:
        expected_dates = sorted({
            _date(row["signal_date"])
            for rows in by_factor.values() for row in rows
        })
    factors: list[dict[str, Any]] = []
    combinations = {
        (factor.name, int(horizon))
        for factor in factor_specs for horizon in horizons
    } | set(by_factor)
    spec_by_name = {factor.name: factor for factor in factor_specs}
    for (factor_name, horizon) in sorted(combinations):
        existing = by_factor.get((factor_name, horizon), [])
        by_date = {_date(row["signal_date"]): row for row in existing}
        rows = [
            by_date.get(day, {
                "signal_date": day.isoformat(), "year": day.year,
                "horizon": horizon, "factor": factor_name,
                "market_stage": (market_stages or {}).get(day, "UNKNOWN"),
                "direction": spec_by_name.get(factor_name).higher_is_better if factor_name in spec_by_name else 0,
                "n_total": 0, "n_label_complete": 0, "n_factor_valid": 0,
                "n_stocks": 0, "n_missing_pairs": 0, "missing_ratio": 1.0,
                "n_incomplete_labels": 0, "rank_ic": None,
                "oriented_rank_ic": None, "group_spread_q5_q1": None,
                "oriented_group_spread_q5_q1": None,
                "size_adjusted_rank_ic": None,
                "calculation_status": "NO_FROZEN_SNAPSHOT_FOR_SIGNAL_DATE",
            })
            for day in expected_dates
        ] if expected_dates else sorted(existing, key=lambda row: str(row["signal_date"]))
        rows.sort(key=lambda row: str(row["signal_date"]))
        for row in rows:
            day = _date(row["signal_date"])
            pool_mean = (relative_pool_means or {}).get((day, horizon))
            row["relative_pool_mean_return"] = pool_mean
            row["relative_group_returns"] = [
                {
                    "group": int(group["group"]),
                    "n_stocks": int(group.get("n_stocks") or 0),
                    "mean_return": (
                        float(group["mean_return"]) - float(pool_mean)
                        if pool_mean is not None and group.get("mean_return") is not None else None
                    ),
                    "median_return": (
                        float(group["median_return"]) - float(pool_mean)
                        if pool_mean is not None and group.get("median_return") is not None else None
                    ),
                }
                for group in row.get("group_returns", ())
            ]
        ic_series = [
            float(row["rank_ic"]) if row.get("rank_ic") is not None else None
            for row in rows
        ]
        spread_series = [
            float(row["group_spread_q5_q1"])
            if row.get("group_spread_q5_q1") is not None else None
            for row in rows
        ]
        oriented_spread_series = [
            float(row["oriented_group_spread_q5_q1"])
            if row.get("oriented_group_spread_q5_q1") is not None else None
            for row in rows
        ]
        ic_values = [value for value in ic_series if value is not None]
        spread_values = [value for value in spread_series if value is not None]
        oriented_values = [float(row["oriented_rank_ic"]) for row in rows if row.get("oriented_rank_ic") is not None]
        oriented_spreads = [float(row["oriented_group_spread_q5_q1"]) for row in rows if row.get("oriented_group_spread_q5_q1") is not None]
        factor_exclusion_reasons: Counter[str] = Counter()
        for row in rows:
            factor_exclusion_reasons.update(row.get("factor_exclusion_reasons", {}))
        group_return_summary = []
        for group_number in range(1, 6):
            group_rows = [
                group
                for row in rows
                for group in row.get("group_returns", ())
                if int(group.get("group", -1)) == group_number
            ]
            mean_returns = [
                float(group["mean_return"])
                for group in group_rows if group.get("mean_return") is not None
            ]
            median_returns = [
                float(group["median_return"])
                for group in group_rows if group.get("median_return") is not None
            ]
            group_return_summary.append({
                "group": group_number,
                "n_valid_dates": len(mean_returns),
                "mean_stocks_per_date": mean([
                    float(group["n_stocks"]) for group in group_rows
                ]) if group_rows else None,
                "mean_daily_cross_sectional_return": mean(mean_returns) if mean_returns else None,
                "mean_daily_cross_sectional_median_return": mean(median_returns) if median_returns else None,
                "mean_daily_relative_cross_sectional_return": mean([
                    float(group["mean_return"])
                    for row in rows
                    for group in row.get("relative_group_returns", ())
                    if int(group.get("group", -1)) == group_number and group.get("mean_return") is not None
                ]) if any(
                    int(group.get("group", -1)) == group_number and group.get("mean_return") is not None
                    for row in rows for group in row.get("relative_group_returns", ())
                ) else None,
                "mean_daily_relative_cross_sectional_median_return": mean([
                    float(group["median_return"])
                    for row in rows
                    for group in row.get("relative_group_returns", ())
                    if int(group.get("group", -1)) == group_number and group.get("median_return") is not None
                ]) if any(
                    int(group.get("group", -1)) == group_number and group.get("median_return") is not None
                    for row in rows for group in row.get("relative_group_returns", ())
                ) else None,
                "stock_date_pairs": sum(int(group.get("n_stocks") or 0) for group in group_rows),
            })
        year_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            year_groups[str(row["year"])].append(row)
        annual = []
        for year, year_rows in sorted(year_groups.items()):
            year_ics = [float(row["rank_ic"]) for row in year_rows if row.get("rank_ic") is not None]
            year_spreads = [float(row["group_spread_q5_q1"]) for row in year_rows if row.get("group_spread_q5_q1") is not None]
            annual.append({
                "year": int(year),
                "n_dates": len(year_rows),
                "n_valid_dates": len(year_ics),
                "rank_ic_mean": mean(year_ics) if year_ics else None,
                "rank_ic_positive_rate": sum(value > 0 for value in year_ics) / len(year_ics) if year_ics else None,
                "group_spread_mean": mean(year_spreads) if year_spreads else None,
            })
        market_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            market_groups[str(row.get("market_stage") or "UNKNOWN")].append(row)
        market_stability = []
        for stage, stage_rows in sorted(market_groups.items()):
            stage_ics = [float(row["rank_ic"]) for row in stage_rows if row.get("rank_ic") is not None]
            stage_spreads = [
                float(row["group_spread_q5_q1"])
                for row in stage_rows if row.get("group_spread_q5_q1") is not None
            ]
            market_stability.append({
                "market_stage": stage,
                "n_dates": len(stage_rows),
                "n_valid_dates": len(stage_ics),
                "mean_rank_ic": mean(stage_ics) if stage_ics else None,
                "rank_ic_positive_rate": sum(value > 0 for value in stage_ics) / len(stage_ics) if stage_ics else None,
                "mean_group_spread_q5_q1": mean(stage_spreads) if stage_spreads else None,
                "confidence_interval_status": "NOT COMPUTED_BY_MARKET_STAGE",
            })
        block_length = int(horizon)
        interval = None
        interval_status = "NOT AVAILABLE_INSUFFICIENT_DATE_COVERAGE"
        if len(ic_values) >= max(min_bootstrap_dates, 2 * block_length):
            interval = _moving_block_interval(
                ic_series, block_length=block_length,
                repetitions=bootstrap_repetitions,
                seed=seed + horizon + sum(ord(char) for char in factor_name),
            )
            interval_status = "AVAILABLE" if interval is not None else interval_status
        spread_interval = None
        spread_interval_status = "NOT AVAILABLE_INSUFFICIENT_DATE_COVERAGE"
        if len(spread_values) >= max(min_bootstrap_dates, 2 * block_length):
            spread_interval = _moving_block_interval(
                spread_series, block_length=block_length,
                repetitions=bootstrap_repetitions,
                seed=seed + 10_000 + horizon + sum(ord(char) for char in factor_name),
            )
            spread_interval_status = "AVAILABLE" if spread_interval is not None else spread_interval_status
        oriented_spread_interval = None
        if len(oriented_spreads) >= max(min_bootstrap_dates, 2 * block_length):
            oriented_spread_interval = _moving_block_interval(
                oriented_spread_series, block_length=block_length,
                repetitions=bootstrap_repetitions,
                seed=seed + 20_000 + horizon + sum(ord(char) for char in factor_name),
            )
        factors.append({
            "factor": factor_name,
            "horizon": horizon,
            "n_dates": len(rows),
            "n_valid_dates": len(ic_values),
            "n_missing_dates": len(rows) - len(ic_values),
            "n_stocks": (
                len(unique_stocks_by_factor.get((factor_name, horizon), set()))
                if unique_stocks_by_factor is not None
                else (unique_stock_counts or {}).get((factor_name, horizon))
            ),
            "n_stock_date_pairs": sum(int(row.get("n_stocks") or 0) for row in rows),
            "n_total_stock_date_pairs": sum(int(row.get("n_total") or 0) for row in rows),
            "n_factor_valid_stock_date_pairs": sum(int(row.get("n_factor_valid") or 0) for row in rows),
            "n_factor_valid_dates": sum(int(row.get("n_factor_valid") or 0) > 0 for row in rows),
            "factor_missing_ratio": (
                1.0 - sum(int(row.get("n_factor_valid") or 0) for row in rows)
                / sum(int(row.get("n_total") or 0) for row in rows)
                if sum(int(row.get("n_total") or 0) for row in rows) else 1.0
            ),
            "factor_exclusion_reasons": dict(sorted(factor_exclusion_reasons.items())),
            "mean_missing_ratio": (
                sum(int(row.get("n_missing_pairs") or 0) for row in rows)
                / sum(int(row.get("n_total") or 0) for row in rows)
                if sum(int(row.get("n_total") or 0) for row in rows) else 1.0
            ),
            "mean_rank_ic": mean(ic_values) if ic_values else None,
            "std_rank_ic": stdev(ic_values) if len(ic_values) > 1 else (0.0 if ic_values else None),
            "rank_ic_positive_rate": sum(value > 0 for value in ic_values) / len(ic_values) if ic_values else None,
            "mean_oriented_rank_ic": mean(oriented_values) if oriented_values else None,
            "mean_group_spread_q5_q1": mean(spread_values) if spread_values else None,
            "mean_oriented_group_spread_q5_q1": mean(oriented_spreads) if oriented_spreads else None,
            "group_return_summary": group_return_summary,
            "rank_ic_confidence_interval_95": list(interval) if interval else None,
            "confidence_interval_status": interval_status,
            "group_spread_confidence_interval_95": list(spread_interval) if spread_interval else None,
            "group_spread_confidence_interval_status": spread_interval_status,
            "oriented_group_spread_confidence_interval_95": (
                list(oriented_spread_interval) if oriented_spread_interval else None
            ),
            "bootstrap_method": "moving_block_by_signal_date",
            "bootstrap_block_length_dates": block_length,
            "annual_stability": annual,
            "market_regime_stability": market_stability,
            "size_adjusted_rank_ic_mean": mean([
                float(row["size_adjusted_rank_ic"])
                for row in rows if row.get("size_adjusted_rank_ic") is not None
            ]) if any(row.get("size_adjusted_rank_ic") is not None for row in rows) else None,
            "mean_size_exposure_rank_ic": mean([
                float(row["size_exposure_rank_ic"])
                for row in rows if row.get("size_exposure_rank_ic") is not None
            ]) if any(row.get("size_exposure_rank_ic") is not None for row in rows) else None,
            "industry_control_status": "NOT AVAILABLE_NO_FROZEN_PIT_INDUSTRY",
            "daily_series": [dict(row) for row in rows],
        })
    missing_combinations = [
        {"horizon": horizon, "status": "NO_EVALUABLE_FACTOR_DATES"}
        for horizon in horizons
        if not any(item["horizon"] == horizon for item in factors)
    ]
    return {
        "evaluation_version": EVALUATION_VERSION,
        "factors": factors,
        "missing_horizon_results": missing_combinations,
        "method_notes": {
            "rank_ic": "Spearman correlation computed separately for each signal date and horizon.",
            "grouping": "Five ascending factor groups, deterministic tie break by security code.",
            "group_return_summary": "Mean of each date's cross-sectional group mean/median returns; dates are equally weighted.",
            "relative_return": "relative_total_return subtracts each date/horizon's equal-weight complete-pool mean; Rank IC and Q5-Q1 spread are invariant to that same-date constant, while group mean returns are shown in both absolute and relative terms.",
            "bootstrap": "Moving blocks over date-level Rank IC; overlapping label horizons are not treated as independent stock rows.",
            "industry": "Unavailable because PIT industry membership is not a frozen Dataset input.",
            "size": "Partial Spearman uses cross-sectional ranks residualized on log(total_mv) when frozen field is available.",
            "market_stage": "UPTREND/DOWNTREND/FLAT from the frozen benchmark's trailing 60-session close return; UNKNOWN when coverage is insufficient.",
        },
    }


__all__ = [
    "classify_market_stages",
    "evaluate_cross_section",
    "evaluate_delist_sensitivity",
    "summarize_evaluations",
]
