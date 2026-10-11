"""Pure calculations for strategy-neutral Market Anomaly Outcome (市场异常后验结果)."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import math
from statistics import mean
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from data_collect.observation.hashing import hash_object


HORIZONS = (5, 10, 20, 60, 120)
OUTCOME_CONTRACT_VERSION = "market-anomaly-outcome-1"
_METRIC_COLUMNS = tuple(
    f"{metric}_{horizon}d"
    for horizon in HORIZONS
    for metric in (
        "future_price_return", "future_total_return", "excess_return",
        "max_drawdown", "max_favorable_excursion", "max_adverse_excursion",
        "volatility", "best_return", "worst_return",
    )
)


def _as_date(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a date") from exc


def _positive(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


@dataclass(frozen=True, slots=True)
class OutcomePolicy:
    """Immutable formulas and source semantics included in the methodology hash."""

    benchmark_code: str = "000985.CSI"
    horizons: tuple[int, ...] = HORIZONS
    contract_version: str = OUTCOME_CONTRACT_VERSION
    calendar_exchange: str = "SSE"
    # This literal is the value persisted by FeatureRepository and consumed by
    # the frozen Observation query; it is not the informal feature table name.
    price_coordinate_version: str = "market-price-coordinate-v1"
    price_return_formula: str = "adj_close_base[t+n]/adj_close_base[t]-1"
    total_return_formula: str = "total_return_price[t+n]/total_return_price[t]-1"
    excess_return_formula: str = "stock_price_return-benchmark_price_return"
    path_includes_anchor_and_endpoint: bool = True
    volatility_ddof: int = 1

    def __post_init__(self) -> None:
        if not self.benchmark_code.strip():
            raise ValueError("benchmark_code must not be blank")
        if tuple(self.horizons) != HORIZONS:
            raise ValueError(f"horizons are fixed by contract: {HORIZONS}")
        if self.volatility_ddof != 1:
            raise ValueError("Outcome volatility uses sample standard deviation (ddof=1)")

    @property
    def methodology_hash(self) -> str:
        return hash_object({
            "contract_version": self.contract_version,
            "benchmark_code": self.benchmark_code,
            "horizons": self.horizons,
            "calendar_exchange": self.calendar_exchange,
            "price_coordinate_version": self.price_coordinate_version,
            "price_return_formula": self.price_return_formula,
            "total_return_formula": self.total_return_formula,
            "excess_return_formula": self.excess_return_formula,
            "path_includes_anchor_and_endpoint": self.path_includes_anchor_and_endpoint,
            "volatility_ddof": self.volatility_ddof,
            "missing_source_policy": "no_shift-no_forward_fill-quality_watermark",
        })


def _path_metrics(values: Sequence[float]) -> tuple[float, float, float, float | None]:
    anchor = values[0]
    peak = anchor
    drawdown = favorable = adverse = 0.0
    for value in values:
        peak = max(peak, value)
        drawdown = min(drawdown, value / peak - 1.0)
        move = value / anchor - 1.0
        favorable = max(favorable, move)
        adverse = min(adverse, move)
    daily_returns = [values[index] / values[index - 1] - 1.0 for index in range(1, len(values))]
    volatility = None
    if len(daily_returns) >= 2:
        average = mean(daily_returns)
        volatility = math.sqrt(
            sum((item - average) ** 2 for item in daily_returns)
            / (len(daily_returns) - 1)
        )
    return drawdown, favorable, adverse, volatility


def _validated_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    expected_code: str,
    calendar: set[date],
    date_field: str = "trade_date",
) -> dict[date, Mapping[str, Any]]:
    result: dict[date, Mapping[str, Any]] = {}
    previous: date | None = None
    for row in rows:
        code = str(row.get("ts_code") or "").strip()
        if code != expected_code:
            raise ValueError(f"price input security code mismatch: {code!r} != {expected_code!r}")
        trade_date = _as_date(row.get(date_field), date_field)
        if trade_date not in calendar:
            raise ValueError(f"price date is not in the supplied trading calendar: {trade_date}")
        if trade_date in result:
            raise ValueError(f"duplicate price date: {trade_date}")
        if previous is not None and trade_date < previous:
            raise ValueError("price rows must be ordered by trade_date")
        result[trade_date] = row
        previous = trade_date
    return result


def _valid_feature_price(
    row: Mapping[str, Any] | None,
    *,
    expected_date: date,
    coordinate_version: str,
    field: str,
) -> float | None:
    if row is None or str(row.get("quality_status") or "").upper() != "VALID":
        return None
    try:
        if _as_date(row.get("source_asof_date"), "source_asof_date") != expected_date:
            return None
    except ValueError:
        return None
    if str(row.get("price_coordinate_contract_version") or "") != coordinate_version:
        return None
    return _positive(row.get(field))


def _benchmark_close(row: Mapping[str, Any] | None) -> float | None:
    if row is None or str(row.get("quality_status") or "VALID").upper() != "VALID":
        return None
    return _positive(row.get("close"))


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        import json
        parsed = json.loads(value)
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _json_values(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        import json
        parsed = json.loads(value)
        value = parsed if isinstance(parsed, (list, tuple, set)) else ()
    if value is None:
        return ()
    return tuple(str(item) for item in value)


def compute_outcome(
    event: Mapping[str, Any],
    *,
    calendar_dates: Sequence[date],
    stock_rows: Sequence[Mapping[str, Any]],
    benchmark_rows: Sequence[Mapping[str, Any]],
    through_date: date,
    policy: OutcomePolicy | None = None,
    existing: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Calculate a bounded, resumable event outcome without database access.

    ``calendar_dates`` is the ordered SSE session sequence beginning at T and
    containing up to T+120. ``through_date`` is the latest session whose market
    quality gate passed. Price rows after that watermark are deliberately not
    consumed even if already present in the source table.
    """
    policy = policy or OutcomePolicy()
    if not event.get("observation_id") or not event.get("snapshot_id"):
        raise ValueError("outcome event requires observation_id and snapshot_id")
    observation_date = _as_date(event.get("observation_date"), "observation_date")
    through_date = _as_date(through_date, "through_date")
    ts_code = str(event.get("ts_code") or "").strip()
    if not ts_code:
        raise ValueError("outcome event requires ts_code")
    if through_date < observation_date:
        raise ValueError("through_date cannot precede observation_date")
    calendar = tuple(_as_date(item, "calendar_date") for item in calendar_dates)
    if not calendar or calendar[0] != observation_date:
        raise ValueError("trading calendar must start on observation_date")
    if any(calendar[index] >= calendar[index + 1] for index in range(len(calendar) - 1)):
        raise ValueError("trading calendar must be strictly increasing")
    if through_date not in calendar and through_date < calendar[-1]:
        raise ValueError("through_date must be an open session in the supplied calendar")
    if len(calendar) > max(policy.horizons) + 1:
        raise ValueError("calendar path exceeds the 121-session hard limit")
    calendar_set = set(calendar)
    stock_by_date = _validated_rows(
        stock_rows, expected_code=ts_code, calendar=calendar_set,
    )
    benchmark_by_date = _validated_rows(
        benchmark_rows, expected_code=policy.benchmark_code, calendar=calendar_set,
    )

    prior = dict(existing or {})
    revision = int(prior.get("outcome_revision") or 1)
    if revision < 1:
        raise ValueError("outcome_revision must be positive")
    reasons = set(_json_values(prior.get("missing_reason_codes")))
    prior_statuses = _json_mapping(prior.get("horizon_status"))
    horizon_status = dict(prior_statuses)
    provenance = _json_mapping(prior.get("input_provenance"))
    horizon_provenance = dict(provenance.get("horizons") or {})
    result: dict[str, Any] = {
        **prior,
        "outcome_id": prior.get("outcome_id") or str(uuid5(
            NAMESPACE_URL,
            f"market-anomaly-outcome|{event['observation_id']}|{policy.methodology_hash}|{revision}",
        )),
        "observation_id": str(event["observation_id"]),
        "observation_snapshot_id": str(event["snapshot_id"]),
        "evaluation_methodology_hash": policy.methodology_hash,
        "outcome_revision": revision,
        "benchmark_code": policy.benchmark_code,
    }
    for column in _METRIC_COLUMNS:
        result.setdefault(column, None)

    anchor_price = _positive(event.get("anchor_adj_close_base"))
    anchor_total = _positive(event.get("anchor_total_return_price"))
    anchor_date = event.get("anchor_source_asof_date")
    anchor_quality = str(event.get("anchor_quality_status") or "").upper()
    anchor_version = str(event.get("anchor_price_coordinate_contract_version") or "")
    anchor_hash = str(event.get("anchor_source_hash") or "")
    observation_manifest_hash = str(event.get("input_manifest_hash") or "").strip()
    observation_hash = str(event.get("observation_hash") or "").strip()
    data_baseline_id = str(event.get("data_baseline_id") or "").strip()
    snapshot_baseline_id = str(event.get("snapshot_baseline_id") or data_baseline_id).strip()
    observation_baseline_id = str(
        event.get("observation_baseline_id") or data_baseline_id,
    ).strip()
    observation_methodology_hash = str(event.get("methodology_hash") or "").strip()
    snapshot_methodology_hash = str(
        event.get("snapshot_methodology_hash") or observation_methodology_hash
    ).strip()
    try:
        anchor_date = _as_date(anchor_date, "anchor_source_asof_date")
    except ValueError:
        anchor_date = None
    anchor_ok = (
        anchor_price is not None
        and anchor_total is not None
        and anchor_date == observation_date
        and anchor_quality == "VALID"
        and anchor_version == policy.price_coordinate_version
        and len(anchor_hash) == 64
    )
    identity_ok = (
        len(observation_manifest_hash) == 64
        and len(observation_hash) == 64
        and len(observation_methodology_hash) == 64
        and data_baseline_id != ""
        and snapshot_baseline_id == data_baseline_id
        and observation_baseline_id == data_baseline_id
        and snapshot_methodology_hash == observation_methodology_hash
    )
    if not anchor_ok or not identity_ok:
        if not anchor_ok:
            reasons.add("ANCHOR_NOT_RECONSTRUCTABLE")
        if not identity_ok:
            reasons.add("OBSERVATION_MANIFEST_INCOMPLETE")
        result["maturity_status"] = "INVALID_INPUT"
        result["next_evaluation_date"] = None
        for horizon in policy.horizons:
            horizon_status[str(horizon)] = {
                "status": "INVALID_INPUT",
                "target_date": None,
                "reason_codes": ["ANCHOR_NOT_RECONSTRUCTABLE"],
            }
    else:
        anchor_benchmark = _positive(event.get("anchor_benchmark_close"))
        if anchor_benchmark is None:
            reasons.add("MISSING_BENCHMARK_ANCHOR")
        for horizon in policy.horizons:
            key = str(horizon)
            prior_horizon = _json_mapping(horizon_status.get(key))
            old_state = str(prior_horizon.get("status") or "PENDING").upper()
            if old_state in {"MATURED", "MISSING_SOURCE", "INVALID_INPUT"}:
                continue
            stored_target = prior_horizon.get("target_date")
            target_date = (
                _as_date(stored_target, "horizon target_date")
                if stored_target else (calendar[horizon] if len(calendar) > horizon else None)
            )
            if target_date is None:
                horizon_status[key] = {
                    "status": "PENDING", "target_date": None, "reason_codes": [],
                }
                continue
            if target_date > through_date:
                horizon_status[key] = {
                    "status": "PENDING", "target_date": target_date.isoformat(),
                    "reason_codes": [],
                }
                continue

            horizon_reasons: set[str] = set()
            target_row = stock_by_date.get(target_date)
            target_price = _valid_feature_price(
                target_row, expected_date=target_date,
                coordinate_version=anchor_version, field="adj_close_base",
            )
            target_total = _valid_feature_price(
                target_row, expected_date=target_date,
                coordinate_version=anchor_version, field="total_return_price",
            )
            if target_price is None:
                horizon_reasons.add(f"MISSING_STOCK_PRICE_AT_TARGET_{horizon}D")
            else:
                result[f"future_price_return_{horizon}d"] = target_price / anchor_price - 1.0
            if target_total is None:
                horizon_reasons.add(f"MISSING_TOTAL_RETURN_AT_TARGET_{horizon}D")
            else:
                result[f"future_total_return_{horizon}d"] = target_total / anchor_total - 1.0

            path_values: list[float] = [anchor_price]
            path_complete = True
            path_source_rows: list[Mapping[str, Any] | None] = []
            for path_date in calendar[1:horizon + 1]:
                if path_date > through_date:
                    path_complete = False
                    break
                path_row = stock_by_date.get(path_date)
                path_source_rows.append(path_row)
                value = _valid_feature_price(
                    path_row, expected_date=path_date,
                    coordinate_version=anchor_version, field="adj_close_base",
                )
                if value is None:
                    path_complete = False
                    continue
                path_values.append(value)
            if target_price is None or not path_complete or len(path_values) != horizon + 1:
                horizon_reasons.add(f"MISSING_PRICE_PATH_{horizon}D")
            else:
                drawdown, favorable, adverse, volatility = _path_metrics(path_values)
                result[f"max_drawdown_{horizon}d"] = drawdown
                result[f"max_favorable_excursion_{horizon}d"] = favorable
                result[f"max_adverse_excursion_{horizon}d"] = adverse
                result[f"volatility_{horizon}d"] = volatility
                result[f"best_return_{horizon}d"] = favorable
                result[f"worst_return_{horizon}d"] = adverse

            benchmark_target = _benchmark_close(benchmark_by_date.get(target_date))
            if anchor_benchmark is None or benchmark_target is None:
                horizon_reasons.add(f"MISSING_BENCHMARK_RETURN_{horizon}D")
            elif target_price is not None:
                benchmark_return = benchmark_target / anchor_benchmark - 1.0
                result[f"excess_return_{horizon}d"] = (
                    result[f"future_price_return_{horizon}d"] - benchmark_return
                )

            horizon_status[key] = {
                "status": "MATURED" if not horizon_reasons else "MISSING_SOURCE",
                "target_date": target_date.isoformat(),
                "reason_codes": sorted(horizon_reasons),
            }
            reasons.update(horizon_reasons)
            horizon_provenance[key] = {
                "target_date": target_date.isoformat(),
                "stock_path_expected_count": horizon,
                "stock_path_row_count": sum(row is not None for row in path_source_rows),
                "stock_path_hash": hash_object([
                    ({
                        "trade_date": path_date,
                        "present": row is not None,
                        "ts_code": row.get("ts_code") if row else None,
                        "adj_close_base": row.get("adj_close_base") if row else None,
                        "total_return_price": row.get("total_return_price") if row else None,
                        "quality_status": row.get("quality_status") if row else None,
                        "source_asof_date": row.get("source_asof_date") if row else None,
                        "price_coordinate_contract_version": (
                            row.get("price_coordinate_contract_version") if row else None
                        ),
                        "calculated_at": row.get("calculated_at") if row else None,
                    })
                    for path_date, row in zip(calendar[1:horizon + 1], path_source_rows)
                ]),
                "benchmark_target": {
                    "source_table": "std.index_daily_tushare",
                    "source": "tushare",
                    "ts_code": policy.benchmark_code,
                    "trade_date": target_date.isoformat(),
                    "present": benchmark_by_date.get(target_date) is not None,
                    "close": benchmark_target,
                },
                "benchmark_target_hash": hash_object({
                    "source_table": "std.index_daily_tushare",
                    "source": "tushare",
                    "ts_code": policy.benchmark_code,
                    "trade_date": target_date.isoformat(),
                    "present": benchmark_by_date.get(target_date) is not None,
                    "close": benchmark_target,
                }),
            }

        states = {str(item.get("status")) for item in horizon_status.values()}
        if "INVALID_INPUT" in states:
            result["maturity_status"] = "INVALID_INPUT"
            result["next_evaluation_date"] = None
        elif "PENDING" in states:
            result["maturity_status"] = "PENDING_MATURITY"
            future_targets = [
                _as_date(item.get("target_date"), "horizon target_date")
                for item in horizon_status.values()
                if item.get("status") == "PENDING" and item.get("target_date")
                and _as_date(item["target_date"], "horizon target_date") > through_date
            ]
            result["next_evaluation_date"] = min(future_targets) if future_targets else through_date + timedelta(days=1)
        elif "MISSING_SOURCE" in states:
            result["maturity_status"] = "INCOMPLETE"
            result["next_evaluation_date"] = None
        else:
            result["maturity_status"] = "MATURED"
            result["next_evaluation_date"] = None

    anchor_provenance = {
        "observation_snapshot_id": str(event["snapshot_id"]),
        "observation_date": observation_date.isoformat(),
        "data_baseline_id": data_baseline_id,
        "observation_snapshot_input_manifest_hash": str(
            event.get("snapshot_input_manifest_hash") or "",
        ),
        "observation_input_manifest_hash": observation_manifest_hash,
        "observation_methodology_hash": observation_methodology_hash,
        "snapshot_methodology_hash": snapshot_methodology_hash,
        "observation_hash": observation_hash,
        "anchor_source_table": "feature.price_daily_v2",
        "anchor_source_hash": anchor_hash,
        "anchor_source_asof_date": anchor_date.isoformat() if anchor_date else None,
        "anchor_quality_status": anchor_quality or None,
        "price_coordinate_contract_version": anchor_version or None,
        "anchor_benchmark_code": policy.benchmark_code,
        "anchor_benchmark_close": _positive(event.get("anchor_benchmark_close")),
    }
    result["anchor_adj_close_base"] = anchor_price
    result["anchor_total_return_price"] = anchor_total
    result["anchor_benchmark_close"] = _positive(event.get("anchor_benchmark_close"))
    result["horizon_status"] = horizon_status
    result["input_provenance"] = {
        **provenance,
        "anchor": anchor_provenance,
        "horizons": horizon_provenance,
    }
    result["missing_reason_codes"] = sorted(reasons)
    result["input_manifest_hash"] = hash_object({
        "contract_version": policy.contract_version,
        "evaluation_methodology_hash": policy.methodology_hash,
        "observation_id": str(event["observation_id"]),
        "observation_input_manifest_hash": str(event.get("input_manifest_hash") or ""),
        "anchor": anchor_provenance,
        "horizons": horizon_provenance,
    })
    result["outcome_hash"] = hash_object({
        "evaluation_methodology_hash": policy.methodology_hash,
        "input_manifest_hash": result["input_manifest_hash"],
        "maturity_status": result["maturity_status"],
        "horizon_status": horizon_status,
        "missing_reason_codes": result["missing_reason_codes"],
        "metrics": {key: result.get(key) for key in _METRIC_COLUMNS},
    })
    return result


__all__ = [
    "HORIZONS", "OUTCOME_CONTRACT_VERSION", "OutcomePolicy", "compute_outcome",
]
