"""Deterministic Observation rules backed by frozen Market Activity facts.

This module is the deep calculation seam for Observation V1.  It consumes
already-frozen ``observation.stock_activity_daily`` facts and emits only
explainable anomaly evidence.  It never reads the database and never makes a
trading recommendation.

The first version deliberately does not emit ``VOLUME_PRICE_UP`` or
``VOLUME_PRICE_DOWN``.  The frozen Market Activity V1 contract does not carry
the target day's return, and deriving it from a mutable upstream table here
would weaken reproducibility.  Those tags require a new input contract.
"""
from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import date, datetime
import math
from typing import Any, Mapping, Sequence
from uuid import NAMESPACE_URL, uuid5

from data_collect.market_activity.contracts import (
    MARKET_ACTIVITY_CONTRACT_HASH,
    MARKET_ACTIVITY_CONTRACT_VERSION,
)
from data_collect.observation.contracts import canonicalize
from data_collect.observation.hashing import hash_object, hash_rows


ACTIVITY_OBSERVATION_CONTRACT_VERSION = "market-activity-observation-v1"
ACTIVITY_OBSERVATION_METHODOLOGY_ID = "MARKET_ACTIVITY_RULES"
ACTIVITY_OBSERVATION_TAGS = (
    "ABNORMAL_VOLUME",
    "ABNORMAL_AMOUNT",
    "SUDDEN_ACTIVITY",
    "HIGH_AMOUNT_SHARE",
)
DEFERRED_ACTIVITY_TAGS = ("VOLUME_PRICE_UP", "VOLUME_PRICE_DOWN")

_SOURCE_HASH_COLUMNS = (
    "activity_snapshot_id", "trade_date", "ts_code", "methodology_hash",
    "data_baseline_id", "volume", "amount", "amount_share", "volume_share",
    "amount_rank", "volume_rank", "amount_ratio_previous",
    "volume_ratio_previous", "history_observation_count", "calculation_status",
    "quality_status", "input_manifest_hash", "activity_hash",
)


def _finite(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _stable_id(*parts: Any) -> str:
    return str(uuid5(NAMESPACE_URL, "|".join(str(part) for part in parts)))


@dataclass(frozen=True, slots=True)
class MarketActivityObservationPolicy:
    """Explicit thresholds and completeness rules for Observation V1."""

    policy_id: str = "market_activity_rules_v1"
    min_history_observation_count: int = 20
    ratio_threshold: float = 2.0
    high_amount_share_threshold: float = 0.005

    def __post_init__(self) -> None:
        if not self.policy_id.strip():
            raise ValueError("policy_id must not be blank")
        if not 1 <= int(self.min_history_observation_count) <= 120:
            raise ValueError("min_history_observation_count must be in 1..120")
        if not math.isfinite(float(self.ratio_threshold)) or self.ratio_threshold <= 0:
            raise ValueError("ratio_threshold must be finite and positive")
        if not math.isfinite(float(self.high_amount_share_threshold)) or not 0 < self.high_amount_share_threshold <= 1:
            raise ValueError("high_amount_share_threshold must be in (0, 1]")

    def as_dict(self) -> dict[str, Any]:
        return {field.name: canonicalize(getattr(self, field.name)) for field in fields(self)}

    @property
    def methodology_hash(self) -> str:
        return hash_object({
            "contract_version": ACTIVITY_OBSERVATION_CONTRACT_VERSION,
            "methodology_id": ACTIVITY_OBSERVATION_METHODOLOGY_ID,
            "market_activity_contract_hash": MARKET_ACTIVITY_CONTRACT_HASH,
            "market_activity_contract_version": MARKET_ACTIVITY_CONTRACT_VERSION,
            "policy": self.as_dict(),
            "supported_tags": ACTIVITY_OBSERVATION_TAGS,
            "deferred_tags": DEFERRED_ACTIVITY_TAGS,
        })

    @property
    def contract_hash(self) -> str:
        return hash_object({
            "contract_version": ACTIVITY_OBSERVATION_CONTRACT_VERSION,
            "methodology_id": ACTIVITY_OBSERVATION_METHODOLOGY_ID,
            "source": "frozen observation.stock_activity_daily",
            "source_columns": _SOURCE_HASH_COLUMNS,
            "evidence_fields": (
                "actual_value", "threshold", "source_metric",
                "history_observation_count", "lookback_trading_days",
            ),
            "policy_id": self.policy_id,
        })


@dataclass(frozen=True, slots=True)
class ActivityObservationRow:
    """One stock row plus all tags triggered on the observation date."""

    observation_id: str
    observation_date: date
    ts_code: str
    source_activity_snapshot_id: str
    methodology_hash: str
    data_baseline_id: str
    activity_volume_ratio_previous: float | None
    activity_amount_ratio_previous: float | None
    activity_amount_share: float | None
    activity_volume_share: float | None
    activity_volume_rank: int | None
    activity_amount_rank: int | None
    activity_history_observation_count: int
    calculation_status: str
    quality_status: str
    anomaly_flag: bool
    tags: tuple[dict[str, Any], ...]
    observation_hash: str

    def as_dict(self) -> dict[str, Any]:
        return {field.name: getattr(self, field.name) for field in fields(self)}


@dataclass(frozen=True, slots=True)
class MarketActivityObservationReport:
    """Pure result for one frozen Market Activity snapshot."""

    observation_date: date
    source_activity_snapshot_id: str
    methodology_hash: str
    data_baseline_id: str
    contract_hash: str
    quality_status: str
    input_row_count: int
    stock_count: int
    tag_count: int
    valid_stock_count: int
    insufficient_history_stock_count: int
    invalid_stock_count: int
    input_manifest_hash: str
    observation_hash: str
    rows: tuple[ActivityObservationRow, ...]

    def quality_summary(self) -> dict[str, Any]:
        return {
            "contract_version": ACTIVITY_OBSERVATION_CONTRACT_VERSION,
            "methodology_id": ACTIVITY_OBSERVATION_METHODOLOGY_ID,
            "observation_date": self.observation_date.isoformat(),
            "source_activity_snapshot_id": self.source_activity_snapshot_id,
            "methodology_hash": self.methodology_hash,
            "data_baseline_id": self.data_baseline_id,
            "contract_hash": self.contract_hash,
            "quality_status": self.quality_status,
            "input_row_count": self.input_row_count,
            "stock_count": self.stock_count,
            "tag_count": self.tag_count,
            "valid_stock_count": self.valid_stock_count,
            "insufficient_history_stock_count": self.insufficient_history_stock_count,
            "invalid_stock_count": self.invalid_stock_count,
            "input_manifest_hash": self.input_manifest_hash,
            "observation_hash": self.observation_hash,
        }


def _evidence(
    tag_code: str,
    actual_value: float,
    threshold: float,
    source_metric: str,
    history_count: int,
    policy: MarketActivityObservationPolicy,
    row: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "tag_code": tag_code,
        "triggered": True,
        "actual_value": actual_value,
        "threshold": threshold,
        "source_metric": source_metric,
        "lookback_trading_days": policy.min_history_observation_count,
        "history_observation_count": history_count,
        "source_activity_snapshot_id": str(row["activity_snapshot_id"]),
        "source_input_manifest_hash": str(row.get("input_manifest_hash") or ""),
        "source_activity_hash": str(row.get("activity_hash") or ""),
    }


def build_market_activity_observations(
    rows: Sequence[Mapping[str, Any]],
    *,
    policy: MarketActivityObservationPolicy,
    observation_date: date,
    source_activity_snapshot_id: str,
    data_baseline_id: str,
    source_activity_methodology_hash: str | None = None,
) -> MarketActivityObservationReport:
    """Build deterministic, explainable tags from frozen activity facts.

    The input is bounded by the Repository.  This function still checks all
    identity and ordering invariants so a malformed adapter cannot silently
    produce a mixed-date or mixed-baseline Observation.
    """
    policy.__post_init__()
    if not rows:
        raise ValueError("market activity observation requires source rows")
    expected_date = _as_date(observation_date)
    expected_snapshot = str(source_activity_snapshot_id).strip()
    expected_baseline = str(data_baseline_id).strip()
    if not expected_snapshot or not expected_baseline:
        raise ValueError("source activity snapshot and baseline are required")

    ordered_rows = sorted(rows, key=lambda item: str(item.get("ts_code") or ""))
    source_manifest, _ = hash_rows(ordered_rows, columns=_SOURCE_HASH_COLUMNS)
    seen: set[str] = set()
    result_rows: list[ActivityObservationRow] = []
    insufficient_count = 0
    invalid_count = 0
    valid_count = 0

    for source in ordered_rows:
        trade_day = _as_date(source.get("trade_date"))
        code = str(source.get("ts_code") or "").strip()
        if trade_day != expected_date:
            raise ValueError(f"activity observation input date mismatch for {code}: {trade_day}")
        if str(source.get("activity_snapshot_id") or "") != expected_snapshot:
            raise ValueError(f"activity observation source snapshot mismatch for {code}")
        if str(source.get("data_baseline_id") or "") != expected_baseline:
            raise ValueError(f"activity observation baseline mismatch for {code}")
        if source_activity_methodology_hash is not None and str(
            source.get("methodology_hash") or ""
        ) != str(source_activity_methodology_hash):
            raise ValueError(f"activity observation methodology mismatch for {code}")
        if not code:
            raise ValueError("activity observation input contains a blank stock code")
        if code in seen:
            raise ValueError(f"duplicate activity observation input: {code}")
        seen.add(code)

        history = int(source.get("history_observation_count") or 0)
        source_status = str(source.get("calculation_status") or "INVALID").upper()
        if source_status == "VALID" and history >= policy.min_history_observation_count:
            calculation_status = "VALID"
            valid_count += 1
        elif source_status == "INVALID":
            calculation_status = "INVALID"
            invalid_count += 1
        else:
            calculation_status = "INSUFFICIENT_HISTORY"
            insufficient_count += 1

        quality_status = "VALID" if str(source.get("quality_status") or "").upper() == "VALID" else "WARNING"
        volume_ratio = _finite(source.get("volume_ratio_previous"))
        amount_ratio = _finite(source.get("amount_ratio_previous"))
        amount_share = _finite(source.get("amount_share"))
        volume_share = _finite(source.get("volume_share"))
        tags: list[dict[str, Any]] = []
        if calculation_status == "VALID":
            if volume_ratio is not None and volume_ratio >= policy.ratio_threshold:
                tags.append(_evidence(
                    "ABNORMAL_VOLUME", volume_ratio, policy.ratio_threshold,
                    "volume_ratio_previous", history, policy, source,
                ))
            if amount_ratio is not None and amount_ratio >= policy.ratio_threshold:
                tags.append(_evidence(
                    "ABNORMAL_AMOUNT", amount_ratio, policy.ratio_threshold,
                    "amount_ratio_previous", history, policy, source,
                ))
            if volume_ratio is not None and amount_ratio is not None and (
                volume_ratio >= policy.ratio_threshold and amount_ratio >= policy.ratio_threshold
            ):
                tags.append(_evidence(
                    "SUDDEN_ACTIVITY", min(volume_ratio, amount_ratio),
                    policy.ratio_threshold, "volume_ratio_previous+amount_ratio_previous",
                    history, policy, source,
                ))
            if amount_share is not None and amount_share >= policy.high_amount_share_threshold:
                tags.append(_evidence(
                    "HIGH_AMOUNT_SHARE", amount_share,
                    policy.high_amount_share_threshold, "amount_share", history, policy, source,
                ))

        identity = (
            expected_snapshot, expected_date.isoformat(), code,
            policy.methodology_hash, expected_baseline,
        )
        observation_id = _stable_id("market-activity-observation", *identity)
        row_payload = {
            "observation_id": observation_id,
            "observation_date": expected_date,
            "ts_code": code,
            "source_activity_snapshot_id": expected_snapshot,
            "methodology_hash": policy.methodology_hash,
            "data_baseline_id": expected_baseline,
            "activity_volume_ratio_previous": volume_ratio,
            "activity_amount_ratio_previous": amount_ratio,
            "activity_amount_share": amount_share,
            "activity_volume_share": volume_share,
            "activity_volume_rank": source.get("volume_rank"),
            "activity_amount_rank": source.get("amount_rank"),
            "activity_history_observation_count": history,
            "calculation_status": calculation_status,
            "quality_status": quality_status,
            "anomaly_flag": bool(tags),
            "tags": tags,
        }
        observation_hash = hash_object(row_payload)
        result_rows.append(ActivityObservationRow(
            observation_id=observation_id,
            observation_date=expected_date,
            ts_code=code,
            source_activity_snapshot_id=expected_snapshot,
            methodology_hash=policy.methodology_hash,
            data_baseline_id=expected_baseline,
            activity_volume_ratio_previous=volume_ratio,
            activity_amount_ratio_previous=amount_ratio,
            activity_amount_share=amount_share,
            activity_volume_share=volume_share,
            activity_volume_rank=int(source["volume_rank"]) if source.get("volume_rank") is not None else None,
            activity_amount_rank=int(source["amount_rank"]) if source.get("amount_rank") is not None else None,
            activity_history_observation_count=history,
            calculation_status=calculation_status,
            quality_status=quality_status,
            anomaly_flag=bool(tags),
            tags=tuple(tags),
            observation_hash=observation_hash,
        ))

    all_tags = tuple(tag for row in result_rows for tag in row.tags)
    if invalid_count or insufficient_count:
        report_quality = "PARTIAL_HISTORY"
    else:
        report_quality = "VALID"
    observation_hash = hash_object({
        "methodology_hash": policy.methodology_hash,
        "source_activity_snapshot_id": expected_snapshot,
        "data_baseline_id": expected_baseline,
        "input_manifest_hash": source_manifest,
        "rows": [row.as_dict() for row in result_rows],
    })
    return MarketActivityObservationReport(
        observation_date=expected_date,
        source_activity_snapshot_id=expected_snapshot,
        methodology_hash=policy.methodology_hash,
        data_baseline_id=expected_baseline,
        contract_hash=policy.contract_hash,
        quality_status=report_quality,
        input_row_count=len(rows),
        stock_count=len(result_rows),
        tag_count=len(all_tags),
        valid_stock_count=valid_count,
        insufficient_history_stock_count=insufficient_count,
        invalid_stock_count=invalid_count,
        input_manifest_hash=source_manifest,
        observation_hash=observation_hash,
        rows=tuple(result_rows),
    )


__all__ = [
    "ACTIVITY_OBSERVATION_CONTRACT_VERSION",
    "ACTIVITY_OBSERVATION_METHODOLOGY_ID",
    "ACTIVITY_OBSERVATION_TAGS",
    "DEFERRED_ACTIVITY_TAGS",
    "MarketActivityObservationPolicy",
    "ActivityObservationRow",
    "MarketActivityObservationReport",
    "build_market_activity_observations",
]
