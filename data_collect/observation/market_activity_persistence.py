"""Lifecycle orchestration for Market Activity Observation V1."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
from typing import Any

from data_collect.data_access.context import RunContext
from data_collect.data_access.repository.market_activity_observation_repository import (
    MarketActivityObservationRepository,
)
from data_collect.observation.hashing import hash_object
from data_collect.observation.market_activity_rules import (
    MarketActivityObservationPolicy,
    MarketActivityObservationReport,
    build_market_activity_observations,
)


@dataclass(frozen=True, slots=True)
class MarketActivityObservationBuildResult:
    """Auditable result for one Observation build/freeze attempt."""

    snapshot_id: str
    status: str
    skipped: bool
    report: MarketActivityObservationReport | None
    validation: dict[str, Any] | None


class MarketActivityObservationBuilder:
    """Small interface hiding source validation, lifecycle and persistence."""

    def __init__(self, repository: MarketActivityObservationRepository) -> None:
        self._repository = repository

    def build(
        self,
        activity_snapshot_id: str,
        policy: MarketActivityObservationPolicy,
        context: RunContext,
        *,
        freeze: bool = False,
    ) -> MarketActivityObservationBuildResult:
        source = self._repository.get_activity_snapshot(activity_snapshot_id, context)
        snapshot = self._repository.ensure_snapshot(source, policy, context)
        snapshot_id = str(snapshot["snapshot_id"])
        observation_date = source["activity_date_start"]

        with self._repository.snapshot_lock(snapshot_id):
            snapshot = self._repository.get_snapshot(snapshot_id, context)
            status = str(snapshot.get("status") or "").upper()
            if status in {"SUPERSEDED", "CORRUPTED"}:
                raise RuntimeError(
                    f"Observation snapshot {snapshot_id} is {status}; "
                    "a new methodology or source snapshot is required"
                )
            if status == "FROZEN":
                return MarketActivityObservationBuildResult(
                    snapshot_id, status, True, None, None
                )
            if status == "VALIDATED" and not freeze:
                return MarketActivityObservationBuildResult(
                    snapshot_id, status, True, None, None
                )
            if status == "VALIDATED" and freeze:
                fields = {
                    "input_manifest_hash": snapshot.get("input_manifest_hash"),
                    "quality_report_hash": snapshot.get("quality_report_hash"),
                    "frozen_at": datetime.now(timezone.utc),
                }
                self._transition(snapshot_id, observation_date, context, ("VALIDATED",), "FROZEN", fields)
                return MarketActivityObservationBuildResult(
                    snapshot_id, "FROZEN", True, None, None
                )
            if status not in {"DRAFT", "FAILED", "BUILDING"}:
                raise RuntimeError(f"unsupported Observation snapshot status: {status}")

            self._transition(
                snapshot_id, observation_date, context, (status,), "BUILDING",
                {"built_at": datetime.now(timezone.utc), "failure_reason": None, "failed_at": None},
            )
            try:
                source_rows = self._repository.activity_rows(
                    activity_snapshot_id, observation_date, context
                )
                report = build_market_activity_observations(
                    source_rows,
                    policy=policy,
                    observation_date=observation_date,
                    source_activity_snapshot_id=activity_snapshot_id,
                    data_baseline_id=str(source["data_baseline_id"]),
                    source_activity_methodology_hash=str(source["methodology_hash"]),
                )
                self._repository.write_rows(report, snapshot_id, context)
                validation = self._repository.validate_snapshot(report, snapshot_id, context)
                quality_report_hash = hash_object({
                    "summary": report.quality_summary(),
                    "validation": validation,
                })
                lifecycle_fields = {
                    "input_manifest_hash": report.input_manifest_hash,
                    "quality_report_hash": quality_report_hash,
                    "quality_report": json.dumps(
                        {
                            "summary": report.quality_summary(),
                            "validation": validation,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                        default=str,
                    ),
                    "row_count": report.stock_count,
                    "stock_count": report.stock_count,
                    "sector_count": 0,
                    "date_count": 1,
                    "validated_at": datetime.now(timezone.utc),
                }
                self._transition(
                    snapshot_id, observation_date, context, ("BUILDING",),
                    "VALIDATED", lifecycle_fields,
                )
                status = "VALIDATED"
                if freeze:
                    frozen_fields = dict(lifecycle_fields)
                    frozen_fields["frozen_at"] = datetime.now(timezone.utc)
                    self._transition(
                        snapshot_id, observation_date, context, ("VALIDATED",),
                        "FROZEN", frozen_fields,
                    )
                    status = "FROZEN"
                return MarketActivityObservationBuildResult(
                    snapshot_id, status, False, report, validation
                )
            except Exception as exc:
                try:
                    self._transition(
                        snapshot_id, observation_date, context, ("BUILDING",),
                        "FAILED", {
                            "failure_reason": str(exc)[:4000],
                            "failed_at": datetime.now(timezone.utc),
                        },
                    )
                except Exception:
                    # Keep the original exception.  The database transition
                    # trigger remains the final lifecycle authority.
                    pass
                raise

    def _transition(
        self,
        snapshot_id: str,
        observation_date: date,
        context: RunContext,
        from_statuses: tuple[str, ...],
        to_status: str,
        fields: dict[str, Any],
    ) -> None:
        changed = self._repository.transition_snapshot(
            snapshot_id, observation_date, context, from_statuses, to_status, fields
        )
        if changed != 1:
            raise RuntimeError(
                "Observation snapshot lifecycle transition affected "
                f"{changed} rows: {from_statuses} -> {to_status}"
            )


__all__ = ["MarketActivityObservationBuildResult", "MarketActivityObservationBuilder"]
