"""Daily persistence orchestration for Market Activity Observation.

The builder is intentionally small: the Repository hides SQL, while this
module coordinates lifecycle, idempotency, validation and the immutable
boundary.  It never exposes or accepts a database handle.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any

from data_collect.data_access.context import RunContext
from data_collect.data_access.repository.market_activity_storage_repository import (
    MarketActivityStorageRepository,
)
from data_collect.market_activity.contracts import MarketActivityReport, MarketActivityRequest
from data_collect.market_activity.monitor import MarketActivityMonitor
from data_collect.observation.hashing import hash_object


@dataclass(frozen=True, slots=True)
class MarketActivityBuildResult:
    """Auditable result of one daily persistence attempt."""

    activity_snapshot_id: str
    status: str
    skipped: bool
    report: MarketActivityReport | None
    validation: dict[str, Any] | None


class MarketActivityBuilder:
    """Build one date into the immutable-ready activity storage model."""

    def __init__(self, repository: MarketActivityStorageRepository) -> None:
        self._repository = repository
        self._monitor = MarketActivityMonitor(repository)

    def build(
        self,
        request: MarketActivityRequest,
        context: RunContext,
        *,
        freeze: bool = False,
    ) -> MarketActivityBuildResult:
        request.require_persistence_identity()
        snapshot = self._repository.ensure_snapshot(request, context)
        snapshot_id = str(snapshot["activity_snapshot_id"])

        with self._repository.snapshot_lock(snapshot_id):
            snapshot = self._repository.get_snapshot(snapshot_id, context)
            status = str(snapshot["status"])
            if status in {"SUPERSEDED", "CORRUPTED"}:
                raise RuntimeError(
                    f"market activity snapshot {snapshot_id} is {status}; "
                    "a new methodology/baseline identity is required"
                )
            if status == "FROZEN":
                return MarketActivityBuildResult(
                    activity_snapshot_id=snapshot_id,
                    status=status,
                    skipped=True,
                    report=None,
                    validation=None,
                )
            if status == "VALIDATED" and not freeze:
                return MarketActivityBuildResult(
                    activity_snapshot_id=snapshot_id,
                    status=status,
                    skipped=True,
                    report=None,
                    validation=None,
                )
            if status == "VALIDATED" and freeze:
                frozen_fields = {
                    "input_manifest_hash": snapshot.get("input_manifest_hash"),
                    "quality_report_hash": snapshot.get("quality_report_hash"),
                    "validated_at": snapshot.get("validated_at"),
                    "frozen_at": datetime.now(timezone.utc),
                }
                self._transition(snapshot_id, request, context, ("VALIDATED",), "FROZEN", frozen_fields)
                return MarketActivityBuildResult(
                    activity_snapshot_id=snapshot_id,
                    status="FROZEN",
                    skipped=True,
                    report=None,
                    validation=None,
                )
            if status not in {"DRAFT", "FAILED", "BUILDING"}:
                raise RuntimeError(f"unsupported market activity snapshot status: {status}")

            self._transition(
                snapshot_id,
                request,
                context,
                (status,),
                "BUILDING",
                {"built_at": datetime.now(timezone.utc), "failure_reason": None},
            )

            try:
                report = self._monitor.daily(request, context)
                if report.data_baseline_id != request.data_baseline_id:
                    raise RuntimeError("market activity report baseline identity mismatch")
                self._repository.write_daily_summary(report, snapshot_id, context)
                self._repository.write_stock_rows(report, snapshot_id, context)
                validation = self._repository.validate_snapshot(report, snapshot_id, context)
                quality_report_hash = hash_object({
                    "summary": report.quality_summary(),
                    "validation": validation,
                })
                lifecycle_fields = {
                    "input_manifest_hash": report.input_manifest_hash,
                    "quality_report_hash": quality_report_hash,
                    "quality_report": json.dumps(
                        report.quality_summary(),
                        ensure_ascii=False,
                        sort_keys=True,
                        default=str,
                    ),
                    "row_count": len(report.rows),
                    "stock_count": len(report.rows),
                    "date_count": 1,
                    "total_input_row_count": report.input_row_count,
                    "target_row_count": report.target_row_count,
                    "invalid_target_row_count": report.invalid_target_row_count,
                    "invalid_history_row_count": report.invalid_history_row_count,
                    "insufficient_history_stock_count": report.insufficient_history_stock_count,
                    "validated_at": datetime.now(timezone.utc),
                }
                self._transition(
                    snapshot_id, request, context, ("BUILDING",), "VALIDATED", lifecycle_fields
                )
                final_status = "VALIDATED"
                if freeze:
                    frozen_fields = dict(lifecycle_fields)
                    frozen_fields["frozen_at"] = datetime.now(timezone.utc)
                    self._transition(
                        snapshot_id, request, context, ("VALIDATED",), "FROZEN", frozen_fields
                    )
                    final_status = "FROZEN"
                return MarketActivityBuildResult(
                    activity_snapshot_id=snapshot_id,
                    status=final_status,
                    skipped=False,
                    report=report,
                    validation=validation,
                )
            except Exception as exc:
                try:
                    self._transition(
                        snapshot_id,
                        request,
                        context,
                        ("BUILDING",),
                        "FAILED",
                        {
                            "failure_reason": str(exc)[:4000],
                            "failed_at": datetime.now(timezone.utc),
                        },
                    )
                except Exception:
                    # Preserve the original build failure.  The database
                    # trigger/lifecycle checks remain the final authority.
                    pass
                raise

    def _transition(
        self,
        snapshot_id: str,
        request: MarketActivityRequest,
        context: RunContext,
        from_statuses: tuple[str, ...],
        to_status: str,
        fields: dict[str, Any],
    ) -> None:
        changed = self._repository.transition_snapshot(
            snapshot_id,
            context,
            request,
            from_statuses,
            to_status,
            fields,
        )
        if changed != 1:
            raise RuntimeError(
                "market activity snapshot lifecycle transition affected "
                f"{changed} rows: {from_statuses} -> {to_status}"
            )


__all__ = ["MarketActivityBuildResult", "MarketActivityBuilder"]
