"""Repositories for the Stage 1 Foundation contract boundary.

The repository exposes contract metadata and Dataset Ready state without
letting callers submit SQL or hold a database handle.  SQL remains here, and
every operation is still governed by DataAccessKernel/Gateway policy.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class FoundationContractRepository(BaseRepository):
    """Typed access to contract metadata and trading-day build state."""

    def publish_contract_version(
        self,
        context: RunContext,
        contract_version: str,
        *,
        status: str = "ACTIVE",
        release_notes: str = "",
        manifest_hash: str | None = None,
        released_at: Any = None,
    ) -> int:
        if not contract_version.strip():
            raise ValueError("contract_version cannot be empty")
        scope = WorkScope(partition=f"contract-version:{contract_version}")
        row = {
            "contract_version": contract_version,
            "status": status,
            "release_notes": release_notes,
            "manifest_hash": manifest_hash,
            "released_at": released_at,
        }
        # Contract metadata is immutable and published through the typed
        # publish gateway.  ON CONFLICT DO NOTHING makes retries idempotent.
        return self.kernel.publish.snapshot(
            context, scope, "contract.contract_version", [row]
        )

    def publish_data_dictionary(
        self,
        context: RunContext,
        contract_version: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        if not rows:
            return 0
        scope = WorkScope(partition=f"data-dictionary:{contract_version}")
        normalized = [
            {**dict(row), "contract_version": contract_version}
            for row in rows
        ]
        return self.kernel.publish.snapshot(
            context, scope, "contract.data_dictionary", normalized
        )

    def record_trading_day_build(
        self,
        context: RunContext,
        *,
        trade_date: date,
        pipeline_version: str,
        run_id: str,
        final_status: str,
        raw_status: str = "BUILDING",
        standard_status: str = "BUILDING",
        pit_status: str = "BUILDING",
        feature_status: str = "BUILDING",
        quality_status: str = "BUILDING",
        snapshot_status: str = "BUILDING",
        started_at: Any = None,
        completed_at: Any = None,
        error_code: str | None = None,
        error_message: str | None = None,
        retry_count: int = 0,
    ) -> int:
        if retry_count < 0:
            raise ValueError("retry_count cannot be negative")
        stage_statuses = (
            raw_status, standard_status, pit_status, feature_status,
            quality_status, snapshot_status,
        )
        if final_status == "READY" and any(
            status != "READY" for status in stage_statuses
        ):
            raise ValueError(
                "final READY requires raw/standard/pit/feature/quality/snapshot READY"
            )
        if final_status == "READY" and completed_at is None:
            raise ValueError("final READY requires completed_at")
        scope = WorkScope(
            date_from=trade_date,
            date_to=trade_date,
            partition=f"trading-day-build:{pipeline_version}",
        )
        row = {
            "trade_date": trade_date,
            "pipeline_version": pipeline_version,
            "run_id": run_id,
            "raw_status": raw_status,
            "standard_status": standard_status,
            "pit_status": pit_status,
            "feature_status": feature_status,
            "quality_status": quality_status,
            "snapshot_status": snapshot_status,
            "started_at": started_at,
            "completed_at": completed_at,
            "error_code": error_code,
            "error_message": error_message,
            "retry_count": retry_count,
            "final_status": final_status,
        }
        return self.kernel.write.upsert(
            context,
            scope,
            "ops.trading_day_build",
            [row],
            key=("trade_date", "pipeline_version"),
            update_columns=(
                "run_id", "raw_status", "standard_status", "pit_status",
                "feature_status", "quality_status", "snapshot_status",
                "started_at", "completed_at", "error_code", "error_message",
                "retry_count", "final_status",
            ),
        )

    def latest_trading_day_build(
        self,
        context: RunContext,
        *,
        trade_date: date,
        pipeline_version: str | None = None,
    ) -> dict[str, Any] | None:
        where = "trade_date=%s"
        params: list[Any] = [trade_date]
        if pipeline_version is not None:
            where += " AND pipeline_version=%s"
            params.append(pipeline_version)
        rows = self.kernel.read._repository_query(
            context,
            "ops.trading_day_build",
            """SELECT trade_date, pipeline_version, run_id, final_status,
                      raw_status, standard_status, pit_status, feature_status,
                      quality_status, snapshot_status, started_at, completed_at,
                      error_code, error_message, retry_count, updated_at
                 FROM ops.trading_day_build
                WHERE """ + where + " ORDER BY updated_at DESC LIMIT 1",
            params,
            WorkScope(date_from=trade_date, date_to=trade_date, partition="trading-day-build:latest"),
            2,
        )
        return rows[0] if rows else None
