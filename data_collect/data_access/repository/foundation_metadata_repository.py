"""Typed metadata, lineage and change-impact contracts for the foundation."""
from __future__ import annotations

import json
from datetime import date
from typing import Any, Mapping, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class FoundationMetadataRepository(BaseRepository):
    """Own Registry, Change Event, Impact Resolver and Run Manifest SQL."""

    def register_source(
        self, context: RunContext, source: Mapping[str, Any]
    ) -> int:
        required = {
            "source_id", "provider", "api_name", "dataset_name",
            "business_key", "frequency", "supports_incremental",
            "supports_backfill", "available_time_rule", "rate_limit", "enabled",
        }
        if set(source) != required:
            raise ValueError(
                f"source registry fields mismatch: {sorted(set(source) ^ required)}"
            )
        row = dict(source)
        row["business_key"] = list(row["business_key"])
        row["rate_limit"] = json.dumps(row["rate_limit"], sort_keys=True)
        return self.kernel.write.upsert(
            context,
            WorkScope(partition=f"source:{row['source_id']}"),
            "ops.source_registry",
            [row],
            key=("source_id",),
            update_columns=tuple(sorted(required - {"source_id"})),
            skip_noop=True,
        )

    def register_feature_version(
        self, context: RunContext, feature: Mapping[str, Any]
    ) -> int:
        required = {
            "feature_name", "feature_version", "business_name", "definition",
            "grain", "source_datasets", "dependency_features", "lookback_rule",
            "asof_rule", "adjustment_rule", "impact_rule", "quality_rule",
            "code_version", "status",
        }
        if set(feature) != required:
            raise ValueError(
                f"feature registry fields mismatch: {sorted(set(feature) ^ required)}"
            )
        row = dict(feature)
        row["source_datasets"] = list(row["source_datasets"])
        row["dependency_features"] = list(row["dependency_features"])
        row["impact_rule"] = json.dumps(row["impact_rule"], sort_keys=True)
        row["quality_rule"] = json.dumps(row["quality_rule"], sort_keys=True)
        attempted, written = self.kernel.write.bulk_insert_ignore(
            context,
            WorkScope(partition=f"feature:{row['feature_name']}:{row['feature_version']}"),
            "feature.feature_registry",
            [row],
        )
        if attempted != 1:
            raise RuntimeError("feature registry insert invariant failed")
        return written

    def record_change_events(
        self,
        context: RunContext,
        scope: WorkScope,
        events: Sequence[Mapping[str, Any]],
    ) -> int:
        if not events:
            return 0
        required = {
            "event_id", "dataset", "business_key", "ts_code", "effective_date",
            "old_hash", "new_hash", "source_run_id", "change_type", "status",
        }
        rows = [dict(event) for event in events]
        if any(set(row) != required for row in rows):
            raise ValueError("change event fields mismatch")
        attempted, written = self.kernel.write.bulk_insert_ignore(
            context, scope, "ops.change_event", rows
        )
        if attempted != len(rows):
            raise RuntimeError("change event insert invariant failed")
        return written

    def pending_change_events(
        self,
        context: RunContext,
        scope: WorkScope,
        *,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if limit <= 0 or limit > 10_000:
            raise ValueError("change event limit must be in 1..10000")
        clauses = ["status='PENDING'"]
        params: list[Any] = []
        if scope.date_from is not None:
            clauses.append("effective_date >= %s")
            params.append(scope.date_from)
        if scope.date_to is not None:
            clauses.append("effective_date <= %s")
            params.append(scope.date_to)
        if scope.codes:
            clauses.append("ts_code = ANY(%s)")
            params.append(list(scope.codes))
        return self.kernel.read._repository_query(
            context,
            "ops.change_event",
            "SELECT event_id,dataset,business_key,ts_code,effective_date,old_hash,"
            "new_hash,source_run_id,detected_at,change_type,status "
            "FROM ops.change_event WHERE " + " AND ".join(clauses)
            + " ORDER BY detected_at,event_id LIMIT %s",
            (*params, limit),
            scope,
            limit,
        )

    def mark_change_events(
        self,
        context: RunContext,
        scope: WorkScope,
        event_ids: Sequence[str],
        status: str,
    ) -> int:
        if status not in {"RESOLVED", "APPLIED", "FAILED"}:
            raise ValueError("unsupported change event terminal status")
        if not event_ids:
            return 0
        return self.kernel.write.transition_change_events(
            context, scope, event_ids, status
        )

    def resolve_impacts(
        self,
        context: RunContext,
        *,
        dataset: str,
        ts_code: str | None,
        effective_date: date | None,
    ) -> list[dict[str, Any]]:
        """Resolve affected features from versioned Registry rules."""
        scope = WorkScope(
            date_from=effective_date,
            date_to=effective_date,
            codes=(ts_code,) if ts_code else None,
            partition=f"impact:{dataset}",
        )
        return self.kernel.read._repository_query(
            context,
            "feature.feature_registry",
            "SELECT feature_name,feature_version,dependency_features,lookback_rule,"
            "asof_rule,impact_rule,quality_rule,code_version "
            "FROM feature.feature_registry "
            "WHERE %s = ANY(source_datasets) AND status='ACTIVE' "
            "ORDER BY feature_name,feature_version",
            (dataset,),
            scope,
            10_000,
        )

    def save_run_manifest(
        self,
        context: RunContext,
        scope: WorkScope,
        values: Mapping[str, Any],
    ) -> int:
        """Create or finish the one unified foundation Run Manifest row."""
        allowed = {
            "input_manifest_hash", "output_manifest_hash", "start_time", "end_time",
            "rows_read", "rows_written", "rows_rejected", "status", "error",
        }
        if set(values) - allowed:
            raise ValueError(f"unsupported run manifest fields: {sorted(set(values) - allowed)}")
        if "status" not in values or "start_time" not in values:
            raise ValueError("run manifest requires status and start_time")
        scope_json = json.dumps(
            {
                "date_from": scope.date_from.isoformat() if scope.date_from else None,
                "date_to": scope.date_to.isoformat() if scope.date_to else None,
                "codes": list(scope.codes or ()),
                "partition": scope.partition,
                "dimensions": dict(scope.dimensions),
            },
            sort_keys=True,
            default=str,
        )
        row = {
            "run_id": context.run_id,
            "job_name": context.job_name,
            "pipeline_version": context.pipeline_version,
            "code_commit": context.code_version,
            "config_hash": context.config_hash,
            "scope": scope_json,
            "input_manifest_hash": values.get("input_manifest_hash"),
            "output_manifest_hash": values.get("output_manifest_hash"),
            "start_time": values["start_time"],
            "end_time": values.get("end_time"),
            "rows_read": int(values.get("rows_read", 0)),
            "rows_written": int(values.get("rows_written", 0)),
            "rows_rejected": int(values.get("rows_rejected", 0)),
            "status": values["status"],
            "error": values.get("error"),
        }
        return self.kernel.write.upsert(
            context,
            scope,
            "ops.run_manifest",
            [row],
            key=("run_id",),
            update_columns=tuple(column for column in row if column != "run_id"),
        )
