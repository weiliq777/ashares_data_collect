"""Controlled lifecycle operations for retention archive candidates."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Sequence
from uuid import UUID

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class RetentionArchiveRepository(BaseRepository):
    """Manage archive metadata without exposing physical cleanup operations.

    This repository can list, approve, reject, or record restoration of a
    registry entry.  It intentionally has no method that drops or deletes a
    source relation; physical archival is a separate, explicitly reviewed
    operation.
    """

    TARGET = "ops.retention_archive_registry"
    MAX_IDS_PER_OPERATION = 1000
    ARCHIVE_STATES = frozenset(
        {"CANDIDATE", "APPROVED", "ARCHIVED", "RESTORED", "REJECTED"}
    )

    @staticmethod
    def _scope(partition: str) -> WorkScope:
        return WorkScope(partition=f"retention-registry:{partition}")

    @classmethod
    def _ids(cls, registry_ids: Sequence[str]) -> tuple[str, ...]:
        ids = tuple(dict.fromkeys(str(value) for value in registry_ids))
        if not ids:
            raise ValueError("at least one registry_id is required")
        if len(ids) > cls.MAX_IDS_PER_OPERATION:
            raise ValueError(
                f"at most {cls.MAX_IDS_PER_OPERATION} registry IDs may be transitioned"
            )
        for value in ids:
            UUID(value)
        return ids

    def list_candidates(
        self,
        context: RunContext,
        *,
        object_type: str | None = None,
        limit: int = 1000,
        archive_state: str = "CANDIDATE",
    ) -> list[dict[str, Any]]:
        if limit <= 0 or limit > self.MAX_IDS_PER_OPERATION:
            raise ValueError(f"limit must be between 1 and {self.MAX_IDS_PER_OPERATION}")
        if archive_state not in self.ARCHIVE_STATES:
            raise ValueError(f"unsupported archive_state: {archive_state}")
        clauses = ["archive_state=%s"]
        params: list[Any] = []
        params.append(archive_state)
        if object_type is not None:
            if object_type not in {"SNAPSHOT", "DATASET", "ROLLBACK_BACKUP", "COMPATIBILITY_TABLE"}:
                raise ValueError(f"unsupported object_type: {object_type}")
            clauses.append("object_type=%s")
            params.append(object_type)
        params.append(limit)
        rows = self.kernel.read._repository_query(
            context,
            self.TARGET,
            f"""SELECT registry_id, object_type, object_name, object_key,
                              source_status, archive_state, candidate_reason,
                              reference_summary, source_state, audit_fingerprint,
                              candidate_since, retention_until, approved_by,
                              approved_at, archived_at, restored_at, notes
                         FROM {self.TARGET}
                        WHERE {' AND '.join(clauses)}
                        ORDER BY candidate_since, object_type, object_key
                        LIMIT %s""",
            tuple(params),
            self._scope("list"),
            max_rows=limit + 1,
        )
        return [dict(row) for row in rows]

    def list_archive_plan(
        self,
        context: RunContext,
        *,
        limit: int = 1000,
        registry_ids: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return a fresh, read-only preflight for approved objects.

        This is deliberately a plan, not an executor.  Physical archival of
        Dataset/Snapshot facts has different dependency and recovery rules and
        must remain a separately reviewed operation.
        """
        if limit <= 0 or limit > self.MAX_IDS_PER_OPERATION:
            raise ValueError(f"limit must be between 1 and {self.MAX_IDS_PER_OPERATION}")
        ids = self._ids(registry_ids) if registry_ids is not None else ()
        query_params: list[Any] = [list(ids)] if ids else []
        query_params.append(limit)
        rows = self.kernel.read._repository_query(
            context,
            self.TARGET,
            f"""SELECT r.registry_id, r.object_type, r.object_name,
                              r.object_key, r.source_status, r.archive_state,
                              r.candidate_reason, r.reference_summary,
                              r.source_state, r.audit_fingerprint,
                              r.candidate_since, r.retention_until,
                              r.approved_by, r.approved_at, r.archived_at,
                              r.restored_at, r.notes,
                              CASE
                                WHEN r.object_type='DATASET' THEN
                                  jsonb_build_object(
                                    'run_manifest', (SELECT COUNT(*) FROM backtest.run_manifest x
                                      WHERE x.dataset_id=r.object_key::uuid),
                                    'dataset_member', (SELECT COUNT(*) FROM backtest.dataset_member x
                                      WHERE x.dataset_id=r.object_key::uuid),
                                    'dataset_benchmark_bar', (SELECT COUNT(*) FROM backtest.dataset_benchmark_bar x
                                      WHERE x.dataset_id=r.object_key::uuid),
                                    'dataset_build_batch_checkpoint', (SELECT COUNT(*) FROM backtest.dataset_build_batch_checkpoint x
                                      WHERE x.dataset_id=r.object_key::uuid),
                                    'dataset_build_checkpoint', (SELECT COUNT(*) FROM backtest.dataset_build_checkpoint x
                                      WHERE x.dataset_id=r.object_key::uuid),
                                    'dataset_calendar', (SELECT COUNT(*) FROM backtest.dataset_calendar x
                                      WHERE x.dataset_id=r.object_key::uuid),
                                    'dataset_dividend_event', (SELECT COUNT(*) FROM backtest.dataset_dividend_event x
                                      WHERE x.dataset_id=r.object_key::uuid),
                                    'dataset_execution_bar', (SELECT COUNT(*) FROM backtest.dataset_execution_bar x
                                      WHERE x.dataset_id=r.object_key::uuid),
                                    'dataset_rule_snapshot', (SELECT COUNT(*) FROM backtest.dataset_rule_snapshot x
                                      WHERE x.dataset_id=r.object_key::uuid),
                                    'portfolio_order_intent', (SELECT COUNT(*) FROM portfolio.order_intent x
                                      WHERE x.source_dataset_id=r.object_key::uuid)
                                  )
                                ELSE '{{}}'::jsonb
                              END AS dependency_breakdown,
                              CASE
                                WHEN r.object_type='SNAPSHOT' THEN
                                  (SELECT COUNT(*) FROM backtest.dataset_member x
                                    WHERE x.research_snapshot_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM backtest.signal x
                                    WHERE x.research_snapshot_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM research.watchlist_member x
                                    WHERE x.snapshot_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM research.evidence_reference x
                                    WHERE x.snapshot_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM research.candidate x
                                    WHERE x.snapshot_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM research.candidate_run x
                                    WHERE x.snapshot_id=r.object_key::uuid)
                                WHEN r.object_type='DATASET' THEN
                                  (SELECT COUNT(*) FROM backtest.run_manifest x
                                    WHERE x.dataset_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM backtest.dataset_member x
                                    WHERE x.dataset_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM backtest.dataset_benchmark_bar x
                                    WHERE x.dataset_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM backtest.dataset_build_batch_checkpoint x
                                    WHERE x.dataset_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM backtest.dataset_build_checkpoint x
                                    WHERE x.dataset_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM backtest.dataset_calendar x
                                    WHERE x.dataset_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM backtest.dataset_dividend_event x
                                    WHERE x.dataset_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM backtest.dataset_execution_bar x
                                    WHERE x.dataset_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM backtest.dataset_rule_snapshot x
                                    WHERE x.dataset_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM portfolio.order_intent x
                                    WHERE x.source_dataset_id=r.object_key::uuid)
                                ELSE 0
                              END AS current_reference_count
                         FROM {self.TARGET} r
                        WHERE r.archive_state='APPROVED'
                          {"AND r.registry_id=ANY(%s::uuid[])" if ids else ""}
                        ORDER BY r.approved_at, r.object_type, r.object_key
                        LIMIT %s""",
            tuple(query_params),
            self._scope("archive-plan"),
            max_rows=limit + 1,
        )
        return [dict(row) for row in rows]

    def _read_for_transition(
        self, context: RunContext, ids: tuple[str, ...], action: str,
    ) -> list[dict[str, Any]]:
        rows = self.kernel.read._repository_query(
            context,
            self.TARGET,
            f"""SELECT r.registry_id, r.object_type, r.object_name,
                              r.object_key, r.source_status, r.archive_state,
                              r.candidate_reason, r.reference_summary,
                              r.source_state, r.audit_fingerprint,
                              r.candidate_since, r.retention_until,
                              r.approved_by, r.approved_at, r.archived_at,
                              r.restored_at, r.notes,
                              CASE
                                WHEN r.object_type='SNAPSHOT' THEN
                                  (SELECT COUNT(*) FROM backtest.dataset_member x
                                    WHERE x.research_snapshot_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM backtest.signal x
                                    WHERE x.research_snapshot_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM research.watchlist_member x
                                    WHERE x.snapshot_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM research.evidence_reference x
                                    WHERE x.snapshot_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM research.candidate x
                                    WHERE x.snapshot_id=r.object_key::uuid)
                                  + (SELECT COUNT(*) FROM research.candidate_run x
                                    WHERE x.snapshot_id=r.object_key::uuid)
                                WHEN r.object_type='DATASET' THEN
                                  (SELECT COUNT(*) FROM backtest.run_manifest x
                                    WHERE x.dataset_id=r.object_key::uuid)
                                ELSE 0
                              END AS current_reference_count
                         FROM {self.TARGET} r
                        WHERE r.registry_id=ANY(%s::uuid[])""",
            (list(ids),),
            self._scope(f"{action}:read"),
            max_rows=len(ids) + 1,
        )
        if len(rows) != len(ids):
            found = {str(row["registry_id"]) for row in rows}
            missing = [value for value in ids if value not in found]
            raise ValueError(f"unknown registry IDs: {missing}")
        return [dict(row) for row in rows]

    def transition(
        self,
        context: RunContext,
        *,
        action: str,
        registry_ids: Sequence[str],
        actor: str,
        reason: str,
    ) -> int:
        if action not in {"approve", "reject", "restore"}:
            raise ValueError("action must be approve, reject, or restore")
        actor = str(actor).strip()
        reason = str(reason).strip()
        if not actor or not reason:
            raise ValueError("actor and reason are required")
        ids = self._ids(registry_ids)
        rows = self._read_for_transition(context, ids, action)

        expected = {
            "approve": {"CANDIDATE"},
            "reject": {"CANDIDATE"},
            "restore": {"ARCHIVED"},
        }[action]
        invalid = [
            (str(row["registry_id"]), row["archive_state"])
            for row in rows if row["archive_state"] not in expected
        ]
        if invalid:
            raise ValueError(
                f"invalid archive state for {action}: {invalid}"
            )
        if action in {"approve", "restore"}:
            referenced = [
                str(row["registry_id"])
                for row in rows if int(row["current_reference_count"] or 0) > 0
            ]
            if referenced:
                raise RuntimeError(
                    "archive candidate gained references; refusing transition: "
                    f"{referenced}"
                )

        now = datetime.now(timezone.utc)
        target_state = {
            "approve": "APPROVED",
            "reject": "REJECTED",
            "restore": "RESTORED",
        }[action]
        update_columns = ["archive_state", "notes"]
        base_columns = (
            "registry_id", "object_type", "object_name", "object_key",
            "source_status", "candidate_reason", "reference_summary",
            "source_state", "audit_fingerprint", "candidate_since",
            "retention_until", "approved_by", "approved_at", "archived_at",
            "restored_at", "notes",
        )
        rows_to_write: list[dict[str, Any]] = []
        for row in rows:
            item: dict[str, Any] = {key: row[key] for key in base_columns}
            # The generic bulk writer deliberately accepts scalar values only.
            # Encode JSONB fields at this Repository boundary instead of
            # teaching the Kernel/Gateway about driver-specific adapters.
            for jsonb_column in ("reference_summary", "source_state"):
                value = item.get(jsonb_column)
                if isinstance(value, (dict, list, tuple)):
                    item[jsonb_column] = json.dumps(
                        value, ensure_ascii=False, sort_keys=True, default=str,
                    )
            item.update({
                "archive_state": target_state,
                "notes": reason,
            })
            if action in {"approve", "reject"}:
                item.update({"approved_by": actor, "approved_at": now})
                update_columns.extend(("approved_by", "approved_at"))
            else:
                item.update({"restored_at": now})
                update_columns.append("restored_at")
            rows_to_write.append(item)
        return self.kernel.write.upsert(
            context,
            self._scope(f"{action}:write"),
            self.TARGET,
            rows_to_write,
            key=("registry_id",),
            update_columns=tuple(dict.fromkeys(update_columns)),
            skip_noop=False,
        )
