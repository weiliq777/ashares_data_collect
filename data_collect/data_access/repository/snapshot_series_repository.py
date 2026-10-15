"""Repository operations used by the multi-day snapshot orchestrator."""
from __future__ import annotations

from datetime import date
from typing import Iterable

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository
from data_collect.data_access.research_contract import is_foundation_schema
from data_collect.data_access.snapshot_lifecycle import (
    MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE,
    require_snapshot_batch,
    snapshot_id_batches,
)


class SnapshotSeriesRepository(BaseRepository):
    def series_inventory(
        self,
        baseline_tag: str,
        schema_version: str,
        start: date,
        end: date,
        context: RunContext,
        *,
        max_rows: int,
    ) -> list[dict]:
        """Read a bounded manifest inventory through the cutoff index.

        The historical series is deliberately read from the small manifest
        table.  Progress monitoring must never join the materialized stock or
        lineage tables while a builder is active.
        """
        if start > end:
            raise ValueError("start must not be after end")
        if max_rows < 1 or max_rows > 100_000:
            raise ValueError("max_rows must be between 1 and 100000")
        return self.kernel.read._repository_query(
            context,
            "research.snapshot_manifest",
            """SELECT snapshot_id, research_cutoff_date, status,
                       lineage_storage_version,
                       input_manifest_hash, quality_report_hash,
                       baseline_quality_report_hash,
                       baseline_source_manifest_hash,
                       code_version, git_commit, git_dirty, diff_hash,
                       code_archive_path,
                       created_at, built_at, validated_at, frozen_at,
                       failed_at, failure_reason
                  FROM research.snapshot_manifest
                 WHERE research_cutoff_date BETWEEN %s AND %s
                   AND data_baseline_tag=%s
                   AND schema_version=%s
                 ORDER BY research_cutoff_date, snapshot_id""",
            params=(start, end, baseline_tag, schema_version),
            scope=WorkScope(
                date_from=start,
                date_to=end,
                partition="snapshot:series-inventory",
            ),
            max_rows=max_rows,
        )

    def select_snapshots(
        self, baseline_tag: str, schema_version: str, start, end, context: RunContext,
    ) -> list[dict]:
        rows = self.kernel.read._repository_query(
            context, "research.snapshot_manifest",
            """SELECT snapshot_id, research_cutoff_date, status
                 FROM research.snapshot_manifest
                WHERE data_baseline_tag=%s AND schema_version=%s
                  AND (%s::date IS NULL OR research_cutoff_date >= %s::date)
                  AND (%s::date IS NULL OR research_cutoff_date <= %s::date)
                ORDER BY research_cutoff_date""",
            params=(baseline_tag, schema_version, start, start, end, end),
            scope=WorkScope(date_from=start, date_to=end, partition="snapshot:freeze-selection"),
            max_rows=100_000,
        )
        return rows

    def open_trade_dates(self, start: date, end: date, exchange: str, context: RunContext) -> tuple[date, ...]:
        rows = self.kernel.read._repository_query(
            context, "std.trading_calendar",
            """SELECT trade_date FROM std.trading_calendar
                WHERE exchange=%s AND is_open IS TRUE AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date""",
            params=(exchange, start, end),
            scope=WorkScope(date_from=start, date_to=end, partition=f"calendar:{exchange}"),
            max_rows=100_000,
        )
        return tuple(row["trade_date"] for row in rows)

    def financial_warmup_count(self, first_date: date, schema_version: str, context: RunContext) -> int:
        if is_foundation_schema(schema_version):
            source = "feature.company_financial_v2"
            sql = """SELECT COUNT(*) AS value FROM feature.company_financial_v2
                      WHERE data_available_timestamp < (%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'
                        AND report_date <= %s"""
            params = (first_date, first_date)
        else:
            source = "pit.financial_asof"
            sql = """SELECT COUNT(*) AS value FROM pit.financial_asof
                      WHERE decision_date=%s AND effective_announce_date IS NOT NULL
                        AND effective_announce_date <= decision_date"""
            params = (first_date,)
        return int(self.kernel.read._repository_query(
            context, source, sql, params=params,
            scope=WorkScope(date_to=first_date, partition="snapshot:financial-warmup"), max_rows=2,
        )[0]["value"] or 0)

    def existing_snapshots(self, dates: Iterable[date], baseline_tag: str, schema_version: str, context: RunContext) -> dict[date, tuple[str, str]]:
        unique_dates = tuple(dict.fromkeys(dates))
        result = {}
        for offset in range(0, len(unique_dates), MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE):
            batch = unique_dates[offset:offset + MAX_SNAPSHOT_LIFECYCLE_BATCH_SIZE]
            rows = self.kernel.read._repository_query(
                context, "research.snapshot_manifest",
                """SELECT research_cutoff_date, snapshot_id, status
                     FROM research.snapshot_manifest
                    WHERE data_baseline_tag=%s AND schema_version=%s
                      AND research_cutoff_date=ANY(%s::date[])""",
                params=(baseline_tag, schema_version, list(batch)),
                scope=WorkScope(
                    date_from=min(batch), date_to=max(batch),
                    partition="snapshot:series",
                ),
                max_rows=len(batch) + 1,
            )
            result.update({
                row["research_cutoff_date"]: (str(row["snapshot_id"]), row["status"])
                for row in rows
            })
        return result

    def incomplete_building_snapshots(
        self,
        snapshot_ids: Iterable[str],
        expected_input_count: int,
        context: RunContext,
    ) -> tuple[str, ...]:
        """Find resumable manifests whose build committed only partially.

        A process can stop after child writes commit but before the input
        manifest is persisted.  Such a row must be rebuilt, not sent directly
        to validation.  The check is scoped to the target ids and reads the
        small snapshot_input index rather than joining the large child tables.
        """
        batches = snapshot_id_batches(snapshot_ids)
        if not batches:
            return ()
        if expected_input_count <= 0:
            raise ValueError("expected_input_count must be positive")
        incomplete = []
        for ids in batches:
            rows = self.kernel.read._repository_query(
                context,
                "research.snapshot_manifest",
                """SELECT m.snapshot_id
                     FROM research.snapshot_manifest m
                     LEFT JOIN (
                        SELECT snapshot_id, COUNT(*) AS input_count
                          FROM research.snapshot_input
                         WHERE snapshot_id=ANY(%s::uuid[])
                         GROUP BY snapshot_id
                     ) i ON i.snapshot_id=m.snapshot_id
                    WHERE m.snapshot_id=ANY(%s::uuid[])
                      AND m.status='BUILDING'
                      AND COALESCE(i.input_count,0)<>%s""",
                (list(ids), list(ids), expected_input_count),
                WorkScope(partition="snapshot:incomplete-builds"),
                max_rows=len(ids) + 1,
            )
            incomplete.extend(str(row["snapshot_id"]) for row in rows)
        return tuple(incomplete)

    def reset_building(self, snapshot_ids: Iterable[str], reason: str, context: RunContext) -> int:
        ids = require_snapshot_batch(snapshot_ids, "snapshot reset")
        if not ids:
            return 0
        scope = WorkScope(partition="snapshot:series-reset", dimensions={"snapshot_ids": ids})
        with self.kernel.lock_manager.session_exclusive_many(
            "research.snapshot", ids, wait=True,
        ):
            return self.kernel.write.fail_building_snapshots(
                context=context, scope=scope, snapshot_ids=ids, reason=reason,
            )

    def reopen_validated_snapshots(
        self, snapshot_ids: Iterable[str], reason: str, context: RunContext,
    ) -> int:
        """Reopen explicitly selected, not-yet-frozen snapshots for repair."""
        ids = require_snapshot_batch(snapshot_ids, "snapshot reopen")
        if not ids:
            return 0
        if not reason.strip():
            raise ValueError("reopen reason must not be blank")
        scope = WorkScope(
            partition="snapshot:series-reopen",
            dimensions={"snapshot_ids": ids},
        )
        with self.kernel.lock_manager.session_exclusive_many(
            "research.snapshot", ids, wait=True,
        ):
            changed = self.kernel.write.transition_snapshot_manifests(
                context=context,
                scope=scope,
                rows=[
                    {
                        "snapshot_id": snapshot_id,
                        "failure_reason": f"PRE_FREEZE_REPAIR: {reason[:1980]}",
                    }
                    for snapshot_id in ids
                ],
                from_statuses=("VALIDATED",),
                to_status="FAILED",
            )
        if changed != len(ids):
            raise RuntimeError(
                f"reopen VALIDATED snapshot count mismatch: expected {len(ids)}, got {changed}"
            )
        return changed

    def prepare_lineage_v2_rebuild(
        self, snapshot_ids: Iterable[str], reason: str, context: RunContext,
    ) -> int:
        """Atomically switch a selected pre-freeze series to compact lineage.

        The named WriteGateway operation also reopens all v1 manifests in the
        same transaction, so a crash cannot leave a manifest claiming v2
        while its children still belong to the legacy representation.
        """
        ids = require_snapshot_batch(snapshot_ids, "lineage v2 rebuild preparation")
        if not ids:
            return 0
        if not reason.strip():
            raise ValueError("lineage v2 rebuild reason must not be blank")
        scope = WorkScope(
            partition="snapshot:lineage-v2-rebuild",
            dimensions={"snapshot_ids": ids},
        )
        with self.kernel.lock_manager.session_exclusive_many(
            "research.snapshot", ids, wait=True,
        ):
            return self.kernel.write.prepare_snapshot_lineage_v2_rebuild(
                context=context, scope=scope, snapshot_ids=ids, reason=reason,
            )
