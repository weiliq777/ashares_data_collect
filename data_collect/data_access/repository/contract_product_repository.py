"""Foundation-owned immutable Snapshot and Dataset product contracts."""
from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class ContractProductRepository(BaseRepository):
    def frozen_snapshot(
        self, context: RunContext, snapshot_id: str, research_date: date
    ) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context,
            "contract.snapshot_manifest",
            "SELECT snapshot_id,research_date,data_baseline,contract_version,"
            "feature_version,pit_version,code_version,input_manifest_hash,"
            "quality_report_hash,manifest_hash,status,frozen_at "
            "FROM contract.snapshot_manifest "
            "WHERE snapshot_id=%s AND research_date=%s AND status='FROZEN'",
            (snapshot_id, research_date),
            WorkScope(
                date_from=research_date,
                date_to=research_date,
                partition=snapshot_id,
                dimensions={"snapshot_id": snapshot_id},
            ),
            2,
        )
        return rows[0] if rows else None

    def frozen_snapshot_rows(
        self,
        context: RunContext,
        snapshot_id: str,
        research_date: date,
        codes: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        scope = WorkScope(
            date_from=research_date,
            date_to=research_date,
            codes=tuple(codes) if codes else None,
            partition=snapshot_id,
            dimensions={"snapshot_id": snapshot_id},
        )
        params: list[Any] = [snapshot_id, research_date]
        code_filter = ""
        if codes:
            code_filter = " AND s.ts_code=ANY(%s)"
            params.append(list(codes))
        return self.kernel.read._repository_query(
            context,
            "contract.stock_snapshot_daily",
            "SELECT s.* FROM contract.stock_snapshot_daily s "
            "JOIN contract.snapshot_manifest m ON m.snapshot_id=s.snapshot_id "
            "WHERE s.snapshot_id=%s AND s.research_date=%s "
            "AND m.status='FROZEN'" + code_filter + " ORDER BY s.ts_code",
            params,
            scope,
            100_000,
        )

    def frozen_dataset_release(
        self, context: RunContext, dataset_id: str
    ) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context,
            "contract.dataset_release",
            "SELECT dataset_id,start_date,end_date,trading_dates,snapshot_ids,"
            "feature_version,pit_version,data_baseline,contract_version,"
            "manifest_hash,quality_report_hash,status,frozen_at "
            "FROM contract.dataset_release WHERE dataset_id=%s AND status='FROZEN'",
            (dataset_id,),
            WorkScope(partition=dataset_id),
            2,
        )
        return rows[0] if rows else None

    def create_snapshot(
        self, context: RunContext, values: Mapping[str, Any]
    ) -> int:
        required = {
            "snapshot_id", "research_date", "data_baseline", "contract_version",
            "feature_version", "pit_version", "code_version", "status",
        }
        if set(values) != required or values["status"] != "DRAFT":
            raise ValueError("snapshot manifest must be a complete DRAFT")
        day = values["research_date"]
        return self.kernel.write.upsert(
            context,
            WorkScope(date_from=day, date_to=day, partition=str(values["snapshot_id"])),
            "contract.snapshot_manifest",
            [dict(values)],
            key=("snapshot_id",),
            update_columns=(),
        )

    def publish_snapshot_rows(
        self,
        context: RunContext,
        snapshot_id: str,
        research_date: date,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        normalized = [
            {**dict(row), "snapshot_id": snapshot_id, "research_date": research_date}
            for row in rows
        ]
        return self.kernel.publish.snapshot(
            context,
            WorkScope(
                date_from=research_date,
                date_to=research_date,
                partition=snapshot_id,
                dimensions={"snapshot_id": snapshot_id},
            ),
            "contract.stock_snapshot_daily",
            normalized,
        )

    def transition_snapshot(
        self,
        context: RunContext,
        snapshot_id: str,
        research_date: date,
        from_statuses: Sequence[str],
        to_status: str,
        fields: Mapping[str, Any] | None = None,
    ) -> int:
        scope = WorkScope(
            date_from=research_date,
            date_to=research_date,
            partition=snapshot_id,
            dimensions={"snapshot_id": snapshot_id},
        )
        return self.kernel.write.transition_contract_snapshot(
            context, scope, snapshot_id, from_statuses, to_status, fields or {}
        )

    def create_dataset_release(
        self, context: RunContext, values: Mapping[str, Any]
    ) -> int:
        required = {
            "dataset_id", "start_date", "end_date", "trading_dates", "snapshot_ids",
            "feature_version", "pit_version", "data_baseline", "contract_version", "status",
        }
        if set(values) != required or values["status"] != "DRAFT":
            raise ValueError("dataset release must be a complete DRAFT")
        scope = WorkScope(
            date_from=values["start_date"],
            date_to=values["end_date"],
            partition=str(values["dataset_id"]),
        )
        return self.kernel.write.upsert(
            context, scope, "contract.dataset_release", [dict(values)],
            key=("dataset_id",), update_columns=(),
        )

    def transition_dataset_release(
        self,
        context: RunContext,
        dataset_id: str,
        scope: WorkScope,
        from_statuses: Sequence[str],
        to_status: str,
        fields: Mapping[str, Any] | None = None,
    ) -> int:
        return self.kernel.write.transition_contract_dataset(
            context, scope, dataset_id, from_statuses, to_status, fields or {}
        )
