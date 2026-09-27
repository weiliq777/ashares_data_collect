"""Repository adapter for Market Activity Observation V1.

This adapter reuses the canonical Observation tables.  It reads only a
FROZEN Market Activity snapshot and writes through the typed Observation
policies; callers never receive a database handle or a generic executor.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import json
from typing import Any, Iterator, Mapping, Sequence
from uuid import NAMESPACE_URL, uuid5

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository
from data_collect.observation.market_activity_rules import (
    MarketActivityObservationReport,
    MarketActivityObservationPolicy,
)


def _stable_uuid(value: str) -> str:
    return str(uuid5(NAMESPACE_URL, value))


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True, default=str)


def _scope(observation_date: date, *, snapshot_id: str | None = None) -> WorkScope:
    dimensions = {"domain": "market_activity_observation"}
    if snapshot_id:
        dimensions["snapshot_id"] = snapshot_id
    return WorkScope(
        date_from=observation_date,
        date_to=observation_date,
        partition="market-activity-observation:daily",
        dimensions=dimensions,
    )


class MarketActivityObservationRepository(BaseRepository):
    """Persist one explainable Observation date from frozen activity facts."""

    def get_activity_snapshot(self, activity_snapshot_id: str, context: RunContext) -> dict[str, Any]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.market_activity_snapshot",
            sql=(
                "SELECT * FROM observation.market_activity_snapshot "
                "WHERE activity_snapshot_id=%s"
            ),
            params=(activity_snapshot_id,),
            scope=WorkScope(partition=f"market-activity-observation:source:{activity_snapshot_id}"),
            max_rows=1,
        )
        if not rows:
            raise ValueError(f"unknown Market Activity snapshot: {activity_snapshot_id}")
        source = rows[0]
        if str(source.get("status") or "").upper() != "FROZEN":
            raise ValueError(
                "Observation V1 requires a FROZEN Market Activity snapshot; "
                f"source status is {source.get('status')!r}"
            )
        return source

    def get_snapshot(self, snapshot_id: str, context: RunContext) -> dict[str, Any]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.snapshot",
            sql="SELECT * FROM observation.snapshot WHERE snapshot_id=%s",
            params=(snapshot_id,),
            scope=WorkScope(partition=f"market-activity-observation:snapshot:{snapshot_id}"),
            max_rows=1,
        )
        if not rows:
            raise ValueError(f"unknown Observation snapshot: {snapshot_id}")
        return rows[0]

    def ensure_snapshot(
        self,
        source: Mapping[str, Any],
        policy: MarketActivityObservationPolicy,
        context: RunContext,
        *,
        calculation_run_id: str | None = None,
    ) -> dict[str, Any]:
        source_id = str(source["activity_snapshot_id"])
        observation_date = source["activity_date_start"]
        baseline_id = str(source["data_baseline_id"])
        requested_run_id = str(calculation_run_id or context.run_id).strip()
        snapshot_id = _stable_uuid(
            "market-activity-observation|"
            f"{source_id}|{observation_date}|{policy.methodology_hash}|{baseline_id}"
        )
        row = {
            "snapshot_id": snapshot_id,
            "observation_date_start": observation_date,
            "observation_date_end": source["activity_date_end"],
            "methodology_hash": policy.methodology_hash,
            "data_baseline_id": baseline_id,
            "observation_policy_id": policy.policy_id,
            "contract_hash": policy.contract_hash,
            "calculation_run_id": requested_run_id,
            "source_activity_snapshot_id": source_id,
            "status": "DRAFT",
            "code_identity": _json({
                "contract_version": "market-activity-observation-v1",
                "methodology_id": "MARKET_ACTIVITY_RULES",
                "policy_hash": policy.methodology_hash,
                "contract_hash": policy.contract_hash,
                "source_activity_snapshot_id": source_id,
                "source_quality_report_hash": source.get("quality_report_hash"),
                "calculation_run_id": requested_run_id,
                "code_version": context.code_version,
                "config_hash": context.config_hash,
                "pipeline_version": context.pipeline_version,
            }),
        }
        self.kernel.write.bulk_insert_ignore(
            context=context,
            scope=_scope(observation_date, snapshot_id=snapshot_id),
            target="observation.snapshot",
            rows=[row],
        )
        stored = self.get_snapshot(snapshot_id, context)
        for field, expected in (
            ("methodology_hash", policy.methodology_hash),
            ("data_baseline_id", baseline_id),
            ("contract_hash", policy.contract_hash),
            ("source_activity_snapshot_id", source_id),
        ):
            if str(stored.get(field) or "").strip() != str(expected):
                raise RuntimeError(
                    f"Observation snapshot identity mismatch for {field}: "
                    f"stored={stored.get(field)!r}, expected={expected!r}"
                )
        if str(stored.get("calculation_run_id") or "").strip() != requested_run_id:
            raise RuntimeError("Observation snapshot calculation_run_id conflict")
        return stored

    @contextmanager
    def snapshot_lock(self, snapshot_id: str) -> Iterator[None]:
        with self.kernel.lock_manager.session_exclusive(
            "observation.snapshot", snapshot_id
        ):
            yield

    def transition_snapshot(
        self,
        snapshot_id: str,
        observation_date: date,
        context: RunContext,
        from_statuses: Sequence[str],
        to_status: str,
        fields: Mapping[str, Any] | None = None,
    ) -> int:
        return self.kernel.write.transition_observation_snapshot(
            context=context,
            scope=_scope(observation_date, snapshot_id=snapshot_id),
            snapshot_id=snapshot_id,
            from_statuses=from_statuses,
            to_status=to_status,
            fields=fields,
        )

    def activity_rows(
        self,
        source_activity_snapshot_id: str,
        observation_date: date,
        context: RunContext,
        *,
        max_rows: int = 10_000,
    ) -> list[dict[str, Any]]:
        if not 1 <= max_rows <= 10_000:
            raise ValueError("activity observation max_rows must be in 1..10000")
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.stock_activity_daily",
            sql="""
                SELECT activity_snapshot_id, trade_date, ts_code,
                       methodology_hash, data_baseline_id, volume, amount,
                       amount_share, volume_share, amount_rank, volume_rank,
                       amount_ratio_previous, volume_ratio_previous,
                       history_observation_count, calculation_status,
                       quality_status, input_manifest_hash, activity_hash
                  FROM observation.stock_activity_daily
                 WHERE activity_snapshot_id=%s
                   AND trade_date=%s
                 ORDER BY ts_code
                 LIMIT %s
            """,
            params=(source_activity_snapshot_id, observation_date, max_rows + 1),
            scope=_scope(observation_date, snapshot_id=source_activity_snapshot_id),
            max_rows=max_rows + 1,
        )
        if len(rows) > max_rows:
            raise ValueError(
                f"Market Activity snapshot exceeds governed stock limit {max_rows}"
            )
        if not rows:
            raise ValueError("frozen Market Activity snapshot has no stock rows")
        return rows

    def write_rows(
        self,
        report: MarketActivityObservationReport,
        snapshot_id: str,
        context: RunContext,
    ) -> tuple[int, int]:
        scope = _scope(report.observation_date, snapshot_id=snapshot_id)
        stock_rows: list[dict[str, Any]] = []
        tag_rows: list[dict[str, Any]] = []
        for row in report.rows:
            stock_rows.append({
                "observation_id": row.observation_id,
                "snapshot_id": snapshot_id,
                "observation_date": row.observation_date,
                "ts_code": row.ts_code,
                "methodology_hash": row.methodology_hash,
                "data_baseline_id": row.data_baseline_id,
                "source_activity_snapshot_id": row.source_activity_snapshot_id,
                "activity_volume_ratio_previous": row.activity_volume_ratio_previous,
                "activity_amount_ratio_previous": row.activity_amount_ratio_previous,
                "activity_amount_share": row.activity_amount_share,
                "activity_volume_share": row.activity_volume_share,
                "activity_volume_rank": row.activity_volume_rank,
                "activity_amount_rank": row.activity_amount_rank,
                "activity_history_observation_count": row.activity_history_observation_count,
                "anomaly_flag": row.anomaly_flag,
                "calculation_status": row.calculation_status,
                "quality_status": "VALID" if row.quality_status == "VALID" else "WARNING",
                "state_reason_codes": _json([]),
                "missing_reason_codes": _json(
                    [] if row.calculation_status == "VALID" else ["INSUFFICIENT_ACTIVITY_HISTORY"]
                ),
                "input_row_count": report.input_row_count,
                "input_manifest_hash": report.input_manifest_hash,
                "observation_hash": row.observation_hash,
                "calculation_run_id": context.run_id,
            })
            for tag in row.tags:
                tag_rows.append({
                    "tag_id": _stable_uuid(f"activity-tag|{row.observation_id}|{tag['tag_code']}"),
                    "observation_id": row.observation_id,
                    "tag_code": tag["tag_code"],
                    "tag_payload": _json(tag),
                })
        written_stocks = self.kernel.write.upsert(
            context=context,
            scope=scope,
            target="observation.stock_anomaly_daily",
            rows=stock_rows,
            key=("observation_id",),
            update_columns=tuple(column for column in stock_rows[0] if column != "observation_id"),
            skip_noop=True,
        ) if stock_rows else 0
        written_tags = self.kernel.write.upsert(
            context=context,
            scope=scope,
            target="observation.stock_anomaly_tag",
            rows=tag_rows,
            key=("observation_id", "tag_code"),
            update_columns=("tag_payload",),
            skip_noop=True,
        ) if tag_rows else 0
        return written_stocks, written_tags

    def validate_snapshot(
        self,
        report: MarketActivityObservationReport,
        snapshot_id: str,
        context: RunContext,
    ) -> dict[str, Any]:
        scope = _scope(report.observation_date, snapshot_id=snapshot_id)
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.stock_anomaly_daily",
            sql="""
                SELECT COUNT(*) AS row_count,
                       COUNT(DISTINCT ts_code) AS stock_count,
                       COUNT(DISTINCT input_manifest_hash) AS manifest_count,
                       COUNT(DISTINCT source_activity_snapshot_id) AS source_count,
                       MIN(input_manifest_hash) AS input_manifest_hash,
                       MIN(source_activity_snapshot_id::text) AS source_activity_snapshot_id,
                       COUNT(*) FILTER (WHERE calculation_status='VALID') AS valid_count,
                       COUNT(*) FILTER (WHERE calculation_status='INSUFFICIENT_HISTORY') AS insufficient_count,
                       COUNT(*) FILTER (WHERE calculation_status='INVALID') AS invalid_count,
                       COUNT(*) FILTER (WHERE anomaly_flag IS TRUE) AS anomaly_count
                  FROM observation.stock_anomaly_daily
                 WHERE snapshot_id=%s AND observation_date=%s
            """,
            params=(snapshot_id, report.observation_date),
            scope=scope,
            max_rows=1,
        )[0]
        tag_rows = self.kernel.read._repository_query(
            context=context,
            source="observation.stock_anomaly_tag",
            sql="""
                SELECT COUNT(*) AS tag_count,
                       COUNT(DISTINCT tag_code) AS tag_type_count
                  FROM observation.stock_anomaly_tag t
                  JOIN observation.stock_anomaly_daily d
                    ON d.observation_id=t.observation_id
                 WHERE d.snapshot_id=%s AND d.observation_date=%s
            """,
            params=(snapshot_id, report.observation_date),
            scope=scope,
            max_rows=1,
        )[0]
        result = {
            "row_count": int(rows.get("row_count") or 0),
            "stock_count": int(rows.get("stock_count") or 0),
            "manifest_count": int(rows.get("manifest_count") or 0),
            "source_count": int(rows.get("source_count") or 0),
            "input_manifest_hash": str(rows.get("input_manifest_hash") or ""),
            "source_activity_snapshot_id": str(rows.get("source_activity_snapshot_id") or ""),
            "valid_count": int(rows.get("valid_count") or 0),
            "insufficient_count": int(rows.get("insufficient_count") or 0),
            "invalid_count": int(rows.get("invalid_count") or 0),
            "anomaly_count": int(rows.get("anomaly_count") or 0),
            "tag_count": int(tag_rows.get("tag_count") or 0),
            "tag_type_count": int(tag_rows.get("tag_type_count") or 0),
        }
        expected = {
            "row_count": report.stock_count,
            "stock_count": report.stock_count,
            "manifest_count": 1,
            "source_count": 1,
            "input_manifest_hash": report.input_manifest_hash,
            "source_activity_snapshot_id": report.source_activity_snapshot_id,
            "valid_count": report.valid_stock_count,
            "insufficient_count": report.insufficient_history_stock_count,
            "invalid_count": report.invalid_stock_count,
            "tag_count": report.tag_count,
        }
        for name, value in expected.items():
            if result[name] != value:
                raise RuntimeError(
                    f"Market Activity Observation validation mismatch {name}: "
                    f"stored={result[name]}, expected={value}"
                )
        return result


__all__ = ["MarketActivityObservationRepository"]
