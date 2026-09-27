"""Persistence seam for the Market Activity Observation domain.

The repository owns all SQL for the three-table activity model.  Jobs and
domain services receive typed reports and never receive a Connection/Cursor or
a generic SQL executor.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
import json
import math
from typing import Any, Iterator, Mapping, Sequence
from uuid import NAMESPACE_URL, uuid5

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.market_activity_repository import (
    MarketActivityRepository,
)
from data_collect.market_activity.contracts import (
    MARKET_ACTIVITY_CONTRACT_HASH,
    MARKET_ACTIVITY_CONTRACT_VERSION,
    MarketActivityReport,
    MarketActivityRequest,
)


_SUPPORTED_BASELINE_SCHEMAS = frozenset({
    "feature-contract-v2.0",
    "market-activity-v1",
})


def _stable_uuid(identity: str) -> str:
    return str(uuid5(NAMESPACE_URL, identity))


def _json(value: Any) -> str:
    return json.dumps(
        value if value is not None else {},
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )


def _scope(start: date, end: date, activity_snapshot_id: str | None = None) -> WorkScope:
    dimensions: dict[str, Any] = {"domain": "market_activity"}
    if activity_snapshot_id:
        dimensions["activity_snapshot_id"] = activity_snapshot_id
    return WorkScope(
        date_from=start,
        date_to=end,
        partition="market-activity:daily",
        dimensions=dimensions,
    )


def enforce_baseline_cutoff(
    trade_date: date,
    baseline_id: str,
    baseline: Mapping[str, Any],
) -> None:
    """Reject activity dates that are outside the selected data release.

    A validated release is a version boundary, not a label attached after the
    fact.  Failing here prevents a newer Standard row from being silently
    persisted under an older baseline.
    """
    cutoff = baseline.get("baseline_cutoff_date")
    if cutoff is None:
        raise ValueError(
            f"validated data baseline has no cutoff date: {baseline_id}"
        )
    if isinstance(cutoff, datetime):
        cutoff = cutoff.date()
    elif not isinstance(cutoff, date):
        try:
            cutoff = date.fromisoformat(str(cutoff))
        except ValueError as exc:
            raise ValueError(
                f"invalid cutoff date for data baseline {baseline_id}: {cutoff!r}"
            ) from exc
    if trade_date > cutoff:
        raise ValueError(
            "market activity date is after the selected data baseline cutoff: "
            f"trade_date={trade_date.isoformat()}, "
            f"baseline={baseline_id}, cutoff={cutoff.isoformat()}"
        )


class MarketActivityStorageRepository(MarketActivityRepository):
    """Read source bars and persist immutable, versioned activity results."""

    def get_snapshot(self, activity_snapshot_id: str, context: RunContext) -> dict[str, Any]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="observation.market_activity_snapshot",
            sql=(
                "SELECT * FROM observation.market_activity_snapshot "
                "WHERE activity_snapshot_id=%s"
            ),
            params=(activity_snapshot_id,),
            scope=WorkScope(
                partition=f"market-activity:snapshot:{activity_snapshot_id}",
                dimensions={"domain": "market_activity"},
            ),
            max_rows=1,
        )
        if not rows:
            raise ValueError(f"unknown market activity snapshot: {activity_snapshot_id}")
        return rows[0]

    def validated_baseline(self, baseline_id: str, context: RunContext) -> dict[str, Any]:
        """Require the explicit data baseline to be currently VALIDATED."""
        baseline_id = str(baseline_id).strip()
        if not baseline_id:
            raise ValueError("market activity data baseline id must not be blank")
        rows = self.kernel.read._repository_query(
            context=context,
            source="ops.tushare_data_release",
            sql="""
                SELECT release_tag, baseline_cutoff_date, status,
                       quality_report_hash, schema_version
                  FROM ops.tushare_data_release
                 WHERE release_tag=%s AND baseline_domain='MARKET_ACTIVITY'
                   AND lower(status)=lower('validated')
                 LIMIT 1
            """,
            params=(baseline_id,),
            scope=WorkScope(partition=f"market-activity:baseline:{baseline_id}"),
            max_rows=1,
        )
        if not rows:
            raise ValueError(f"validated data baseline not found: {baseline_id}")
        baseline = rows[0]
        schema_version = str(baseline.get("schema_version") or "").strip()
        if schema_version not in _SUPPORTED_BASELINE_SCHEMAS:
            raise ValueError(
                "unsupported data baseline schema for Market Activity: "
                f"baseline={baseline_id}, schema_version={schema_version!r}"
            )
        return baseline

    def ensure_snapshot(
        self,
        request: MarketActivityRequest,
        context: RunContext,
        *,
        calculation_run_id: str | None = None,
    ) -> dict[str, Any]:
        methodology_hash, baseline_id = request.require_persistence_identity()
        baseline = self.validated_baseline(baseline_id, context)
        enforce_baseline_cutoff(request.trade_date, baseline_id, baseline)
        snapshot_identity = (
            "market-activity-snapshot|"
            f"{request.trade_date.isoformat()}|{request.trade_date.isoformat()}|"
            f"{methodology_hash}|{baseline_id}"
        )
        snapshot_id = _stable_uuid(snapshot_identity)
        requested_run_id = str(calculation_run_id or context.run_id).strip()
        row = {
            "activity_snapshot_id": snapshot_id,
            "activity_date_start": request.trade_date,
            "activity_date_end": request.trade_date,
            "methodology_hash": methodology_hash,
            "data_baseline_id": baseline_id,
            "contract_hash": MARKET_ACTIVITY_CONTRACT_HASH,
            "calculation_run_id": requested_run_id,
            "status": "DRAFT",
            "code_identity": _json({
                "contract_version": MARKET_ACTIVITY_CONTRACT_VERSION,
                "contract_hash": MARKET_ACTIVITY_CONTRACT_HASH,
                "methodology_hash": methodology_hash,
                "calculation_run_id": requested_run_id,
                "code_version": context.code_version,
                "config_hash": context.config_hash,
                "pipeline_version": context.pipeline_version,
            }),
        }
        self.kernel.write.bulk_insert_ignore(
            context=context,
            scope=_scope(request.trade_date, request.trade_date, snapshot_id),
            target="observation.market_activity_snapshot",
            rows=[row],
        )
        snapshot = self.get_snapshot(snapshot_id, context)
        for field, expected in (
            ("methodology_hash", methodology_hash),
            ("data_baseline_id", baseline_id),
            ("contract_hash", MARKET_ACTIVITY_CONTRACT_HASH),
        ):
            if str(snapshot.get(field) or "").strip() != expected:
                raise RuntimeError(
                    f"market activity snapshot identity mismatch for {field}: "
                    f"stored={snapshot.get(field)!r}, expected={expected!r}"
                )
        stored_run_id = str(snapshot.get("calculation_run_id") or "").strip()
        if stored_run_id != requested_run_id:
            raise RuntimeError(
                "market activity snapshot calculation_run_id conflict; "
                f"stored={stored_run_id}, requested={requested_run_id}"
            )
        return snapshot

    @contextmanager
    def snapshot_lock(self, activity_snapshot_id: str) -> Iterator[None]:
        """Hold one session lock for the short daily build workflow."""
        with self.kernel.lock_manager.session_exclusive(
            "observation.market_activity_snapshot", activity_snapshot_id
        ):
            yield

    def transition_snapshot(
        self,
        activity_snapshot_id: str,
        context: RunContext,
        request: MarketActivityRequest,
        from_statuses: Sequence[str],
        to_status: str,
        fields: Mapping[str, Any] | None = None,
    ) -> int:
        return self.kernel.write.transition_market_activity_snapshot(
            context=context,
            scope=_scope(request.trade_date, request.trade_date, activity_snapshot_id),
            activity_snapshot_id=activity_snapshot_id,
            from_statuses=from_statuses,
            to_status=to_status,
            fields=fields,
        )

    def write_daily_summary(
        self,
        report: MarketActivityReport,
        activity_snapshot_id: str,
        context: RunContext,
    ) -> int:
        self._validate_report_identity(report)
        row = {
            "activity_daily_id": _stable_uuid(
                f"market-activity-daily|{activity_snapshot_id}|{report.trade_date}"
            ),
            "activity_snapshot_id": activity_snapshot_id,
            "trade_date": report.trade_date,
            "methodology_hash": report.methodology_hash,
            "data_baseline_id": report.data_baseline_id,
            "top_n": report.top_n,
            "total_volume": report.total_volume,
            "total_amount": report.total_amount,
            "top_n_volume": report.top_n_volume,
            "top_n_amount": report.top_n_amount,
            "top_n_volume_share": report.top_n_volume_share,
            "top_n_amount_share": report.top_n_amount_share,
            "input_row_count": report.input_row_count,
            "target_row_count": report.target_row_count,
            "invalid_target_row_count": report.invalid_target_row_count,
            "invalid_history_row_count": report.invalid_history_row_count,
            "insufficient_history_stock_count": report.insufficient_history_stock_count,
            "quality_status": report.quality_status,
            "input_manifest_hash": report.input_manifest_hash,
            "activity_hash": report.activity_hash,
            "calculation_run_id": context.run_id,
        }
        columns = tuple(row)
        return self.kernel.write.upsert(
            context=context,
            scope=_scope(report.trade_date, report.trade_date, activity_snapshot_id),
            target="observation.market_activity_daily",
            rows=[row],
            key=("trade_date", "methodology_hash", "data_baseline_id"),
            update_columns=tuple(column for column in columns if column != "activity_daily_id"),
            skip_noop=True,
        )

    def write_stock_rows(
        self,
        report: MarketActivityReport,
        activity_snapshot_id: str,
        context: RunContext,
    ) -> int:
        self._validate_report_identity(report)
        if not report.rows:
            return 0
        rows = []
        for activity in report.rows:
            rows.append({
                "stock_activity_id": _stable_uuid(
                    f"market-activity-stock|{activity_snapshot_id}|"
                    f"{activity.trade_date}|{activity.ts_code}"
                ),
                "activity_snapshot_id": activity_snapshot_id,
                "trade_date": activity.trade_date,
                "ts_code": activity.ts_code,
                "methodology_hash": report.methodology_hash,
                "data_baseline_id": report.data_baseline_id,
                "close": activity.close,
                "volume": activity.volume,
                "amount": activity.amount,
                "amount_share": activity.amount_share,
                "volume_share": activity.volume_share,
                "amount_rank": activity.amount_rank,
                "volume_rank": activity.volume_rank,
                "amount_avg_previous": activity.amount_avg_previous,
                "amount_ratio_previous": activity.amount_ratio_previous,
                "volume_avg_previous": activity.volume_avg_previous,
                "volume_ratio_previous": activity.volume_ratio_previous,
                "history_observation_count": activity.history_observation_count,
                "reason_codes": _json(activity.reason_codes),
                "calculation_status": (
                    "INSUFFICIENT_HISTORY"
                    if "INSUFFICIENT_LOOKBACK" in activity.reason_codes else "VALID"
                ),
                "quality_status": report.quality_status,
                "input_manifest_hash": report.input_manifest_hash,
                "activity_hash": report.activity_hash,
                "calculation_run_id": context.run_id,
            })
        columns = tuple(rows[0])
        return self.kernel.write.upsert(
            context=context,
            scope=_scope(report.trade_date, report.trade_date, activity_snapshot_id),
            target="observation.stock_activity_daily",
            rows=rows,
            key=("trade_date", "ts_code", "methodology_hash", "data_baseline_id"),
            update_columns=tuple(column for column in columns if column != "stock_activity_id"),
            skip_noop=True,
        )

    def validate_snapshot(
        self,
        report: MarketActivityReport,
        activity_snapshot_id: str,
        context: RunContext,
    ) -> dict[str, Any]:
        scope = _scope(report.trade_date, report.trade_date, activity_snapshot_id)
        summary = self.kernel.read._repository_query(
            context=context,
            source="observation.market_activity_daily",
            sql="""
                SELECT COUNT(*) AS summary_count,
                       MIN(input_manifest_hash) AS input_manifest_hash,
                       MIN(activity_hash) AS activity_hash,
                       MIN(quality_status) AS quality_status,
                       MIN(top_n) AS top_n,
                       MIN(total_volume) AS total_volume,
                       MIN(total_amount) AS total_amount,
                       MIN(top_n_volume) AS top_n_volume,
                       MIN(top_n_amount) AS top_n_amount,
                       MIN(top_n_volume_share) AS top_n_volume_share,
                       MIN(top_n_amount_share) AS top_n_amount_share,
                       MIN(input_row_count) AS input_row_count,
                       MIN(target_row_count) AS target_row_count,
                       MIN(invalid_target_row_count) AS invalid_target_row_count,
                       MIN(invalid_history_row_count) AS invalid_history_row_count,
                       MIN(insufficient_history_stock_count) AS insufficient_history_stock_count
                  FROM observation.market_activity_daily
                 WHERE activity_snapshot_id=%s AND trade_date=%s
            """,
            params=(activity_snapshot_id, report.trade_date),
            scope=scope,
            max_rows=1,
        )[0]
        stock = self.kernel.read._repository_query(
            context=context,
            source="observation.stock_activity_daily",
            sql="""
                SELECT COUNT(*) AS row_count,
                       COUNT(DISTINCT ts_code) AS stock_count,
                       COUNT(DISTINCT input_manifest_hash) AS manifest_count,
                       MIN(input_manifest_hash) AS input_manifest_hash,
                       COUNT(*) FILTER (WHERE quality_status <> %s) AS nonmatching_quality
                  FROM observation.stock_activity_daily
                 WHERE activity_snapshot_id=%s AND trade_date=%s
            """,
            params=(report.quality_status, activity_snapshot_id, report.trade_date),
            scope=scope,
            max_rows=1,
        )[0]
        result = {
            "summary_count": int(summary.get("summary_count") or 0),
            "row_count": int(stock.get("row_count") or 0),
            "stock_count": int(stock.get("stock_count") or 0),
            "manifest_count": int(stock.get("manifest_count") or 0),
            "summary_manifest_hash": str(summary.get("input_manifest_hash") or ""),
            "stock_manifest_hash": str(stock.get("input_manifest_hash") or ""),
            "summary_activity_hash": str(summary.get("activity_hash") or ""),
            "summary_quality_status": str(summary.get("quality_status") or ""),
            "nonmatching_quality": int(stock.get("nonmatching_quality") or 0),
        }
        if result["summary_count"] != 1:
            raise RuntimeError("market activity daily summary count must equal 1")
        if result["row_count"] != len(report.rows) or result["stock_count"] != len(report.rows):
            raise RuntimeError(
                "market activity stock row count mismatch: "
                f"stored={result['row_count']}, expected={len(report.rows)}"
            )
        if result["manifest_count"] != 1:
            raise RuntimeError("market activity stock rows have inconsistent input manifests")
        if result["summary_manifest_hash"] != report.input_manifest_hash:
            raise RuntimeError("market activity summary input manifest mismatch")
        if result["stock_manifest_hash"] != report.input_manifest_hash:
            raise RuntimeError("market activity stock input manifest mismatch")
        if result["summary_activity_hash"] != report.activity_hash:
            raise RuntimeError("market activity summary result hash mismatch")
        if result["summary_quality_status"] != report.quality_status:
            raise RuntimeError("market activity summary quality status mismatch")
        expected_summary = {
            "top_n": report.top_n,
            "total_volume": report.total_volume,
            "total_amount": report.total_amount,
            "top_n_volume": report.top_n_volume,
            "top_n_amount": report.top_n_amount,
            "top_n_volume_share": report.top_n_volume_share,
            "top_n_amount_share": report.top_n_amount_share,
            "input_row_count": report.input_row_count,
            "target_row_count": report.target_row_count,
            "invalid_target_row_count": report.invalid_target_row_count,
            "invalid_history_row_count": report.invalid_history_row_count,
            "insufficient_history_stock_count": report.insufficient_history_stock_count,
        }
        for column, expected in expected_summary.items():
            actual = summary.get(column)
            if isinstance(expected, float) or actual is not None and isinstance(actual, float):
                if expected is None or actual is None:
                    if expected is not None or actual is not None:
                        raise RuntimeError(f"market activity summary mismatch: {column}")
                elif not math.isclose(float(actual), float(expected), rel_tol=1e-12, abs_tol=1e-12):
                    raise RuntimeError(f"market activity summary mismatch: {column}")
            elif int(actual or 0) != int(expected):
                raise RuntimeError(f"market activity summary mismatch: {column}")
        if result["nonmatching_quality"]:
            raise RuntimeError("market activity stock quality status mismatch")
        return result

    @staticmethod
    def _validate_report_identity(report: MarketActivityReport) -> None:
        if len(str(report.methodology_hash or "")) != 64:
            raise ValueError("market activity report methodology_hash must be a SHA-256 hex digest")
        if not str(report.data_baseline_id or "").strip():
            raise ValueError("market activity report requires data_baseline_id")
        for name in ("input_manifest_hash", "activity_hash"):
            value = str(getattr(report, name) or "")
            if len(value) != 64:
                raise ValueError(f"market activity report {name} must be a SHA-256 hex digest")


__all__ = ["MarketActivityStorageRepository"]
