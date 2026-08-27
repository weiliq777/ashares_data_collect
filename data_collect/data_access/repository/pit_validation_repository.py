"""Point-in-time cutoff validation queries."""
from __future__ import annotations

from datetime import date

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository
from data_collect.data_access.research_contract import is_foundation_schema


class PitValidationRepository(BaseRepository):
    CHECKS = (
        ("financial_asof_announcement", "pit.financial_asof", "SELECT COUNT(*) AS value FROM pit.financial_asof WHERE decision_date=%s AND effective_announce_date>decision_date"),
        ("price_feature_source_asof", "feature.price_daily_v2", "SELECT COUNT(*) AS value FROM feature.price_daily_v2 WHERE trade_date=%s AND source_asof_date>trade_date"),
        ("valuation_feature_source_asof", "feature.valuation_daily_v1", "SELECT COUNT(*) AS value FROM feature.valuation_daily_v1 WHERE trade_date=%s AND source_asof_date>trade_date"),
        ("tradeability_source_asof", "pit.security_tradeability_daily", "SELECT COUNT(*) AS value FROM pit.security_tradeability_daily WHERE trade_date=%s AND source_asof_date>trade_date"),
        ("research_snapshot_financial_visibility", "research.stock_snapshot_daily", "SELECT COUNT(*) AS value FROM research.stock_snapshot_daily WHERE snapshot_id=%s AND effective_announce_date>research_date"),
    )

    def verify(self, target_date: date, snapshot_id: str | None, context: RunContext) -> dict:
        scope = WorkScope(date_from=target_date, date_to=target_date, partition=f"pit-validation:{target_date}")
        checks, issues = [], []
        for name, source, sql in self.CHECKS:
            if name == "research_snapshot_financial_visibility":
                # A snapshot-specific validation must stay snapshot-scoped.
                # Falling back to research_date would scan an entire yearly
                # partition and can consume the governed statement deadline.
                if not snapshot_id:
                    checks.append({"name": name, "violations": 1, "reason": "snapshot_id_required"})
                    issues.append(f"{name}=snapshot_id_required")
                    continue
                params = (snapshot_id,)
            else:
                params = (target_date,)
            count = int(self.kernel.read._repository_query(context, source, sql, params, scope=scope, max_rows=2)[0]["value"] or 0)
            checks.append({"name": name, "violations": count})
            if count:
                issues.append(f"{name}={count}")
        if snapshot_id:
            rows = self.kernel.read._repository_query(context, "research.snapshot_manifest", "SELECT status, research_cutoff_date FROM research.snapshot_manifest WHERE snapshot_id=%s", (snapshot_id,), scope=WorkScope(partition=str(snapshot_id)), max_rows=2)
            if not rows:
                issues.append(f"snapshot_not_found={snapshot_id}")
                checks.append({"name": "snapshot_manifest", "violations": 1})
            else:
                row = rows[0]
                # Validation runs before the lifecycle transition to
                # VALIDATED; BUILDING is therefore a valid pre-publication
                # state. Freeze still enforces VALIDATED separately.
                violations = int(row["status"] not in {"BUILDING", "VALIDATED", "FROZEN"} or row["research_cutoff_date"] != target_date)
                checks.append({"name": "snapshot_manifest", "violations": violations, "status": row["status"], "research_cutoff_date": row["research_cutoff_date"].isoformat()})
                if violations:
                    issues.append(f"snapshot_manifest status/cutoff invalid: {row['status']}/{row['research_cutoff_date']}")
        return {"target_date": target_date.isoformat(), "snapshot_id": snapshot_id, "passed": not issues, "checks": checks, "issues": issues}

    def verify_batch(
        self, cutoffs: list[date], snapshot_ids: list[str],
        schema_version: str, context: RunContext,
    ) -> None:
        if not cutoffs:
            return
        checks = [] if is_foundation_schema(schema_version) else [
            ("financial_asof_announcement", "pit.financial_asof",
             "SELECT COUNT(*) AS value FROM pit.financial_asof WHERE decision_date=ANY(%s::date[]) AND effective_announce_date>decision_date"),
        ]
        checks.extend((
            ("price_feature_source_asof", "feature.price_daily_v2", "SELECT COUNT(*) AS value FROM feature.price_daily_v2 WHERE trade_date=ANY(%s::date[]) AND source_asof_date>trade_date"),
            ("valuation_feature_source_asof", "feature.valuation_daily_v1", "SELECT COUNT(*) AS value FROM feature.valuation_daily_v1 WHERE trade_date=ANY(%s::date[]) AND source_asof_date>trade_date"),
            ("tradeability_source_asof", "pit.security_tradeability_daily", "SELECT COUNT(*) AS value FROM pit.security_tradeability_daily WHERE trade_date=ANY(%s::date[]) AND source_asof_date>trade_date"),
            ("research_snapshot_financial_visibility", "research.stock_snapshot_daily", "SELECT COUNT(*) AS value FROM research.stock_snapshot_daily WHERE snapshot_id=ANY(%s::uuid[]) AND effective_announce_date>research_date"),
        ))
        scope = WorkScope(date_from=min(cutoffs), date_to=max(cutoffs), partition="pit-validation-batch")
        failures = []
        for name, source, sql in checks:
            params = (list(snapshot_ids),) if name == "research_snapshot_financial_visibility" else (list(cutoffs),)
            count = int(self.kernel.read._repository_query(context, source, sql, params, scope, 2)[0]["value"] or 0)
            if count:
                failures.append({"name": name, "violations": count})
        if failures:
            raise RuntimeError(f"batch PIT cutoff validation failed: {failures}")
