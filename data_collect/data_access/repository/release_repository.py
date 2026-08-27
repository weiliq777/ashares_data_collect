"""Repository for validated Tushare release baselines."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.repository.base import BaseRepository
from data_collect.data_access.repository.foundation_quality_repository import FoundationQualityRepository
from data_collect.data_access.repository.quality_inspection_repository import QualityInspectionRepository


class ReleaseRepository(BaseRepository):
    def create(
        self,
        tag: str,
        cutoff: date,
        notes: str,
        schema_version: str,
        context: RunContext,
        *,
        data_cutoff_timestamp: datetime | None = None,
        temporal_evidence_level: str = "WAREHOUSE_OBSERVED",
        visibility_policy_version: str = "business-pit-v2",
    ) -> int:
        if data_cutoff_timestamp is not None and data_cutoff_timestamp.tzinfo is None:
            raise ValueError("data_cutoff_timestamp must be timezone-aware")
        if temporal_evidence_level not in {"OFFICIAL_BUSINESS_PIT", "WAREHOUSE_OBSERVED", "LEGACY"}:
            raise ValueError("unsupported temporal_evidence_level")
        row = {
            "release_tag": tag,
            "baseline_cutoff_date": cutoff,
            "status": "draft",
            "notes": notes,
            "schema_version": schema_version or None,
            "data_cutoff_timestamp": data_cutoff_timestamp or datetime.now(timezone.utc),
            "temporal_evidence_level": temporal_evidence_level,
            "visibility_policy_version": visibility_policy_version,
        }
        return self.kernel.write.upsert(
            context=context, scope=WorkScope(date_to=cutoff, partition=f"release:{tag}"),
            target="ops.tushare_data_release", rows=[row], key=("release_tag",),
            update_columns=("baseline_cutoff_date", "status", "notes", "schema_version",
                            "data_cutoff_timestamp", "temporal_evidence_level",
                            "visibility_policy_version"),
        )

    def source_manifest(self, cutoff: date, context: RunContext) -> tuple[dict, str]:
        manifest = {"schema_version": "tushare-release-source-v1", "cutoff_date": cutoff.isoformat(), "market_versions": {}, "legacy_market_raw": {}, "append_only_sources": {}}
        for dataset, column in {"daily": "trade_date", "daily_basic": "trade_date", "adj_factor": "trade_date", "trade_cal": "cal_date"}.items():
            manifest["market_versions"][dataset] = self._manifest_date_ranges(
                "tushare_market_raw_version", column, cutoff, context,
                dataset=dataset, include_record_id=True,
                partition=f"release:market:{dataset}",
            )
        for table, column in {"tushare_daily_raw": "trade_date", "tushare_daily_basic_raw": "trade_date", "tushare_adj_factor_raw": "trade_date", "tushare_trade_cal_raw": "cal_date"}.items():
            manifest["legacy_market_raw"][table] = self._manifest_date_ranges(
                table, column, cutoff, context, include_record_id=False,
                partition=f"release:legacy:{table}",
            )
        specs = {
            "tushare_financial_raw_version": ("record_id", "COALESCE(f_ann_date,ann_date,fetched_at::date)<=%s"),
            "fina_indicator_raw_version": ("record_id", "COALESCE(f_ann_date,ann_date,fetched_at::date)<=%s"),
            "tushare_stock_st_raw": ("record_id", "trade_date<=%s"), "tushare_suspend_d_raw": ("record_id", "trade_date<=%s"),
            "tushare_stk_limit_raw": ("record_id", "trade_date<=%s"), "tushare_namechange_raw": ("record_id", "COALESCE(ann_date,start_date,fetched_at::date)<=%s"),
            "tushare_dividend_raw": ("record_id", "COALESCE(ann_date,ex_date,fetched_at::date)<=%s"), "tushare_index_daily_raw": ("record_id", "trade_date<=%s"),
            "tushare_stock_basic_raw_version": ("record_id", "TRUE"),
        }
        for table, (id_column, predicate) in specs.items():
            params = () if predicate == "TRUE" else (cutoff,)
            row = self._aggregate(table, f"SELECT COUNT(*) AS count, MAX({id_column}) AS max_record_id, MAX(fetched_at) AS max_fetched_at FROM {table} WHERE {predicate}", params, context, f"release:append:{table}")
            manifest["append_only_sources"][table] = self._manifest_row(row)
        encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        return manifest, hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def quality_gate(self, cutoff: date | None, include_foundation_v2: bool, context: RunContext) -> dict:
        quality = QualityInspectionRepository(self.kernel)
        # The v2 foundation deliberately replaces the legacy v1 revenue
        # feature.  Do not rescan that obsolete compatibility table on every
        # v2 Snapshot quality check; it is neither a v2 input nor a release
        # blocker in this mode.
        skipped_invariants = (
            {"financial_feature_missing_revenue_mapping"}
            if include_foundation_v2 else set()
        )
        invariants = quality.phase2_invariants(context, cutoff, skipped_invariants)
        checkpoints = quality.phase2_checkpoint_completeness(context, cutoff)
        lifecycle = self._scalar("std.security_lifecycle", "SELECT COUNT(*) AS value FROM std.security_lifecycle a JOIN std.security_lifecycle b ON a.ts_code=b.ts_code AND a.lifecycle_id<b.lifecycle_id AND a.effective_from<=COALESCE(b.effective_to,DATE '9999-12-31') AND b.effective_from<=COALESCE(a.effective_to,DATE '9999-12-31')", context, "release:lifecycle")
        duplicates = self._scalar("std.dividend_event", "SELECT COUNT(*) AS value FROM (SELECT business_key FROM std.dividend_event GROUP BY business_key HAVING COUNT(*)>1) d", context, "release:dividend-duplicates")
        missing_ex_date = self._scalar("std.dividend_event", "SELECT COUNT(*) AS value FROM std.dividend_event WHERE implementation_status='瀹炴柦' AND ex_date IS NULL", context, "release:dividend-ex-date")
        failures = {}
        for name, value in invariants.items():
            if (isinstance(value, dict) or value != 0) and not (include_foundation_v2 and name == "financial_feature_missing_revenue_mapping"):
                failures[name] = value
        for name, value in checkpoints.items():
            if value.get("missing", 0) or value.get("failed", 0):
                failures[f"checkpoint:{name}"] = value
        if lifecycle or duplicates:
            failures.update({"lifecycle_overlap": lifecycle, "dividend_business_key_duplicates": duplicates})
        foundation = None
        run_count = None
        if include_foundation_v2:
            if cutoff is None:
                raise ValueError("foundation v2 release quality requires cutoff")
            start = max(date(2021, 8, 2), cutoff.replace(year=cutoff.year - 5))
            foundation = FoundationQualityRepository(self.kernel).inspect_foundation_v2(start, cutoff, context)
            run_count = self._scalar("ops.foundation_rebuild_run", "SELECT COUNT(*) AS value FROM ops.foundation_rebuild_run WHERE run_mode='PRODUCTION' AND status='VALIDATED' AND contract_version='feature-contract-v2.0' AND output_start<=%s AND output_end>=%s", context, "release:foundation-run", (start, cutoff))
            for name, value in foundation["failures"].items():
                failures[f"foundation_v2:{name}"] = value
            if run_count <= 0:
                failures["foundation_v2:validated_production_run"] = 0
        return {"cutoff_date": cutoff.isoformat() if cutoff else None, "invariants": invariants, "skipped_invariants": sorted(skipped_invariants), "checkpoints": checkpoints, "lifecycle_overlap": lifecycle, "dividend_business_key_duplicates": duplicates, "implementation_without_ex_date": missing_ex_date, "foundation_v2": foundation, "foundation_v2_validated_run_count": run_count, "failures": failures}

    def validate(self, tag: str, quality_report_hash: str, context: RunContext) -> dict:
        rows = self.kernel.read._repository_query(context, "ops.tushare_data_release", "SELECT baseline_cutoff_date, schema_version, data_cutoff_timestamp, temporal_evidence_level, visibility_policy_version FROM ops.tushare_data_release WHERE release_tag=%s AND status='draft'", (tag,), WorkScope(partition=f"release:{tag}"), 2)
        if not rows:
            raise RuntimeError(f"draft release not found: {tag}")
        cutoff, schema_version = rows[0]["baseline_cutoff_date"], rows[0]["schema_version"]
        gate = self.quality_gate(cutoff, schema_version == "feature-contract-v2.0", context)
        if gate["failures"]:
            raise RuntimeError(f"release quality gate failed: {gate['failures']}")
        encoded = json.dumps(gate, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        if quality_report_hash and quality_report_hash != digest:
            raise ValueError("quality_report_hash mismatch")
        manifest, manifest_hash = self.source_manifest(cutoff, context)
        return {
            "tag": tag, "cutoff": cutoff, "schema_version": schema_version,
            "data_cutoff_timestamp": rows[0]["data_cutoff_timestamp"],
            "temporal_evidence_level": rows[0]["temporal_evidence_level"],
            "visibility_policy_version": rows[0]["visibility_policy_version"],
            "gate": gate, "quality_hash": digest,
            "source_manifest": manifest, "source_manifest_hash": manifest_hash,
        }

    def commit_validation(self, result: dict, context: RunContext) -> int:
        return self.kernel.write.commit_release_validation(
            context=context,
            scope=WorkScope(partition=f"release:commit:{result['tag']}"),
            result={
                **result,
                "gate_json": json.dumps(result["gate"], default=str),
                "manifest_json": json.dumps(result["source_manifest"], default=str),
            },
        )

    def status_rows(self, context: RunContext) -> list[dict]:
        return self.kernel.read._repository_query(context, "ops.tushare_data_release", "SELECT release_tag, baseline_cutoff_date, status, data_cutoff_timestamp, temporal_evidence_level, quality_report_hash, created_at, validated_at, revoked_at, revocation_reason FROM ops.tushare_data_release ORDER BY release_id", scope=WorkScope(partition="release:status"), max_rows=100_000)

    def revoke(self, tag: str, reason: str, evidence_hash: str, actor: str, context: RunContext) -> int:
        """Quarantine a contaminated release without deleting its audit record."""
        if not reason.strip() or len(evidence_hash.strip()) != 64:
            raise ValueError("revocation requires reason and a 64-character evidence hash")
        return self.kernel.write.revoke_release(
            context, WorkScope(partition=f"release:revoke:{tag}"),
            tag, reason[:4000], evidence_hash.lower(), actor[:120],
        )

    def _aggregate(self, source, sql, params, context, partition):
        return self.kernel.read._repository_query(context, source, sql, params=params, scope=WorkScope(partition=partition), max_rows=2)[0]

    def _manifest_date_ranges(
        self, table: str, date_column: str, cutoff: date, context: RunContext,
        *, dataset: str | None = None, include_record_id: bool,
        partition: str,
    ) -> dict:
        """Aggregate a large append-only source in bounded date windows."""
        if date_column not in {"trade_date", "cal_date"}:
            raise ValueError("unsupported source-manifest date column")
        dataset_clause = " AND dataset=%s" if dataset is not None else ""
        min_params = (dataset,) if dataset is not None else ()
        minimum = self._aggregate(
            table,
            f"SELECT MIN({date_column}) AS min_date FROM {table} "
            f"WHERE {date_column} IS NOT NULL{dataset_clause}",
            min_params, context, f"{partition}:minimum",
        )["min_date"]
        if minimum is None or minimum > cutoff:
            return {"row_count": 0, **({"max_record_id": None} if include_record_id else {}), "max_fetched_at": None}

        total = 0
        max_record_id = None
        max_fetched_at = None
        window_start = minimum
        while window_start <= cutoff:
            window_end = min(cutoff, window_start + timedelta(days=30))
            select = "COUNT(*) AS count, MAX(fetched_at) AS max_fetched_at"
            if include_record_id:
                select = "COUNT(*) AS count, MAX(record_id) AS max_record_id, MAX(fetched_at) AS max_fetched_at"
            row = self._aggregate(
                table,
                f"SELECT {select} FROM {table} WHERE {date_column} BETWEEN %s AND %s{dataset_clause}",
                ((window_start, window_end, dataset) if dataset is not None
                 else (window_start, window_end)),
                context, f"{partition}:{window_start}:{window_end}",
            )
            total += int(row["count"] or 0)
            if include_record_id and row.get("max_record_id") is not None:
                max_record_id = max(max_record_id or 0, int(row["max_record_id"]))
            if row.get("max_fetched_at") is not None and (
                max_fetched_at is None or row["max_fetched_at"] > max_fetched_at
            ):
                max_fetched_at = row["max_fetched_at"]
            window_start = window_end + timedelta(days=1)
        result = {"row_count": total, "max_fetched_at": (
            max_fetched_at.isoformat() if max_fetched_at else None
        )}
        if include_record_id:
            result["max_record_id"] = max_record_id
        return result

    def _scalar(self, source, sql, context, partition, params=()):
        return int(self.kernel.read._repository_query(context, source, sql, params=params, scope=WorkScope(partition=partition), max_rows=2)[0]["value"] or 0)

    @staticmethod
    def _manifest_row(row):
        return {"row_count": int(row.get("count") or 0), "max_record_id": int(row["max_record_id"]) if row.get("max_record_id") is not None else None, "max_fetched_at": row["max_fetched_at"].isoformat() if row.get("max_fetched_at") else None}
