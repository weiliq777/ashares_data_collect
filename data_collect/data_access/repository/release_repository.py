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
from data_collect.data_access.release_delta_impact import (
    DeltaImpactPlan,
    resolve_delta_impact,
)
from data_collect.data_access.repository.market_activity_repository import MarketActivityRepository
from data_collect.market_activity.contracts import MarketActivityReport, MarketActivityRequest
from data_collect.market_activity.monitor import MarketActivityMonitor


class ReleaseRepository(BaseRepository):
    _MANIFEST_WINDOW_DAYS = 180
    RESEARCH_DOMAIN = "RESEARCH"
    MARKET_ACTIVITY_DOMAIN = "MARKET_ACTIVITY"

    @classmethod
    def _baseline_domain(cls, schema_version: str | None) -> str:
        return (
            cls.MARKET_ACTIVITY_DOMAIN
            if str(schema_version or "").strip() == "market-activity-v1"
            else cls.RESEARCH_DOMAIN
        )

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
        baseline_domain = self._baseline_domain(schema_version)
        row = {
            "release_tag": tag,
            "baseline_domain": baseline_domain,
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
            update_columns=("baseline_domain", "baseline_cutoff_date", "status", "notes", "schema_version",
                            "data_cutoff_timestamp", "temporal_evidence_level",
                            "visibility_policy_version"),
        )

    def get_release(self, tag: str, context: RunContext) -> dict | None:
        """Read one release identity by its unique tag; never scan the ledger."""
        rows = self.kernel.read._repository_query(
            context,
            "ops.tushare_data_release",
            """SELECT release_tag, baseline_domain, baseline_cutoff_date,
                      schema_version, status
                 FROM ops.tushare_data_release
                WHERE release_tag=%s
                LIMIT 2""",
            (str(tag).strip(),),
            WorkScope(partition=f"release:{tag}"),
            2,
        )
        if len(rows) > 1:
            raise RuntimeError(f"duplicate release tag: {tag}")
        return rows[0] if rows else None

    def current_validated_release(self, baseline_domain: str, context: RunContext) -> dict | None:
        domain = str(baseline_domain).strip().upper()
        if domain not in {self.RESEARCH_DOMAIN, self.MARKET_ACTIVITY_DOMAIN}:
            raise ValueError(f"unsupported release baseline domain: {domain!r}")
        rows = self.kernel.read._repository_query(
            context,
            "ops.tushare_data_release",
            """SELECT release_tag, baseline_domain, baseline_cutoff_date,
                      schema_version, status
                 FROM ops.tushare_data_release
                WHERE baseline_domain=%s AND status='validated'
                LIMIT 2""",
            (domain,),
            WorkScope(partition=f"release:current:{domain}"),
            2,
        )
        if len(rows) > 1:
            raise RuntimeError(f"multiple current validated releases for domain: {domain}")
        return rows[0] if rows else None

    def source_manifest(self, cutoff: date, context: RunContext) -> tuple[dict, str]:
        manifest = {"schema_version": "tushare-release-source-v1", "cutoff_date": cutoff.isoformat(), "market_versions": {}, "legacy_market_raw": {}, "append_only_sources": {}}
        manifest["market_versions"] = self._market_versions_manifest(cutoff, context)
        # trade_cal uses cal_date instead of trade_date.  Keeping this tiny
        # dataset separate lets the three large market datasets use the
        # (dataset, trade_date) covering index without an OR predicate.
        manifest["market_versions"]["trade_cal"] = self._manifest_date_ranges(
            "tushare_market_raw_version", "cal_date", cutoff, context,
            dataset="trade_cal", include_record_id=True,
            partition="release:market:trade_cal",
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

    def _market_versions_manifest(
        self,
        cutoff: date,
        context: RunContext,
        *,
        start_date: date | None = None,
    ) -> dict[str, dict]:
        """Aggregate all versioned market datasets in shared date windows.

        ``tushare_market_raw_version`` stores daily, daily_basic and adj_factor
        in one large append-only table with the same trade_date dimension.
        Scanning that table once per dataset multiplies the I/O cost without
        adding evidence.  This method keeps the same per-dataset counts and
        maxima, but groups the three compatible datasets in one index-backed
        query per window.  trade_cal is intentionally handled separately
        because it uses cal_date.  A timed-out window is still bisected so the
        statement and transaction safety contract is unchanged.
        """
        datasets = ("daily", "daily_basic", "adj_factor")
        empty = {
            dataset: {
                "row_count": 0,
                "max_record_id": None,
                "max_fetched_at": None,
            }
            for dataset in datasets
        }
        if start_date is not None and start_date > cutoff:
            raise ValueError("market manifest start_date must not be after cutoff")
        if start_date is None:
            date_predicate = "trade_date IS NOT NULL AND trade_date <= %s"
            date_params = [cutoff]
        else:
            date_predicate = "trade_date IS NOT NULL AND trade_date BETWEEN %s AND %s"
            date_params = [start_date, cutoff]
        minimum_rows = self.kernel.read._repository_query(
            context,
            "tushare_market_raw_version",
            f"""SELECT dataset, min_date FROM (
                    SELECT 'daily'::text AS dataset,
                           (SELECT MIN(trade_date) FROM tushare_market_raw_version
                             WHERE dataset='daily' AND {date_predicate}) AS min_date
                    UNION ALL
                    SELECT 'daily_basic',
                           (SELECT MIN(trade_date) FROM tushare_market_raw_version
                             WHERE dataset='daily_basic' AND {date_predicate})
                    UNION ALL
                    SELECT 'adj_factor',
                           (SELECT MIN(trade_date) FROM tushare_market_raw_version
                             WHERE dataset='adj_factor' AND {date_predicate})
                ) minima WHERE min_date IS NOT NULL""",
            params=date_params * 3,
            scope=WorkScope(
                date_from=start_date,
                date_to=cutoff,
                partition="release:market:minimum",
            ),
            max_rows=10,
        )
        minimums = {
            str(row["dataset"]): row["min_date"]
            for row in minimum_rows
            if row.get("min_date") is not None and row["min_date"] <= cutoff
        }
        if not minimums:
            return empty

        def aggregate_window(window_start: date, window_end: date) -> dict[str, dict]:
            try:
                rows = self.kernel.read._repository_query(
                    context,
                    "tushare_market_raw_version",
                    """SELECT dataset, COUNT(*) AS count,
                               MAX(record_id) AS max_record_id,
                               MAX(fetched_at) AS max_fetched_at
                          FROM tushare_market_raw_version
                         WHERE dataset=ANY(%s)
                           AND trade_date BETWEEN %s AND %s
                         GROUP BY dataset""",
                    params=[list(datasets), window_start, window_end],
                    scope=WorkScope(
                        date_from=window_start, date_to=window_end,
                        partition=f"release:market:shared:{window_start}:{window_end}",
                    ),
                    max_rows=10,
                )
                return {
                    str(row["dataset"]): {
                        "row_count": int(row.get("count") or 0),
                        "max_record_id": int(row["max_record_id"]) if row.get("max_record_id") is not None else None,
                        "max_fetched_at": row["max_fetched_at"].isoformat() if row.get("max_fetched_at") else None,
                    }
                    for row in rows
                }
            except Exception as exc:
                if "statement timeout" not in str(exc).lower() or window_start >= window_end:
                    raise
                midpoint = window_start + (window_end - window_start) // 2
                left = aggregate_window(window_start, midpoint)
                right = aggregate_window(midpoint + timedelta(days=1), window_end)
                merged = {}
                for dataset in datasets:
                    left_row = left.get(dataset, empty[dataset])
                    right_row = right.get(dataset, empty[dataset])
                    timestamps = [
                        value for value in (left_row["max_fetched_at"], right_row["max_fetched_at"])
                        if value is not None
                    ]
                    record_ids = [
                        value for value in (left_row["max_record_id"], right_row["max_record_id"])
                        if value is not None
                    ]
                    merged[dataset] = {
                        "row_count": left_row["row_count"] + right_row["row_count"],
                        "max_record_id": max(record_ids) if record_ids else None,
                        "max_fetched_at": max(timestamps) if timestamps else None,
                    }
                return merged

        result = {dataset: dict(values) for dataset, values in empty.items()}
        window_start = min(minimums.values())
        while window_start <= cutoff:
            window_end = min(cutoff, window_start + timedelta(days=self._MANIFEST_WINDOW_DAYS - 1))
            window_rows = aggregate_window(window_start, window_end)
            for dataset, row in window_rows.items():
                result[dataset]["row_count"] += row["row_count"]
                if row["max_record_id"] is not None:
                    result[dataset]["max_record_id"] = max(
                        result[dataset]["max_record_id"] or 0, row["max_record_id"],
                    )
                if row["max_fetched_at"] is not None and (
                    result[dataset]["max_fetched_at"] is None
                    or row["max_fetched_at"] > result[dataset]["max_fetched_at"]
                ):
                    result[dataset]["max_fetched_at"] = row["max_fetched_at"]
            window_start = window_end + timedelta(days=1)
        return result

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
        missing_ex_date = self._scalar("std.dividend_event", "SELECT COUNT(*) AS value FROM std.dividend_event WHERE implementation_status='实施' AND ex_date IS NULL", context, "release:dividend-ex-date")
        failures = {}
        for name, value in invariants.items():
            if (isinstance(value, dict) or value != 0) and not (include_foundation_v2 and name == "financial_feature_missing_revenue_mapping"):
                failures[name] = value
        for name, value in checkpoints.items():
            if value.get("missing", 0) or value.get("failed", 0):
                failures[f"checkpoint:{name}"] = value
        if lifecycle or duplicates:
            failures.update({"lifecycle_overlap": lifecycle, "dividend_business_key_duplicates": duplicates})
        if missing_ex_date:
            failures["implementation_without_ex_date"] = missing_ex_date
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

    def market_activity_quality_gate(
        self,
        cutoff: date,
        context: RunContext,
        report: MarketActivityReport | None = None,
    ) -> dict:
        """Validate only the inputs required by the Market Activity release.

        A market-only release must not be blocked by unrelated financial,
        dividend or lifecycle invariants.  It still fails closed on the two
        facts consumed by the observation domain: the market coverage gate and
        the materialized Standard daily bars for the release cutoff.
        """
        gate_rows = self.kernel.read._repository_query(
            context,
            "ops.market_data_quality_gate",
            """
                SELECT dataset, trade_date, actual_rows, expected_rows,
                       status, batch_id, reason, created_at
                  FROM ops.market_data_quality_gate
                 WHERE dataset='daily' AND trade_date=%s
                 ORDER BY created_at DESC, gate_id DESC
                 LIMIT 1
            """,
            (cutoff,),
            WorkScope(date_to=cutoff, partition=f"release:market-activity:gate:{cutoff}"),
            2,
        )
        failures: dict[str, object] = {}
        gate = gate_rows[0] if gate_rows else None
        if gate is None:
            failures["market_data_gate_missing"] = {
                "dataset": "daily",
                "trade_date": cutoff.isoformat(),
            }
        elif str(gate.get("status") or "").upper() != "PASS":
            failures["market_data_gate_not_pass"] = {
                "dataset": "daily",
                "trade_date": cutoff.isoformat(),
                "status": gate.get("status"),
                "reason": gate.get("reason"),
            }

        standard_rows = self.kernel.read._repository_query(
            context,
            "std.daily_kline",
            """
                SELECT COUNT(*) AS row_count,
                       COUNT(DISTINCT stock_code) AS stock_count,
                       COUNT(*) FILTER (
                           WHERE stock_code IS NULL
                              OR close IS NULL
                              OR volume IS NULL
                              OR amount IS NULL
                       ) AS invalid_count
                  FROM std.daily_kline
                 WHERE trade_date=%s
            """,
            (cutoff,),
            WorkScope(date_from=cutoff, date_to=cutoff, partition=f"release:market-activity:standard:{cutoff}"),
            2,
        )
        standard = standard_rows[0] if standard_rows else {
            "row_count": 0, "stock_count": 0, "invalid_count": 0,
        }
        row_count = int(standard.get("row_count") or 0)
        stock_count = int(standard.get("stock_count") or 0)
        invalid_count = int(standard.get("invalid_count") or 0)
        if row_count <= 0 or stock_count <= 0:
            failures["standard_daily_kline_empty"] = {
                "trade_date": cutoff.isoformat(),
                "row_count": row_count,
                "stock_count": stock_count,
            }
        if invalid_count:
            failures["standard_daily_kline_invalid_rows"] = invalid_count
        if gate is not None and str(gate.get("status") or "").upper() == "PASS":
            expected = int(gate.get("actual_rows") or 0)
            if expected > 0 and stock_count != expected:
                failures["standard_daily_kline_coverage_mismatch"] = {
                    "market_gate_actual_rows": expected,
                    "standard_stock_count": stock_count,
                }
        result = {
            "cutoff_date": cutoff.isoformat(),
            "market_data_gate": gate,
            "standard_daily_kline": {
                "row_count": row_count,
                "stock_count": stock_count,
                "invalid_count": invalid_count,
            },
            "market_activity_report": None,
            "failures": failures,
        }
        return self._include_market_activity_report(result, cutoff, report) if report else result

    @staticmethod
    def _include_market_activity_report(
        gate: dict, cutoff: date, report: MarketActivityReport,
    ) -> dict:
        if report.trade_date != cutoff:
            raise ValueError("market activity report date must match release cutoff")
        result = {**gate, "failures": dict(gate["failures"])}
        result["market_activity_report"] = report.quality_summary()
        failures = result["failures"]
        standard_stock_count = int(result["standard_daily_kline"]["stock_count"])
        if report.quality_status not in {"VALID", "PARTIAL_HISTORY"}:
            failures["market_activity_report_quality"] = report.quality_status
        if report.invalid_target_row_count:
            failures["market_activity_invalid_target_rows"] = report.invalid_target_row_count
        if report.target_row_count != standard_stock_count or len(report.rows) != standard_stock_count:
            failures["market_activity_report_coverage_mismatch"] = {
                "report_target_rows": report.target_row_count,
                "report_valid_rows": len(report.rows),
                "standard_stock_count": standard_stock_count,
            }
        if len(report.input_manifest_hash) != 64 or len(report.activity_hash) != 64:
            failures["market_activity_report_hash_missing"] = True
        return result

    def market_activity_source_manifest(
        self, cutoff: date, report: MarketActivityReport,
    ) -> tuple[dict, str]:
        """Hash exactly the Standard rows consumed by the Market Activity report."""
        if report.trade_date != cutoff:
            raise ValueError("market activity manifest date must match release cutoff")
        manifest = {
            "schema_version": "market-activity-input-v2",
            "cutoff_date": cutoff.isoformat(),
            "input_window": {
                "source": "std.daily_kline",
                "date_from": report.lookback_start.isoformat(),
                "date_to": cutoff.isoformat(),
                "row_count": report.input_row_count,
            },
            "report": report.quality_summary(),
        }
        encoded = json.dumps(
            manifest, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), default=str,
        )
        return manifest, hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def validate(self, tag: str, quality_report_hash: str, context: RunContext) -> dict:
        rows = self.kernel.read._repository_query(context, "ops.tushare_data_release", "SELECT baseline_domain, baseline_cutoff_date, schema_version, data_cutoff_timestamp, temporal_evidence_level, visibility_policy_version FROM ops.tushare_data_release WHERE release_tag=%s AND status='draft'", (tag,), WorkScope(partition=f"release:{tag}"), 2)
        if not rows:
            raise RuntimeError(f"draft release not found: {tag}")
        cutoff, schema_version = rows[0]["baseline_cutoff_date"], rows[0]["schema_version"]
        expected_domain = self._baseline_domain(schema_version)
        if rows[0]["baseline_domain"] != expected_domain:
            raise RuntimeError(
                f"release domain/schema mismatch for {tag}: "
                f"domain={rows[0]['baseline_domain']!r}, expected={expected_domain!r}"
            )
        # When transactional change capture is enabled, fence the full audit
        # with start/end event watermarks. Any source write during the audit
        # makes publication fail closed and requires a fresh audit.
        release_change_watermark = self.kernel.write.begin_release_validation_boundary(
            context, WorkScope(partition=f"release:validation-boundary:{tag}"),
        )
        if schema_version == "market-activity-v1":
            # Run the cheap, one-day coverage checks first. If the market gate
            # is absent/blocked or today's Standard partition is empty, do not
            # stream the 45-day monitor input at all.
            gate = self.market_activity_quality_gate(cutoff, context)
            if gate["failures"]:
                raise RuntimeError(f"release quality gate failed: {gate['failures']}")
            request = MarketActivityRequest(trade_date=cutoff)
            report = MarketActivityMonitor(
                MarketActivityRepository(self.kernel)
            ).daily(request, context)
            gate = self._include_market_activity_report(gate, cutoff, report)
            if gate["failures"]:
                raise RuntimeError(f"release quality gate failed: {gate['failures']}")
            encoded = json.dumps(gate, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
            quality_hash = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
            if quality_report_hash and quality_report_hash != quality_hash:
                raise ValueError("quality_report_hash mismatch")
            manifest, manifest_hash = self.market_activity_source_manifest(cutoff, report)
            return {
                "tag": tag,
                "baseline_domain": expected_domain,
                "cutoff": cutoff,
                "schema_version": schema_version,
                "data_cutoff_timestamp": rows[0]["data_cutoff_timestamp"],
                "temporal_evidence_level": rows[0]["temporal_evidence_level"],
                "visibility_policy_version": rows[0]["visibility_policy_version"],
                "release_change_watermark": release_change_watermark,
                "gate": gate,
                "quality_hash": quality_hash,
                "source_manifest": manifest,
                "source_manifest_hash": manifest_hash,
            }
        gate = self.quality_gate(cutoff, schema_version == "feature-contract-v2.0", context)
        if gate["failures"]:
            raise RuntimeError(f"release quality gate failed: {gate['failures']}")
        encoded = json.dumps(gate, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        if quality_report_hash and quality_report_hash != digest:
            raise ValueError("quality_report_hash mismatch")
        manifest, manifest_hash = self.source_manifest(cutoff, context)
        return {
            "tag": tag, "baseline_domain": expected_domain,
            "cutoff": cutoff, "schema_version": schema_version,
            "data_cutoff_timestamp": rows[0]["data_cutoff_timestamp"],
            "temporal_evidence_level": rows[0]["temporal_evidence_level"],
            "visibility_policy_version": rows[0]["visibility_policy_version"],
            "release_change_watermark": release_change_watermark,
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
        return self.kernel.read._repository_query(context, "ops.tushare_data_release", "SELECT release_tag, baseline_domain, baseline_cutoff_date, status, data_cutoff_timestamp, temporal_evidence_level, quality_report_hash, created_at, validated_at, revoked_at, revocation_reason FROM ops.tushare_data_release ORDER BY release_id", scope=WorkScope(partition="release:status"), max_rows=100_000)

    def revoke(self, tag: str, reason: str, evidence_hash: str, actor: str, context: RunContext) -> int:
        """Quarantine a contaminated release without deleting its audit record."""
        if not reason.strip() or len(evidence_hash.strip()) != 64:
            raise ValueError("revocation requires reason and a 64-character evidence hash")
        return self.kernel.write.revoke_release(
            context, WorkScope(partition=f"release:revoke:{tag}"),
            tag, reason[:4000], evidence_hash.lower(), actor[:120],
        )

    def capture_delta_candidate(
        self,
        baseline_release_id: int,
        baseline_domain: str,
        context: RunContext,
        *,
        max_events: int = 1_000,
    ) -> dict:
        """Seal a bounded event candidate; quality validation remains separate."""
        return self.kernel.write.capture_release_delta_candidate(
            context,
            WorkScope(partition=f"release-delta:{baseline_domain}:{baseline_release_id}"),
            baseline_release_id,
            baseline_domain,
            max_events=max_events,
        )

    def delta_candidate(self, package_id: str, context: RunContext) -> dict | None:
        """Read one bounded candidate and its exact event evidence for validation."""
        scope = WorkScope(
            partition=f"release-delta:{package_id}",
            dimensions={"package_id": package_id},
        )
        packages = self.kernel.read._repository_query(
            context,
            "ops.release_delta_package",
            """SELECT package_id, baseline_release_id, baseline_domain,
                      event_from_id, event_through_id, event_count,
                      event_manifest_hash, scope_manifest, captured_run_id,
                      package_status, created_at
                 FROM ops.release_delta_package
                WHERE package_id=%s::uuid""",
            (package_id,), scope, 2,
        )
        if not packages:
            return None
        if len(packages) != 1:
            raise RuntimeError(f"duplicate release delta package id: {package_id}")
        package = packages[0]
        events = self.kernel.read._repository_query(
            context,
            "ops.release_delta_event",
            """SELECT l.event_id, e.event_key, e.run_id, e.job_name,
                      e.code_version, e.pipeline_version, e.target, e.operation,
                      e.scope_hash, s.scope, s.scope_precision, s.delta_eligible,
                      s.full_audit_reason, e.input_hash, e.input_row_count,
                      e.affected_rows, e.created_at, r.status AS producer_run_status
                 FROM ops.release_delta_event l
                 JOIN ops.release_change_event e USING (event_id)
                 JOIN ops.release_change_scope s USING (scope_hash)
                 LEFT JOIN ops.run_manifest r USING (run_id)
                WHERE l.package_id=%s::uuid AND l.baseline_domain=%s
                ORDER BY l.event_id
                LIMIT 1001""",
            (package_id, package["baseline_domain"]), scope, 1_001,
        )
        if len(events) != int(package["event_count"]):
            raise RuntimeError(
                f"release delta event membership mismatch for {package_id}: "
                f"expected={package['event_count']} actual={len(events)}"
            )
        return {**package, "events": events}

    def plan_delta_impact(
        self, package_id: str, context: RunContext,
    ) -> DeltaImpactPlan | None:
        """Resolve an immutable candidate using bounded control-plane reads only.

        This is a preflight planner, not a quality gate.  It intentionally
        reads no Standard/PIT/Feature facts; a later implementation must add a
        separate bounded checker before any delta can be validated/published.
        """
        candidate = self.delta_candidate(package_id, context)
        if candidate is None:
            return None
        scope = WorkScope(
            partition=f"release-delta:{package_id}",
            dimensions={"package_id": package_id},
        )
        parents = self.kernel.read._repository_query(
            context,
            "ops.tushare_data_release",
            """SELECT baseline_cutoff_date
                 FROM ops.tushare_data_release
                WHERE release_id=%s AND baseline_domain=%s AND status='validated'""",
            (candidate["baseline_release_id"], candidate["baseline_domain"]),
            scope,
            2,
        )
        if len(parents) != 1:
            raise RuntimeError(
                f"delta candidate parent is not exactly one current validated release: {package_id}"
            )
        cutoff = parents[0]["baseline_cutoff_date"]
        if isinstance(cutoff, str):
            cutoff = date.fromisoformat(cutoff[:10])
        return resolve_delta_impact(candidate, cutoff=cutoff)

    def _aggregate(self, source, sql, params, context, partition):
        return self.kernel.read._repository_query(context, source, sql, params=params, scope=WorkScope(partition=partition), max_rows=2)[0]

    def _manifest_date_ranges(
        self, table: str, date_column: str, cutoff: date, context: RunContext,
        *, start_date: date | None = None, dataset: str | None = None, include_record_id: bool,
        partition: str,
    ) -> dict:
        """Aggregate an append-only source in adaptive bounded date windows.

        The manifest stores only totals and maxima, not per-window ranges. A
        single full-history aggregate can exceed the statement deadline on a
        large raw table, while the old 30-day loop caused excessive repeated
        scans. Start with 180-day windows and split only the window that times
        out; this preserves the per-statement safety limit without imposing
        hundreds of fixed scans on every release validation.
        """
        if date_column not in {"trade_date", "cal_date"}:
            raise ValueError("unsupported source-manifest date column")
        if start_date is not None and start_date > cutoff:
            raise ValueError("source manifest start_date must not be after cutoff")
        dataset_clause = " AND dataset=%s" if dataset is not None else ""
        if start_date is None:
            date_clause = f"{date_column} IS NOT NULL AND {date_column} <= %s"
            minimum_params = (cutoff,) + ((dataset,) if dataset is not None else ())
        else:
            date_clause = f"{date_column} IS NOT NULL AND {date_column} BETWEEN %s AND %s"
            minimum_params = (start_date, cutoff) + ((dataset,) if dataset is not None else ())
        minimum = self._aggregate(
            table,
            f"SELECT MIN({date_column}) AS min_date FROM {table} "
            f"WHERE {date_clause}{dataset_clause}",
            minimum_params,
            context, f"{partition}:minimum",
        )["min_date"]
        if minimum is None or minimum > cutoff:
            return {
                "row_count": 0,
                **({"max_record_id": None} if include_record_id else {}),
                "max_fetched_at": None,
            }

        def aggregate_window(window_start: date, window_end: date) -> dict:
            select = "COUNT(*) AS count, MAX(fetched_at) AS max_fetched_at"
            if include_record_id:
                select = "COUNT(*) AS count, MAX(record_id) AS max_record_id, MAX(fetched_at) AS max_fetched_at"
            params = (
                (window_start, window_end, dataset)
                if dataset is not None else (window_start, window_end)
            )
            try:
                return self._aggregate(
                    table,
                    f"SELECT {select} FROM {table} "
                    f"WHERE {date_column} BETWEEN %s AND %s{dataset_clause}",
                    params, context, f"{partition}:{window_start}:{window_end}",
                )
            except Exception as exc:
                # PostgreSQL SQLSTATE 57014 is also used for explicit client
                # cancellation. Only the statement-timeout message is safe
                # to retry by splitting; other failures must remain visible.
                if "statement timeout" not in str(exc).lower() or window_start >= window_end:
                    raise
                midpoint = window_start + (window_end - window_start) // 2
                left = aggregate_window(window_start, midpoint)
                right = aggregate_window(midpoint + timedelta(days=1), window_end)
                merged = {
                    "count": int(left.get("count") or 0) + int(right.get("count") or 0),
                    "max_fetched_at": max(
                        (value for value in (left.get("max_fetched_at"), right.get("max_fetched_at")) if value is not None),
                        default=None,
                    ),
                }
                if include_record_id:
                    merged["max_record_id"] = max(
                        (value for value in (left.get("max_record_id"), right.get("max_record_id")) if value is not None),
                        default=None,
                    )
                return merged

        total = 0
        max_record_id = None
        max_fetched_at = None
        window_start = max(minimum, start_date) if start_date is not None else minimum
        while window_start <= cutoff:
            window_end = min(
                cutoff,
                window_start + timedelta(days=self._MANIFEST_WINDOW_DAYS - 1),
            )
            row = aggregate_window(window_start, window_end)
            total += int(row.get("count") or 0)
            if include_record_id and row.get("max_record_id") is not None:
                max_record_id = max(max_record_id or 0, int(row["max_record_id"]))
            if row.get("max_fetched_at") is not None and (
                max_fetched_at is None or row["max_fetched_at"] > max_fetched_at
            ):
                max_fetched_at = row["max_fetched_at"]
            window_start = window_end + timedelta(days=1)
        result = {
            "row_count": total,
            "max_fetched_at": max_fetched_at.isoformat() if max_fetched_at else None,
        }
        if include_record_id:
            result["max_record_id"] = max_record_id
        return result

    def _scalar(self, source, sql, context, partition, params=()):
        return int(self.kernel.read._repository_query(context, source, sql, params=params, scope=WorkScope(partition=partition), max_rows=2)[0]["value"] or 0)

    @staticmethod
    def _manifest_row(row):
        return {"row_count": int(row.get("count") or 0), "max_record_id": int(row["max_record_id"]) if row.get("max_record_id") is not None else None, "max_fetched_at": row["max_fetched_at"].isoformat() if row.get("max_fetched_at") else None}
