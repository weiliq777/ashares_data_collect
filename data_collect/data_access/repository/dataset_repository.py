"""DatasetRepository: backtest materialization and frozen-dataset reads."""
from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.context import Operation, SqlPlan
from data_collect.data_access.repository.base import BaseRepository


class DatasetRepository(BaseRepository):
    def __init__(self, kernel) -> None:
        super().__init__(kernel)
        # A streaming backtest asks for bars/observations once per date. Cache
        # the tiny component manifest once per Dataset so those reads never
        # turn into one extra metadata query per trading day.
        self._component_cache: dict[str, list[dict[str, Any]]] = {}
        self._dataset_kind_cache: dict[str, str] = {}

    def composition_components(
        self, dataset_id: str, context: RunContext, *, allow_empty: bool = False,
    ) -> list[dict[str, Any]]:
        key = str(dataset_id)
        if key not in self._component_cache:
            rows = self.kernel.read._repository_query(
                context, "backtest.dataset_component",
                """SELECT ordinal, component_dataset_id, effective_from, effective_to,
                          component_input_manifest_hash, component_manifest_hash,
                          component_baseline_tag, component_baseline_quality_report_hash
                     FROM backtest.dataset_component
                    WHERE composite_dataset_id=%s ORDER BY ordinal""",
                (key,), WorkScope(partition=f"dataset-components:{key}"), 129,
            )
            if len(rows) > 128:
                raise RuntimeError("组合数据集分段超过安全上限 128")
            self._component_cache[key] = list(rows)
            if rows:
                self._dataset_kind_cache[key] = "COMPOSITE"
        components = self._component_cache[key]
        if not components:
            kind = self._dataset_kind_cache.get(key)
            if kind is None:
                manifests = self.kernel.read._repository_query(
                    context, "backtest.dataset_manifest",
                    "SELECT dataset_kind FROM backtest.dataset_manifest WHERE dataset_id=%s",
                    (key,), WorkScope(partition=f"dataset-kind:{key}"), 2,
                )
                kind = (
                    str(manifests[0].get("dataset_kind") or "MATERIALIZED")
                    if manifests else "MATERIALIZED"
                )
                if manifests:
                    self._dataset_kind_cache[key] = kind
            if kind == "COMPOSITE" and not allow_empty:
                raise RuntimeError(f"COMPOSITE Dataset has no component rows: {key}")
        return components

    def create_composition_components(
        self, composite_dataset_id: str, rows: Sequence[dict[str, Any]],
        context: RunContext,
    ) -> int:
        if not rows:
            raise ValueError("组合数据集必须包含至少一个组件")
        materialized = [
            {"composite_dataset_id": composite_dataset_id, **dict(row)}
            for row in rows
        ]
        changed = self.kernel.write.upsert(
            context,
            WorkScope(partition=f"dataset-components:{composite_dataset_id}",
                      dimensions={"dataset_id": composite_dataset_id}),
            "backtest.dataset_component", materialized,
            key=("composite_dataset_id", "ordinal"),
            update_columns=(
                "component_dataset_id", "effective_from", "effective_to",
                "component_input_manifest_hash", "component_manifest_hash",
                "component_baseline_tag", "component_baseline_quality_report_hash",
            ),
            skip_noop=True,
        )
        self._component_cache.pop(composite_dataset_id, None)
        return changed

    def _component_for_date(
        self, dataset_id: str, day, context: RunContext,
    ) -> dict[str, Any] | None:
        for component in self.composition_components(dataset_id, context):
            if component["effective_from"] <= day <= component["effective_to"]:
                return component
        return None

    @staticmethod
    def _component_cte() -> str:
        return """WITH components AS (
            SELECT ordinal, component_dataset_id AS dataset_id,
                   effective_from, effective_to
              FROM backtest.dataset_component
             WHERE composite_dataset_id=%s
        )"""

    def _set_based_plan(self, target: str, scope: WorkScope, *, allow_delete: bool = False) -> SqlPlan:
        return SqlPlan(operation=Operation.REBUILD, target=target, scope=scope, expected_mode="SET_BASED", allow_delete=allow_delete, estimated_rows=100_000)

    def transition_manifest(
        self,
        context: RunContext,
        dataset_id: str,
        from_statuses: Sequence[str],
        to_status: str,
        fields: dict[str, Any] | None = None,
    ) -> int:
        """Transition a Dataset manifest without exposing Gateway selection to Jobs."""
        scope = WorkScope(
            partition=dataset_id,
            dimensions={"dataset_id": dataset_id},
        )
        return self.kernel.write.transition_dataset_manifest(
            context, scope, dataset_id, tuple(from_statuses), to_status, fields or {}
        )

    def update_manifest(
        self,
        context: RunContext,
        dataset_id: str,
        allowed_statuses: Sequence[str],
        fields: dict[str, Any],
    ) -> int:
        """Update typed Dataset metadata through the Repository boundary."""
        scope = WorkScope(
            partition=dataset_id,
            dimensions={"dataset_id": dataset_id},
        )
        return self.kernel.write.update_dataset_manifest(
            context, scope, dataset_id, tuple(allowed_statuses), fields
        )

    def batch_checkpoint(self, dataset_id: str, batch_start, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_build_batch_checkpoint",
            """SELECT status,batch_end,row_count,batch_hash,code_version,details
                         FROM backtest.dataset_build_batch_checkpoint
                        WHERE dataset_id=%s AND stage='BUILD' AND batch_start=%s""",
            (dataset_id, batch_start), WorkScope(partition=f"batch:{dataset_id}"), 2,
        )
        return rows[0] if rows else None

    def set_batch_checkpoint(self, dataset_id: str, batch_start, batch_end, status: str, context: RunContext, **fields) -> int:
        row = {"dataset_id": dataset_id, "stage": "BUILD", "batch_start": batch_start, "batch_end": batch_end, "status": status, **fields}
        if status == "COMPLETED":
            row["completed_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
        elif status in {"RUNNING", "FAILED"}:
            row["completed_at"] = None
        if "details" in row:
            row["details"] = json.dumps(row["details"] or {}, ensure_ascii=False, default=str)
        return self.kernel.write.upsert(
            context, WorkScope(partition=f"batch:{dataset_id}"), "backtest.dataset_build_batch_checkpoint", [row],
            key=("dataset_id", "stage", "batch_start"), update_columns=tuple(key for key in row if key not in {"dataset_id", "stage", "batch_start"}), skip_noop=True,
        )

    def delete_batch_checkpoints(self, dataset_id: str, context: RunContext) -> int:
        scope = WorkScope(partition=f"batch:{dataset_id}", dimensions={"dataset_id": dataset_id})
        sql = "DELETE FROM backtest.dataset_build_batch_checkpoint WHERE dataset_id=%s"
        return self.kernel.rebuild.run_scoped_merge(sql, self._set_based_plan("backtest.dataset_build_batch_checkpoint", scope, allow_delete=True), context, (dataset_id,))

    def delete_execution_range(self, dataset_id: str, start, end, context: RunContext) -> int:
        scope = WorkScope(date_from=start, date_to=end, partition=dataset_id, dimensions={"dataset_id": dataset_id})
        sql = "DELETE FROM backtest.dataset_execution_bar WHERE dataset_id=%s AND trade_date BETWEEN %s AND %s"
        return self.kernel.rebuild.run_scoped_merge(sql, self._set_based_plan("backtest.dataset_execution_bar", scope, allow_delete=True), context, (dataset_id, start, end))

    def write_execution_rows(self, rows: Sequence[dict[str, Any]], context: RunContext, dataset_id: str) -> int:
        return self.kernel.write.bulk_insert(
            context,
            WorkScope(partition=dataset_id, dimensions={"dataset_id": dataset_id}),
            "backtest.dataset_execution_bar",
            list(rows),
            restart_checkpoint=True,
        )

    def batch_member_inputs(self, dataset_id: str, day, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_member",
            """SELECT snapshot_input_hash, research_snapshot_id,
                              (SELECT COUNT(*) FROM research.stock_snapshot_daily s
                                WHERE s.snapshot_id=m.research_snapshot_id AND s.research_date=m.research_date) AS observation_row_count
                         FROM backtest.dataset_member m
                        WHERE m.dataset_id=%s AND m.research_date=%s""",
            (dataset_id, day), WorkScope(partition=dataset_id, date_from=day, date_to=day), 2,
        )
        return rows[0] if rows else None

    def update_batch_member(self, dataset_id: str, day, fields: dict[str, Any], context: RunContext) -> int:
        changed = self.kernel.write.update_dataset_member_stats(
            context,
            WorkScope(partition=dataset_id, dimensions={"dataset_id": dataset_id}),
            dataset_id,
            day,
            fields,
        )
        if changed != 1:
            raise RuntimeError(f"Dataset 成员不存在或已冻结，拒绝更新统计: {dataset_id}/{day}")
        return changed

    def clear_auxiliary(self, dataset_id: str, context: RunContext) -> None:
        scope = WorkScope(partition=dataset_id, dimensions={"dataset_id": dataset_id})
        for target in ("backtest.dataset_calendar", "backtest.dataset_dividend_event", "backtest.dataset_rule_snapshot", "backtest.dataset_benchmark_bar"):
            self.kernel.rebuild.run_scoped_merge(
                f"DELETE FROM {target} WHERE dataset_id=%s",
                self._set_based_plan(target, scope, allow_delete=True), context, (dataset_id,),
            )

    def rebuild_auxiliary(self, dataset_id: str, manifest: dict[str, Any], boundaries: dict[str, Any], context: RunContext) -> None:
        self.clear_auxiliary(dataset_id, context)
        scope = WorkScope(date_from=manifest["warmup_start"], date_to=manifest["evaluation_end"], partition=dataset_id, dimensions={"dataset_id": dataset_id})
        calendar_rows = self.kernel.read._repository_query(
            context, "tushare_market_raw_version",
            """WITH days AS (SELECT generate_series(%s::date,%s::date,interval '1 day')::date AS trade_date)
                SELECT d.trade_date, COALESCE(v.payload->>'exchange',r.payload->>'exchange','SSE') AS exchange,
                       COALESCE(NULLIF(v.payload->>'is_open','')::integer,NULLIF(r.payload->>'is_open','')::integer)=1 AS is_open,
                       to_date(NULLIF(COALESCE(v.payload->>'pretrade_date',r.payload->>'pretrade_date'),''),'YYYYMMDD') AS pretrade_date,
                       v.record_id, COALESCE(v.payload_hash, encode(digest(convert_to(r.payload::text,'UTF8'),'sha256'),'hex')) AS source_payload_hash
                  FROM days d
                  LEFT JOIN LATERAL (SELECT record_id,payload,payload_hash FROM tushare_market_raw_version mv WHERE mv.dataset='trade_cal' AND mv.exchange='SSE' AND mv.cal_date=d.trade_date AND mv.record_id <= %s ORDER BY mv.fetched_at DESC,mv.record_id DESC LIMIT 1) v ON TRUE
                  LEFT JOIN LATERAL (SELECT payload FROM tushare_trade_cal_raw legacy WHERE v.record_id IS NULL AND legacy.exchange='SSE' AND legacy.cal_date=d.trade_date AND legacy.fetched_at <= %s::timestamp ORDER BY legacy.fetched_at DESC LIMIT 1) r ON TRUE
                 WHERE COALESCE(v.payload,r.payload) IS NOT NULL ORDER BY d.trade_date""",
            (manifest["warmup_start"], manifest["evaluation_end"], boundaries["trade_cal"], boundaries["legacy_trade_cal"]), scope, 100_000,
        )
        expected = (manifest["evaluation_end"] - manifest["warmup_start"]).days + 1
        if len(calendar_rows) != expected:
            raise RuntimeError(f"交易日历覆盖不完整: rows={len(calendar_rows)}, expected={expected}")
        self.kernel.write.bulk_insert(
            context, scope, "backtest.dataset_calendar",
            [{"dataset_id": dataset_id, "trade_date": row["trade_date"], "exchange": row["exchange"], "is_open": row["is_open"], "pretrade_date": row["pretrade_date"], "source_record_key": f"{row['exchange']}:{row['trade_date']}", "source_record_version": row["record_id"], "source_payload_hash": row["source_payload_hash"]} for row in calendar_rows],
            restart_checkpoint=True,
        )
        dividend_rows = self.kernel.read._repository_query(
            context, "tushare_dividend_raw",
            """WITH candidates AS (SELECT record_id,ts_code,end_date AS report_date,ann_date AS announce_date,div_proc AS implementation_status,
                       NULLIF(payload->>'stk_div','')::double precision AS stock_div,NULLIF(payload->>'stk_bo_rate','')::double precision AS stock_bonus_rate,NULLIF(payload->>'stk_co_rate','')::double precision AS stock_conversion_rate,NULLIF(payload->>'cash_div','')::double precision AS cash_div,NULLIF(payload->>'cash_div_tax','')::double precision AS cash_div_tax,record_date,ex_date,to_date(NULLIF(payload->>'pay_date',''),'YYYYMMDD') AS pay_date,to_date(NULLIF(payload->>'div_listdate',''),'YYYYMMDD') AS div_list_date,to_date(NULLIF(payload->>'imp_ann_date',''),'YYYYMMDD') AS implementation_announce_date,ts_code||':'||COALESCE(end_date::text,'')||':'||COALESCE(div_proc,'')||':'||COALESCE(record_date::text,'')||':'||COALESCE(ex_date::text,'')||':'||COALESCE(payload->>'imp_ann_date','') AS event_key,payload_hash,fetched_at FROM tushare_dividend_raw WHERE record_id <= %s AND div_proc='实施' AND ex_date IS NOT NULL AND ex_date <= %s AND COALESCE(to_date(NULLIF(payload->>'pay_date',''),'YYYYMMDD'),ex_date) >= %s AND COALESCE(to_date(NULLIF(payload->>'imp_ann_date',''),'YYYYMMDD'),ann_date,end_date,ex_date) <= ex_date), chosen AS (SELECT DISTINCT ON (event_key) * FROM candidates ORDER BY event_key,fetched_at DESC,record_id DESC)
                SELECT ts_code,report_date,announce_date,implementation_status,stock_div,stock_bonus_rate,stock_conversion_rate,cash_div,cash_div_tax,record_date,ex_date,pay_date,div_list_date,implementation_announce_date,event_key,record_id,payload_hash FROM chosen ORDER BY ex_date,ts_code,event_key,record_id""",
            (boundaries["dividend"], manifest["evaluation_end"], manifest["warmup_start"]), scope, 100_000,
        )
        self.kernel.write.bulk_insert(
            context, scope, "backtest.dataset_dividend_event",
            [{"dataset_id": dataset_id, **dict(zip(("ts_code","report_date","announce_date","implementation_status","stock_div","stock_bonus_rate","stock_conversion_rate","cash_div","cash_div_tax","record_date","ex_date","pay_date","div_list_date","implementation_announce_date","source_record_key","source_record_version","source_payload_hash"), (row[key] for key in ("ts_code","report_date","announce_date","implementation_status","stock_div","stock_bonus_rate","stock_conversion_rate","cash_div","cash_div_tax","record_date","ex_date","pay_date","div_list_date","implementation_announce_date","event_key","record_id","payload_hash"))))} for row in dividend_rows],
            restart_checkpoint=True,
        )
        source_rules = self.kernel.read._repository_query(
            context, "foundation.market_rule_effective",
            """SELECT m.rule_version,m.market_rule_version,e.fee_rule_version,m.effective_from,m.effective_to,
                       m.market,m.min_buy_lot,m.allow_sell_odd_lot,m.t_plus_one,m.price_tick,
                       m.cash_settlement_days,m.limit_pct,e.commission_rate,e.min_commission,
                       t.stamp_duty_sell_rate,t.transfer_fee_rate,e.slippage_bps,e.parameters,
                       m.approval_status,m.approved_by,m.approved_at,e.evidence_hash AS approval_hash
                  FROM foundation.market_rule_effective m
                  JOIN backtest.app_execution_params e
                    ON e.market=m.market AND e.effective_from=m.effective_from
                   AND (e.effective_to IS NULL OR e.effective_to=m.effective_to)
                   AND e.fee_rule_version=%s
                  JOIN foundation.tax_rule_effective t
                    ON t.market=m.market AND t.tax_rule_version=e.fee_rule_version
                   AND t.effective_from=m.effective_from
                 WHERE m.effective_from <= %s
                   AND COALESCE(m.effective_to,DATE '9999-12-31') >= %s
                   AND m.market_rule_version=%s
                   AND m.approval_status='APPROVED'
                   AND e.approval_status='APPROVED'
                   AND t.approval_status='APPROVED'
                   AND m.approved_by IS NOT NULL AND m.approved_at IS NOT NULL
                   AND e.evidence_hash IS NOT NULL
                 ORDER BY m.effective_from,m.market,m.rule_version""",
            (manifest["fee_rule_version"], manifest["evaluation_end"], manifest["warmup_start"], manifest["market_rule_version"]), scope, 100_000,
        )
        if not source_rules:
            raise RuntimeError("Dataset 区间没有已审批市场/费用规则")
        self.kernel.write.bulk_insert(
            context, scope, "backtest.dataset_rule_snapshot",
            [{"dataset_id": dataset_id, **{key: row[key] for key in ("rule_version","market_rule_version","fee_rule_version","effective_from","effective_to","market","min_buy_lot","allow_sell_odd_lot","t_plus_one","price_tick","cash_settlement_days","limit_pct","commission_rate","min_commission","stamp_duty_sell_rate","transfer_fee_rate","slippage_bps","approval_status","approved_by","approved_at","approval_hash")}, "parameters": json.dumps(row["parameters"] or {}, ensure_ascii=False, default=str), "rule_hash": hashlib.sha256(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()} for row in source_rules],
            restart_checkpoint=True,
        )
        benchmark_rows = self.kernel.read._repository_query(
            context, "tushare_index_daily_raw",
            """WITH chosen AS (SELECT DISTINCT ON (r.trade_date) r.trade_date,r.ts_code,NULLIF(r.payload->>'close','')::double precision AS close,r.record_id,r.payload_hash FROM tushare_index_daily_raw r JOIN backtest.dataset_member m ON m.dataset_id=%s AND m.research_date=r.trade_date WHERE r.ts_code=%s AND r.record_id <= %s ORDER BY r.trade_date,r.fetched_at DESC,r.record_id DESC) SELECT trade_date,ts_code,close,record_id,payload_hash FROM chosen ORDER BY trade_date""",
            (dataset_id, manifest["benchmark_code"], boundaries["index"]), scope, 100_000,
        )
        repair_rows = self.benchmark_source_repair_rows(manifest, context)
        if repair_rows:
            baseline_by_date = {row["trade_date"]: row for row in benchmark_rows}
            overlap = baseline_by_date.keys() & {row["trade_date"] for row in repair_rows}
            if overlap:
                raise RuntimeError(
                    "benchmark repair is only allowed for dates absent at baseline: "
                    f"{sorted(overlap)[:5]}"
                )
            benchmark_rows = sorted(
                [*benchmark_rows, *repair_rows],
                key=lambda row: row["trade_date"],
            )
        member_dates = self.member_dates(dataset_id, context)
        if len(benchmark_rows) != len(member_dates) or any(row["close"] is None or float(row["close"]) <= 0 for row in benchmark_rows):
            raise RuntimeError("基准指数没有覆盖全部 Dataset 日期或存在非法收盘价")
        self.kernel.write.bulk_insert(
            context, scope, "backtest.dataset_benchmark_bar",
            [{"dataset_id": dataset_id, "trade_date": row["trade_date"], "ts_code": row["ts_code"], "close": row["close"], "source_record_key": f"{row['ts_code']}:{row['trade_date']}", "source_record_version": row["record_id"], "source_payload_hash": row["payload_hash"]} for row in benchmark_rows],
            restart_checkpoint=True,
        )

    @staticmethod
    def _canonical_json(value: Any) -> str:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), default=str,
        )

    @classmethod
    def _manifest_hash(cls, value: Any) -> str:
        return hashlib.sha256(cls._canonical_json(value).encode("utf-8")).hexdigest()

    def bind_benchmark_source_repair(
        self,
        dataset_id: str,
        context: RunContext,
    ) -> dict[str, Any] | None:
        """Bind only late-arriving benchmark rows absent at the baseline.

        A validated data baseline is immutable.  If an auxiliary benchmark
        row was missing when that baseline was published, the Dataset may
        explicitly bind a tiny repair manifest after the source has been
        collected.  Existing baseline-bound benchmark rows are never replaced
        by newer versions, and the repair is persisted before materialization.
        """
        manifest = self.manifest(dataset_id, context)
        if manifest["status"] not in {"BUILDING", "FAILED"}:
            raise RuntimeError(
                f"benchmark repair requires BUILDING/FAILED Dataset: {manifest['status']}"
            )
        existing = manifest.get("benchmark_source_manifest")
        existing_hash = manifest.get("benchmark_source_manifest_hash")
        if existing is not None or existing_hash:
            if not existing or not existing_hash:
                raise RuntimeError("Dataset benchmark repair manifest is only partially bound")
            actual_hash = self._manifest_hash(existing)
            if actual_hash != str(existing_hash).strip():
                raise RuntimeError("Dataset benchmark repair manifest hash mismatch")
            return existing

        baseline = self.baseline(manifest["baseline"], context)
        if not baseline or baseline["status"] != "validated":
            raise RuntimeError("Dataset benchmark repair requires a validated baseline")
        source = baseline.get("source_manifest") or {}
        baseline_hash = str(baseline.get("source_manifest_hash") or "").strip()
        boundary = (
            source.get("append_only_sources", {})
            .get("tushare_index_daily_raw", {})
            .get("max_record_id")
        )
        if not baseline_hash or boundary is None:
            raise RuntimeError("validated baseline has no index Raw boundary")

        member_dates = self.member_dates(dataset_id, context)
        if not member_dates:
            raise RuntimeError("Dataset has no member dates for benchmark repair")
        benchmark_code = manifest["benchmark_code"]
        baseline_rows = self.kernel.read._repository_query(
            context,
            "tushare_index_daily_raw",
            """WITH chosen AS (
                    SELECT DISTINCT ON (r.trade_date)
                           r.trade_date, r.ts_code,
                           NULLIF(r.payload->>'close','')::double precision AS close,
                           r.record_id, r.payload_hash
                      FROM tushare_index_daily_raw r
                      JOIN backtest.dataset_member m
                        ON m.dataset_id=%s AND m.research_date=r.trade_date
                     WHERE r.ts_code=%s
                       AND r.record_id <= %s
                     ORDER BY r.trade_date,r.fetched_at DESC,r.record_id DESC
                )
                SELECT trade_date,ts_code,close,record_id,payload_hash
                  FROM chosen
                 WHERE close IS NOT NULL AND close > 0
                 ORDER BY trade_date""",
            (dataset_id, benchmark_code, int(boundary)),
            WorkScope(
                date_from=min(member_dates), date_to=max(member_dates),
                partition=f"dataset:benchmark:baseline:{dataset_id}",
            ),
            100_000,
        )
        covered = {row["trade_date"] for row in baseline_rows}
        missing_dates = [day for day in member_dates if day not in covered]
        if not missing_dates:
            return None

        upper_rows = self.kernel.read._repository_query(
            context,
            "tushare_index_daily_raw",
            "SELECT MAX(record_id) AS max_record_id FROM tushare_index_daily_raw",
            (),
            WorkScope(partition=f"dataset:benchmark:repair-boundary:{dataset_id}"),
            2,
        )
        upper = upper_rows[0]["max_record_id"] if upper_rows else None
        if upper is None or int(upper) <= int(boundary):
            raise RuntimeError(
                f"benchmark dates missing at baseline but no later Raw version exists: {missing_dates[:3]}"
            )
        repair_rows = self.kernel.read._repository_query(
            context,
            "tushare_index_daily_raw",
            """SELECT DISTINCT ON (trade_date)
                       trade_date,ts_code,
                       NULLIF(payload->>'close','')::double precision AS close,
                       record_id,payload_hash
                  FROM tushare_index_daily_raw
                 WHERE ts_code=%s
                   AND trade_date = ANY(%s::date[])
                   AND record_id > %s AND record_id <= %s
                 ORDER BY trade_date,fetched_at DESC,record_id DESC""",
            (benchmark_code, missing_dates, int(boundary), int(upper)),
            WorkScope(
                date_from=min(missing_dates), date_to=max(missing_dates),
                partition=f"dataset:benchmark:repair:{dataset_id}",
            ),
            100_000,
        )
        if len(repair_rows) != len(missing_dates) or any(
            row["close"] is None or float(row["close"]) <= 0
            for row in repair_rows
        ):
            found = {row["trade_date"] for row in repair_rows}
            raise RuntimeError(
                "benchmark repair Raw coverage remains incomplete: "
                f"missing={sorted(set(missing_dates) - found)[:5]}"
            )

        repair_manifest = {
            "schema_version": "backtest-benchmark-repair-v1",
            "dataset_id": str(dataset_id),
            "benchmark_code": benchmark_code,
            "base_source_manifest_hash": baseline_hash,
            "source_table": "tushare_index_daily_raw",
            "baseline_record_upper_bound": int(boundary),
            "repair_record_upper_bound": int(upper),
            "repairs": [
                {
                    "trade_date": row["trade_date"].isoformat(),
                    "ts_code": row["ts_code"],
                    "record_id": int(row["record_id"]),
                    "payload_hash": str(row["payload_hash"]),
                    "close": float(row["close"]),
                }
                for row in sorted(repair_rows, key=lambda item: item["trade_date"])
            ],
        }
        repair_hash = self._manifest_hash(repair_manifest)
        changed = self.kernel.write.update_dataset_manifest(
            context,
            WorkScope(
                date_from=min(missing_dates), date_to=max(missing_dates),
                partition=f"dataset:benchmark:bind:{dataset_id}",
                dimensions={"dataset_id": str(dataset_id)},
            ),
            dataset_id,
            ("BUILDING", "FAILED"),
            {
                "benchmark_source_manifest": self._canonical_json(repair_manifest),
                "benchmark_source_manifest_hash": repair_hash,
            },
        )
        if changed != 1:
            raise RuntimeError(f"Dataset benchmark repair binding changed concurrently: {dataset_id}")
        return repair_manifest

    def benchmark_source_repair_rows(
        self,
        manifest: dict[str, Any],
        context: RunContext,
    ) -> list[dict[str, Any]]:
        """Load and verify the exact Raw rows named by a repair manifest."""
        repair = manifest.get("benchmark_source_manifest")
        repair_hash = manifest.get("benchmark_source_manifest_hash")
        if not repair and not repair_hash:
            return []
        if not repair or not repair_hash or self._manifest_hash(repair) != str(repair_hash).strip():
            raise RuntimeError("Dataset benchmark repair manifest is invalid")
        if repair.get("dataset_id") != str(manifest["dataset_id"]):
            raise RuntimeError("Dataset benchmark repair belongs to another Dataset")
        if repair.get("benchmark_code") != manifest["benchmark_code"]:
            raise RuntimeError("Dataset benchmark repair code mismatch")
        entries = repair.get("repairs") or []
        if not entries:
            raise RuntimeError("Dataset benchmark repair manifest has no rows")
        record_ids = [int(entry["record_id"]) for entry in entries]
        if len(set(record_ids)) != len(record_ids):
            raise RuntimeError("Dataset benchmark repair contains duplicate Raw record ids")
        rows = self.kernel.read._repository_query(
            context,
            "tushare_index_daily_raw",
            """SELECT trade_date,ts_code,
                       NULLIF(payload->>'close','')::double precision AS close,
                       record_id,payload_hash
                  FROM tushare_index_daily_raw
                 WHERE record_id = ANY(%s::bigint[])""",
            (record_ids,),
            WorkScope(partition=f"dataset:benchmark:repair-verify:{manifest['dataset_id']}"),
            100_000,
        )
        by_id = {int(row["record_id"]): row for row in rows}
        if len(by_id) != len(entries):
            raise RuntimeError("Dataset benchmark repair Raw records are no longer available")
        verified = []
        seen_dates = set()
        for entry in entries:
            record_id = int(entry["record_id"])
            row = by_id[record_id]
            expected_date = entry["trade_date"]
            if (
                row["ts_code"] != entry["ts_code"]
                or row["trade_date"].isoformat() != expected_date
                or str(row["payload_hash"]) != str(entry["payload_hash"])
                or float(row["close"]) != float(entry["close"])
                or row["trade_date"] in seen_dates
            ):
                raise RuntimeError("Dataset benchmark repair Raw row differs from its bound manifest")
            seen_dates.add(row["trade_date"])
            verified.append(row)
        return verified

    def baseline(self, baseline_tag: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "ops.tushare_data_release",
            """SELECT status, baseline_domain, baseline_cutoff_date, quality_report_hash,
                              source_manifest_hash, source_manifest
                         FROM ops.tushare_data_release
                        WHERE release_tag=%s""",
            (baseline_tag,), WorkScope(partition=f"baseline:{baseline_tag}"), 2,
        )
        if not rows:
            return None
        if rows[0]["baseline_domain"] != "RESEARCH":
            raise ValueError(f"backtest Dataset requires a RESEARCH baseline: {baseline_tag}")
        return rows[0]

    def calendar_status(self, days: Sequence, context: RunContext) -> dict[Any, Any]:
        if not days:
            return {}
        # A five-year Dataset has more than 100 boundary dates.  Keep this
        # lookup bounded and deterministic; the old single ANY() query used
        # max_rows=100 and therefore failed before Dataset creation.
        unique_days = tuple(dict.fromkeys(days))
        result: dict[Any, Any] = {}
        batch_size = 100
        for offset in range(0, len(unique_days), batch_size):
            batch = unique_days[offset:offset + batch_size]
            rows = self.kernel.read._repository_query(
                context, "std.trading_calendar",
                """SELECT trade_date, BOOL_AND(is_open) AS min_is_open,
                                  BOOL_OR(is_open) AS max_is_open
                    FROM std.trading_calendar
                   WHERE exchange='SSE' AND trade_date = ANY(%s)
                   GROUP BY trade_date""",
                (list(batch),),
                WorkScope(partition=f"calendar:boundary:{offset // batch_size}"),
                len(batch),
            )
            for row in rows:
                if row["min_is_open"] != row["max_is_open"]:
                    raise RuntimeError(
                        f"SSE trading calendar has conflicting is_open values: {row['trade_date']}"
                    )
                result[row["trade_date"]] = row["min_is_open"]
        return result

    def snapshot_manifests(self, snapshot_ids: Sequence[str], context: RunContext) -> list[dict[str, Any]]:
        if not snapshot_ids:
            return []
        return self.kernel.read._repository_query(
            context, "research.snapshot_manifest",
            """SELECT snapshot_id, research_cutoff_date, status, input_manifest_hash,
                              schema_version, data_baseline_tag, baseline_quality_report_hash,
                              baseline_source_manifest_hash
                         FROM research.snapshot_manifest
                        WHERE snapshot_id = ANY(%s::uuid[])""",
            (list(snapshot_ids),), WorkScope(partition="dataset:snapshots"), 100_000,
        )

    def open_dates(self, start, end, context: RunContext) -> list[Any]:
        rows = self.kernel.read._repository_query(
            context, "std.trading_calendar",
            """SELECT trade_date FROM std.trading_calendar
                WHERE exchange='SSE' AND is_open IS TRUE
                  AND trade_date BETWEEN %s AND %s ORDER BY trade_date""",
            (start, end), WorkScope(date_from=start, date_to=end, partition="calendar:open"), 100_000,
        )
        return [row["trade_date"] for row in rows]

    def create_manifest(self, row: dict[str, Any], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(
            context, WorkScope(partition=str(row["dataset_id"]), dimensions={"dataset_id": row["dataset_id"]}),
            "backtest.dataset_manifest", [row],
        )

    def create_members(self, rows: Sequence[dict[str, Any]], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(
            context, WorkScope(partition=str(rows[0]["dataset_id"]) if rows else "dataset:empty"),
            "backtest.dataset_member", list(rows),
        )

    def member_dates(self, dataset_id: str, context: RunContext) -> list[Any]:
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_member",
            "SELECT research_date FROM backtest.dataset_member WHERE dataset_id=%s ORDER BY research_date",
            (dataset_id,), WorkScope(partition=dataset_id), 100_000,
        )
        return [row["research_date"] for row in rows]

    def member_validation_rows(self, dataset_id: str, context: RunContext) -> list[tuple]:
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_member",
            """SELECT m.research_date, m.member_role, m.snapshot_input_hash,
                              s.status, s.schema_version, m.observation_row_count,
                              m.execution_row_count, m.execution_hash,
                              m.corporate_action_hash, m.quality_status
                         FROM backtest.dataset_member m
                         JOIN research.snapshot_manifest s ON s.snapshot_id=m.research_snapshot_id
                        WHERE m.dataset_id=%s ORDER BY m.research_date""",
            (dataset_id,), WorkScope(partition=dataset_id), 100_000,
        )
        keys = ("research_date", "member_role", "snapshot_input_hash", "status", "schema_version", "observation_row_count", "execution_row_count", "execution_hash", "corporate_action_hash", "quality_status")
        return [tuple(row[key] for key in keys) for row in rows]

    def update_member_stats(self, dataset_id: str, rows: Sequence[dict[str, Any]], context: RunContext) -> int:
        materialized = [
            {
                "dataset_id": dataset_id,
                "research_date": row["research_date"],
                "observation_row_count": row["observation_row_count"],
                "execution_row_count": row["execution_row_count"],
                "execution_hash": row["execution_hash"],
                "corporate_action_hash": row["corporate_action_hash"],
                "quality_status": row["quality_status"],
            }
            for row in rows
        ]
        changed = self.kernel.write.update_dataset_member_stats_batch(
            context,
            WorkScope(partition=dataset_id, dimensions={"dataset_id": dataset_id}),
            materialized,
        )
        if changed != len(materialized):
            raise RuntimeError(
                f"Dataset member stats update missed rows: expected={len(materialized)}, changed={changed}"
            )
        return changed

    def materialized_calendar_rows(self, dataset_id: str, context: RunContext) -> list[tuple]:
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_calendar",
            """SELECT trade_date, exchange, is_open, pretrade_date,
                              source_record_key, source_record_version, source_payload_hash
                         FROM backtest.dataset_calendar
                        WHERE dataset_id=%s ORDER BY trade_date, exchange""",
            (dataset_id,), WorkScope(partition=dataset_id), 100_000,
        )
        keys = ("trade_date", "exchange", "is_open", "pretrade_date", "source_record_key", "source_record_version", "source_payload_hash")
        return [tuple(row[key] for key in keys) for row in rows]

    def materialized_dividend_rows(self, dataset_id: str, context: RunContext) -> list[tuple]:
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_dividend_event",
            """SELECT ts_code, report_date, announce_date, implementation_status,
                              stock_div, stock_bonus_rate, stock_conversion_rate, cash_div,
                              cash_div_tax, record_date, ex_date, pay_date, div_list_date,
                              implementation_announce_date, source_record_key,
                              source_record_version, source_payload_hash
                         FROM backtest.dataset_dividend_event
                        WHERE dataset_id=%s
                        ORDER BY ex_date, ts_code, source_record_key, source_record_version""",
            (dataset_id,), WorkScope(partition=dataset_id), 100_000,
        )
        keys = ("ts_code", "report_date", "announce_date", "implementation_status", "stock_div", "stock_bonus_rate", "stock_conversion_rate", "cash_div", "cash_div_tax", "record_date", "ex_date", "pay_date", "div_list_date", "implementation_announce_date", "source_record_key", "source_record_version", "source_payload_hash")
        return [tuple(row[key] for key in keys) for row in rows]

    def materialized_rule_rows(self, dataset_id: str, context: RunContext, *, audit: bool = False) -> list[tuple]:
        selected = """rule_version, market_rule_version, fee_rule_version,
                              effective_from, effective_to, market,
                              min_buy_lot, allow_sell_odd_lot, t_plus_one, price_tick,
                              cash_settlement_days, limit_pct, commission_rate, min_commission,
                              stamp_duty_sell_rate, transfer_fee_rate, slippage_bps,
                              parameters, approval_status, approved_by, approved_at,
                              approval_hash, rule_hash""" if audit else """rule_version, market_rule_version, fee_rule_version,
                              effective_from, effective_to, market,
                              min_buy_lot, allow_sell_odd_lot, t_plus_one, price_tick,
                              cash_settlement_days, limit_pct, commission_rate, min_commission,
                              stamp_duty_sell_rate, transfer_fee_rate, slippage_bps,
                              parameters, rule_hash"""
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_rule_snapshot",
            f"SELECT {selected} FROM backtest.dataset_rule_snapshot WHERE dataset_id=%s ORDER BY effective_from, market, rule_version",
            (dataset_id,), WorkScope(partition=dataset_id), 100_000,
        )
        keys = ("rule_version", "market_rule_version", "fee_rule_version", "effective_from", "effective_to", "market", "min_buy_lot", "allow_sell_odd_lot", "t_plus_one", "price_tick", "cash_settlement_days", "limit_pct", "commission_rate", "min_commission", "stamp_duty_sell_rate", "transfer_fee_rate", "slippage_bps", "parameters", "approval_status", "approved_by", "approved_at", "approval_hash", "rule_hash") if audit else ("rule_version", "market_rule_version", "fee_rule_version", "effective_from", "effective_to", "market", "min_buy_lot", "allow_sell_odd_lot", "t_plus_one", "price_tick", "cash_settlement_days", "limit_pct", "commission_rate", "min_commission", "stamp_duty_sell_rate", "transfer_fee_rate", "slippage_bps", "parameters", "rule_hash")
        return [tuple(row[key] for key in keys) for row in rows]

    def materialized_benchmark_rows(self, dataset_id: str, context: RunContext) -> list[tuple]:
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_benchmark_bar",
            """SELECT trade_date,ts_code,close,source_record_key,
                              source_record_version,source_payload_hash
                         FROM backtest.dataset_benchmark_bar
                        WHERE dataset_id=%s ORDER BY trade_date""",
            (dataset_id,), WorkScope(partition=dataset_id), 100_000,
        )
        keys = ("trade_date", "ts_code", "close", "source_record_key", "source_record_version", "source_payload_hash")
        return [tuple(row[key] for key in keys) for row in rows]

    def dataset_open_dates(self, dataset_id: str, start, end, context: RunContext) -> list[Any]:
        components = self.composition_components(dataset_id, context)
        if components:
            rows = self.kernel.read._repository_query(
                context, "backtest.dataset_calendar",
                self._component_cte() + """
                SELECT DISTINCT d.trade_date
                  FROM components c
                  JOIN backtest.dataset_calendar d ON d.dataset_id=c.dataset_id
                   AND d.trade_date BETWEEN c.effective_from AND c.effective_to
                 WHERE d.is_open IS TRUE AND d.trade_date BETWEEN %s AND %s
                 ORDER BY d.trade_date
                """,
                (dataset_id, start, end),
                WorkScope(date_from=start, date_to=end, partition=dataset_id),
                100_000,
            )
            return [row["trade_date"] for row in rows]
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_calendar",
            """SELECT trade_date FROM backtest.dataset_calendar
                        WHERE dataset_id=%s AND is_open IS TRUE
                          AND trade_date BETWEEN %s AND %s ORDER BY trade_date""",
            (dataset_id, start, end), WorkScope(date_from=start, date_to=end, partition=dataset_id), 100_000,
        )
        return [row["trade_date"] for row in rows]

    def validation_counts(self, dataset_id: str, context: RunContext) -> dict[str, Any]:
        queries = {
            "missing_execution_rows": (
                "backtest.dataset_execution_bar",
                "SELECT COUNT(*) AS value FROM backtest.dataset_execution_bar WHERE dataset_id=%s AND quality_status='MISSING_SOURCE'",
            ),
            "missing_execution_lineage": (
                "backtest.dataset_execution_bar",
                "SELECT COUNT(*) AS value FROM backtest.dataset_execution_bar WHERE dataset_id=%s AND (((can_buy IS TRUE OR can_sell IS TRUE) AND (source_payload_hash IS NULL OR limit_source_record_id IS NULL OR limit_source_payload_hash IS NULL)) OR (is_suspended IS TRUE AND suspend_source_record_id IS NULL))",
            ),
            "missing_calendar_lineage": (
                "backtest.dataset_calendar",
                "SELECT COUNT(*) AS value FROM backtest.dataset_calendar WHERE dataset_id=%s AND source_payload_hash IS NULL",
            ),
            "missing_limit_rows": (
                "backtest.dataset_execution_bar",
                "SELECT COUNT(*) AS value FROM backtest.dataset_execution_bar WHERE dataset_id=%s AND quality_status='VALID' AND is_suspended IS NOT TRUE AND (can_buy IS TRUE OR can_sell IS TRUE) AND (up_limit IS NULL OR down_limit IS NULL)",
            ),
            "future_dividend_rows": (
                "backtest.dataset_dividend_event",
                "SELECT COUNT(*) AS value FROM backtest.dataset_dividend_event WHERE dataset_id=%s AND COALESCE(implementation_announce_date, announce_date, report_date, ex_date) > ex_date",
            ),
            "invalid_dividend_dates": (
                "backtest.dataset_dividend_event",
                "SELECT COUNT(*) AS value FROM backtest.dataset_dividend_event WHERE dataset_id=%s AND ((pay_date IS NOT NULL AND pay_date < ex_date) OR (div_list_date IS NOT NULL AND div_list_date < ex_date))",
            ),
            "missing_dividend_lineage": (
                "backtest.dataset_dividend_event",
                "SELECT COUNT(*) AS value FROM backtest.dataset_dividend_event WHERE dataset_id=%s AND (source_record_version IS NULL OR source_payload_hash IS NULL)",
            ),
            "inconsistent_stock_rates": (
                "backtest.dataset_dividend_event",
                "SELECT COUNT(*) AS value FROM backtest.dataset_dividend_event WHERE dataset_id=%s AND stock_div IS NOT NULL AND (stock_bonus_rate IS NOT NULL OR stock_conversion_rate IS NOT NULL) AND ABS(stock_div - (COALESCE(stock_bonus_rate, 0) + COALESCE(stock_conversion_rate, 0))) > 1e-8",
            ),
        }
        result = {}
        for name, (source, sql) in queries.items():
            rows = self.kernel.read._repository_query(context, source, sql, (dataset_id,), WorkScope(partition=dataset_id), 2)
            result[name] = int(rows[0]["value"]) if rows else 0
        return result

    def manifest(self, dataset_id: str, context: RunContext) -> dict[str, Any]:
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_manifest",
            """SELECT dataset_id,dataset_version,dataset_name,data_baseline_tag AS baseline,
                dataset_kind,
                baseline_quality_report_hash,baseline_source_manifest_hash,schema_version,adapter_version,
                feature_version,price_adjustment_mode,warmup_start,evaluation_start,evaluation_end,out_of_sample_start,
                calendar_rule_version,market_rule_version,fee_rule_version,status,built_at,validated_at,frozen_at,input_manifest_hash,
                selection_predicate,selected_columns,hash_algorithm,availability_rules,calendar_hash,rules_hash,
                execution_hash,corporate_action_hash,benchmark_code,benchmark_hash,
                benchmark_source_manifest,benchmark_source_manifest_hash,quality_report,quality_report_hash,
                code_version,git_commit,git_dirty,diff_hash,code_archive_path,code_archive_sha256,
                code_archive_size_bytes FROM backtest.dataset_manifest WHERE dataset_id=%s""",
            (dataset_id,), WorkScope(partition=str(dataset_id), dimensions={"dataset_id": dataset_id}), 2,
        )
        if not rows:
            raise ValueError(f"不存在的 Backtest Dataset: {dataset_id}")
        self._dataset_kind_cache[str(dataset_id)] = str(
            rows[0].get("dataset_kind") or "MATERIALIZED"
        )
        return rows[0]

    def frozen_snapshot_ids(self, baseline_tag: str, schema_version: str, start, end, context: RunContext) -> list[str]:
        rows = self.kernel.read._repository_query(
            context, "research.snapshot_manifest",
            """SELECT snapshot_id FROM research.snapshot_manifest
                WHERE data_baseline_tag=%s AND schema_version=%s AND status='FROZEN'
                  AND research_cutoff_date BETWEEN %s AND %s ORDER BY research_cutoff_date""",
            (baseline_tag, schema_version, start, end), WorkScope(date_from=start, date_to=end, partition="dataset:snapshots"), 100_000,
        )
        return [str(row["snapshot_id"]) for row in rows]

    def checkpoint(self, dataset_id: str, stage: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_build_checkpoint",
            """SELECT status,progress_done,progress_total,last_trade_date,stage_hash,details,error_message,
                      started_at,heartbeat_at,completed_at FROM backtest.dataset_build_checkpoint
                WHERE dataset_id=%s AND stage=%s""", (dataset_id, stage),
            WorkScope(partition=f"checkpoint:{dataset_id}:{stage}", dimensions={"dataset_id": dataset_id}), 2,
        )
        return rows[0] if rows else None

    def execution_row_count(self, dataset_id: str, start, end, context: RunContext) -> int:
        """Return the bounded materialized-row count for a build batch.

        A COMPLETED checkpoint is only a resume hint.  The physical rows may
        have been rolled back or removed by a later failed retry, so callers
        must verify the corresponding bounded range before skipping it.
        """
        rows = self.kernel.read._repository_query(
            context,
            "backtest.dataset_execution_bar",
            """SELECT COUNT(*) AS row_count
                 FROM backtest.dataset_execution_bar
                WHERE dataset_id=%s AND trade_date BETWEEN %s AND %s""",
            (dataset_id, start, end),
            WorkScope(partition=dataset_id, date_from=start, date_to=end),
            2,
        )
        return int(rows[0]["row_count"]) if rows else 0

    def set_checkpoint(self, dataset_id: str, stage: str, status: str, context: RunContext, **fields) -> int:
        row = {"dataset_id": dataset_id, "stage": stage, "status": status, **fields}
        if "details" in row:
            row["details"] = json.dumps(row["details"] or {}, ensure_ascii=False, default=str)
        return self.kernel.write.upsert(
            context, WorkScope(partition=f"checkpoint:{dataset_id}:{stage}", dimensions={"dataset_id": dataset_id}),
            "backtest.dataset_build_checkpoint", [row], key=("dataset_id", "stage"),
            update_columns=tuple(key for key in row if key not in {"dataset_id", "stage"}), skip_noop=True,
        )

    def resume_failed(self, dataset_id: str, context: RunContext) -> int:
        return self.kernel.write.resume_failed_dataset_manifest(
            context,
            WorkScope(partition=str(dataset_id), dimensions={"dataset_id": dataset_id}),
            dataset_id,
        )
    """Typed repository boundary for backtest Dataset artifacts.

    Dataset jobs do not receive SQL or a database handle. Complex selection
    remains in repository methods and enters the database through Kernel
    gateways only.
    """

    def load_manifest(self, dataset_id: str, context: RunContext) -> dict[str, Any]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="backtest.dataset_manifest",
            sql="SELECT * FROM backtest.dataset_manifest WHERE dataset_id=%s",
            params=(dataset_id,),
            scope=WorkScope(partition=dataset_id, dimensions={"dataset_id": dataset_id}),
            max_rows=1,
        )
        if not rows:
            raise ValueError(f"unknown Dataset: {dataset_id}")
        return rows[0]

    def read_members(self, dataset_id: str, context: RunContext) -> list[dict[str, Any]]:
        components = self.composition_components(dataset_id, context)
        if components:
            return self.kernel.read._repository_query(
                context, "backtest.dataset_member",
                self._component_cte() + """
                SELECT m.research_date, m.member_role, m.research_snapshot_id
                  FROM components c
                  JOIN backtest.dataset_member m ON m.dataset_id=c.dataset_id
                 WHERE m.research_date BETWEEN c.effective_from AND c.effective_to
                 ORDER BY m.research_date
                """,
                (dataset_id,), WorkScope(partition=dataset_id), 100_000,
            )
        return self.kernel.read._repository_query(
            context=context,
            source="backtest.dataset_member",
            sql="""
                SELECT research_date, member_role, research_snapshot_id
                  FROM backtest.dataset_member
                 WHERE dataset_id=%s
                 ORDER BY research_date
            """,
            params=(dataset_id,),
            scope=WorkScope(partition=dataset_id, dimensions={"dataset_id": dataset_id}),
            max_rows=100_000,
        )

    def research_snapshot_contracts(
        self, dataset_id: str, context: RunContext,
    ) -> list[dict[str, Any]]:
        """Read Dataset members with their immutable Research contract identity."""
        components = self.composition_components(dataset_id, context)
        if components:
            return self.kernel.read._repository_query(
                context,
                "backtest.dataset_member",
                self._component_cte() + """
                SELECT m.research_date, m.research_snapshot_id,
                       s.status AS snapshot_status,
                       s.schema_version AS research_schema_version,
                       s.data_baseline_tag AS research_baseline
                  FROM components c
                  JOIN backtest.dataset_member m ON m.dataset_id=c.dataset_id
                  JOIN research.snapshot_manifest s
                    ON s.snapshot_id=m.research_snapshot_id
                 WHERE m.research_date BETWEEN c.effective_from AND c.effective_to
                 ORDER BY m.research_date, m.research_snapshot_id
                """,
                (dataset_id,),
                WorkScope(partition=dataset_id),
                max_rows=100_000,
            )
        return self.kernel.read._repository_query(
            context,
            "backtest.dataset_member",
            """
                SELECT m.research_date, m.research_snapshot_id,
                       s.status AS snapshot_status,
                       s.schema_version AS research_schema_version,
                       s.data_baseline_tag AS research_baseline
                  FROM backtest.dataset_member m
                  JOIN research.snapshot_manifest s
                    ON s.snapshot_id=m.research_snapshot_id
                 WHERE m.dataset_id=%s
                 ORDER BY m.research_date, m.research_snapshot_id
            """,
            (dataset_id,),
            WorkScope(partition=dataset_id, dimensions={"dataset_id": dataset_id}),
            max_rows=100_000,
        )

    def execution_page(
        self,
        dataset_id: str,
        context: RunContext,
        last_key: tuple | None = None,
        limit: int = 5_000,
    ) -> list[dict[str, Any]]:
        if limit <= 0 or limit > 50_000:
            raise ValueError("execution page limit must be between 1 and 50000")
        components = self.composition_components(dataset_id, context)
        if components:
            raise RuntimeError(
                "execution_page is a materialized-table validation reader; "
                "COMPOSITE validation must inherit frozen component proofs and "
                "must not rescan merged execution facts"
            )
        keyset = "" if last_key is None else " AND (trade_date, ts_code) > (%s, %s)"
        params = (dataset_id,) if last_key is None else (dataset_id, *last_key)
        return self.kernel.read._repository_query(
            context=context,
            source="backtest.dataset_execution_bar",
            sql=f"""
                SELECT trade_date, ts_code, market, raw_open, raw_high, raw_low,
                       raw_close, volume, amount, up_limit, down_limit,
                       is_suspended, can_buy, can_sell, limit_status,
                       source_record_key, source_record_version, source_payload_hash,
                       limit_source_record_id, limit_source_payload_hash,
                       suspend_source_record_id, suspend_source_payload_hash,
                       quality_status
                  FROM backtest.dataset_execution_bar
                 WHERE dataset_id=%s {keyset}
                 ORDER BY trade_date, ts_code
                 LIMIT {int(limit)}
            """,
            params=params,
            scope=WorkScope(partition=dataset_id, dimensions={"dataset_id": dataset_id}),
            max_rows=limit,
        )

    def open_calendar(self, dataset_id: str, context: RunContext) -> tuple:
        components = self.composition_components(dataset_id, context)
        if components:
            rows = self.kernel.read._repository_query(
                context, "backtest.dataset_calendar",
                self._component_cte() + """
                SELECT DISTINCT d.trade_date
                  FROM components c
                  JOIN backtest.dataset_calendar d ON d.dataset_id=c.dataset_id
                   AND d.trade_date BETWEEN c.effective_from AND c.effective_to
                 WHERE d.is_open IS TRUE
                 ORDER BY d.trade_date
                """,
                (dataset_id,), WorkScope(partition=dataset_id), 100_000,
            )
            return tuple(row["trade_date"] for row in rows)
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_calendar",
            """SELECT trade_date FROM backtest.dataset_calendar
                 WHERE dataset_id=%s AND is_open IS TRUE ORDER BY trade_date""",
            (dataset_id,), WorkScope(partition=dataset_id), max_rows=100_000,
        )
        return tuple(row["trade_date"] for row in rows)

    def bar_page(
        self, dataset_id: str, context: RunContext,
        last_key: tuple | None = None, limit: int = 10_000,
        date_from=None, date_to=None,
    ) -> list[dict[str, Any]]:
        if limit <= 0 or limit > 50_000:
            raise ValueError("bar page limit must be between 1 and 50000")
        components = self.composition_components(dataset_id, context)
        if components:
            if date_from is not None and date_to is not None and date_from == date_to:
                component = self._component_for_date(dataset_id, date_from, context)
                if component is None:
                    return []
                return self.bar_page(
                    str(component["component_dataset_id"]), context, last_key, limit,
                    date_from=date_from, date_to=date_to,
                )
            keyset = "" if last_key is None else " AND (b.trade_date, b.ts_code) > (%s, %s)"
            bounds = ""
            params: tuple[Any, ...] = (dataset_id,)
            if date_from is not None:
                bounds += " AND b.trade_date >= %s"
                params += (date_from,)
            if date_to is not None:
                bounds += " AND b.trade_date <= %s"
                params += (date_to,)
            if last_key is not None:
                params += tuple(last_key)
            return self.kernel.read._repository_query(
                context, "backtest.dataset_execution_bar",
                self._component_cte() + f"""
                SELECT b.trade_date, b.ts_code, b.raw_open, b.raw_high, b.raw_low,
                       b.raw_close, b.volume, b.amount, b.up_limit, b.down_limit,
                       b.is_suspended, b.can_buy, b.can_sell, b.limit_status, b.market
                  FROM components c
                  JOIN backtest.dataset_execution_bar b ON b.dataset_id=c.dataset_id
                   AND b.trade_date BETWEEN c.effective_from AND c.effective_to
                 WHERE TRUE {bounds} {keyset}
                 ORDER BY b.trade_date, b.ts_code LIMIT {int(limit)}
                """,
                params,
                WorkScope(partition=dataset_id, date_from=date_from, date_to=date_to),
                limit,
            )
        keyset = "" if last_key is None else " AND (trade_date, ts_code) > (%s, %s)"
        bounds = ""
        params: tuple[Any, ...] = (dataset_id,)
        if date_from is not None:
            bounds += " AND trade_date >= %s"
            params += (date_from,)
        if date_to is not None:
            bounds += " AND trade_date <= %s"
            params += (date_to,)
        if last_key is not None:
            params += tuple(last_key)
        return self.kernel.read._repository_query(
            context, "backtest.dataset_execution_bar",
            f"""SELECT trade_date, ts_code, raw_open, raw_high, raw_low, raw_close,
                              volume, amount, up_limit, down_limit, is_suspended,
                              can_buy, can_sell, limit_status, market
                         FROM backtest.dataset_execution_bar
                        WHERE dataset_id=%s {bounds} {keyset}
                        ORDER BY trade_date, ts_code LIMIT {int(limit)}""",
            params, WorkScope(partition=dataset_id, date_from=date_from, date_to=date_to), limit,
        )

    def observation_page(
        self, dataset_id: str, columns: Sequence[str], context: RunContext,
        last_key: tuple | None = None, limit: int = 10_000,
        date_from=None, date_to=None,
    ) -> list[dict[str, Any]]:
        if limit <= 0 or limit > 50_000:
            raise ValueError("observation page limit must be between 1 and 50000")
        if not columns or any(not str(column).isidentifier() for column in columns):
            raise ValueError("observation columns must be identifiers")
        selected = ", ".join(f"s.{column}" for column in columns)
        components = self.composition_components(dataset_id, context)
        if components:
            if date_from is not None and date_to is not None and date_from == date_to:
                component = self._component_for_date(dataset_id, date_from, context)
                if component is None:
                    return []
                return self.observation_page(
                    str(component["component_dataset_id"]), columns, context,
                    last_key, limit, date_from=date_from, date_to=date_to,
                )
            keyset = "" if last_key is None else " AND (s.research_date, s.ts_code) > (%s, %s)"
            bounds = ""
            params: tuple[Any, ...] = (dataset_id,)
            if date_from is not None:
                bounds += " AND s.research_date >= %s"
                params += (date_from,)
            if date_to is not None:
                bounds += " AND s.research_date <= %s"
                params += (date_to,)
            if last_key is not None:
                params += tuple(last_key)
            return self.kernel.read._repository_query(
                context, "backtest.dataset_member",
                self._component_cte() + f"""
                SELECT {selected}
                  FROM components c
                  JOIN backtest.dataset_member m ON m.dataset_id=c.dataset_id
                   AND m.research_date BETWEEN c.effective_from AND c.effective_to
                  JOIN research.stock_snapshot_daily s
                    ON s.snapshot_id=m.research_snapshot_id
                   AND s.research_date=m.research_date
                 WHERE TRUE {bounds} {keyset}
                 ORDER BY s.research_date, s.ts_code LIMIT {int(limit)}
                """,
                params,
                WorkScope(partition=dataset_id, date_from=date_from, date_to=date_to),
                limit,
            )
        keyset = "" if last_key is None else " AND (s.research_date, s.ts_code) > (%s, %s)"
        bounds = ""
        params: tuple[Any, ...] = (dataset_id,)
        if date_from is not None:
            bounds += " AND s.research_date >= %s"
            params += (date_from,)
        if date_to is not None:
            bounds += " AND s.research_date <= %s"
            params += (date_to,)
        if last_key is not None:
            params += tuple(last_key)
        return self.kernel.read._repository_query(
            context, "backtest.dataset_member",
            f"""SELECT {selected}
                       FROM backtest.dataset_member m
                       JOIN research.stock_snapshot_daily s
                         ON s.snapshot_id=m.research_snapshot_id
                        AND s.research_date=m.research_date
                      WHERE m.dataset_id=%s{bounds}{keyset}
                      ORDER BY s.research_date, s.ts_code LIMIT {int(limit)}""",
            params, WorkScope(partition=dataset_id, date_from=date_from, date_to=date_to), limit,
        )

    def load_dividends(self, dataset_id: str, context: RunContext) -> list[tuple]:
        components = self.composition_components(dataset_id, context)
        if components:
            rows = self.kernel.read._repository_query(
                context, "backtest.dataset_dividend_event",
                self._component_cte() + """
                SELECT d.ts_code, d.report_date, d.announce_date, d.implementation_status,
                       d.stock_div, d.stock_bonus_rate, d.stock_conversion_rate, d.cash_div,
                       d.cash_div_tax, d.record_date, d.ex_date, d.pay_date, d.div_list_date,
                       d.implementation_announce_date, d.source_record_key,
                       d.source_record_version, d.source_payload_hash
                  FROM components c
                  JOIN backtest.dataset_dividend_event d ON d.dataset_id=c.dataset_id
                   AND d.ex_date BETWEEN c.effective_from AND c.effective_to
                 ORDER BY d.ex_date, d.ts_code, d.source_record_key, d.source_record_version
                """,
                (dataset_id,), WorkScope(partition=dataset_id), 100_000,
            )
            keys = (
                "ts_code", "report_date", "announce_date", "implementation_status",
                "stock_div", "stock_bonus_rate", "stock_conversion_rate", "cash_div",
                "cash_div_tax", "record_date", "ex_date", "pay_date", "div_list_date",
                "implementation_announce_date", "source_record_key",
                "source_record_version", "source_payload_hash",
            )
            return [tuple(row[key] for key in keys) for row in rows]
        return self.materialized_dividend_rows(dataset_id, context)

    def load_rules(self, dataset_id: str, context: RunContext) -> list[tuple]:
        components = self.composition_components(dataset_id, context)
        if components:
            rows = self.kernel.read._repository_query(
                context, "backtest.dataset_rule_snapshot",
                self._component_cte() + """
                SELECT c.ordinal, r.rule_version, r.market_rule_version, r.fee_rule_version,
                       r.effective_from, r.effective_to, r.market, r.min_buy_lot,
                       r.allow_sell_odd_lot, r.t_plus_one, r.price_tick,
                       r.cash_settlement_days, r.limit_pct, r.commission_rate,
                       r.min_commission, r.stamp_duty_sell_rate, r.transfer_fee_rate,
                       r.slippage_bps, r.parameters, r.rule_hash
                  FROM components c
                  JOIN backtest.dataset_rule_snapshot r ON r.dataset_id=c.dataset_id
                   AND r.effective_from <= c.effective_to
                   AND COALESCE(r.effective_to, DATE '9999-12-31') >= c.effective_from
                 ORDER BY r.effective_from, r.market, r.rule_version, c.ordinal DESC
                """,
                (dataset_id,), WorkScope(partition=dataset_id), 100_000,
            )
            deduplicated: dict[tuple[Any, ...], tuple] = {}
            keys = (
                "rule_version", "market_rule_version", "fee_rule_version",
                "effective_from", "effective_to", "market", "min_buy_lot",
                "allow_sell_odd_lot", "t_plus_one", "price_tick",
                "cash_settlement_days", "limit_pct", "commission_rate",
                "min_commission", "stamp_duty_sell_rate", "transfer_fee_rate",
                "slippage_bps", "parameters", "rule_hash",
            )
            identity_keys = ("rule_version", "market_rule_version", "fee_rule_version", "effective_from", "market")
            for row in rows:
                identity = tuple(row[key] for key in identity_keys)
                value = tuple(row[key] for key in keys)
                previous = deduplicated.get(identity)
                if previous is not None and previous != value:
                    raise RuntimeError(
                        "组合 Dataset 的重复生效规则内容不一致: "
                        f"{identity}"
                    )
                deduplicated[identity] = value
            return sorted(
                deduplicated.values(), key=lambda row: (row[3], row[5], row[0])
            )
        return self.materialized_rule_rows(dataset_id, context)

    def load_benchmark_closes(self, dataset_id: str, benchmark_code: str, context: RunContext) -> dict[Any, float]:
        components = self.composition_components(dataset_id, context)
        if components:
            rows = self.kernel.read._repository_query(
                context, "backtest.dataset_benchmark_bar",
                self._component_cte() + """
                SELECT b.trade_date, b.close
                  FROM components c
                  JOIN backtest.dataset_benchmark_bar b ON b.dataset_id=c.dataset_id
                   AND b.trade_date BETWEEN c.effective_from AND c.effective_to
                 WHERE b.ts_code=%s ORDER BY b.trade_date
                """,
                (dataset_id, benchmark_code),
                WorkScope(partition=dataset_id), 100_000,
            )
            return {row["trade_date"]: float(row["close"]) for row in rows}
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_benchmark_bar",
            "SELECT trade_date, close FROM backtest.dataset_benchmark_bar WHERE dataset_id=%s AND ts_code=%s ORDER BY trade_date",
            (dataset_id, benchmark_code), WorkScope(partition=dataset_id), 100_000,
        )
        return {row["trade_date"]: float(row["close"]) for row in rows}

    def member_roles_and_snapshots(self, dataset_id: str, context: RunContext) -> tuple[dict[Any, str], dict[Any, str]]:
        components = self.composition_components(dataset_id, context)
        if components:
            rows = self.kernel.read._repository_query(
                context, "backtest.dataset_member",
                self._component_cte() + """
                SELECT m.research_date, m.member_role, m.research_snapshot_id
                  FROM components c
                  JOIN backtest.dataset_member m ON m.dataset_id=c.dataset_id
                   AND m.research_date BETWEEN c.effective_from AND c.effective_to
                 ORDER BY m.research_date
                """,
                (dataset_id,), WorkScope(partition=dataset_id), 100_000,
            )
            dates = [row["research_date"] for row in rows]
            if len(dates) != len(set(dates)):
                raise RuntimeError("组合 Dataset 同一交易日存在多个快照成员")
            return (
                {row["research_date"]: row["member_role"] for row in rows},
                {row["research_date"]: str(row["research_snapshot_id"]) for row in rows},
            )
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_member",
            "SELECT research_date, member_role, research_snapshot_id FROM backtest.dataset_member WHERE dataset_id=%s",
            (dataset_id,), WorkScope(partition=dataset_id), 100_000,
        )
        return ({row["research_date"]: row["member_role"] for row in rows}, {row["research_date"]: str(row["research_snapshot_id"]) for row in rows})

    def execution_source_batches(
        self,
        dataset_id: str,
        boundaries: dict[str, Any],
        batch_start,
        batch_end,
        context: RunContext,
        batch_rows: int = 5_000,
    ):
        # Resolve each bounded date/code key once per source table.  Source
        # rows are narrowed by the current source date window before joining
        # the Dataset keys.  This avoids one index probe per snapshot row and
        # keeps the query set-based and bounded even when PostgreSQL has stale
        # statistics for the partitioned snapshot table.
        sql = """
            WITH base AS MATERIALIZED (
                SELECT m.research_date AS day, s.ts_code AS code, s.market AS market,
                       s.is_suspended_asof, s.can_buy, s.can_sell, s.limit_status
                  FROM backtest.dataset_member m
                  JOIN research.stock_snapshot_daily s
                    ON s.snapshot_id=m.research_snapshot_id AND s.research_date=m.research_date
                 WHERE m.dataset_id=%s AND m.research_date BETWEEN %s AND %s
            ), keys AS MATERIALIZED (
                SELECT DISTINCT day, code FROM base
            ), market_source AS MATERIALIZED (
                SELECT ts_code, trade_date, record_id, payload, payload_hash, fetched_at
                  FROM public.tushare_market_raw_version
                 WHERE dataset='daily' AND trade_date BETWEEN %s AND %s
                   AND record_id <= %s
            ), market AS MATERIALIZED (
                SELECT DISTINCT ON (k.day, k.code)
                       k.day, k.code, mv.record_id, mv.payload, mv.payload_hash
                  FROM keys k
                  JOIN market_source mv
                    ON mv.ts_code=k.code AND mv.trade_date=k.day
                 ORDER BY k.day, k.code, mv.fetched_at DESC, mv.record_id DESC
            ), legacy_source AS MATERIALIZED (
                SELECT ts_code, trade_date, payload, fetched_at
                  FROM public.tushare_daily_raw
                 WHERE source='tushare' AND trade_date BETWEEN %s AND %s
                   AND fetched_at <= %s::timestamp
            ), legacy AS MATERIALIZED (
                SELECT DISTINCT ON (k.day, k.code)
                       k.day, k.code, legacy.payload
                  FROM keys k
                  LEFT JOIN market v ON v.day=k.day AND v.code=k.code
                  JOIN legacy_source legacy
                    ON legacy.ts_code=k.code AND legacy.trade_date=k.day
                 WHERE v.record_id IS NULL
                 ORDER BY k.day, k.code, legacy.fetched_at DESC
            ), limit_source AS MATERIALIZED (
                SELECT ts_code, trade_date, record_id, payload, payload_hash, fetched_at
                  FROM public.tushare_stk_limit_raw
                 WHERE trade_date BETWEEN %s AND %s AND record_id <= %s
            ), limits AS MATERIALIZED (
                SELECT DISTINCT ON (k.day, k.code)
                       k.day, k.code, raw_limit.record_id, raw_limit.payload, raw_limit.payload_hash
                  FROM keys k
                  JOIN limit_source raw_limit
                    ON raw_limit.ts_code=k.code AND raw_limit.trade_date=k.day
                 ORDER BY k.day, k.code, raw_limit.fetched_at DESC, raw_limit.record_id DESC
            ), suspend_source AS MATERIALIZED (
                SELECT ts_code, trade_date, record_id, payload_hash, fetched_at
                  FROM public.tushare_suspend_d_raw
                 WHERE trade_date BETWEEN %s AND %s AND record_id <= %s
            ), suspends AS MATERIALIZED (
                SELECT DISTINCT ON (k.day, k.code)
                       k.day, k.code, raw_suspend.record_id, raw_suspend.payload_hash
                  FROM keys k
                  JOIN suspend_source raw_suspend
                    ON raw_suspend.ts_code=k.code AND raw_suspend.trade_date=k.day
                 ORDER BY k.day, k.code, raw_suspend.fetched_at DESC, raw_suspend.record_id DESC
            )
            SELECT b.day, b.code, b.market,
                   NULLIF(COALESCE(v.payload, r.payload)->>'open', '')::double precision AS opn,
                   NULLIF(COALESCE(v.payload, r.payload)->>'high', '')::double precision AS high,
                   NULLIF(COALESCE(v.payload, r.payload)->>'low', '')::double precision AS low,
                   NULLIF(COALESCE(v.payload, r.payload)->>'close', '')::double precision AS close,
                   NULLIF(COALESCE(v.payload, r.payload)->>'vol', '')::double precision * 100 AS volume,
                   NULLIF(COALESCE(v.payload, r.payload)->>'amount', '')::double precision * 1000 AS amount,
                   NULLIF(lim.payload->>'up_limit', '')::double precision AS up_limit,
                   NULLIF(lim.payload->>'down_limit', '')::double precision AS down_limit,
                   (COALESCE(b.is_suspended_asof, FALSE) OR sus.record_id IS NOT NULL) AS suspended,
                   (COALESCE(b.can_buy, FALSE) AND NOT (COALESCE(b.is_suspended_asof, FALSE) OR sus.record_id IS NOT NULL)) AS can_buy,
                   (COALESCE(b.can_sell, FALSE) AND NOT (COALESCE(b.is_suspended_asof, FALSE) OR sus.record_id IS NOT NULL)) AS can_sell,
                   (COALESCE(v.payload, r.payload) IS NOT NULL AND lim.record_id IS NOT NULL) AS status_source_present,
                   b.limit_status AS limit_status, COALESCE(v.payload, r.payload) AS payload,
                   v.record_id AS version, v.payload_hash AS payload_hash,
                   lim.record_id AS limit_version, lim.payload_hash AS limit_hash,
                   sus.record_id AS suspend_version, sus.payload_hash AS suspend_hash
              FROM base b
              LEFT JOIN market v ON v.day=b.day AND v.code=b.code
              LEFT JOIN legacy r ON r.day=b.day AND r.code=b.code
              LEFT JOIN limits lim ON lim.day=b.day AND lim.code=b.code
              LEFT JOIN suspends sus ON sus.day=b.day AND sus.code=b.code
             ORDER BY b.day, b.code
        """
        params = (
            dataset_id, batch_start, batch_end,
            batch_start, batch_end, boundaries["daily"],
            batch_start, batch_end, boundaries["legacy_daily"],
            batch_start, batch_end, boundaries["limit"],
            batch_start, batch_end, boundaries["suspend"],
        )
        columns = (
            "day", "code", "market", "opn", "high", "low", "close", "volume", "amount",
            "up_limit", "down_limit", "suspended", "can_buy", "can_sell",
            "status_source_present", "limit_status", "payload", "version", "payload_hash",
            "limit_version", "limit_hash", "suspend_version", "suspend_hash",
        )
        for batch in self.kernel.read._repository_stream(
            context, "backtest.dataset_member", sql, params,
            WorkScope(date_from=batch_start, date_to=batch_end, partition=dataset_id), batch_rows,
        ):
            yield [tuple(row[column] for column in columns) for row in batch]

    def write_rows(
        self,
        target: str,
        rows: Iterable[dict[str, Any]],
        key: tuple[str, ...],
        context: RunContext,
        scope: WorkScope,
    ) -> int:
        materialized = list(rows)
        return self.kernel.write.upsert(
            context=context, scope=scope, target=target, rows=materialized,
            key=key, update_columns=[column for column in materialized[0] if column not in key]
            if materialized else [],
        )
