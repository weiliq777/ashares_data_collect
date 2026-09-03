"""Repository for Research watchlists and evidence references."""
from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class ResearchWatchlistRepository(BaseRepository):
    CANDIDATE_SQL = """
    WITH base AS (
        SELECT s.*,
               CASE WHEN s.debt_to_assets < 50 AND s.has_full_pe_5y_history IS TRUE
                          AND s.valuation_observation_count >= 1000 AND s.annual_report_count_5y = 5
                          AND s.roe_5y_all_above_15 IS TRUE AND s.roe_5y_min > 15
                          AND s.ocf_report_count_3y = 3 AND s.ocf_3y_all_positive IS TRUE
                          AND s.is_pe_meaningful IS TRUE AND s.pe_pct_5y IS NOT NULL AND s.pe_pct_5y <= 0.30
                     THEN 'value_quality' END AS value_track,
               CASE WHEN s.debt_to_assets < 50 AND s.list_date IS NOT NULL
                          AND s.list_date <= s.research_date - INTERVAL '2 years'
                          AND s.quarterly_report_count_4q = 4 AND s.q_sales_yoy_4q_all_above_30 IS TRUE
                          AND s.q_sales_yoy_above_30_quarters_4q = 4
                     THEN 'growth_quality' END AS growth_track
          FROM research.stock_snapshot_daily s
         WHERE s.snapshot_id=%s AND s.can_buy IS TRUE
           AND s.quality_status NOT IN ('MISSING_SOURCE','INVALID')
    ), tracks AS (
        SELECT b.*, b.value_track AS selection_track FROM base b WHERE b.value_track IS NOT NULL
        UNION ALL SELECT b.*, b.growth_track AS selection_track FROM base b WHERE b.growth_track IS NOT NULL
    ), scored AS (
        SELECT t.*, CASE WHEN t.selection_track='value_quality' THEN
                 COALESCE(t.roe_5y_min,0) / 15.0 + COALESCE(1 - t.pe_pct_5y,0)
                 + CASE WHEN t.ocf_3y_all_positive THEN 1 ELSE 0 END
             ELSE COALESCE(t.q_sales_yoy_above_30_quarters_4q,0) / 4.0
                 + GREATEST(0, 1 - COALESCE(t.debt_to_assets,50) / 50.0) END AS score
          FROM tracks t
    ), deduped AS (
        SELECT scored.*, ROW_NUMBER() OVER (
            PARTITION BY ts_code ORDER BY CASE WHEN selection_track='value_quality' THEN 1 ELSE 2 END,
            score DESC, selection_track) AS track_no FROM scored
    ), ranked AS (
        SELECT deduped.*, ROW_NUMBER() OVER (ORDER BY score DESC, selection_track, ts_code) AS rank_no
          FROM deduped WHERE track_no=1
    )
    SELECT ts_code, research_date, rank_no, score, selection_track,
           jsonb_build_object('debt_to_assets', debt_to_assets, 'pe_pct_5y', pe_pct_5y,
             'has_full_pe_5y_history', has_full_pe_5y_history, 'valuation_observation_count', valuation_observation_count,
             'annual_report_count_5y', annual_report_count_5y, 'roe_5y_min', roe_5y_min,
             'roe_5y_all_above_15', roe_5y_all_above_15, 'ocf_report_count_3y', ocf_report_count_3y,
             'ocf_3y_all_positive', ocf_3y_all_positive, 'quarterly_report_count_4q', quarterly_report_count_4q,
             'q_sales_yoy_above_30_quarters_4q', q_sales_yoy_above_30_quarters_4q,
             'q_sales_yoy_4q_all_above_30', q_sales_yoy_4q_all_above_30,
             'list_date', list_date, 'can_buy', can_buy, 'quality_status', quality_status) AS rationale
      FROM ranked WHERE rank_no <= %s ORDER BY selection_track, rank_no
    """

    def snapshot_status(self, snapshot_id: str, context: RunContext) -> tuple[str, date]:
        rows = self.kernel.read._repository_query(context, "research.snapshot_manifest",
            "SELECT status, research_cutoff_date FROM research.snapshot_manifest WHERE snapshot_id=%s",
            (snapshot_id,), WorkScope(partition=str(snapshot_id)), 2)
        if not rows:
            raise ValueError(f"不存在的 snapshot_id: {snapshot_id}")
        return rows[0]["status"], rows[0]["research_cutoff_date"]

    def create_watchlist(self, snapshot_id: str, name: str, max_members: int,
                         notes: str, created_by: str, rule_version: str,
                         allow_preview: bool, context: RunContext) -> str:
        status, cutoff = self.snapshot_status(snapshot_id, context)
        if status != "FROZEN" and not (allow_preview and status == "VALIDATED"):
            raise RuntimeError(f"观察池默认只允许 FROZEN 快照: {snapshot_id} ({status})")
        watchlist_id = str(uuid.uuid4())
        scope = WorkScope(date_from=cutoff, date_to=cutoff, partition=watchlist_id,
                          dimensions={"snapshot_id": snapshot_id})
        self.kernel.write.upsert(context, scope, "research.watchlist", [{
            "watchlist_id": watchlist_id, "snapshot_id": snapshot_id, "name": name,
            "rule_version": rule_version, "status": "DRAFT", "max_members": max_members,
            "created_by": created_by, "notes": notes,
        }], key=("watchlist_id",), update_columns=("snapshot_id", "name", "rule_version", "status", "max_members", "created_by", "notes"), skip_noop=True)
        rows = self.kernel.read._repository_query(context, "research.stock_snapshot_daily", self.CANDIDATE_SQL,
            (snapshot_id, max_members), scope, max(2, max_members))
        members = [{
            "watchlist_id": watchlist_id, "snapshot_id": snapshot_id, "research_date": row["research_date"],
            "ts_code": row["ts_code"], "rank_no": row["rank_no"], "score": row["score"],
            "selection_track": row["selection_track"], "rule_version": rule_version,
            "rationale": json.dumps(row["rationale"], ensure_ascii=False, default=str), "quality_status": "VALID",
        } for row in rows]
        if members:
            self.kernel.write.bulk_insert(context, scope, "research.watchlist_member", members)
        return watchlist_id

    def activate(self, watchlist_id: str, context: RunContext) -> None:
        rows = self.kernel.read._repository_query(context, "research.watchlist",
            "SELECT watchlist_id, snapshot_id, name, rule_version, max_members, created_by, notes FROM research.watchlist WHERE watchlist_id=%s",
            (watchlist_id,), WorkScope(partition=str(watchlist_id)), 2)
        if not rows:
            raise ValueError(f"不存在的 watchlist_id: {watchlist_id}")
        row = rows[0]
        if self.kernel.write.upsert(context, WorkScope(partition=str(watchlist_id), dimensions={"snapshot_id": str(row["snapshot_id"])}),
            "research.watchlist", [{**row, "status": "ACTIVE"}], key=("watchlist_id",), update_columns=("status",), skip_noop=True) != 1:
            raise RuntimeError("观察池状态更新失败")

    def add_evidence(self, snapshot_id: str, ts_code: str, fields: dict[str, Any], context: RunContext) -> str:
        status, cutoff = self.snapshot_status(snapshot_id, context)
        if status not in {"VALIDATED", "FROZEN"}:
            raise RuntimeError(f"证据引用要求 VALIDATED/FROZEN 快照: {snapshot_id}")
        published = fields.get("published_at")
        published_day = published.date() if hasattr(published, "date") else published
        if published_day is not None and published_day > cutoff:
            raise ValueError("证据发布时间晚于 Research 截止日")
        if fields.get("effective_date") is not None and fields["effective_date"] > cutoff:
            raise ValueError("证据生效日期晚于 Research 截止日")
        scope = WorkScope(date_from=cutoff, date_to=cutoff, partition=str(snapshot_id), dimensions={"snapshot_id": snapshot_id})
        if not self.kernel.read._repository_query(context, "research.stock_snapshot_daily",
            "SELECT ts_code FROM research.stock_snapshot_daily WHERE snapshot_id=%s AND research_date=%s AND ts_code=%s",
            (snapshot_id, cutoff, ts_code), scope, 2):
            raise ValueError(f"股票不在快照中: {ts_code}")
        watchlist_id = fields.get("watchlist_id")
        if watchlist_id:
            parent = self.kernel.read._repository_query(context, "research.watchlist", "SELECT snapshot_id FROM research.watchlist WHERE watchlist_id=%s",
                (watchlist_id,), WorkScope(partition=str(watchlist_id)), 2)
            if not parent or str(parent[0]["snapshot_id"]) != str(snapshot_id):
                raise ValueError("watchlist_id 与 snapshot_id 不匹配")
            if not self.kernel.read._repository_query(context, "research.watchlist_member",
                "SELECT ts_code FROM research.watchlist_member WHERE watchlist_id=%s AND snapshot_id=%s AND ts_code=%s",
                (watchlist_id, snapshot_id, ts_code), scope, 2):
                raise ValueError(f"股票不在观察池中: {ts_code}")
        evidence_id = str(uuid.uuid4())
        row = {"evidence_id": evidence_id, **fields, "snapshot_id": snapshot_id, "ts_code": ts_code}
        row["locator"] = json.dumps(row.get("locator") or {}, ensure_ascii=False, default=str)
        self.kernel.write.bulk_insert(context, scope, "research.evidence_reference", [row])
        return evidence_id

    def add_snapshot_lineage_evidence(self, watchlist_id: str, context: RunContext) -> int:
        parent_rows = self.kernel.read._repository_query(context, "research.watchlist",
            """SELECT w.snapshot_id, w.status, m.status AS snapshot_status,
                              m.research_cutoff_date, m.lineage_storage_version
                 FROM research.watchlist w JOIN research.snapshot_manifest m ON m.snapshot_id=w.snapshot_id
                WHERE w.watchlist_id=%s""", (watchlist_id,), WorkScope(partition=str(watchlist_id)), 2)
        if not parent_rows or parent_rows[0]["status"] != "ACTIVE" or parent_rows[0]["snapshot_status"] != "FROZEN":
            raise RuntimeError("快照事实引用要求 ACTIVE 观察池和 FROZEN 快照")
        parent = parent_rows[0]
        if parent.get("lineage_storage_version") == "v2":
            rows = self.kernel.read._repository_query(
                context, "research.snapshot_lineage_bundle",
                """SELECT m.ts_code, refs.field_group, d.source_table,
                          d.source_record_key, d.source_record_version,
                          d.source_payload_hash
                     FROM research.watchlist_member m
                     JOIN research.snapshot_lineage_bundle l
                       ON l.snapshot_id=m.snapshot_id
                      AND l.research_date=m.research_date
                      AND l.ts_code=m.ts_code
                     CROSS JOIN LATERAL (VALUES
                       ('financial', l.financial_source_ref_id),
                       ('price', l.price_source_ref_id),
                       ('valuation', l.valuation_source_ref_id)
                     ) refs(field_group, source_ref_id)
                     JOIN research.lineage_source_record d
                       ON d.source_ref_id=refs.source_ref_id
                    WHERE m.watchlist_id=%s
                      AND d.source_record_key NOT LIKE '%%:missing'
                      AND d.source_payload_hash IS NOT NULL""",
                (watchlist_id,),
                WorkScope(
                    partition=str(watchlist_id),
                    dimensions={"snapshot_id": str(parent["snapshot_id"])}
                ),
                100_000,
            )
        else:
            rows = self.kernel.read._repository_query(
                context, "research.snapshot_lineage",
                """SELECT m.ts_code, l.field_group, l.source_table, l.source_record_key,
                          l.source_record_version, l.source_payload_hash
                     FROM research.watchlist_member m JOIN research.snapshot_lineage l
                       ON l.snapshot_id=m.snapshot_id AND l.research_date=m.research_date AND l.ts_code=m.ts_code
                    WHERE m.watchlist_id=%s AND l.field_group IN ('financial','price','valuation')
                      AND l.source_record_key NOT LIKE '%%:missing' AND l.source_payload_hash IS NOT NULL""",
                (watchlist_id,),
                WorkScope(
                    partition=str(watchlist_id),
                    dimensions={"snapshot_id": str(parent["snapshot_id"])}
                ),
                100_000,
            )
        evidence = [{
            "evidence_id": str(uuid.uuid4()), "watchlist_id": watchlist_id, "snapshot_id": parent["snapshot_id"],
            "ts_code": row["ts_code"], "source_system": "postgres", "source_type": "snapshot_lineage",
            "source_locator": f"{row['source_table']}:{row['source_record_key']}", "document_id": row["source_record_key"],
            "content_hash": row["source_payload_hash"], "claim_role": "context", "authority_level": "INTERNAL_FACT",
            "effective_date": parent["research_cutoff_date"],
            "locator": json.dumps({"field_group": row["field_group"], "source_table": row["source_table"], "source_record_key": row["source_record_key"], "source_record_version": row["source_record_version"]}, ensure_ascii=False),
            "note": "Research 快照事实来源；不代表外部新闻或公告证据。",
        } for row in rows]
        if not evidence:
            return 0
        return self.kernel.write.bulk_insert(context, WorkScope(partition=str(watchlist_id), dimensions={"snapshot_id": str(parent["snapshot_id"])}), "research.evidence_reference", evidence)
