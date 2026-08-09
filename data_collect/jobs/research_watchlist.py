"""从 Research 快照生成可解释观察池，并写入证据引用元数据。

默认只消费 FROZEN 快照；``allow_preview=True`` 仅供联调时使用 VALIDATED 快照。
模块只写 research 观察池/证据表，不访问 Tushare，不修改 Raw/Standard/PIT/Feature。
"""
from __future__ import annotations

import argparse
import json
import uuid
from datetime import date

from data_collect.utils.db import get_connection, require_psycopg2


RULE_VERSION = "watchlist-v2"


def _snapshot_status(conn, snapshot_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, research_cutoff_date FROM research.snapshot_manifest WHERE snapshot_id=%s",
            (snapshot_id,),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"不存在的 snapshot_id: {snapshot_id}")
    return row


def _candidate_sql():
    return """
    WITH base AS (
        SELECT s.*,
               CASE WHEN s.debt_to_assets < 50
                          AND s.has_full_pe_5y_history IS TRUE
                          AND s.valuation_observation_count >= 1000
                          AND s.annual_report_count_5y = 5
                          AND s.roe_5y_all_above_15 IS TRUE
                          AND s.roe_5y_min > 15
                          AND s.ocf_report_count_3y = 3
                          AND s.ocf_3y_all_positive IS TRUE
                          AND s.is_pe_meaningful IS TRUE
                          AND s.pe_pct_5y IS NOT NULL AND s.pe_pct_5y <= 0.30
                    THEN 'value_quality' END AS value_track,
               CASE WHEN s.debt_to_assets < 50
                          AND s.list_date IS NOT NULL
                          AND s.list_date <= s.research_date - INTERVAL '2 years'
                          AND s.quarterly_report_count_4q = 4
                          AND s.q_sales_yoy_4q_all_above_30 IS TRUE
                          AND s.q_sales_yoy_above_30_quarters_4q = 4
                    THEN 'growth_quality' END AS growth_track
        FROM research.stock_snapshot_daily s
        WHERE s.snapshot_id=%s AND s.can_buy IS TRUE
          AND s.quality_status NOT IN ('MISSING_SOURCE','INVALID')
    ), tracks AS (
        SELECT b.*, b.value_track AS selection_track FROM base b WHERE b.value_track IS NOT NULL
        UNION ALL
        SELECT b.*, b.growth_track AS selection_track FROM base b WHERE b.growth_track IS NOT NULL
    ), scored AS (
        SELECT t.*,
               CASE WHEN t.selection_track='value_quality' THEN
                    COALESCE(t.roe_5y_min,0) / 15.0
                    + COALESCE(1 - t.pe_pct_5y,0)
                    + CASE WHEN t.ocf_3y_all_positive THEN 1 ELSE 0 END
                    ELSE COALESCE(t.q_sales_yoy_above_30_quarters_4q,0) / 4.0
                    + GREATEST(0, 1 - COALESCE(t.debt_to_assets,50) / 50.0)
               END AS score
        FROM tracks t
    ), deduped AS (
        SELECT scored.*, ROW_NUMBER() OVER (
            PARTITION BY ts_code
            ORDER BY CASE WHEN selection_track='value_quality' THEN 1 ELSE 2 END,
                     score DESC, selection_track
        ) AS track_no
        FROM scored
    ), ranked AS (
        SELECT deduped.*, ROW_NUMBER() OVER (
            ORDER BY score DESC, selection_track, ts_code
        ) AS rank_no
        FROM deduped WHERE track_no=1
    )
    SELECT ts_code, research_date, rank_no, score, selection_track,
           jsonb_build_object(
               'debt_to_assets', debt_to_assets, 'pe_pct_5y', pe_pct_5y,
               'has_full_pe_5y_history', has_full_pe_5y_history,
               'valuation_observation_count', valuation_observation_count,
               'annual_report_count_5y', annual_report_count_5y,
               'roe_5y_min', roe_5y_min,
               'roe_5y_all_above_15', roe_5y_all_above_15,
               'ocf_report_count_3y', ocf_report_count_3y,
               'ocf_3y_all_positive', ocf_3y_all_positive,
               'quarterly_report_count_4q', quarterly_report_count_4q,
               'q_sales_yoy_above_30_quarters_4q', q_sales_yoy_above_30_quarters_4q,
               'q_sales_yoy_4q_all_above_30', q_sales_yoy_4q_all_above_30,
               'list_date', list_date,
               'can_buy', can_buy, 'quality_status', quality_status
           ) AS rationale
    FROM ranked WHERE rank_no <= %s
    ORDER BY selection_track, rank_no
    """


def create_watchlist(
    snapshot_id: str,
    name: str,
    max_members: int = 50,
    *,
    allow_preview: bool = False,
    notes: str = "",
    created_by: str = "system",
) -> str:
    """生成观察池；默认只允许 FROZEN，预览模式允许 VALIDATED。"""
    if max_members <= 0:
        raise ValueError("max_members 必须大于 0")
    watchlist_id = str(uuid.uuid4())
    psycopg2, execute_values = require_psycopg2()
    Json = psycopg2.extras.Json
    with get_connection() as conn:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=False, autocommit=False)
        status, cutoff = _snapshot_status(conn, snapshot_id)
        if status != "FROZEN" and not (allow_preview and status == "VALIDATED"):
            raise RuntimeError(f"观察池默认只允许 FROZEN 快照: {snapshot_id} ({status})")
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO research.watchlist
                       (watchlist_id,snapshot_id,name,rule_version,status,max_members,created_by,notes)
                       VALUES (%s,%s,%s,%s,'DRAFT',%s,%s,%s)""",
                    (watchlist_id, snapshot_id, name, RULE_VERSION, max_members, created_by, notes),
                )
                cur.execute(_candidate_sql(), (snapshot_id, max_members))
                rows = cur.fetchall()
                values = [
                    (watchlist_id, snapshot_id, row[1], row[0], row[2], row[3], row[4], RULE_VERSION, row[5], "VALID")
                    for row in rows
                ]
                if values:
                    execute_values(
                        cur,
                        """INSERT INTO research.watchlist_member
                           (watchlist_id,snapshot_id,research_date,ts_code,rank_no,score,
                            selection_track,rule_version,rationale,quality_status)
                           VALUES %s""",
                        [(a,b,c,d,e,f,g,h,Json(i),j) for a,b,c,d,e,f,g,h,i,j in values],
                        page_size=500,
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return watchlist_id


def activate_watchlist(watchlist_id: str) -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("UPDATE research.watchlist SET status='ACTIVE' WHERE watchlist_id=%s", (watchlist_id,))
        if cur.rowcount != 1:
            raise ValueError(f"不存在的 watchlist_id: {watchlist_id}")
        conn.commit()


def add_evidence(
    snapshot_id: str,
    ts_code: str,
    source_system: str,
    source_type: str,
    source_locator: str,
    *,
    watchlist_id: str | None = None,
    document_index: str | None = None,
    document_id: str | None = None,
    source_url: str | None = None,
    content_hash: str | None = None,
    claim_role: str = "context",
    authority_level: str = "OTHER",
    published_at=None,
    effective_date=None,
    locator: dict | None = None,
    note: str | None = None,
) -> str:
    if authority_level not in {"OFFICIAL", "PROFESSIONAL", "MEDIA", "INTERNAL_FACT", "OTHER"}:
        raise ValueError("authority_level 不受支持")
    if claim_role not in {"supporting", "contrary", "context"}:
        raise ValueError("claim_role 不受支持")
    if claim_role in {"supporting", "contrary"} and (
        not content_hash or len(str(content_hash).strip()) != 64
    ):
        raise ValueError("支持/反对证据必须保存 64 位内容哈希")
    evidence_id = str(uuid.uuid4())
    psycopg2, _ = require_psycopg2()
    Json = psycopg2.extras.Json
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT status, research_cutoff_date
               FROM research.snapshot_manifest WHERE snapshot_id=%s""",
            (snapshot_id,),
        )
        snapshot = cur.fetchone()
        if not snapshot or snapshot[0] not in {"VALIDATED", "FROZEN"}:
            raise RuntimeError(f"证据引用要求 VALIDATED/FROZEN 快照: {snapshot_id}")
        cutoff = snapshot[1]
        published_day = published_at.date() if hasattr(published_at, "date") else published_at
        if published_day is not None and published_day > cutoff:
            raise ValueError("证据发布时间晚于 Research 截止日")
        if effective_date is not None and effective_date > cutoff:
            raise ValueError("证据生效日期晚于 Research 截止日")
        cur.execute(
            """SELECT 1 FROM research.stock_snapshot_daily
               WHERE snapshot_id=%s AND research_date=%s AND ts_code=%s""",
            (snapshot_id, snapshot[1], ts_code),
        )
        if cur.fetchone() is None:
            raise ValueError(f"股票不在快照中: {ts_code}")
        if watchlist_id:
            cur.execute(
                """SELECT snapshot_id FROM research.watchlist WHERE watchlist_id=%s""",
                (watchlist_id,),
            )
            watchlist = cur.fetchone()
            if not watchlist or watchlist[0] != snapshot_id:
                raise ValueError("watchlist_id 与 snapshot_id 不匹配")
            cur.execute(
                """SELECT 1 FROM research.watchlist_member
                   WHERE watchlist_id=%s AND snapshot_id=%s AND ts_code=%s""",
                (watchlist_id, snapshot_id, ts_code),
            )
            if cur.fetchone() is None:
                raise ValueError(f"股票不在观察池中: {ts_code}")
        cur.execute(
            """INSERT INTO research.evidence_reference
               (evidence_id,watchlist_id,snapshot_id,ts_code,source_system,source_type,
                source_locator,document_index,document_id,source_url,content_hash,
                claim_role,authority_level,published_at,effective_date,locator,note)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (evidence_id, watchlist_id, snapshot_id, ts_code, source_system, source_type,
             source_locator, document_index, document_id, source_url, content_hash,
             claim_role, authority_level, published_at, effective_date,
             Json(locator or {}), note),
        )
        conn.commit()
    return evidence_id


def add_snapshot_lineage_evidence(watchlist_id: str) -> int:
    """把观察池使用的财务、价格和估值快照血缘登记为 PostgreSQL 事实引用。

    这不是新闻/公告证据；外部证据仍需通过 ``add_evidence`` 绑定真实文档 ID。
    """
    psycopg2, _ = require_psycopg2()
    Json = psycopg2.extras.Json
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT w.snapshot_id, w.status, m.status, m.research_cutoff_date
               FROM research.watchlist w
               JOIN research.snapshot_manifest m ON m.snapshot_id=w.snapshot_id
               WHERE w.watchlist_id=%s""",
            (watchlist_id,),
        )
        parent = cur.fetchone()
        if not parent or parent[1] != "ACTIVE" or parent[2] != "FROZEN":
            raise RuntimeError("快照事实引用要求 ACTIVE 观察池和 FROZEN 快照")
        snapshot_id = parent[0]
        cur.execute(
            """SELECT m.ts_code, l.field_group, l.source_table, l.source_record_key,
                      l.source_record_version, l.source_payload_hash
               FROM research.watchlist_member m
               JOIN research.snapshot_lineage l
                 ON l.snapshot_id=m.snapshot_id AND l.research_date=m.research_date
                AND l.ts_code=m.ts_code
               WHERE m.watchlist_id=%s
                 AND l.field_group IN ('financial','price','valuation')
                 AND l.source_record_key NOT LIKE '%%:missing'
                 AND l.source_payload_hash IS NOT NULL""",
            (watchlist_id,),
        )
        inserted = 0
        for ts_code, group, source_table, key, version, payload_hash in cur.fetchall():
            cur.execute(
                """INSERT INTO research.evidence_reference
                   (evidence_id,watchlist_id,snapshot_id,ts_code,source_system,source_type,
                    source_locator,document_id,content_hash,claim_role,authority_level,
                    effective_date,locator,note)
                   VALUES (%s,%s,%s,%s,'postgres','snapshot_lineage',%s,%s,%s,
                           'context','INTERNAL_FACT',%s,%s,%s)
                   ON CONFLICT DO NOTHING""",
                (
                    str(uuid.uuid4()), watchlist_id, snapshot_id, ts_code,
                    f"{source_table}:{key}", key, payload_hash,
                    parent[3],
                    Json({"field_group": group, "source_table": source_table,
                          "source_record_key": key, "source_record_version": version}),
                    "Research 快照事实来源；不代表外部新闻或公告证据。",
                ),
            )
            inserted += cur.rowcount
        conn.commit()
    return inserted


def main():
    parser = argparse.ArgumentParser(description="Research 观察池与证据引用")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create")
    create.add_argument("--snapshot-id", required=True)
    create.add_argument("--name", required=True)
    create.add_argument("--max-members", type=int, default=50)
    create.add_argument("--allow-preview", action="store_true")
    create.add_argument("--notes", default="")
    activate = sub.add_parser("activate")
    activate.add_argument("--watchlist-id", required=True)
    lineage = sub.add_parser("lineage-evidence")
    lineage.add_argument("--watchlist-id", required=True)
    args = parser.parse_args()
    if args.command == "create":
        print(create_watchlist(args.snapshot_id, args.name, args.max_members,
                               allow_preview=args.allow_preview, notes=args.notes))
    elif args.command == "activate":
        activate_watchlist(args.watchlist_id)
    else:
        print(add_snapshot_lineage_evidence(args.watchlist_id))


if __name__ == "__main__":
    main()
