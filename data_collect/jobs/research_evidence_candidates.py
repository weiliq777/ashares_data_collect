"""Research 外部证据候选检索。

本模块只读查询已激活观察池对应的 OpenSearch 新闻/公告索引，返回供人工复核的
候选元数据；不会自动写入 ``research.evidence_reference``，也不会复制正文。
人工确认后使用 ``research_watchlist.add_evidence`` 绑定真实 document_id、URL 和
claim_role（supporting/contrary/context）。
"""
from __future__ import annotations

import argparse
import json
from typing import Any

from data_collect.utils.db import get_connection
from data_collect.utils.news_search import search_news


def _load_watchlist_members(watchlist_id: str) -> list[dict[str, Any]]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT w.snapshot_id, w.status, m.research_date, m.ts_code,
                      COALESCE(s.name, m.ts_code) AS display_name
                 FROM research.watchlist w
                 JOIN research.watchlist_member m ON m.watchlist_id=w.watchlist_id
                 JOIN research.stock_snapshot_daily s
                   ON s.snapshot_id=m.snapshot_id
                  AND s.research_date=m.research_date
                  AND s.ts_code=m.ts_code
                WHERE w.watchlist_id=%s
                ORDER BY m.rank_no, m.ts_code""",
            (watchlist_id,),
        )
        rows = cur.fetchall()
    if not rows:
        raise ValueError(f"不存在或没有成员的 watchlist_id: {watchlist_id}")
    if rows[0][1] != "ACTIVE":
        raise RuntimeError(f"外部证据候选只允许 ACTIVE 观察池: {watchlist_id} ({rows[0][1]})")
    return [
        {
            "snapshot_id": str(snapshot_id),
            "research_date": research_date.isoformat(),
            "ts_code": ts_code,
            "display_name": display_name,
        }
        for snapshot_id, _, research_date, ts_code, display_name in rows
    ]


def search_candidates(
    watchlist_id: str,
    *,
    top_k: int = 5,
    mode: str = "bm25",
    channel: str | list[str] | None = None,
) -> list[dict[str, Any]]:
    """按观察池成员检索候选文档，不产生数据库写入。

    查询上限使用观察池快照的 ``research_date``，防止把截止日之后的新闻混入
    历史研究。候选仅保留标题、来源、时间、URL、OpenSearch 文档身份和摘要片段。
    """
    if top_k <= 0:
        raise ValueError("top_k 必须大于 0")
    members = _load_watchlist_members(watchlist_id)
    candidates: list[dict[str, Any]] = []
    for member in members:
        result = search_news(
            member["display_name"],
            top_k=top_k,
            channel=channel,
            date_range=(None, member["research_date"]),
            stocks=[member["ts_code"]],
            mode=mode,
            include_identity=True,
        )
        for hit in result.get("hits", []):
            candidates.append(
                {
                    "snapshot_id": member["snapshot_id"],
                    "research_date": member["research_date"],
                    "ts_code": member["ts_code"],
                    "display_name": member["display_name"],
                    "source_system": "opensearch",
                    "source_type": "news_candidate",
                    "document_index": hit.get("document_index"),
                    "document_id": hit.get("document_id"),
                    "source_url": hit.get("url"),
                    "title": hit.get("title"),
                    "source": hit.get("source"),
                    "channel": hit.get("channel"),
                    "published_at": hit.get("pub_time"),
                    "score": hit.get("score"),
                    "highlight": hit.get("highlight"),
                    "review_status": "REVIEW_REQUIRED",
                }
            )
    return candidates


def main() -> None:
    parser = argparse.ArgumentParser(description="只读检索 Research 外部证据候选")
    parser.add_argument("--watchlist-id", required=True)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--mode", choices=("bm25", "rrf", "hybrid", "knn"), default="bm25")
    parser.add_argument("--channel", action="append")
    args = parser.parse_args()
    print(json.dumps(search_candidates(
        args.watchlist_id, top_k=args.top_k, mode=args.mode, channel=args.channel
    ), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
