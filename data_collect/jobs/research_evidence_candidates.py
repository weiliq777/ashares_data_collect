"""Search external evidence candidates for an active research watchlist."""
from __future__ import annotations

import argparse
import json
from typing import Any

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.snapshot_repository import SnapshotRepository
from data_collect.utils.news_search import search_news


def _load_watchlist_members(watchlist_id: str) -> list[dict[str, Any]]:
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"research-evidence-{watchlist_id}",
        job_name="research_evidence_candidates",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="v1",
    )
    try:
        return SnapshotRepository(kernel).load_watchlist_members(watchlist_id, context)
    finally:
        kernel.close()


def search_candidates(
    watchlist_id: str,
    *,
    top_k: int = 5,
    mode: str = "bm25",
    channel: str | list[str] | None = None,
) -> list[dict[str, Any]]:
    if top_k <= 0:
        raise ValueError("top_k must be positive")
    candidates: list[dict[str, Any]] = []
    for member in _load_watchlist_members(watchlist_id):
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
    parser = argparse.ArgumentParser(description="Search research evidence candidates")
    parser.add_argument("--watchlist-id", required=True)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--mode", choices=("bm25", "rrf", "hybrid", "knn"), default="bm25")
    parser.add_argument("--channel", action="append")
    args = parser.parse_args()
    print(json.dumps(
        search_candidates(args.watchlist_id, top_k=args.top_k, mode=args.mode, channel=args.channel),
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()
