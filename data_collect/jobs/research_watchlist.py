"""Research 观察池编排。

数据库访问全部委托给 ``ResearchWatchlistRepository``；本模块不持有连接、
游标，也不保存业务 SQL。
"""
from __future__ import annotations

import argparse
import uuid
from datetime import date

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.research_watchlist_repository import ResearchWatchlistRepository


RULE_VERSION = "watchlist-v2"


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"research-watchlist-{label}-{uuid.uuid4().hex}", job_name="research_watchlist",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, ResearchWatchlistRepository(kernel), context


def _candidate_sql():
    """Compatibility for tests/documentation; SQL remains Repository-owned."""
    return ResearchWatchlistRepository.CANDIDATE_SQL


def create_watchlist(
    snapshot_id: str, name: str, max_members: int = 50, *, allow_preview: bool = False,
    notes: str = "", created_by: str = "system",
) -> str:
    if max_members <= 0:
        raise ValueError("max_members 必须大于 0")
    kernel, repository, context = _service("create")
    try:
        return repository.create_watchlist(snapshot_id, name, max_members, notes, created_by,
                                           RULE_VERSION, allow_preview, context)
    finally:
        kernel.close()


def activate_watchlist(watchlist_id: str) -> None:
    kernel, repository, context = _service("activate")
    try:
        repository.activate(watchlist_id, context)
    finally:
        kernel.close()


def add_evidence(
    snapshot_id: str, ts_code: str, source_system: str, source_type: str, source_locator: str,
    *, watchlist_id: str | None = None, document_index: str | None = None,
    document_id: str | None = None, source_url: str | None = None, content_hash: str | None = None,
    claim_role: str = "context", authority_level: str = "OTHER", published_at=None,
    effective_date=None, locator: dict | None = None, note: str | None = None,
) -> str:
    if authority_level not in {"OFFICIAL", "PROFESSIONAL", "MEDIA", "INTERNAL_FACT", "OTHER"}:
        raise ValueError("authority_level 不受支持")
    if claim_role not in {"supporting", "contrary", "context"}:
        raise ValueError("claim_role 不受支持")
    if claim_role in {"supporting", "contrary"} and (not content_hash or len(str(content_hash).strip()) != 64):
        raise ValueError("支持/反对证据必须保存 64 位内容哈希")
    fields = {
        "watchlist_id": watchlist_id, "source_system": source_system, "source_type": source_type,
        "source_locator": source_locator, "document_index": document_index, "document_id": document_id,
        "source_url": source_url, "content_hash": content_hash, "claim_role": claim_role,
        "authority_level": authority_level, "published_at": published_at, "effective_date": effective_date,
        "locator": locator or {}, "note": note,
    }
    kernel, repository, context = _service("evidence")
    try:
        return repository.add_evidence(snapshot_id, ts_code, fields, context)
    finally:
        kernel.close()


def add_snapshot_lineage_evidence(watchlist_id: str) -> int:
    """把观察池使用的财务、价格和估值快照血缘登记为 PostgreSQL 事实引用。

    这不是新闻/公告证据；外部证据仍需通过 ``add_evidence`` 绑定真实文档 ID。
    """
    kernel, repository, context = _service("lineage-evidence")
    try:
        return repository.add_snapshot_lineage_evidence(watchlist_id, context)
    finally:
        kernel.close()


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
