"""管理 Tushare 历史数据基线发布点。

发布点只控制增量起始日期，不修改任何 Raw 或 Standard 数据。
"""
from __future__ import annotations

import argparse
from datetime import datetime

from data_collect.utils.db import get_connection


def _quality_gate() -> dict:
    """发布前执行阶段二硬门禁；失败时不改变任何发布状态。"""
    from data_collect.jobs.tushare_data_quality import inspect_phase2_checkpoint_completeness, inspect_phase2_invariants

    invariants = inspect_phase2_invariants()
    checkpoint = inspect_phase2_checkpoint_completeness()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM std.security_lifecycle a
            JOIN std.security_lifecycle b
              ON a.ts_code=b.ts_code AND a.lifecycle_id < b.lifecycle_id
             AND a.effective_from <= COALESCE(b.effective_to, DATE '9999-12-31')
             AND b.effective_from <= COALESCE(a.effective_to, DATE '9999-12-31')
        """)
        lifecycle_overlap = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM (
              SELECT business_key FROM std.dividend_event
              GROUP BY business_key HAVING COUNT(*) > 1
            ) d
        """)
        dividend_duplicates = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM std.dividend_event
            WHERE implementation_status='实施' AND ex_date IS NULL
        """)
        implementation_without_ex_date = cur.fetchone()[0]

    failures = {}
    for name, value in invariants.items():
        if isinstance(value, dict) or value != 0:
            failures[name] = value
    for name, value in checkpoint.items():
        if value.get("missing", 0) != 0 or value.get("failed", 0) != 0:
            failures[f"checkpoint:{name}"] = value
    # 实施事件缺少 ex_date 属于来源事实警告：Standard 保留，Feature 已排除；
    # 不应因为上游缺字段而替换已验证基线。真正影响可审计性的重复/穿越/重叠仍阻断发布。
    for name, value in {
        "lifecycle_overlap": lifecycle_overlap,
        "dividend_business_key_duplicates": dividend_duplicates,
    }.items():
        if value != 0:
            failures[name] = value
    return {"invariants": invariants, "checkpoints": checkpoint,
            "lifecycle_overlap": lifecycle_overlap,
            "dividend_business_key_duplicates": dividend_duplicates,
            "implementation_without_ex_date": implementation_without_ex_date,
            "failures": failures}


def create(tag: str, cutoff: str, notes: str = "", schema_version: str = "") -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO tushare_data_release
               (release_tag, baseline_cutoff_date, status, notes, schema_version)
               VALUES (%s, %s, 'draft', %s, %s)""",
            (tag, datetime.strptime(cutoff, "%Y%m%d").date(), notes, schema_version or None),
        )
        conn.commit()


def validate(tag: str, quality_report_hash: str = "") -> None:
    gate = _quality_gate()
    if gate["failures"]:
        raise RuntimeError(f"发布质量门禁未通过，保留现有 validated 基线: {gate['failures']}")
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE tushare_data_release
               SET status='validated', quality_report_hash=%s,
                   validated_at=CURRENT_TIMESTAMP
               WHERE release_tag=%s AND status='draft'""",
            (quality_report_hash or None, tag),
        )
        if cur.rowcount != 1:
            conn.rollback()
            raise RuntimeError(f"找不到 draft 发布点或发布点状态不正确: {tag}")
        cur.execute(
            "UPDATE tushare_data_release SET status='superseded' WHERE status='validated' AND release_tag<>%s",
            (tag,),
        )
        conn.commit()


def status() -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT release_tag, baseline_cutoff_date, status,
                      quality_report_hash, created_at, validated_at
               FROM tushare_data_release ORDER BY release_id"""
        )
        for row in cur.fetchall():
            print(" | ".join("" if value is None else str(value) for value in row))


def main() -> None:
    parser = argparse.ArgumentParser(description="Tushare 历史基线发布点管理")
    sub = parser.add_subparsers(dest="command", required=True)
    create_parser = sub.add_parser("create")
    create_parser.add_argument("--tag", required=True)
    create_parser.add_argument("--cutoff", required=True, help="YYYYMMDD")
    create_parser.add_argument("--notes", default="")
    create_parser.add_argument("--schema-version", default="")
    validate_parser = sub.add_parser("validate")
    validate_parser.add_argument("--tag", required=True)
    validate_parser.add_argument("--quality-report-hash", default="")
    sub.add_parser("status")
    args = parser.parse_args()
    if args.command == "create":
        create(args.tag, args.cutoff, args.notes, args.schema_version)
    elif args.command == "validate":
        validate(args.tag, args.quality_report_hash)
    else:
        status()


if __name__ == "__main__":
    main()
