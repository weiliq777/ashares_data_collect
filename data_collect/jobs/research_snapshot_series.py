"""多日期 Research Snapshot 编排器。

一个 Snapshot 仍然只对应一个研究截止日；本模块只负责把多个单日 Snapshot
按确定顺序串成一组，供 Backtest Dataset Manifest 显式绑定。它不复制或修改
Raw/Standard/PIT/Feature，也不绕过单日 Snapshot 的质量门禁和 Git 冻结规则。
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from typing import Iterable

from data_collect.jobs.research_snapshot import (
    SCHEMA_VERSION,
    build_snapshots_batch,
    create_snapshots_batch,
    _baseline,
    _canonical,
    _quality_report,
    validate_snapshots_batch,
    existing_snapshot,
    freeze_snapshots_batch,
)
from data_collect.utils.db import get_connection


def normalize_cutoff_dates(cutoff_dates: Iterable[date | str]) -> tuple[date, ...]:
    """规范化并校验快照日期；重复日期必须显式消除，避免一个 Dataset 绑定歧义。"""
    parsed = tuple(
        item if isinstance(item, date) else date.fromisoformat(str(item))
        for item in cutoff_dates
    )
    if not parsed:
        raise ValueError("至少需要一个 Research Snapshot 截止日")
    unique = tuple(sorted(set(parsed)))
    if len(unique) != len(parsed):
        raise ValueError("Research Snapshot 截止日不能重复")
    return unique


def open_trade_dates(start: date, end: date, exchange: str = "SSE") -> tuple[date, ...]:
    """从已标准化交易日历读取开市日，不自行推算周末或节假日。"""
    if start > end:
        raise ValueError("交易日历起止日期无效")
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT trade_date
                 FROM trading_calendar
                WHERE exchange=%s AND is_open IS TRUE
                  AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date""",
            (exchange, start, end),
        )
        return tuple(row[0] for row in cur.fetchall())


def validate_cutoff_dates_are_open(cutoff_dates: Iterable[date | str], exchange: str = "SSE") -> tuple[date, ...]:
    """确保每个快照截止日都是标准交易日历中的开市日。"""
    dates = normalize_cutoff_dates(cutoff_dates)
    open_dates = set(open_trade_dates(dates[0], dates[-1], exchange))
    missing = [day.isoformat() for day in dates if day not in open_dates]
    if missing:
        raise ValueError(
            f"Research Snapshot 截止日必须是 {exchange} 开市日，以下日期无开市记录: {missing}"
        )
    return dates


def create_snapshot_series(
    cutoff_dates: Iterable[date | str],
    baseline_tag: str,
    schema_version: str = SCHEMA_VERSION,
    notes_prefix: str = "",
    build: bool = True,
    validate: bool = True,
    freeze: bool = False,
    code_archive_path: str | None = None,
    max_snapshots: int | None = 50,
    batch_size: int = 10,
    exchange: str = "SSE",
) -> tuple[dict[str, str], ...]:
    """按日期创建一组 Snapshot，并返回每个阶段的确定结果。

    ``freeze=True`` 必须同时执行 build/validate；正式冻结仍由单日
    ``research_snapshot.freeze`` 强制检查工作区和输入/质量哈希。
    """
    dates = normalize_cutoff_dates(cutoff_dates)
    if max_snapshots is not None:
        if max_snapshots <= 0:
            raise ValueError("max_snapshots 必须为正数，或使用 None 表示不设上限")
        if len(dates) > max_snapshots:
            raise ValueError(
                f"本次请求 {len(dates)} 个 Snapshot，超过安全上限 {max_snapshots}；"
                "请先做容量评估，确认后显式提高上限"
            )
    if batch_size <= 0:
        raise ValueError("batch_size 必须为正数")
    if validate and not build:
        raise ValueError("validate=True 必须同时 build=True")
    if freeze and not validate:
        raise ValueError("freeze=True 必须同时 validate=True")

    # 范围模式本身来自开市日查询；显式日期也必须经过同一门禁。
    dates = validate_cutoff_dates_are_open(dates, exchange=exchange)

    results: list[dict[str, str]] = []
    statuses: dict[str, str] = {}
    snapshots: dict[str, str] = {}
    for cutoff in dates:
        existing = existing_snapshot(cutoff.isoformat(), baseline_tag, schema_version)
        if existing:
            snapshot_id, status = existing
            # BUILDING is resumable: a process can exit after the build
            # transaction commits but before batch validation.  The next run
            # must continue through pending_validate instead of failing here.
        else:
            snapshot_id = ""
            status = "DRAFT"
        snapshots[cutoff.isoformat()] = snapshot_id
        statuses[cutoff.isoformat()] = status

    # Create all missing manifests in one batch; each date remains an
    # independent immutable Snapshot and can still be resumed by date.
    missing_dates = [cutoff for cutoff in dates if not snapshots[cutoff.isoformat()]]
    if missing_dates:
        created = create_snapshots_batch(
            missing_dates, baseline_tag, schema_version=schema_version,
            notes_prefix=notes_prefix,
        )
        for item in created:
            snapshots[item["research_cutoff_date"]] = item["snapshot_id"]
            statuses[item["research_cutoff_date"]] = "DRAFT"

    pending_build = [
        snapshots[cutoff.isoformat()]
        for cutoff in dates
        if build and statuses[cutoff.isoformat()] in {"DRAFT", "FAILED"}
    ]
    pending_validate_existing = [
        snapshots[cutoff.isoformat()]
        for cutoff in dates
        if validate and statuses[cutoff.isoformat()] == "BUILDING"
    ]
    batch_baseline = None
    batch_quality_reports = None
    if pending_build or pending_validate_existing:
        with get_connection() as conn:
            batch_baseline = _baseline(max(dates), baseline_tag, conn)
        base_report, _ = _quality_report(max(dates))
        batch_quality_reports = {}
        for cutoff in dates:
            report = dict(base_report)
            report["cutoff_date"] = cutoff.isoformat()
            encoded = json.dumps(
                _canonical(report), ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False,
            )
            batch_quality_reports[snapshots[cutoff.isoformat()]] = (
                report, hashlib.sha256(encoded.encode("utf-8")).hexdigest()
            )
    for offset in range(0, len(pending_build), batch_size):
        chunk = pending_build[offset:offset + batch_size]
        build_snapshots_batch(
            chunk,
            baseline=batch_baseline,
            quality_reports=batch_quality_reports,
        )
        print(
            f"snapshot build progress {min(offset + len(chunk), len(pending_build))}/{len(pending_build)}",
            flush=True,
        )
        for snapshot_id in chunk:
            for cutoff_key, current_id in snapshots.items():
                if current_id == snapshot_id:
                    statuses[cutoff_key] = "BUILDING"

    pending_validate = [
        snapshots[cutoff.isoformat()]
        for cutoff in dates
        if validate and statuses[cutoff.isoformat()] == "BUILDING"
    ]
    for offset in range(0, len(pending_validate), batch_size):
        chunk = pending_validate[offset:offset + batch_size]
        validate_snapshots_batch(
            chunk, baseline=batch_baseline, quality_reports=batch_quality_reports,
        )
        print(
            f"snapshot validate progress {min(offset + len(chunk), len(pending_validate))}/{len(pending_validate)}",
            flush=True,
        )
        for snapshot_id in chunk:
            for cutoff_key, current_id in snapshots.items():
                if current_id == snapshot_id:
                    statuses[cutoff_key] = "VALIDATED"

    # Freeze in bounded batches; no stock rows are written in this phase.
    if freeze:
        pending_freeze = [
            snapshots[cutoff.isoformat()]
            for cutoff in dates
            if statuses[cutoff.isoformat()] == "VALIDATED"
        ]
        for offset in range(0, len(pending_freeze), batch_size):
            chunk = pending_freeze[offset:offset + batch_size]
            freeze_snapshots_batch(chunk, code_archive_path=code_archive_path)
            print(
                f"snapshot freeze progress {min(offset + len(chunk), len(pending_freeze))}/{len(pending_freeze)}",
                flush=True,
            )
            for snapshot_id in chunk:
                for cutoff_key, current_id in snapshots.items():
                    if current_id == snapshot_id:
                        statuses[cutoff_key] = "FROZEN"

    for cutoff in dates:
        cutoff_key = cutoff.isoformat()
        results.append({"snapshot_id": snapshots[cutoff_key], "research_cutoff_date": cutoff_key})
    return tuple(results)


def main(argv=None):
    parser = argparse.ArgumentParser(description="创建多日期 Research Snapshot 编排任务")
    parser.add_argument("--cutoff-date", action="append", help="重复指定单日截止日 YYYY-MM-DD")
    parser.add_argument("--calendar-start", help="从标准化 SSE 交易日历读取范围起点")
    parser.add_argument("--calendar-end", help="从标准化 SSE 交易日历读取范围终点")
    parser.add_argument("--exchange", default="SSE")
    parser.add_argument("--baseline-tag", required=True)
    parser.add_argument("--schema-version", default=SCHEMA_VERSION)
    parser.add_argument("--notes-prefix", default="")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--no-validate", action="store_true")
    parser.add_argument("--freeze", action="store_true")
    parser.add_argument("--code-archive-path")
    parser.add_argument("--max-snapshots", type=int, default=50,
                        help="单次安全上限；传 0 表示取消上限（需自行确认容量）")
    parser.add_argument("--batch-size", type=int, default=10,
                        help="每批集合写入的 Snapshot 数量，默认 10")
    args = parser.parse_args(argv)

    explicit = tuple(args.cutoff_date or ())
    ranged = (args.calendar_start, args.calendar_end)
    if any(ranged) and not all(ranged):
        parser.error("--calendar-start 和 --calendar-end 必须同时提供")
    if explicit and any(ranged):
        parser.error("--cutoff-date 与交易日历范围只能选择一种")
    if not explicit and not all(ranged):
        parser.error("至少提供 --cutoff-date 或完整交易日历范围")
    dates = explicit or open_trade_dates(
        date.fromisoformat(args.calendar_start), date.fromisoformat(args.calendar_end), args.exchange
    )
    results = create_snapshot_series(
        dates,
        args.baseline_tag,
        schema_version=args.schema_version,
        notes_prefix=args.notes_prefix,
        build=not args.no_build,
        validate=not args.no_validate,
        freeze=args.freeze,
        code_archive_path=args.code_archive_path,
        max_snapshots=None if args.max_snapshots == 0 else args.max_snapshots,
        batch_size=args.batch_size,
        exchange=args.exchange,
    )
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
