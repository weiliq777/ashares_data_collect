"""阶段三回测输入的时间截面与样本外校验工具。"""
from __future__ import annotations

from dataclasses import replace
from datetime import date
import argparse
import json
from typing import Iterable

from .models import BacktestDataset, Observation


def validate_observation_cutoffs(
    observations: Iterable[Observation],
    decision_date: date,
) -> list[str]:
    """返回决策日之后仍被暴露或使用的 PIT 记录问题。"""
    issues: list[str] = []
    for observation in observations:
        if observation.research_date > decision_date:
            issues.append(f"observation date after decision date: {observation.ts_code}/{observation.research_date}")
        effective = observation.get("effective_announce_date")
        if effective is not None and effective > observation.research_date:
            issues.append(
                f"financial availability after research date: "
                f"{observation.ts_code}/{effective}>{observation.research_date}"
            )
    return issues


def truncate_dataset_asof(dataset: BacktestDataset, cutoff_date: date) -> BacktestDataset:
    """生成只包含 cutoff 之前数据的测试副本，不修改原 Dataset。"""
    if cutoff_date < dataset.evaluation_start:
        raise ValueError("cutoff_date 不能早于 evaluation_start")
    calendar = tuple(day for day in dataset.calendar if day <= cutoff_date)
    if not calendar:
        raise ValueError("cutoff_date 前没有可用交易日")
    evaluation_end = min(dataset.evaluation_end, cutoff_date, calendar[-1])
    observations = {
        day: rows for day, rows in dataset.observations.items() if day <= cutoff_date
    }
    bars = {
        key: value for key, value in dataset.bars.items() if key[0] <= cutoff_date
    }
    dividends = {
        day: rows for day, rows in dataset.dividends.items() if day <= cutoff_date
    }
    member_roles = {day: role for day, role in dataset.member_roles.items() if day <= cutoff_date}
    snapshot_ids = {day: value for day, value in dataset.snapshot_ids.items() if day <= cutoff_date}
    rules = {key: value for key, value in dataset.rules.items() if key[0] <= cutoff_date}
    truncated = replace(
        dataset,
        calendar=calendar,
        evaluation_end=evaluation_end,
        observations=observations,
        bars=bars,
        dividends=dividends,
        member_roles=member_roles,
        snapshot_ids=snapshot_ids,
        rules=rules,
    )
    issues = validate_observation_cutoffs(
        (observation for rows in truncated.observations.values() for observation in rows),
        cutoff_date,
    )
    if issues:
        raise ValueError(f"截面校验失败: {issues[:5]}")
    return truncated


def assert_no_future_rows(dataset: BacktestDataset, cutoff_date: date) -> None:
    """测试/验收门禁：Dataset 不得含 cutoff 后的日历、行情、快照或公司行为。"""
    future_calendar = [day for day in dataset.calendar if day > cutoff_date]
    future_bars = [key for key in dataset.bars if key[0] > cutoff_date]
    future_observations = [day for day in dataset.observations if day > cutoff_date]
    future_dividends = [day for day in dataset.dividends if day > cutoff_date]
    if future_calendar or future_bars or future_observations or future_dividends:
        raise AssertionError({
            "calendar": future_calendar[:3],
            "bars": future_bars[:3],
            "observations": future_observations[:3],
            "dividends": future_dividends[:3],
        })


def verify_pit_cutoff_consistency(
    target_date: date | str,
    snapshot_id: str | None = None,
    connection=None,
) -> dict:
    """只读检查数据库中的 PIT/Feature/Research 输入没有越过截面日期。

    这不是重建任务，也不修改任何表。它检查财务公告可见日、Feature 的
    ``source_asof_date``、交易状态来源日期以及指定 Research 快照的截止日。
    ``connection`` 仅用于测试注入；正式调用会开启只读事务。
    """
    from data_collect.utils.db import get_connection

    cutoff = target_date if isinstance(target_date, date) else date.fromisoformat(str(target_date))
    owned = connection is None
    conn = connection or get_connection()
    checks: list[dict] = []
    issues: list[str] = []

    try:
        if owned:
            conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            queries = (
                (
                    "financial_asof_announcement",
                    """SELECT COUNT(*) FROM pit.financial_asof
                        WHERE decision_date=%s
                          AND effective_announce_date > decision_date""",
                    (cutoff,),
                ),
                (
                    "price_feature_source_asof",
                    """SELECT COUNT(*) FROM feature.price_daily_v2
                        WHERE trade_date=%s AND source_asof_date > trade_date""",
                    (cutoff,),
                ),
                (
                    "valuation_feature_source_asof",
                    """SELECT COUNT(*) FROM feature.valuation_daily_v1
                        WHERE trade_date=%s AND source_asof_date > trade_date""",
                    (cutoff,),
                ),
                (
                    "tradeability_source_asof",
                    """SELECT COUNT(*) FROM pit.security_tradeability_daily
                        WHERE trade_date=%s AND source_asof_date > trade_date""",
                    (cutoff,),
                ),
                (
                    "research_snapshot_financial_visibility",
                    """SELECT COUNT(*) FROM research.stock_snapshot_daily
                        WHERE research_date=%s
                          AND effective_announce_date > research_date""",
                    (cutoff,),
                ),
            )
            for name, sql, params in queries:
                cur.execute(sql, params)
                count = int(cur.fetchone()[0])
                checks.append({"name": name, "violations": count})
                if count:
                    issues.append(f"{name}={count}")

            if snapshot_id:
                cur.execute(
                    """SELECT status, research_cutoff_date
                         FROM research.snapshot_manifest
                        WHERE snapshot_id=%s""",
                    (snapshot_id,),
                )
                manifest = cur.fetchone()
                if not manifest:
                    issues.append(f"snapshot_not_found={snapshot_id}")
                    checks.append({"name": "snapshot_manifest", "violations": 1})
                else:
                    status, research_cutoff = manifest
                    manifest_violations = int(
                        status not in {"VALIDATED", "FROZEN"} or research_cutoff != cutoff
                    )
                    checks.append({
                        "name": "snapshot_manifest",
                        "violations": manifest_violations,
                        "status": status,
                        "research_cutoff_date": research_cutoff.isoformat(),
                    })
                    if manifest_violations:
                        issues.append(
                            f"snapshot_manifest status/cutoff invalid: {status}/{research_cutoff}"
                        )
    finally:
        if owned:
            try:
                conn.rollback()
            finally:
                conn.close()

    return {
        "target_date": cutoff.isoformat(),
        "snapshot_id": snapshot_id,
        "passed": not issues,
        "checks": checks,
        "issues": issues,
    }


def assert_pit_cutoff_consistency(target_date: date | str, snapshot_id: str | None = None) -> None:
    report = verify_pit_cutoff_consistency(target_date, snapshot_id=snapshot_id)
    if not report["passed"]:
        raise RuntimeError(f"PIT 截面校验失败: {report['issues']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="阶段三 PIT 截面只读校验")
    parser.add_argument("--target-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--snapshot-id")
    args = parser.parse_args()
    report = verify_pit_cutoff_consistency(args.target_date, args.snapshot_id)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
