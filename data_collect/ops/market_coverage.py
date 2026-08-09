"""按日市场覆盖率门禁与熔断记录。

该模块只读取已有 Raw 版本计数并追加质量门禁记录；它不负责抓取数据，也不修改
任何 Raw/Standard/PIT/Feature。抽样模式的结果永远不可用于正式发布。
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from statistics import median
from typing import Iterable

from data_collect.utils.db import get_connection


@dataclass(frozen=True)
class CoverageGateResult:
    dataset: str
    trade_date: date
    actual_rows: int
    expected_rows: float | None
    baseline_days: int
    threshold_pct: float
    drop_pct: float | None
    status: str
    publishable: bool
    reason: str
    sample_mode: bool = False


def _as_date(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def evaluate_market_coverage(
    dataset: str,
    trade_date: date | str,
    actual_rows: int,
    baseline_counts: Iterable[int],
    threshold_pct: float = 0.01,
    sample_mode: bool = False,
) -> CoverageGateResult:
    """根据前几个完整交易日的股票数判断当日快照是否可发布。"""
    if not dataset or not str(dataset).strip():
        raise ValueError("dataset 不能为空")
    if int(actual_rows) < 0:
        raise ValueError("actual_rows 不能为负数")
    if not 0 <= float(threshold_pct) < 1:
        raise ValueError("threshold_pct 必须位于 [0, 1)")

    day = _as_date(trade_date)
    if sample_mode:
        return CoverageGateResult(
            str(dataset), day, int(actual_rows), None, 0, float(threshold_pct),
            None, "SKIPPED_SAMPLE", False, "抽样结果不可代表全市场覆盖率", True,
        )

    positive = [int(value) for value in baseline_counts if int(value) > 0]
    if not positive:
        return CoverageGateResult(
            str(dataset), day, int(actual_rows), None, 0, float(threshold_pct),
            None, "NO_BASELINE", False, "没有可用的历史全市场基准，拒绝发布", False,
        )

    expected = float(median(positive))
    drop_pct = max(0.0, (expected - int(actual_rows)) / expected)
    passed = int(actual_rows) >= expected * (1.0 - float(threshold_pct))
    return CoverageGateResult(
        str(dataset), day, int(actual_rows), expected, len(positive),
        float(threshold_pct), drop_pct, "PASS" if passed else "BLOCKED", passed,
        "覆盖率在允许阈值内" if passed else f"覆盖率下降 {drop_pct:.2%}，超过阈值 {threshold_pct:.2%}",
        False,
    )


def _previous_counts(conn, dataset: str, trade_date: date, baseline_days: int) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT trade_date, COUNT(DISTINCT ts_code)
                 FROM tushare_market_raw_version
                WHERE dataset=%s AND trade_date < %s
                GROUP BY trade_date
                ORDER BY trade_date DESC
                LIMIT %s""",
            (dataset, trade_date, int(baseline_days)),
        )
        return [int(row[1]) for row in cur.fetchall()]


def persist_market_coverage_gate(
    dataset: str,
    trade_date: date | str,
    actual_rows: int,
    batch_id: str,
    *,
    threshold_pct: float = 0.01,
    baseline_days: int = 5,
    sample_mode: bool = False,
) -> CoverageGateResult:
    """读取历史 Raw 覆盖量并追加一条幂等门禁记录。"""
    if not batch_id:
        raise ValueError("batch_id 不能为空")
    if int(baseline_days) <= 0:
        raise ValueError("baseline_days 必须大于 0")
    day = _as_date(trade_date)
    with get_connection() as conn, conn.cursor() as cur:
        counts = [] if sample_mode else _previous_counts(conn, dataset, day, baseline_days)
        result = evaluate_market_coverage(
            dataset, day, actual_rows, counts, threshold_pct, sample_mode
        )
        cur.execute(
            """INSERT INTO ops.market_data_quality_gate
                 (dataset, trade_date, actual_rows, expected_rows, baseline_days,
                  threshold_pct, drop_pct, status, sample_mode, batch_id, reason)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (dataset, trade_date, batch_id) DO NOTHING""",
            (result.dataset, result.trade_date, result.actual_rows, result.expected_rows,
             result.baseline_days, result.threshold_pct, result.drop_pct, result.status,
             result.sample_mode, batch_id, result.reason),
        )
        conn.commit()
    return result


def persist_market_coverage_blocked(
    dataset: str,
    trade_date: date | str,
    actual_rows: int,
    batch_id: str,
    reason: str,
) -> CoverageGateResult:
    """追加一条发布阻断事实，覆盖“门禁通过后物化失败”的窗口。

    覆盖率 PASS 只证明接口响应的行数达到历史基准，并不证明 Raw、Standard
    和派生层已经完成写入。同步中途失败时必须追加 BLOCKED，而不能修改或
    删除先前的 PASS；Research 冻结读取该日最新记录并因此 fail-closed。
    """
    if not batch_id:
        raise ValueError("batch_id 不能为空")
    if not reason or not str(reason).strip():
        raise ValueError("阻断原因不能为空")
    day = _as_date(trade_date)
    with get_connection() as conn, conn.cursor() as cur:
        counts = _previous_counts(conn, dataset, day, 5)
        result = evaluate_market_coverage(dataset, day, actual_rows, counts)
        blocked = replace(
            result,
            status="BLOCKED",
            publishable=False,
            reason=f"同步/物化失败：{str(reason)[:500]}",
        )
        cur.execute(
            """INSERT INTO ops.market_data_quality_gate
                 (dataset, trade_date, actual_rows, expected_rows, baseline_days,
                  threshold_pct, drop_pct, status, sample_mode, batch_id, reason)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,FALSE,%s,%s)
               ON CONFLICT (dataset, trade_date, batch_id) DO NOTHING""",
            (blocked.dataset, blocked.trade_date, blocked.actual_rows,
             blocked.expected_rows, blocked.baseline_days, blocked.threshold_pct,
             blocked.drop_pct, blocked.status, batch_id, blocked.reason),
        )
        conn.commit()
    return blocked


def latest_market_coverage_failures(
    trade_date: date | str,
    connection=None,
) -> list[tuple]:
    """返回指定日期每个数据集的最新非 PASS 门禁；没有记录不视为失败。"""
    day = _as_date(trade_date)
    if connection is None:
        with get_connection() as conn:
            return latest_market_coverage_failures(day, connection=conn)
    with connection.cursor() as cur:
        cur.execute(
            """SELECT DISTINCT ON (dataset) dataset, status, reason, batch_id, created_at
                 FROM ops.market_data_quality_gate
                WHERE trade_date=%s
                ORDER BY dataset, created_at DESC, gate_id DESC""",
            (day,),
        )
        rows = cur.fetchall()
    return [row for row in rows if row[1] != "PASS"]


def latest_market_coverage_failures_in_range(
    start_date: date | str,
    end_date: date | str,
    connection=None,
) -> list[tuple]:
    """返回区间内每个数据集/日期最新的非 PASS 门禁。

    没有记录的历史日期不视为失败：它们由已验证历史基线覆盖；一旦某日有
    门禁记录，则必须以该日该数据集的最新 PASS 才能继续物化或发布。
    """
    start = _as_date(start_date)
    end = _as_date(end_date)
    if start > end:
        raise ValueError("门禁区间起点不能晚于终点")
    if connection is None:
        with get_connection() as conn:
            return latest_market_coverage_failures_in_range(start, end, connection=conn)
    with connection.cursor() as cur:
        cur.execute(
            """SELECT dataset, trade_date, status, reason, batch_id, created_at
                 FROM (
                   SELECT DISTINCT ON (dataset, trade_date)
                          dataset, trade_date, status, reason, batch_id, created_at
                     FROM ops.market_data_quality_gate
                    WHERE trade_date BETWEEN %s AND %s
                    ORDER BY dataset, trade_date, created_at DESC, gate_id DESC
                 ) latest
                WHERE status <> 'PASS'
                ORDER BY trade_date, dataset""",
            (start, end),
        )
        rows = cur.fetchall()
    return rows


def assert_market_coverage_publishable(trade_date: date | str) -> None:
    failures = latest_market_coverage_failures(trade_date)
    if failures:
        raise RuntimeError(f"市场覆盖率门禁未通过，禁止发布 {trade_date}: {failures}")


def assert_market_coverage_range_publishable(
    start_date: date | str,
    end_date: date | str,
    connection=None,
) -> None:
    failures = latest_market_coverage_failures_in_range(
        start_date, end_date, connection=connection
    )
    if failures:
        raise RuntimeError(
            "市场覆盖率区间门禁未通过，禁止物化 Dataset: "
            f"{failures[:20]}"
        )
