"""按交易日批量同步 Tushare 行情 Raw，并按日重建 Standard。

历史初始化中的 ``adj_factor`` 继续按股票请求，便于长任务按股票断点续传；
日常增量使用 Tushare 的 ``trade_date`` 全市场接口，每个交易日每个数据集只
请求一次。Raw 只追加版本，不修改或删除已有 Raw 记录。
"""
from __future__ import annotations

import argparse
import logging
import time
import uuid
from datetime import date
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd

import data_collect.jobs.tushare_daily_incremental as market_shared
from data_collect.jobs.tushare_job_lock import warehouse_write_job
from data_collect.normalize.tushare_daily import normalize_daily, normalize_daily_basic
from data_collect.ops.market_coverage import (
    persist_market_coverage_blocked,
    persist_market_coverage_gate,
)
from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.data_access import DataAccessKernel
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_repository import MarketRepository
from data_collect.data_access.run_context import build_run_context


logger = logging.getLogger("tushare_market_incremental_by_date")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    log_dir = Path(__file__).resolve().parents[2] / "logs"
    log_dir.mkdir(exist_ok=True)
    handler = RotatingFileHandler(
        log_dir / "tushare_market_incremental_by_date.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())


DATASETS = {
    "daily": ("daily_kline", "daily"),
    "daily_basic": ("valuation_daily", "daily_basic"),
    "adj_factor": ("adj_factor_daily", "adj_factor"),
}

# Tushare 单次返回上限。达到上限时不能默认为数据完整。
ROW_LIMITS = {
    "daily": 6000,
    "daily_basic": 6000,
    "adj_factor": 6000,
}
MAX_INCREMENTAL_TRADE_DAYS = 31


def _research_baseline_cutoff(explicit: str | None = None):
    """读取 Research 发布水位；没有当前 validated 时可回退到 superseded。"""
    if explicit:
        return market_shared._date(explicit)
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = build_run_context(
        "tushare_market_incremental_baseline",
        pipeline_version="v1",
    )
    try:
        return MarketRepository(kernel).baseline_cutoff(context)
    finally:
        kernel.close()


def _normalize_for_count(dataset: str, frame: pd.DataFrame) -> pd.DataFrame:
    if dataset == "daily":
        return normalize_daily(frame)
    if dataset == "daily_basic":
        return normalize_daily_basic(frame)
    return frame


def _filter_sample(frame: pd.DataFrame, sample_codes: set[str] | None) -> pd.DataFrame:
    if not sample_codes or frame is None or frame.empty:
        return frame
    column = "ts_code" if "ts_code" in frame.columns else "stock_code"
    return frame[frame[column].isin(sample_codes)].copy()


def _request_market_frame(pro, dataset: str, day: str) -> pd.DataFrame:
    """按日请求；命中接口上限时按代码分片，避免静默截断。"""
    api = getattr(pro, DATASETS[dataset][1])
    frame = call_api(api, trade_date=day)
    limit = ROW_LIMITS[dataset]
    if frame is None or len(frame) < limit:
        return frame

    logger.warning(
        "dataset=%s trade_date=%s rows=%s reached limit=%s; fallback to code chunks",
        dataset,
        day,
        len(frame),
        limit,
    )
    chunks = []
    codes = market_shared._codes()
    for offset in range(0, len(codes), 1000):
        code_batch = ",".join(codes[offset : offset + 1000])
        if not code_batch:
            continue
        part = call_api(api, ts_code=code_batch, trade_date=day)
        if part is not None and not part.empty:
            chunks.append(part)
    if not chunks:
        return pd.DataFrame(columns=frame.columns)
    key = [column for column in ("ts_code", "trade_date") if column in chunks[0].columns]
    result = pd.concat(chunks, ignore_index=True)
    return result.drop_duplicates(subset=key) if key else result


def _sync_one_date(
    pro,
    dataset: str,
    day: str,
    batch_id: str,
    sample_codes: set[str] | None = None,
    dry_run: bool = False,
) -> int:
    """执行单日同步；任何物化异常都会追加 BLOCKED 门禁。"""
    state = {"actual_rows": 0}
    try:
        return _sync_one_date_impl(
            pro, dataset, day, batch_id, sample_codes, dry_run, state
        )
    except Exception as exc:
        if not dry_run and not sample_codes:
            try:
                blocked_batch = (
                    f"{batch_id}-blocked-{dataset}-{market_shared._date(day).strftime('%Y%m%d')}"
                )
                persist_market_coverage_blocked(
                    dataset, market_shared._date(day), state["actual_rows"], blocked_batch, str(exc)
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "failed to persist BLOCKED coverage gate dataset=%s trade_date=%s",
                    dataset,
                    day,
                )
        raise


def _sync_one_date_impl(
    pro,
    dataset: str,
    day: str,
    batch_id: str,
    sample_codes: set[str] | None = None,
    dry_run: bool = False,
    state: dict | None = None,
) -> int:
    standard_table, api_name = DATASETS[dataset]
    checkpoint_dataset = f"{dataset}_incremental_by_date"

    # 关键点：三类接口均按 trade_date 请求全市场，不再传入 ts_code 循环股票。
    frame = _request_market_frame(pro, dataset, day)
    frame = _filter_sample(frame, sample_codes)
    rows = 0 if frame is None else len(frame)
    if state is not None:
        state["actual_rows"] = (
            int(frame["ts_code"].nunique())
            if frame is not None and not frame.empty and "ts_code" in frame.columns
            else rows
        )
    if frame is None or frame.empty:
        message = "Tushare 返回空数据，未删除已有 Standard"
        logger.warning("dataset=%s trade_date=%s rows=0: %s", dataset, day, message)
        if not dry_run:
            gate = persist_market_coverage_gate(
                dataset, market_shared._date(day), 0, batch_id, sample_mode=bool(sample_codes)
            )
            if not sample_codes and not gate.publishable:
                raise RuntimeError(
                    f"{dataset} {day} 覆盖率门禁失败: {gate.status} {gate.reason}"
                )
            market_shared._checkpoint(
                checkpoint_dataset, day,
                "sample" if sample_codes else "failed",
                0,
                message if sample_codes else f"{message}; coverage_gate={gate.status}",
            )
        return 0

    limit = ROW_LIMITS[dataset]
    if rows >= limit and not sample_codes:
        raise RuntimeError(f"{dataset} {day} 分片后仍达到接口上限 {limit}，拒绝标记完成")

    if dry_run:
        transformed = _normalize_for_count(dataset, frame)
        logger.info(
            "dry-run dataset=%s trade_date=%s api_rows=%s normalized_rows=%s",
            dataset,
            day,
            rows,
            len(transformed),
        )
        return rows

    gate = persist_market_coverage_gate(
        dataset,
        market_shared._date(day),
        int(frame["ts_code"].nunique()) if "ts_code" in frame.columns else rows,
        batch_id,
        sample_mode=bool(sample_codes),
    )
    if not sample_codes and not gate.publishable:
        raise RuntimeError(
            f"{dataset} {day} 覆盖率门禁失败: {gate.status} {gate.reason}"
        )

    versions, snapshots = market_shared._version_and_snapshot(dataset, frame, batch_id)
    market_shared._rebuild_market_date(frame, dataset, market_shared._date(day))
    actual = market_shared._assert_standard_count(dataset, market_shared._date(day), standard_table)
    market_shared._checkpoint(checkpoint_dataset, day, "sample" if sample_codes else "done", versions)
    logger.info(
        "dataset=%s trade_date=%s api_rows=%s versions=%s raw_attempted=%s standard_rows=%s",
        dataset,
        day,
        rows,
        versions,
        snapshots,
        actual,
    )
    return versions


@warehouse_write_job
def run(
    end_date: str | None = None,
    lookback_days: int = 5,
    batch_pause: float = 1.0,
    dataset: str = "all",
    limit_stocks: int | None = None,
    dry_run: bool = False,
    baseline_cutoff: str | None = None,
    rebuild_derived: bool = False,
    run_date: str | None = None,
) -> str:
    """同步最近交易日。

    ``limit_stocks`` 和 ``dry_run`` 仅用于小范围验证，正式任务不应使用
    ``limit_stocks``，否则该批次不是完整的全市场快照。
    """
    if not 1 <= int(lookback_days) <= MAX_INCREMENTAL_TRADE_DAYS:
        raise ValueError("lookback_days 必须在 1..31；历史回填请使用专用分批任务")
    if dataset not in {"all", *DATASETS}:
        raise ValueError(f"不支持的数据集: {dataset}")
    if limit_stocks is not None and int(limit_stocks) <= 0:
        raise ValueError("limit_stocks 必须大于 0")
    if limit_stocks is not None and not dry_run:
        raise ValueError(
            "limit_stocks 仅允许与 dry_run 一起使用；小样本真实写入必须使用隔离 smoke 表"
        )

    if end_date and run_date and str(end_date).replace("-", "") != str(run_date).replace("-", ""):
        raise ValueError("end_date 与 run_date 不一致")
    end = (end_date or run_date or date.today().strftime("%Y%m%d")).replace("-", "")
    batch_id = uuid.uuid4().hex
    pro = get_pro()
    calendar = market_shared._trade_dates(pro, end, int(lookback_days))
    if calendar is None or calendar.empty:
        return "没有找到需要同步的交易日"
    if not dry_run:
        market_shared._version_and_snapshot("trade_cal", calendar, batch_id)

    cutoff = _research_baseline_cutoff(baseline_cutoff)
    calendar = calendar[pd.to_datetime(calendar["cal_date"]) > pd.Timestamp(cutoff)]
    if calendar.empty:
        return f"最近 {lookback_days} 个交易日均不晚于历史基线 {cutoff}，无需增量"

    days = calendar["cal_date"].astype(str).tolist()
    selected = list(DATASETS) if dataset == "all" else [dataset]
    sample_codes = None
    if limit_stocks:
        sample_codes = set(market_shared._codes()[: int(limit_stocks)])
        logger.warning("sample mode enabled: only %s stocks", len(sample_codes))

    logger.info(
        "date-batch incremental started batch_id=%s dates=%s datasets=%s",
        batch_id,
        days,
        selected,
    )
    total = 0
    materialized_partitions = 0
    for day in days:
        for current in selected:
            try:
                total += _sync_one_date(
                    pro,
                    current,
                    day,
                    batch_id,
                    sample_codes=sample_codes,
                    dry_run=dry_run,
                )
                materialized_partitions += int(not dry_run)
            except Exception as exc:  # noqa: BLE001
                logger.exception("date-batch failed dataset=%s trade_date=%s", current, day)
                if not dry_run:
                    market_shared._checkpoint(
                        f"{current}_incremental_by_date",
                        day,
                        "failed",
                        0,
                        str(exc)[:500],
                    )
                raise
            time.sleep(float(batch_pause))

    # Re-run the bounded derived interval even when every Raw payload was an
    # idempotent duplicate.  A previous attempt may have committed Raw and
    # Standard before failing in Feature; using only newly inserted Raw rows
    # as the trigger would make that failure impossible to resume.
    if rebuild_derived and not dry_run and materialized_partitions:
        from data_collect.jobs.tushare_derived_rebuild import rebuild_after_market
        try:
            rebuild_after_market(market_shared._date(days[0]), market_shared._date(days[-1]))
        except Exception as exc:
            # Per-date PASS 只证明接口覆盖率；全局派生重建失败时也必须
            # 追加一个最新 BLOCKED 事实，防止 Research freeze 误放行。
            last_day = market_shared._date(days[-1])
            try:
                persist_market_coverage_blocked(
                    "phase2_derived",
                    last_day,
                    0,
                    f"{batch_id}-derived-blocked",
                    str(exc),
                )
            except Exception:  # noqa: BLE001
                logger.exception("failed to persist derived rebuild BLOCKED gate")
            raise
    return (
        f"按交易日增量完成 batch_id={batch_id} dates={days} versions={total} "
        f"derived_rebuilt={rebuild_derived and not dry_run and bool(materialized_partitions)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Tushare 全市场按交易日增量同步")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--lookback-days", type=int, default=5)
    parser.add_argument("--batch-pause", type=float, default=1.0)
    parser.add_argument("--dataset", choices=["all", *DATASETS], default="all")
    parser.add_argument("--limit-stocks", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--baseline-cutoff",
        help="显式指定历史基线截止日 YYYYMMDD；未指定时读取数据库 validated 基线",
    )
    parser.add_argument("--rebuild-derived", action="store_true")
    parser.add_argument(
        "--no-rebuild-derived", dest="rebuild_derived", action="store_false",
        help=argparse.SUPPRESS,
    )
    parser.set_defaults(rebuild_derived=False)
    args = parser.parse_args()
    print(
        run(
            end_date=args.end_date,
            lookback_days=args.lookback_days,
            batch_pause=args.batch_pause,
            dataset=args.dataset,
            limit_stocks=args.limit_stocks,
            dry_run=args.dry_run,
            baseline_cutoff=args.baseline_cutoff,
            rebuild_derived=args.rebuild_derived,
        )
    )


if __name__ == "__main__":
    main()
