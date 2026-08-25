"""阶段二低频/日常增量 Raw 同步。

历史初始化由 ``tushare_phase2_raw_init`` 完成并在数据库中冻结基线；本任务只处理
基线之后的最近交易日和事件窗口，不重跑历史。所有响应先追加到 Raw，后续派生表
可选择重建；Raw 永不更新、删除或覆盖。
"""
from __future__ import annotations

import argparse
import logging
import time
import uuid
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from uuid import uuid4

import pandas as pd

from data_collect.jobs.tushare_phase2_raw_init import (
    _date,
    _save_frame,
    _trade_dates,
)
from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.jobs.tushare_job_lock import warehouse_write_job
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.history_repository import HistoryRepository
from data_collect.data_access.repository.phase2_raw_repository import Phase2RawRepository


logger = logging.getLogger("tushare_phase2_incremental")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    log_dir = Path(__file__).resolve().parents[2] / "logs"
    log_dir.mkdir(exist_ok=True)
    handler = RotatingFileHandler(
        log_dir / "tushare_phase2_incremental.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())


DATE_DATASETS = {
    "stock_st": "tushare_stock_st_raw",
    "suspend_d": "tushare_suspend_d_raw",
    "stk_limit": "tushare_stk_limit_raw",
}
ROW_LIMITS = {"stock_st": 1000, "stk_limit": 5800}
INDEX_CODES = ("000300.SH", "000905.SH", "000852.SH", "000016.SH")


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"phase2-incremental-{label}-{uuid4().hex}", job_name="tushare_phase2_incremental",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, Phase2RawRepository(kernel), HistoryRepository(kernel), context


def _baseline_cutoff(explicit: str | None = None):
    if explicit:
        return _date(explicit)
    kernel, repository, _, context = _service("baseline")
    try:
        value = repository.baseline_cutoff(context)
    finally:
        kernel.close()
    if value is None:
        raise RuntimeError("未找到 validated 数据基线，请先建立历史发布点")
    return value


def _checkpoint(dataset: str, key: str, status: str, rows: int = 0, error: str | None = None):
    kernel, _, history, context = _service("checkpoint")
    try:
        return history.save(dataset, key, status, rows, context, error)
    finally:
        kernel.close()


def _calendar_days(start, end) -> list[str]:
    current = start
    result = []
    while current <= end:
        result.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return result


def _trading_days(end: str, lookback_days: int, cutoff):
    frame = _trade_dates(
        (pd.to_datetime(end) - timedelta(days=max(lookback_days * 3, 10))).strftime("%Y%m%d"),
        end,
        lookback_days,
    )
    if not frame:
        return []
    return [day for day in frame if _date(day) > cutoff]


def _refresh_trade_calendar(pro, start: str, end: str, dry_run: bool) -> int:
    """Refresh the local calendar before deriving phase-2 trading-day windows.

    The market-by-date job queries Tushare's calendar directly, while the
    phase-2 job historically read only ``tushare_trade_cal_raw``.  Without
    this refresh, a successful market increment could still leave status,
    limit, tradeability and universe derivations stuck at the old calendar
    cutoff.  Calendar rows are append-only and idempotent in Raw.
    """
    frame = call_api(pro.trade_cal, exchange="SSE", start_date=start, end_date=end)
    rows = 0 if frame is None else len(frame)
    if dry_run:
        logger.info("dry-run trade_cal window=%s:%s api_rows=%s", start, end, rows)
        return rows
    if rows:
        from data_collect.jobs.tushare_daily_incremental import _version_and_snapshot
        from data_collect.jobs.tushare_raw_to_standard import rebuild_calendar

        _version_and_snapshot(
            "trade_cal", frame, f"phase2-calendar-{uuid.uuid4().hex}"
        )
        # The calendar refresh is an incremental window. Rebuilding the
        # entire historical calendar here turns every phase2 run into a full
        # Raw->Standard scan and defeats the bounded-work contract.
        rebuild_calendar(start_date=_date(start), end_date=_date(end))
    logger.info("trade_cal refreshed window=%s:%s api_rows=%s", start, end, rows)
    return rows


def _stock_codes() -> list[str]:
    kernel, _, history, context = _service("stock-codes")
    try:
        return history.active_stock_codes(context)
    finally:
        kernel.close()


def _request_date_frame(pro, dataset: str, day: str) -> pd.DataFrame:
    frame = call_api(getattr(pro, dataset), trade_date=day)
    limit = ROW_LIMITS.get(dataset)
    if frame is None or not limit or len(frame) < limit:
        return frame

    # 接近/达到上限时按股票分片，防止把截断结果误当作全市场快照。
    logger.warning(
        "dataset=%s trade_date=%s rows=%s reached limit=%s; fallback to code chunks",
        dataset,
        day,
        len(frame),
        limit,
    )
    parts = []
    codes = _stock_codes()
    for offset in range(0, len(codes), 1000):
        code_batch = ",".join(codes[offset : offset + 1000])
        if not code_batch:
            continue
        part = call_api(getattr(pro, dataset), ts_code=code_batch, trade_date=day)
        if part is not None and not part.empty:
            parts.append(part)
    if not parts:
        return pd.DataFrame(columns=frame.columns)
    result = pd.concat(parts, ignore_index=True)
    key = [column for column in ("ts_code", "trade_date") if column in result.columns]
    return result.drop_duplicates(subset=key) if key else result


def _run_date_datasets(pro, days: list[str], batch_id: str, pause: float, dry_run: bool) -> int:
    total = 0
    for day in days:
        for dataset, table in DATE_DATASETS.items():
            try:
                frame = _request_date_frame(pro, dataset, day)
                rows = 0 if frame is None else len(frame)
                limit = ROW_LIMITS.get(dataset)
                if limit and rows >= limit:
                    raise RuntimeError(
                        f"{dataset} {day} 分片后仍达到接口上限 {limit}，拒绝完成"
                    )
                if dry_run:
                    logger.info("dry-run dataset=%s date=%s api_rows=%s", dataset, day, rows)
                elif rows == 0:
                    _checkpoint(f"{dataset}_incremental", day, "empty", 0, "接口返回空数据")
                else:
                    inserted = _save_frame(table, frame, batch_id)
                    total += inserted
                    _checkpoint(f"{dataset}_incremental", day, "done", inserted)
                    logger.info(
                        "dataset=%s date=%s api_rows=%s inserted=%s",
                        dataset,
                        day,
                        rows,
                        inserted,
                    )
            except Exception as exc:  # noqa: BLE001
                logger.exception("date dataset failed dataset=%s date=%s", dataset, day)
                if not dry_run:
                    _checkpoint(f"{dataset}_incremental", day, "failed", 0, str(exc)[:500])
                raise
            time.sleep(float(pause))
    return total


def _run_dividend(
    pro,
    days: list[str],
    batch_id: str,
    pause: float,
    dry_run: bool,
    affected_ex_dates: list[date] | None = None,
) -> int:
    """按公告/实施公告日期查询，不再逐只股票扫描。"""
    total = 0
    for field in ("ann_date", "imp_ann_date"):
        for day in days:
            key = f"{field}:{day}"
            try:
                frame = call_api(pro.dividend, **{field: day})
                rows = 0 if frame is None else len(frame)
                if dry_run:
                    logger.info("dry-run dividend filter=%s date=%s api_rows=%s", field, day, rows)
                elif rows == 0:
                    _checkpoint("dividend_incremental", key, "empty", 0, "接口返回空数据")
                else:
                    if affected_ex_dates is not None and "ex_date" in frame.columns:
                        affected_ex_dates.extend(
                            value.date()
                            for value in pd.to_datetime(
                                frame["ex_date"], format="%Y%m%d", errors="coerce"
                            ).dropna()
                        )
                    inserted = _save_frame("tushare_dividend_raw", frame, batch_id)
                    total += inserted
                    _checkpoint("dividend_incremental", key, "done", inserted)
                    logger.info(
                        "dividend filter=%s date=%s api_rows=%s inserted=%s",
                        field,
                        day,
                        rows,
                        inserted,
                    )
            except Exception as exc:  # noqa: BLE001
                logger.exception("dividend failed filter=%s date=%s", field, day)
                if not dry_run:
                    _checkpoint("dividend_incremental", key, "failed", 0, str(exc)[:500])
                raise
            time.sleep(float(pause))
    return total


def _run_namechange(pro, start: str, end: str, batch_id: str, pause: float, dry_run: bool) -> int:
    # 本地接口文档提供 start_date/end_date；该任务低频运行，保留窗口结果并由 hash 幂等。
    key = f"{start}:{end}"
    try:
        frame = call_api(pro.namechange, start_date=start, end_date=end)
        rows = 0 if frame is None else len(frame)
        if dry_run:
            logger.info("dry-run namechange window=%s:%s api_rows=%s", start, end, rows)
            return 0
        if rows == 0:
            _checkpoint("namechange_incremental", key, "empty", 0, "接口返回空数据")
            return 0
        inserted = _save_frame("tushare_namechange_raw", frame, batch_id)
        _checkpoint("namechange_incremental", key, "done", inserted)
        logger.info("namechange window=%s:%s api_rows=%s inserted=%s", start, end, rows, inserted)
        time.sleep(float(pause))
        return inserted
    except Exception as exc:  # noqa: BLE001
        logger.exception("namechange failed window=%s:%s", start, end)
        if not dry_run:
            _checkpoint("namechange_incremental", key, "failed", 0, str(exc)[:500])
        raise


def _run_index(pro, start: str, end: str, batch_id: str, pause: float, dry_run: bool) -> int:
    total = 0
    for code in INDEX_CODES:
        key = f"{code}:{start}:{end}"
        try:
            frame = call_api(pro.index_daily, ts_code=code, start_date=start, end_date=end)
            rows = 0 if frame is None else len(frame)
            if dry_run:
                logger.info("dry-run index_daily code=%s rows=%s", code, rows)
            elif rows == 0:
                _checkpoint("index_daily_incremental", key, "empty", 0, "接口返回空数据")
            else:
                inserted = _save_frame("tushare_index_daily_raw", frame, batch_id)
                total += inserted
                _checkpoint("index_daily_incremental", key, "done", inserted)
                logger.info("index_daily code=%s api_rows=%s inserted=%s", code, rows, inserted)
        except Exception as exc:  # noqa: BLE001
            logger.exception("index_daily failed code=%s", code)
            if not dry_run:
                _checkpoint("index_daily_incremental", key, "failed", 0, str(exc)[:500])
            raise
        time.sleep(float(pause))
    return total


def _rebuild_standard(
    datasets: set[str],
    start_date: date | str | None = None,
    end_date: date | str | None = None,
):
    from data_collect.jobs.tushare_phase2_standard import run as rebuild

    tasks = set()
    if datasets & set(DATE_DATASETS):
        tasks.update({"status", "universe", "tradeability"})
    if "dividend" in datasets:
        tasks.add("dividend")
    if "namechange" in datasets:
        tasks.add("lifecycle")
    if "index_daily" in datasets:
        tasks.add("index_daily")
    for task in sorted(tasks):
        logger.info("rebuilding phase2 standard task=%s", task)
        rebuild(task, start_date, end_date)
    from data_collect.jobs.tushare_phase2_features import run as rebuild_features

    feature_tasks = set()
    if datasets & set(DATE_DATASETS):
        # price_daily_v1 is retained as a read-only legacy archive.  Do not
        # refresh it during daily production increments; otherwise every
        # market run performs the old final-adj-factor rebuild in addition to
        # the point-in-time-safe v2 rebuild.
        feature_tasks.update({"price_v2", "valuation"})
    if "dividend" in datasets:
        feature_tasks.update({"price_v2", "shareholder_return"})
    if feature_tasks:
        for task in sorted(feature_tasks):
            logger.info("rebuilding phase2 feature task=%s", task)
            rebuild_features(task, start_date, end_date)


@warehouse_write_job
def run(
    end_date: str | None = None,
    lookback_days: int = 5,
    event_lookback_days: int = 30,
    batch_pause: float = 1.0,
    dataset: str = "all",
    baseline_cutoff: str | None = None,
    dry_run: bool = False,
    rebuild_standard: bool = False,
    run_date: str | None = None,
) -> str:
    allowed = {"all", "date", "dividend", "namechange", "index_daily"}
    if dataset not in allowed:
        raise ValueError(f"不支持的数据集: {dataset}")
    if lookback_days <= 0 or event_lookback_days <= 0:
        raise ValueError("lookback_days 和 event_lookback_days 必须大于 0")

    if end_date and run_date and str(end_date).replace("-", "") != str(run_date).replace("-", ""):
        raise ValueError("end_date 与 run_date 不一致")
    end = (end_date or run_date or date.today().strftime("%Y%m%d")).replace("-", "")
    end_dt = datetime.strptime(end, "%Y%m%d").date()
    cutoff = _baseline_cutoff(baseline_cutoff)
    calendar_start = (
        pd.to_datetime(end) - timedelta(days=max(lookback_days * 3, 10))
    ).strftime("%Y%m%d")
    pro = get_pro()
    _refresh_trade_calendar(pro, calendar_start, end, dry_run)
    trade_days = _trading_days(end, lookback_days, cutoff)
    event_start = max(
        cutoff + timedelta(days=1),
        end_dt - timedelta(days=event_lookback_days - 1),
    )
    event_days = _calendar_days(event_start, end_dt)
    if not trade_days and dataset in {"all", "date", "index_daily"}:
        logger.warning("基线之后没有可用交易日，date/index 任务跳过")
    if not event_days and dataset in {"all", "dividend", "namechange"}:
        logger.warning("基线之后没有可用事件日期，event 任务跳过")

    batch_id = f"phase2-incremental-{uuid.uuid4().hex}"
    selected = {"stock_st", "suspend_d", "stk_limit", "dividend", "namechange", "index_daily"} if dataset == "all" else set()
    if dataset == "date":
        selected.update(DATE_DATASETS)
    elif dataset != "all":
        selected.add(dataset)
    total = 0
    if selected & set(DATE_DATASETS) and trade_days:
        total += _run_date_datasets(pro, trade_days, batch_id, batch_pause, dry_run)
    dividend_ex_dates: list[date] = []
    if "dividend" in selected and event_days:
        total += _run_dividend(
            pro,
            event_days,
            batch_id,
            batch_pause,
            dry_run,
            affected_ex_dates=dividend_ex_dates,
        )
    if "namechange" in selected and event_days:
        total += _run_namechange(pro, event_days[0], event_days[-1], batch_id, batch_pause, dry_run)
    if "index_daily" in selected and trade_days:
        total += _run_index(pro, trade_days[0], trade_days[-1], batch_id, batch_pause, dry_run)
    if rebuild_standard and not dry_run:
        affected_days = [
            datetime.strptime(value, "%Y%m%d").date()
            for value in [*trade_days, *event_days]
        ]
        # A corrected historical dividend changes total-return coordinates and
        # rolling shareholder-return features from its ex-date onward.
        affected_days.extend(value for value in dividend_ex_dates if value <= end_dt)
        derived_start = min(affected_days) if affected_days else None
        derived_end = end_dt if affected_days else None
        if affected_days:
            _rebuild_standard(selected, derived_start, derived_end)
    return f"阶段二增量完成 batch_id={batch_id} cutoff={cutoff} total_inserted={total}"


def main():
    parser = argparse.ArgumentParser(description="阶段二基线之后的 Tushare 增量同步")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--lookback-days", type=int, default=5)
    parser.add_argument("--event-lookback-days", type=int, default=30)
    parser.add_argument("--batch-pause", type=float, default=1.0)
    parser.add_argument("--dataset", choices=["all", "date", "dividend", "namechange", "index_daily"], default="all")
    parser.add_argument("--baseline-cutoff")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--rebuild-derived", "--rebuild-standard", dest="rebuild_derived",
        action="store_true",
        help="显式重建 Standard/PIT/Feature；增量任务默认只写 Raw",
    )
    parser.add_argument(
        "--no-rebuild-derived", dest="rebuild_derived", action="store_false",
        help=argparse.SUPPRESS,
    )
    parser.set_defaults(rebuild_derived=False)
    args = parser.parse_args()
    values = vars(args)
    values["rebuild_standard"] = values.pop("rebuild_derived")
    print(run(**values))


if __name__ == "__main__":
    main()
