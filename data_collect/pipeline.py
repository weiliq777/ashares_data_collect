"""
轻量级 DAG 任务编排框架

从 config.yaml 读取 pipeline 定义，按拓扑排序执行任务。
支持：任务依赖、平台过滤、失败跳过下游、单任务执行。

每个任务在独立子进程中执行，完成后自动回收内存和资源。
主进程只负责调度，不执行业务逻辑。
"""

from __future__ import annotations

import datetime
import importlib
import platform
import time
import traceback
import uuid
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from typing import Any, Dict, List

from data_collect.config import get_pipeline_config
from data_collect.utils.date_utils import add_mark_day, minus_one_market_day
from data_collect.utils.notify import send_dingtalk

import logging

logger = logging.getLogger(__name__)


_STABLE_RETRY_ARGUMENTS = {
    # Financial page checkpoints are scoped to this fetch batch.  The parent
    # scheduler must generate it once so ProcessPool retries resume the same
    # bounded fetch rather than creating a new checkpoint namespace.
    "data_collect.jobs.tushare_financial_vip_incremental": "batch_id",
    "data_collect.jobs.tushare_production_financial_derived": "execution_id",
    # The price-v2 feature checkpoints must be fresh for each production run,
    # while all child-process retries of this derived task share one identity.
    "data_collect.jobs.tushare_production_market_derived": "execution_id",
    # A daily activity release and its frozen observations must reuse the
    # same identity if the subprocess is retried after a timeout.
    "data_collect.jobs.market_activity_production_daily": "execution_id",
    # Outcome retries resume from the same persisted watermark and row identity.
    "data_collect.jobs.market_anomaly_outcome_daily": "execution_id",
}

_RETRY_ATTEMPT_ARGUMENTS = {
    "data_collect.jobs.tushare_financial_vip_incremental": "attempt",
    "data_collect.jobs.tushare_production_financial_derived": "attempt",
}

_PRODUCTION_PIPELINE_CONTRACT = {
    "tushare_daily": {
        "producer_job": "data_collect.jobs.tushare_phase2_incremental",
        "producer_name": "tushare_phase2_raw_first",
        "quality_job": "data_collect.jobs.tushare_production_quality",
        "quality_name": "tushare_production_quality",
    },
    "tushare_production_daily": {
        "producer_job": "data_collect.jobs.tushare_production_market_derived",
        "producer_name": "production_market_derived",
        "quality_job": "data_collect.jobs.tushare_production_quality",
        "quality_name": "production_quality",
    },
}

_PRODUCTION_DAILY_TASK_CONTRACT = {
    "production_instrument_raw": (
        "data_collect.jobs.a_share_instrument_tushare", (),
    ),
    "production_market_raw": (
        "data_collect.jobs.tushare_market_incremental_by_date",
        ("production_instrument_raw",),
    ),
    "production_phase2_raw": (
        "data_collect.jobs.tushare_phase2_incremental",
        ("production_market_raw",),
    ),
    "production_market_derived": (
        "data_collect.jobs.tushare_production_market_derived",
        ("production_phase2_raw",),
    ),
    "production_financial_raw": (
        "data_collect.jobs.tushare_financial_vip_incremental",
        ("production_phase2_raw",),
    ),
    "production_financial_derived": (
        "data_collect.jobs.tushare_production_financial_derived",
        ("production_financial_raw",),
    ),
    "production_quality": (
        "data_collect.jobs.tushare_production_quality",
        ("production_market_derived", "production_financial_derived"),
    ),
    "production_market_activity": (
        "data_collect.jobs.market_activity_production_daily",
        ("production_quality",),
    ),
}

_OPTIONAL_PRODUCTION_DAILY_TASK_CONTRACT = {
    # Activate only after migration 113 and read-only EXPLAIN plan review.
    "production_market_anomaly_outcome": (
        "data_collect.jobs.market_anomaly_outcome_daily",
        ("production_market_activity",),
    ),
}

_PRODUCTION_SPLIT_PIPELINES = ("tushare_daily", "tushare_financial")
_UNBOUNDED_DAILY_PIPELINE_JOBS = frozenset({
    # These entry points can run full-history release/snapshot/dataset audits.
    # They must never be attached to the daily production DAG until a separate
    # bounded delta validator is implemented and verified.
    "data_collect.jobs.tushare_release",
    "data_collect.jobs.research_snapshot",
    "data_collect.jobs.backtest_dataset",
})


@dataclass
class TaskResult:
    name: str
    success: bool
    message: str
    duration: float = 0.0
    skipped: bool = False


def _get_current_platform() -> str:
    """返回 'windows' 或 'linux'。"""
    system = platform.system().lower()
    if system == "windows":
        return "windows"
    return "linux"


def _topological_sort(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """对任务列表按 depends_on 进行拓扑排序。"""
    name_to_task = {t["name"]: t for t in tasks}
    in_degree = {t["name"]: 0 for t in tasks}
    dependents = {t["name"]: [] for t in tasks}

    for t in tasks:
        for dep in t.get("depends_on", []):
            if dep not in name_to_task:
                raise ValueError(f"任务 '{t['name']}' 依赖的 '{dep}' 不存在")
            in_degree[t["name"]] += 1
            dependents[dep].append(t["name"])

    name_order = {name: i for i, name in enumerate(name_to_task)}
    queue = [name for name, deg in in_degree.items() if deg == 0]
    sorted_names = []

    while queue:
        queue.sort(key=lambda n: name_order[n])
        current = queue.pop(0)
        sorted_names.append(current)
        for dep_name in dependents[current]:
            in_degree[dep_name] -= 1
            if in_degree[dep_name] == 0:
                queue.append(dep_name)

    if len(sorted_names) != len(tasks):
        raise ValueError("检测到循环依赖，请检查 pipeline 配置")

    return [name_to_task[n] for n in sorted_names]


def print_dag(pipeline_name: str = "daily") -> None:
    """在终端打印 pipeline 的 DAG 可视化图。"""
    try:
        import networkx as nx
        from phart import ASCIIRenderer
    except ImportError:
        logger.warning("缺少 phart/networkx，跳过 DAG 可视化（pip install phart）")
        return

    pipelines = get_pipeline_config()
    pipeline_cfg = pipelines.get(pipeline_name)
    if not pipeline_cfg:
        return

    tasks = pipeline_cfg.get("tasks", [])
    if not tasks:
        return

    G = nx.DiGraph()
    for t in tasks:
        G.add_node(t["name"])
        for dep in t.get("depends_on", []):
            G.add_edge(dep, t["name"])

    renderer = ASCIIRenderer(G)
    print(f"\n{'='*60}")
    print(f"  Pipeline [{pipeline_name}] 任务编排")
    print(f"{'='*60}")
    print(renderer.render())
    print(f"{'='*60}\n")


def _call_job_fn(job_path: str, fn_name: str, **kwargs):
    """在子进程中调用 job 模块的指定函数（被 submit 到 ProcessPoolExecutor）。

    子进程在 Windows(spawn)/POSIX 下是全新进程，不继承主进程的 logging 配置，
    root logger 默认 WARNING → 各 job 的 logger.info（"快讯源 X 本轮 N 条""扫 pending
    M 篇"等现场进度）全部被吞。任务超时/崩溃时错误里只有主进程的调度日志，看不到
    子进程走到哪一步（news_fulltext worker abruptly terminated 无法定位即此盲区）。
    故在子进程入口配置 INFO 级 basicConfig，stdout 经 ProcessPoolExecutor 回流主进程
    即可见。"""
    import logging as _logging
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    module = importlib.import_module(job_path)
    fn = getattr(module, fn_name, None)
    if fn is None:
        raise AttributeError(f"模块 {job_path} 未实现 {fn_name}() 函数")
    return fn(**kwargs)


def _kill_executor_workers(executor: ProcessPoolExecutor) -> None:
    """强杀 ProcessPoolExecutor 的所有 worker（用于超时硬退出，释放 xtquant 资源）。

    注意：依赖 CPython 私有属性 `_processes`（dict[pid, Process]）。
    Python 3.4-3.13 一直存在；若未来移除，回退到 shutdown(wait=False)
    （不会真正强杀，需重新实现）。
    """
    workers = list(getattr(executor, "_processes", {}).values())
    for p in workers:
        try:
            p.terminate()
        except Exception:
            pass
    for p in workers:
        try:
            p.join(timeout=2)
            if p.is_alive():
                p.kill()
                p.join(timeout=2)
        except Exception:
            pass


def execute_in_subprocess(
    job_path: str,
    fn_name: str = "run",
    timeout: float | None = None,
    **kwargs,
):
    """在独立子进程中执行 job 模块的指定函数，超时则硬杀子进程并抛 TimeoutError。

    timeout 单位秒；None 表示不限时（兼容旧调用）。
    """
    executor = ProcessPoolExecutor(max_workers=1)
    timed_out = False
    try:
        future = executor.submit(_call_job_fn, job_path, fn_name, **kwargs)
        try:
            return future.result(timeout=timeout)
        except FuturesTimeoutError:
            timed_out = True
            _kill_executor_workers(executor)
            raise TimeoutError(
                f"任务执行超时（{timeout}s）— 外部依赖/网络请求可能挂死，已强杀子进程"
            )
    finally:
        if timed_out:
            # 兜底：超时后再次确保 worker 已死，避免僵尸进程
            _kill_executor_workers(executor)
        executor.shutdown(wait=False)


def _resolve_invocation(
    task_cfg: Dict[str, Any], run_date: str | None, base_kwargs: Dict[str, Any],
) -> tuple[str, Dict[str, Any]]:
    """根据任务配置决定子进程要调的 fn 名和它的 kwargs。

    - fn=run（默认）：传 run_date + base_kwargs（CLI 透传的 limit_stocks 等）
    - fn=run_verify：传 start_date/end_date，由 days_back 推算
    """
    fn_name = task_cfg.get("fn", "run")
    configured_kwargs = dict(task_cfg.get("kwargs") or {})
    duplicate = set(configured_kwargs) & set(base_kwargs)
    if duplicate:
        raise ValueError(f"任务配置 kwargs 与运行参数重复: {sorted(duplicate)}")
    invocation_kwargs = {**configured_kwargs, **base_kwargs}
    if fn_name == "run_verify":
        days_back = int(task_cfg.get("days_back", 5))
        end_date = minus_one_market_day(datetime.datetime.now())
        start_date = add_mark_day(end_date, -(days_back - 1))
        # 透传 base_kwargs（如 CLI --limit-stocks），run_verify 接收 **kwargs
        return fn_name, {"start_date": start_date, "end_date": end_date, **invocation_kwargs}
    return fn_name, {"run_date": run_date, **invocation_kwargs}


def _validate_production_pipeline_contract(pipeline_name: str, tasks: list[Dict[str, Any]]) -> None:
    """Fail closed when a production DAG omits its final quality gate.

    The deployment config is intentionally local because it contains secrets,
    so this version-controlled contract prevents an old/partial config from
    silently running only Raw/derived stages after a clean deployment.
    """
    contract = _PRODUCTION_PIPELINE_CONTRACT.get(pipeline_name)
    if contract is None:
        return
    by_name = {task.get("name"): task for task in tasks}
    producer = by_name.get(contract["producer_name"])
    quality = by_name.get(contract["quality_name"])
    if producer is None or producer.get("job") != contract["producer_job"]:
        raise RuntimeError(
            f"{pipeline_name} missing required producer task "
            f"{contract['producer_name']} ({contract['producer_job']})"
        )
    if quality is None or quality.get("job") != contract["quality_job"]:
        raise RuntimeError(
            f"{pipeline_name} missing final production quality task "
            f"{contract['quality_name']} ({contract['quality_job']})"
        )
    if contract["producer_name"] not in set(quality.get("depends_on", ())):
        raise RuntimeError(
            f"{pipeline_name} quality task must depend on "
            f"{contract['producer_name']}"
        )
    if pipeline_name == "tushare_daily":
        kwargs = dict(producer.get("kwargs") or {})
        if kwargs.get("rebuild_standard") is not True and kwargs.get("rebuild_derived") is not True:
            raise RuntimeError(
                "tushare_daily phase2 task must enable the bounded derived rebuild"
            )
    if pipeline_name == "tushare_production_daily":
        _validate_production_daily_tasks(tasks)


def _validate_production_daily_tasks(tasks: list[Dict[str, Any]]) -> None:
    """Require the full Raw → derived → quality → bounded observer chain."""
    unbounded = sorted({
        str(task.get("job"))
        for task in tasks
        if task.get("job") in _UNBOUNDED_DAILY_PIPELINE_JOBS
    })
    if unbounded:
        raise RuntimeError(
            "daily production cannot invoke full-history release/snapshot/dataset "
            f"jobs without a bounded delta contract: {unbounded}"
        )

    reviewed_tasks = {
        **_PRODUCTION_DAILY_TASK_CONTRACT,
        **_OPTIONAL_PRODUCTION_DAILY_TASK_CONTRACT,
    }
    allowed_jobs = {job for job, _dependencies in reviewed_tasks.values()}
    unknown_jobs = sorted({
        str(task.get("job"))
        for task in tasks
        if task.get("job") not in allowed_jobs
    })
    if unknown_jobs:
        raise RuntimeError(
            "daily production contains an unreviewed job; add a bounded query "
            f"contract and tests before scheduling it: {unknown_jobs}"
        )

    by_name = {task.get("name"): task for task in tasks}
    duplicate_names = len(by_name) != len(tasks)
    extra_names = sorted(set(by_name) - set(reviewed_tasks))
    if duplicate_names or extra_names:
        raise RuntimeError(
            "tushare_production_daily task topology is not explicitly reviewed: "
            f"duplicate_names={duplicate_names}, extra_names={extra_names}"
        )
    missing = set(_PRODUCTION_DAILY_TASK_CONTRACT) - set(by_name)
    if missing:
        raise RuntimeError(
            "tushare_production_daily missing required tasks: "
            f"{sorted(missing)}"
        )

    for name, (expected_job, expected_dependencies) in reviewed_tasks.items():
        if name in _OPTIONAL_PRODUCTION_DAILY_TASK_CONTRACT and name not in by_name:
            continue
        task = by_name[name]
        if task.get("job") != expected_job:
            raise RuntimeError(
                f"tushare_production_daily task {name} must use {expected_job}"
            )
        if tuple(task.get("depends_on") or ()) != expected_dependencies:
            raise RuntimeError(
                f"tushare_production_daily task {name} dependencies must be "
                f"{expected_dependencies}"
            )
        if name == "production_market_anomaly_outcome":
            lookback = int(dict(task.get("kwargs") or {}).get("lookback_days", 31))
            if not 1 <= lookback <= 31:
                raise RuntimeError(
                    "tushare_production_daily production_market_anomaly_outcome."
                    "lookback_days must be in 1..31"
                )

    expected_false_flags = {
        "production_market_raw": "rebuild_derived",
        "production_phase2_raw": "rebuild_standard",
        "production_financial_raw": "rebuild_derived",
    }
    for name, flag in expected_false_flags.items():
        if dict(by_name[name].get("kwargs") or {}).get(flag) is not False:
            raise RuntimeError(
                f"tushare_production_daily {name}.{flag} must be false; "
                "the dedicated derived task owns that stage"
            )

    for name in ("production_market_raw", "production_phase2_raw"):
        lookback = int(dict(by_name[name].get("kwargs") or {}).get("lookback_days", 5))
        if not 1 <= lookback <= 31:
            raise RuntimeError(
                f"tushare_production_daily {name}.lookback_days must be in 1..31"
            )
    phase2_event_window = int(
        dict(by_name["production_phase2_raw"].get("kwargs") or {}).get(
            "event_lookback_days", 30,
        )
    )
    if not 1 <= phase2_event_window <= 90:
        raise RuntimeError(
            "tushare_production_daily production_phase2_raw.event_lookback_days "
            "must be in 1..90"
        )

    market_window = int(dict(by_name["production_market_derived"].get("kwargs") or {}).get("lookback_days", 5))
    quality_window = int(dict(by_name["production_quality"].get("kwargs") or {}).get("lookback_days", 5))
    if not 1 <= market_window <= 31 or quality_window != market_window:
        raise RuntimeError(
            "tushare_production_daily market-derived and quality lookback_days "
            "must match and be in 1..31"
        )


def _validate_production_schedule_contract(pipelines: Dict[str, Any]) -> None:
    """Ensure one scheduled Tushare writer owns the entire daily production chain."""
    production = pipelines.get("tushare_production_daily")
    if production is None:
        return
    schedule = production.get("schedule") or {}
    if not schedule:
        raise RuntimeError(
            "tushare_production_daily must be the scheduled Tushare production pipeline"
        )
    timezone = schedule.get("timezone", "Asia/Shanghai")
    if timezone != "Asia/Shanghai":
        raise RuntimeError(
            "tushare_production_daily schedule timezone must be Asia/Shanghai"
        )
    for name in _PRODUCTION_SPLIT_PIPELINES:
        split = pipelines.get(name) or {}
        if split.get("schedule"):
            raise RuntimeError(
                f"{name} is a manual/diagnostic pipeline and must not have a schedule; "
                "use tushare_production_daily to avoid split or duplicate production runs"
            )
    _validate_production_pipeline_contract(
        "tushare_production_daily", production.get("tasks") or [],
    )


def _notify_retry(task_name: str, attempt: int, total: int, exc: Exception) -> None:
    """重试前发钉钉告警；通知失败仅 log，不影响主流程。"""
    is_timeout = isinstance(exc, TimeoutError)
    warn = (
        f"[pipeline] 任务 {task_name} 第 {attempt}/{total} 次"
        f"{'超时' if is_timeout else '失败'}: {exc}，准备重试..."
    )
    print(warn)
    try:
        send_dingtalk(warn)
    except Exception as notify_exc:
        logger.warning(f"钉钉重试告警发送失败: {notify_exc}")


def _run_one_task(
    task_cfg: Dict[str, Any], run_date: str | None, base_kwargs: Dict[str, Any],
) -> TaskResult:
    """在子进程中执行一个任务，自动处理超时和重试，返回 TaskResult。"""
    task_name = task_cfg["name"]
    job_path = task_cfg["job"]
    timeout = task_cfg.get("timeout")
    max_retries = max(0, int(task_cfg.get("retries", 0)))
    attempts_total = 1 + max_retries
    fn_name, call_kwargs = _resolve_invocation(task_cfg, run_date, base_kwargs)
    stable_argument = _STABLE_RETRY_ARGUMENTS.get(job_path)
    if stable_argument and stable_argument not in call_kwargs:
        call_kwargs[stable_argument] = f"{task_name}-{uuid.uuid4().hex}"
    attempt_argument = _RETRY_ATTEMPT_ARGUMENTS.get(job_path)
    if attempt_argument and attempt_argument in call_kwargs:
        raise ValueError(f"浠诲姟 {task_name} 涓嶅厑璁哥敤淇℃伅 {attempt_argument}")

    print(f"[pipeline] 启动任务: {task_name} (fn={fn_name}, timeout={timeout}, retries={max_retries})")
    start_time = time.monotonic()
    last_exc: Exception | None = None
    last_tb = ""

    for attempt in range(1, attempts_total + 1):
        try:
            attempt_kwargs = dict(call_kwargs)
            if attempt_argument:
                attempt_kwargs[attempt_argument] = attempt
            message = execute_in_subprocess(job_path, fn_name, timeout=timeout, **attempt_kwargs)
            duration = time.monotonic() - start_time
            print(f"[pipeline] 完成: {task_name} ({duration:.1f}s)")
            return TaskResult(name=task_name, success=True, message=message, duration=duration)
        except Exception as exc:
            last_exc = exc
            last_tb = traceback.format_exc()
            if attempt < attempts_total:
                _notify_retry(task_name, attempt, attempts_total, exc)

    duration = time.monotonic() - start_time
    error_msg = f"失败（{attempts_total}次后放弃）: {last_exc}\n{last_tb}"
    print(f"[pipeline] 失败: {task_name} ({duration:.1f}s)")
    print(error_msg)
    return TaskResult(name=task_name, success=False, message=error_msg, duration=duration)


def run_pipeline(
    pipeline_name: str = "daily",
    run_date: str | None = None,
    only_task: str | None = None,
    show_dag: bool = True,
    **kwargs,
) -> List[TaskResult]:
    """
    执行指定 pipeline。

    每个任务在独立子进程中运行，完成后进程退出、内存释放。
    主进程只负责调度和结果收集。
    """
    pipelines = get_pipeline_config()
    if pipeline_name not in pipelines:
        raise ValueError(f"Pipeline '{pipeline_name}' 不存在，可用: {list(pipelines.keys())}")

    pipeline_cfg = pipelines[pipeline_name]
    tasks = pipeline_cfg.get("tasks", [])
    if not tasks:
        raise ValueError(f"Pipeline '{pipeline_name}' 没有定义任务")
    _validate_production_pipeline_contract(pipeline_name, tasks)

    if show_dag:
        print_dag(pipeline_name)

    current_platform = _get_current_platform()
    sorted_tasks = _topological_sort(tasks)

    if only_task:
        matched = [t for t in sorted_tasks if t["name"] == only_task]
        if not matched:
            available = [t["name"] for t in sorted_tasks]
            raise ValueError(f"任务 '{only_task}' 不存在，可用: {available}")
        sorted_tasks = matched

    results: List[TaskResult] = []
    failed_tasks: set = set()

    for task_cfg in sorted_tasks:
        task_name = task_cfg["name"]
        task_platform = task_cfg.get("platform")
        depends = task_cfg.get("depends_on", [])

        # 平台过滤
        if task_platform and task_platform != current_platform:
            results.append(TaskResult(
                name=task_name, success=True, skipped=True,
                message=f"跳过（平台不匹配：需要 {task_platform}，当前 {current_platform}）",
            ))
            continue

        # 依赖失败检查
        if not only_task:
            failed_deps = [d for d in depends if d in failed_tasks]
            if failed_deps:
                results.append(TaskResult(
                    name=task_name, success=False, skipped=True,
                    message=f"跳过（上游任务失败: {failed_deps}）",
                ))
                failed_tasks.add(task_name)
                continue

        # 在子进程中执行任务（含超时 + 重试）
        result = _run_one_task(task_cfg, run_date, kwargs)
        results.append(result)
        if not result.success:
            failed_tasks.add(task_name)

    if _should_notify(pipeline_cfg, results):
        _send_pipeline_summary(pipeline_name, run_date, results)
    return results


def _should_notify(pipeline_cfg: Dict[str, Any], results: List[TaskResult]) -> bool:
    """通知策略：pipeline 配 `notify: on_failure` 且**全部成功**时静默（不发汇总），
    用于高频 pipeline（如 news_flash */15）避免钉钉刷屏；有失败照发。缺省（无 notify）维持总发。

    注意：任务重试的告警由 `_notify_retry` 独立发送，不受本策略影响——"失败可见"仍成立。
    平台不匹配跳过 success=True 不算失败；上游失败导致的跳过 success=False 算失败。
    """
    if pipeline_cfg.get("notify") == "on_failure":
        return any(not r.success for r in results)
    return True


def _send_pipeline_summary(pipeline_name: str, run_date: str | None, results: List[TaskResult]) -> None:
    """发送 pipeline 执行汇总通知。"""
    # 未显式传日期时（scheduler 定时触发即如此）标注实际日期，便于事后核查
    date_label = run_date or f"今天 {datetime.datetime.now().strftime('%Y%m%d')}"
    lines = [f"流水线 [{pipeline_name}] 执行完毕 (日期: {date_label})"]
    for r in results:
        if r.skipped:
            status = "⏭跳过"
        elif r.success:
            status = "✅成功"
        else:
            status = "❌失败"
        duration_str = f" ({r.duration:.1f}s)" if r.duration > 0 else ""
        lines.append(f"  {status} {r.name}{duration_str}")
        if not r.success and not r.skipped and r.message:
            first_line = r.message.split("\n")[0][:200]
            lines.append(f"    ↳ {first_line}")

    total = len(results)
    success_cnt = sum(1 for r in results if r.success and not r.skipped)
    failed_cnt = sum(1 for r in results if not r.success)
    skipped_cnt = sum(1 for r in results if r.skipped)
    lines.append(f"合计: {total}个任务, {success_cnt}成功, {failed_cnt}失败, {skipped_cnt}跳过")

    send_dingtalk("\n".join(lines))
