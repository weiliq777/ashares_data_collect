"""pipeline.py 测试（不依赖 xtquant 或数据库）。"""

import pytest

from data_collect.pipeline import (
    _topological_sort,
    _get_current_platform,
    _run_one_task,
    TaskResult,
    execute_in_subprocess,
)


def test_topological_sort_simple():
    tasks = [
        {"name": "a", "job": "mod_a"},
        {"name": "b", "job": "mod_b", "depends_on": ["a"]},
    ]
    result = _topological_sort(tasks)
    names = [t["name"] for t in result]
    assert names.index("a") < names.index("b")


def test_topological_sort_no_deps():
    tasks = [
        {"name": "x", "job": "mod_x"},
        {"name": "y", "job": "mod_y"},
    ]
    result = _topological_sort(tasks)
    assert len(result) == 2


def test_topological_sort_diamond():
    tasks = [
        {"name": "a", "job": "m"},
        {"name": "b", "job": "m", "depends_on": ["a"]},
        {"name": "c", "job": "m", "depends_on": ["a"]},
        {"name": "d", "job": "m", "depends_on": ["b", "c"]},
    ]
    result = _topological_sort(tasks)
    names = [t["name"] for t in result]
    assert names.index("a") < names.index("b")
    assert names.index("a") < names.index("c")
    assert names.index("b") < names.index("d")
    assert names.index("c") < names.index("d")


def test_topological_sort_cycle_detection():
    tasks = [
        {"name": "a", "job": "m", "depends_on": ["b"]},
        {"name": "b", "job": "m", "depends_on": ["a"]},
    ]
    with pytest.raises(ValueError, match="循环依赖"):
        _topological_sort(tasks)


def test_topological_sort_missing_dep():
    tasks = [
        {"name": "a", "job": "m", "depends_on": ["nonexistent"]},
    ]
    with pytest.raises(ValueError, match="不存在"):
        _topological_sort(tasks)


def test_get_current_platform():
    plat = _get_current_platform()
    assert plat in ("windows", "linux")


def test_task_result_dataclass():
    r = TaskResult(name="test", success=True, message="ok", duration=1.5)
    assert r.name == "test"
    assert r.success
    assert not r.skipped


def test_financial_retry_reuses_one_parent_batch_id(monkeypatch):
    import data_collect.pipeline as pipeline

    seen = []

    def fake_execute(job_path, fn_name, timeout=None, **kwargs):
        seen.append(kwargs["batch_id"])
        if len(seen) == 1:
            raise RuntimeError("transient")
        return "ok"

    monkeypatch.setattr(pipeline, "execute_in_subprocess", fake_execute)
    result = _run_one_task(
        {
            "name": "financial_retry",
            "job": "data_collect.jobs.tushare_financial_vip_incremental",
            "retries": 1,
        },
        "20260905",
        {},
    )
    assert result.success
    assert len(seen) == 2
    assert seen[0] == seen[1]


def test_financial_consumer_retry_reuses_execution_id_and_marks_attempt(monkeypatch):
    import data_collect.pipeline as pipeline

    seen = []

    def fake_execute(job_path, fn_name, timeout=None, **kwargs):
        seen.append((kwargs["execution_id"], kwargs["attempt"]))
        if len(seen) == 1:
            raise RuntimeError("transient")
        return "ok"

    monkeypatch.setattr(pipeline, "execute_in_subprocess", fake_execute)
    result = _run_one_task(
        {
            "name": "financial_consumer_retry",
            "job": "data_collect.jobs.tushare_production_financial_derived",
            "retries": 1,
        },
        "20260905",
        {},
    )
    assert result.success
    assert seen[0][0] == seen[1][0]
    assert [item[1] for item in seen] == [1, 2]


def test_market_derived_retry_reuses_execution_id_but_new_run_gets_new_id(monkeypatch):
    import data_collect.pipeline as pipeline

    seen = []

    def fake_execute(job_path, fn_name, timeout=None, **kwargs):
        seen.append(kwargs["execution_id"])
        if len(seen) == 1:
            raise RuntimeError("transient")
        return "ok"

    monkeypatch.setattr(pipeline, "execute_in_subprocess", fake_execute)
    task = {
        "name": "market_derived",
        "job": "data_collect.jobs.tushare_production_market_derived",
        "retries": 1,
    }
    assert _run_one_task(task, "20260925", {}).success
    first_execution_id = seen[0]
    assert seen[1] == first_execution_id

    assert _run_one_task(task, "20260925", {}).success
    assert seen[2] != first_execution_id


# ===== execute_in_subprocess 超时/成功 =====

def test_execute_in_subprocess_success():
    """正常返回时透传 job 函数的返回值。"""
    result = execute_in_subprocess(
        "tests._sleep_job", "run", timeout=5, seconds=0.1,
    )
    assert result == "slept 0.1s"


def test_execute_in_subprocess_timeout_raises():
    """超时时抛 TimeoutError 并强杀子进程。"""
    with pytest.raises(TimeoutError, match="任务执行超时"):
        execute_in_subprocess(
            "tests._sleep_job", "run", timeout=1, seconds=10,
        )


def test_execute_in_subprocess_no_timeout():
    """timeout=None 时不限时（兼容旧调用）。"""
    result = execute_in_subprocess(
        "tests._sleep_job", "run", timeout=None, seconds=0.05,
    )
    assert result == "slept 0.05s"


def test_execute_in_subprocess_propagates_exception(tmp_path):
    """job 内部抛错时原样向上抛。"""
    marker = tmp_path / "counter.txt"
    with pytest.raises(RuntimeError, match="flaky failure"):
        execute_in_subprocess(
            "tests._sleep_job", "run_flaky",
            timeout=5, fail_times=99, marker_path=str(marker),
        )
