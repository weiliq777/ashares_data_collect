"""RunContext 统一构造（所有操作必须携带 RunContext，§3.2）。

从环境派生 run_id / code_version / config_hash，集中构造，避免各 job 各自拼。
"""
from __future__ import annotations

import hashlib
import subprocess
import uuid
from pathlib import Path

from data_collect.data_access.context import RunContext


def _git_commit() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5, check=True,
        )
        return out.stdout.strip()
    except Exception:
        return "unknown"


def _config_hash() -> str:
    try:
        from data_collect.config import _PROJECT_ROOT

        path = Path(_PROJECT_ROOT) / "config.yaml"
        if not path.exists():
            return "unknown"
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    except Exception:
        return "unknown"


def build_run_context(job_name: str, pipeline_version: str = "v1") -> RunContext:
    """从环境构造 RunContext。run_id 每次唯一；code_version/config_hash 标识代码与配置。"""
    return RunContext(
        run_id=uuid.uuid4().hex,
        job_name=job_name,
        code_version=_git_commit(),
        config_hash=_config_hash(),
        pipeline_version=pipeline_version,
    )
