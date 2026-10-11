"""RunContext 统一构造（所有操作必须携带 RunContext，§3.2）。

从环境派生 run_id / code_version / config_hash，集中构造，避免各 job 各自拼。
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import uuid
from functools import lru_cache
from pathlib import Path

from data_collect.data_access.context import RunContext


def _git_commit() -> str:
    try:
        from data_collect.config import _PROJECT_ROOT

        return _git_commit_for(str(_PROJECT_ROOT))
    except Exception:
        return "unknown"


@lru_cache(maxsize=8)
def _git_commit_for(project_root: str) -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5, check=True,
            cwd=project_root,
        )
        return out.stdout.strip()
    except Exception:
        return "unknown"


def _config_hash() -> str:
    try:
        from data_collect.config import _PROJECT_ROOT

        configured_path = os.environ.get("DATA_COLLECT_CONFIG")
        path = (
            Path(configured_path)
            if configured_path
            else Path(_PROJECT_ROOT) / "config.yaml"
        )
        resolved_path = path.resolve()
        if not resolved_path.exists():
            return "unknown"
        stat = resolved_path.stat()
        return _config_hash_for(str(resolved_path), stat.st_mtime_ns, stat.st_size)
    except Exception:
        return "unknown"


@lru_cache(maxsize=16)
def _config_hash_for(path: str, _modified_ns: int, _size: int) -> str:
    """Hash one observed config-file version, avoiding repeat disk reads per batch."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()[:16]


def build_run_context(
    job_name: str,
    pipeline_version: str = "v1",
    *,
    run_id: str | None = None,
) -> RunContext:
    """Build a context with source metadata and a unique ID unless one is supplied."""
    return RunContext(
        run_id=run_id or uuid.uuid4().hex,
        job_name=job_name,
        code_version=_git_commit(),
        config_hash=_config_hash(),
        pipeline_version=pipeline_version,
    )
