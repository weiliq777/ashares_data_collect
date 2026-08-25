"""Tushare-compatible client with rate limiting and safe configuration."""
from __future__ import annotations

import os
import time
from functools import wraps
from pathlib import Path

from data_collect.config import get_tushare_config


def _local_env_value(name: str) -> str:
    """Read one secret from the ignored project-local .env fallback."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return ""
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() != name:
                continue
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            return value.strip()
    except OSError:
        return ""
    return ""


def get_pro():
    import tushare as ts

    cfg = get_tushare_config()
    token_name = str(cfg.get("token_env", "TUSHARE_API_KEY"))
    token = os.environ.get(token_name, "").strip() or _local_env_value(token_name)
    if not token:
        raise RuntimeError("未设置 Tushare Token 环境变量")
    # Passing the token directly avoids tushare.set_token(), which writes a
    # legacy ``tk.csv`` under the process user's home directory.  That side
    # effect is unnecessary for this client and can fail in restricted or
    # service-account environments even when the configured token is valid.
    pro = ts.pro_api(token)
    pro._DataApi_token = token
    pro._DataApi__http_url = str(cfg.get("api_url", "https://teajoin.com")).rstrip("/")
    return pro


def call_api(fn, *args, **kwargs):
    cfg = get_tushare_config()
    interval = float(cfg.get("request_interval", 0.25))
    retries = int(cfg.get("retries", 3))
    last = None
    for attempt in range(retries):
        try:
            result = fn(*args, **kwargs)
            time.sleep(interval)
            return result
        except Exception as exc:  # noqa: BLE001
            last = exc
            if attempt + 1 < retries:
                time.sleep(max(interval, 2 ** attempt))
    raise RuntimeError(f"Tushare 接口 {getattr(fn, '__name__', fn)} 调用失败: {last}") from last
