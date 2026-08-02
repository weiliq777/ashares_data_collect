"""Tushare-compatible client with rate limiting and safe configuration."""
from __future__ import annotations

import os
import time
from functools import wraps

from data_collect.config import get_tushare_config


def get_pro():
    import tushare as ts

    cfg = get_tushare_config()
    token = os.environ.get(str(cfg.get("token_env", "TUSHARE_API_KEY")), "").strip()
    if not token:
        raise RuntimeError("未设置 Tushare Token 环境变量")
    ts.set_token(token)
    pro = ts.pro_api()
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
