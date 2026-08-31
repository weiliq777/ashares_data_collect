"""观察域策略构造和方法论身份入口。"""
from __future__ import annotations

from data_collect.observation.contracts import ObservationPolicy


def default_policy() -> ObservationPolicy:
    """Return a fresh immutable production policy instance."""
    return ObservationPolicy()


__all__ = ["ObservationPolicy", "default_policy"]
