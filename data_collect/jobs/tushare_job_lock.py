"""Mutual exclusion for high-volume Tushare warehouse writers."""
from __future__ import annotations

from contextlib import contextmanager
from functools import wraps

from data_collect.data_access import DataAccessKernel
from data_collect.data_access.policy_catalog import build_default_registry


# Keep this value aligned with the already deployed foundation worker.  The
# name is historical; the lock now protects every high-volume warehouse writer.
WAREHOUSE_WRITE_LOCK = "foundation-rebuild"


@contextmanager
def warehouse_write_lock():
    """Wait for exclusive warehouse-write ownership without holding an MVCC snapshot."""
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        with kernel.lock_manager.session_exclusive("warehouse-write", WAREHOUSE_WRITE_LOCK):
            yield
    finally:
        kernel.close()


def warehouse_write_job(function):
    """Serialize a complete scheduled write job against other heavy writers."""
    @wraps(function)
    def wrapped(*args, **kwargs):
        with warehouse_write_lock():
            return function(*args, **kwargs)

    return wrapped
