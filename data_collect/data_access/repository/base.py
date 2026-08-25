"""Repository 基类。

领域仓储是 SET-based 复杂 SQL 的唯一合法归属（§8）。所有仓储持有 Kernel，
通过 Kernel 的 Gateway 访问数据库；业务层只调用 Repository 方法，看不见 SQL。
"""
from __future__ import annotations

from data_collect.data_access.kernel import DataAccessKernel
from data_collect.data_access.context import RunContext, WorkScope


class BaseRepository:
    def __init__(self, kernel: DataAccessKernel) -> None:
        self.kernel = kernel

    def atomic(
        self, context: RunContext, scope: WorkScope, operation, *,
        estimated_rows: int = 1, idempotent: bool = False,
    ):
        """Execute a short typed Repository workflow atomically.

        The callback can compose Repository/Gateway methods only; it never
        receives a database handle. Bulk workloads must use normal Gateway
        chunking instead of this bounded workflow boundary.
        """
        return self.kernel.run_atomic(
            context, scope, operation,
            estimated_rows=estimated_rows, idempotent=idempotent,
        )
