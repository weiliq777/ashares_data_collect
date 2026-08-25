"""Data Foundation Access Framework（数据基座统一访问框架）。

业务代码唯一入口是 DataAccessKernel；禁止直接 import psycopg2 / 持有连接 / 手写 SQL。
"""
from data_collect.data_access.kernel import DataAccessKernel
from data_collect.data_access.context import (
    Operation,
    RebuildMode,
    RunContext,
    SqlPlan,
    WorkScope,
)
from data_collect.data_access.dataset_policy import (
    DatasetPolicy,
    DatasetPolicyRegistry,
    ScanPolicy,
    WriteMode,
)
from data_collect.data_access.errors import (
    DataAccessError,
    GuardViolation,
    PolicyViolation,
    TransactionBudgetExceeded,
)

__all__ = [
    "DataAccessKernel",
    "Operation",
    "RebuildMode",
    "RunContext",
    "SqlPlan",
    "WorkScope",
    "DatasetPolicy",
    "DatasetPolicyRegistry",
    "ScanPolicy",
    "WriteMode",
    "DataAccessError",
    "GuardViolation",
    "PolicyViolation",
    "TransactionBudgetExceeded",
]
