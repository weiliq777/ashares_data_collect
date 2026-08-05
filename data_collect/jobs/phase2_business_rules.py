"""阶段二业务规则的无数据库纯函数，供任务和测试复用。"""
from __future__ import annotations

from datetime import date, timedelta


def derive_lifecycle_interval(start: date, end: date | None, next_start: date | None,
                              list_date: date | None = None, delist_date: date | None = None):
    effective_from = max(start, list_date) if list_date else start
    candidates = [value for value in (end, (next_start - timedelta(days=1)) if next_start else None, delist_date) if value]
    effective_to = min(candidates) if candidates else None
    if effective_to is not None and effective_from > effective_to:
        return None
    return effective_from, effective_to


def dividend_business_key(ts_code: str, report_date, announce_date, status: str | None,
                          record_date, ex_date, implementation_announce_date) -> str:
    values = (ts_code, report_date, announce_date, status, record_date, ex_date, implementation_announce_date)
    return ":".join("" if value is None else str(value) for value in values)


def enters_shareholder_return(status: str | None, ex_date: date | None, decision_date: date) -> bool:
    return status == "实施" and ex_date is not None and ex_date <= decision_date
