"""Versioned evidence for implemented share actions without an ex-date.

This registry records official notices that a restructuring transfer was not
distributed to existing shareholders and that no stock-price ex-rights
adjustment was made. It does not manufacture an ex-date or change source data.
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

EX_DATE_EVIDENCE_VERSION = "corporate-action-ex-date-evidence-v2"

# ``ReleaseRepository`` reads each affected row as JSON so an added database
# column cannot disappear behind a hand-maintained SELECT list. Any schema
# extension forces a fresh review before old evidence can be reused.
DIVIDEND_EVENT_SUPPORTED_COLUMNS = frozenset({
    "dividend_id", "ts_code", "report_date", "announce_date",
    "implementation_status", "stock_div", "stock_bonus_rate",
    "stock_conversion_rate", "cash_div", "cash_div_tax", "record_date",
    "ex_date", "pay_date", "div_list_date", "implementation_announce_date",
    "source", "source_record_key", "record_version", "quality_status",
    "fetched_at", "business_key", "payload_hash",
})

_NO_EX_DATE_REQUIRED: dict[tuple[str, int], dict[str, str]] = {
    ("000523.SZ:2021-10-23:实施", 3062): {
        "classification": "NOT_APPLICABLE_NO_PRICE_ADJUSTMENT",
        "reason": "Court-approved restructuring transfer was not distributed to existing shareholders; the issuer states that the stock price is not adjusted ex-rights.",
        "notice_title": "关于重整计划中资本公积转增股本事项实施的公告",
        "notice_url": "https://static.cninfo.com.cn/finalpage/2021-12-03/1211779449.PDF",
    },
    ("000980.SZ:2021-10-23:实施", 11380): {
        "classification": "NOT_APPLICABLE_NO_PRICE_ADJUSTMENT",
        "reason": "Court-approved restructuring transfer was not distributed to existing shareholders; the issuer states that the stock price is not adjusted ex-rights.",
        "notice_title": "公司关于公司资本公积金转增股本后股票价格不实施除权的公告",
        "notice_url": "https://static.cninfo.com.cn/finalpage/2021-12-09/1211847800.PDF",
    },
    ("002072.SZ:2021-11-19:实施", 14556): {
        "classification": "NOT_APPLICABLE_NO_PRICE_ADJUSTMENT",
        "reason": "Court-approved restructuring transfer was not distributed to existing shareholders; the issuer states that the stock price is not adjusted ex-rights.",
        "notice_title": "关于公司资本公积金转增股本后股票价格不实施除权的公告",
        "notice_url": "https://static.cninfo.com.cn/finalpage/2021-12-17/1211932991.PDF",
    },
    ("002175.SZ:2021-10-12:实施", 16986): {
        "classification": "NOT_APPLICABLE_NO_PRICE_ADJUSTMENT",
        "reason": "Court-approved restructuring transfer was not distributed to existing shareholders; the issuer states that the stock price is not adjusted ex-rights.",
        "notice_title": "关于资本公积转增股本后股票价格不实施除权的公告",
        "notice_url": "https://static.cninfo.com.cn/finalpage/2021-12-21/1211970923.PDF",
    },
}


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _is_zero_modeled_action(row: Mapping[str, Any]) -> bool:
    cash = _number(row.get("cash_div"))
    stock = _number(row.get("stock_div"))
    if cash != 0 or stock != 0:
        return False
    components = (
        _number(row.get("cash_div_tax")),
        _number(row.get("stock_bonus_rate")),
        _number(row.get("stock_conversion_rate")),
    )
    return all(value in {None, 0.0} for value in components)


def classify_missing_ex_date(
    row: Mapping[str, Any], *, scope_start: date, scope_end: date,
) -> dict[str, Any]:
    """Classify a missing ex-date without inferring or filling one.

    An exact source-record/version match is required for official evidence.
    An event is considered outside the requested period only when a known
    later lifecycle milestone predates the period. Unknown dates fail closed.
    """
    source_key = str(row.get("source_record_key") or "")
    try:
        record_version = int(row.get("record_version"))
    except (TypeError, ValueError, OverflowError):
        record_version = -1

    official = _NO_EX_DATE_REQUIRED.get((source_key, record_version))
    common = {
        "ts_code": str(row.get("ts_code") or ""),
        "source_record_key": source_key,
        "source_record_version": record_version if record_version >= 0 else None,
    }
    unexpected_fields = sorted(set(row) - DIVIDEND_EVENT_SUPPORTED_COLUMNS)
    if unexpected_fields:
        return {
            **common,
            "classification": "UNRESOLVED_SCHEMA_EXTENSION",
            "reason": "dividend-event schema contains fields that have not been reviewed by the ex-date evidence contract.",
            "unexpected_fields": unexpected_fields,
            "evidence_version": EX_DATE_EVIDENCE_VERSION,
        }
    if official is not None:
        return {
            **common,
            **official,
            "evidence_version": EX_DATE_EVIDENCE_VERSION,
        }

    if _is_zero_modeled_action(row):
        return {
            **common,
            "classification": "NO_MODELED_CASH_OR_SHARE_EFFECT",
            "reason": "cash_div and stock_div are explicitly zero and no cash-tax, bonus-share, or conversion-share amount is recorded; this row has no effect represented by the current dividend-event model.",
            "evidence_version": EX_DATE_EVIDENCE_VERSION,
        }

    milestone_fields = (
        "implementation_announce_date", "record_date", "pay_date", "div_list_date",
    )
    milestones = {
        field: parsed
        for field in milestone_fields
        if (parsed := _as_date(row.get(field))) is not None
    }
    in_scope = {
        field: value
        for field, value in milestones.items()
        if scope_start <= value <= scope_end
    }
    if in_scope:
        return {
            **common,
            "classification": "IN_SCOPE_UNRESOLVED",
            "reason": "known implementation or entitlement milestone falls within the requested research period, but no verified ex-date or no-adjustment notice is available.",
            "milestones": {field: value.isoformat() for field, value in in_scope.items()},
            "evidence_version": EX_DATE_EVIDENCE_VERSION,
        }

    downstream = {
        field: value
        for field, value in milestones.items()
        if field in {"record_date", "pay_date", "div_list_date"}
    }
    if downstream and all(value < scope_start for value in downstream.values()) and not any(
        value > scope_end for value in milestones.values()
    ):
        basis_field, basis_date = max(downstream.items(), key=lambda item: item[1])
        return {
            **common,
            "classification": "OUT_OF_SCOPE_BY_LIFECYCLE_MILESTONE",
            "reason": "a known downstream entitlement or listing milestone is before the research period; project event contracts require these dates not to precede ex-date.",
            "scope_basis_field": basis_field,
            "scope_basis_date": basis_date.isoformat(),
            "evidence_version": EX_DATE_EVIDENCE_VERSION,
        }

    return {
        **common,
        "classification": "UNRESOLVED",
        "reason": "available dates do not establish whether the event affects the requested period.",
        "milestones": {field: value.isoformat() for field, value in milestones.items()},
        "evidence_version": EX_DATE_EVIDENCE_VERSION,
    }


def summarize_missing_ex_date_scope(
    rows: list[Mapping[str, Any]], *, scope_start: date, scope_end: date,
    schema_columns: set[str] | None = None,
) -> dict[str, Any]:
    if scope_start > scope_end:
        raise ValueError("scope_start must not exceed scope_end")
    schema_issues = None
    if schema_columns is not None:
        schema_issues = {
            "missing_columns": sorted(DIVIDEND_EVENT_SUPPORTED_COLUMNS - schema_columns),
            "unexpected_columns": sorted(schema_columns - DIVIDEND_EVENT_SUPPORTED_COLUMNS),
        }
        if not schema_issues["missing_columns"] and not schema_issues["unexpected_columns"]:
            schema_issues = {}
    if schema_issues:
        entries = [
            {
                "ts_code": str(row.get("ts_code") or ""),
                "source_record_key": str(row.get("source_record_key") or ""),
                "classification": "UNRESOLVED_SCHEMA_EXTENSION",
                "reason": "dividend-event database schema differs from the reviewed ex-date evidence contract.",
                "schema_issues": schema_issues,
                "evidence_version": EX_DATE_EVIDENCE_VERSION,
            }
            for row in rows
        ]
    else:
        entries = [
            classify_missing_ex_date(row, scope_start=scope_start, scope_end=scope_end)
            for row in rows
        ]
    counts: dict[str, int] = {}
    for entry in entries:
        name = entry["classification"]
        counts[name] = counts.get(name, 0) + 1
    unresolved = sum(
        counts.get(name, 0)
        for name in (
            "IN_SCOPE_UNRESOLVED", "UNRESOLVED", "UNRESOLVED_SCHEMA_EXTENSION",
        )
    )
    return {
        "audit_version": EX_DATE_EVIDENCE_VERSION,
        "scope_start_date": scope_start.isoformat(),
        "scope_end_date": scope_end.isoformat(),
        "global_missing_count": len(entries),
        "out_of_scope_count": counts.get("OUT_OF_SCOPE_BY_LIFECYCLE_MILESTONE", 0),
        "documented_not_applicable_count": counts.get("NOT_APPLICABLE_NO_PRICE_ADJUSTMENT", 0),
        "zero_modeled_effect_count": counts.get("NO_MODELED_CASH_OR_SHARE_EFFECT", 0),
        "in_scope_unresolved_count": counts.get("IN_SCOPE_UNRESOLVED", 0),
        "schema_extension_unresolved_count": counts.get(
            "UNRESOLVED_SCHEMA_EXTENSION", 0,
        ),
        "schema_contract_status": (
            "NOT_CHECKED" if schema_columns is None
            else "FAIL" if schema_issues
            else "PASS"
        ),
        "schema_contract_issues": schema_issues,
        "unresolved_count": unresolved,
        "classification_counts": counts,
        "records": entries,
    }


def quality_report_has_current_ex_date_evidence(
    quality_report: Mapping[str, Any] | None,
) -> bool:
    """Return whether a release report contains complete, current scope evidence.

    A passing legacy release is not sufficient: the version, global population,
    per-record evidence, and classification totals must all agree.
    """
    if not isinstance(quality_report, Mapping):
        return False
    if (
        quality_report.get("corporate_action_evidence_version")
        != EX_DATE_EVIDENCE_VERSION
    ):
        return False
    scope = quality_report.get("implementation_without_ex_date_scope")
    if not isinstance(scope, Mapping):
        return False
    if scope.get("audit_version") != EX_DATE_EVIDENCE_VERSION:
        return False
    if scope.get("schema_contract_status") != "PASS":
        return False
    if scope.get("schema_contract_issues") != {}:
        return False

    count_values = (
        quality_report.get("implementation_without_ex_date"),
        scope.get("global_missing_count"),
        scope.get("unresolved_count"),
    )
    if any(
        isinstance(value, bool) or not isinstance(value, int)
        for value in count_values
    ):
        return False
    global_count, classified_count, unresolved_count = count_values
    if global_count < 0 or classified_count != global_count or unresolved_count != 0:
        return False

    records = scope.get("records")
    counts = scope.get("classification_counts")
    if not isinstance(records, list) or len(records) != global_count:
        return False
    if not isinstance(counts, Mapping):
        return False
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in counts.values()
    ):
        return False
    if sum(counts.values()) != global_count:
        return False
    record_counts: dict[str, int] = {}
    for record in records:
        if (
            not isinstance(record, Mapping)
            or record.get("evidence_version") != EX_DATE_EVIDENCE_VERSION
        ):
            return False
        classification = record.get("classification")
        if not isinstance(classification, str):
            return False
        record_counts[classification] = record_counts.get(classification, 0) + 1
    supported_classifications = {
        "OUT_OF_SCOPE_BY_LIFECYCLE_MILESTONE",
        "NOT_APPLICABLE_NO_PRICE_ADJUSTMENT",
        "NO_MODELED_CASH_OR_SHARE_EFFECT",
    }
    if set(record_counts) - supported_classifications:
        return False
    if record_counts != counts:
        return False
    if scope.get("in_scope_unresolved_count") != record_counts.get(
        "IN_SCOPE_UNRESOLVED", 0,
    ):
        return False
    if scope.get("schema_extension_unresolved_count") != record_counts.get(
        "UNRESOLVED_SCHEMA_EXTENSION", 0,
    ):
        return False
    return not any(
        record_counts.get(name, 0)
        for name in (
            "IN_SCOPE_UNRESOLVED", "UNRESOLVED",
            "UNRESOLVED_SCHEMA_EXTENSION",
        )
    )
