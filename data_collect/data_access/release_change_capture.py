"""Canonical, bounded change-event descriptions for release delta audits.

This module describes *where* a committed write may have changed source data.
It does not claim that the change has passed release quality gates; that remains
the responsibility of the release workflow.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any, Mapping, Sequence

from data_collect.data_access.context import RunContext, WorkScope


_SOURCE_SCHEMAS = {"public", "raw", "std", "pit", "feature"}
_DATE_COLUMNS = (
    "trade_date", "cal_date", "observation_date", "report_date",
    "f_ann_date", "ann_date", "available_date", "end_date", "ex_date", "start_date",
)
_CODE_COLUMNS = ("ts_code", "stock_code", "symbol")
_MAX_CODES_IN_EVENT = 2_000
_MAX_DATE_ONLY_SCOPE_DAYS = 31
_MAX_WIDE_HISTORY_SCOPE_DAYS = 366
_MAX_CODES_FOR_WIDE_HISTORY = 100
# Shared by source-write transactions and short release boundary transactions.
# Advisory-lock keys are application-private; keep one stable signed-bigint key.
RELEASE_CHANGE_LOCK_KEY = 0x52454C44454C5441


def is_release_input_target(target: str) -> bool:
    """True only for fact/derived-data schemas, never ops/control or staging."""
    parts = str(target).strip().lower().split(".")
    if len(parts) == 1:
        parts.insert(0, "public")
    if len(parts) != 2 or parts[0] not in _SOURCE_SCHEMAS:
        return False
    if parts[0] == "public" and not parts[1].startswith(("tushare_", "fina_indicator_", "daily_kline")):
        return False
    return not parts[1].startswith(("_staging_", "_tmp_", "_old_"))


def build_change_event(
    context: RunContext,
    operation: str,
    target: str,
    scope: WorkScope,
    *,
    rows: Sequence[Mapping[str, Any]] = (),
    statement_fingerprint: str | None = None,
    force_full_audit_reason: str | None = None,
) -> dict[str, Any] | None:
    """Build an idempotent event and conservatively classify its impact scope.

    If the declared/inferred scope cannot safely bound the write, the event is
    still useful audit evidence but is marked ``delta_eligible=False``. A
    downstream release must then require a full audit.
    """
    if not isinstance(context, RunContext) or not isinstance(scope, WorkScope):
        raise TypeError("change capture requires RunContext and WorkScope")
    target = str(target).strip().lower()
    if not is_release_input_target(target):
        return None
    if "." not in target:
        target = f"public.{target}"
    if not operation.strip():
        raise ValueError("change capture operation is required")

    normalized_rows = [dict(row) for row in rows]
    normalized_scope, precision, scope_error = _bounded_scope(scope, normalized_rows)
    if force_full_audit_reason:
        scope_error = force_full_audit_reason
    scope_identity = {
        "scope": normalized_scope,
        "scope_precision": precision,
        "delta_eligible": scope_error is None,
        "full_audit_reason": scope_error,
    }
    scope_hash = hashlib.sha256(_canonical_json(scope_identity).encode("utf-8")).hexdigest()
    input_hash = _input_hash(normalized_rows, statement_fingerprint)
    identity = {
        "run_id": context.run_id,
        "target": target,
        "operation": operation.strip().upper(),
        "scope_hash": scope_hash,
        "input_hash": input_hash,
    }
    event_key = hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()
    return {
        "event_key": event_key,
        "run_id": context.run_id,
        "job_name": context.job_name,
        "code_version": context.code_version,
        "pipeline_version": context.pipeline_version,
        "target": target,
        "operation": operation.strip().upper(),
        "scope": normalized_scope,
        "scope_hash": scope_hash,
        "scope_precision": precision,
        "input_hash": input_hash,
        "input_row_count": len(normalized_rows),
        "delta_eligible": scope_error is None,
        "full_audit_reason": scope_error,
    }


def _bounded_scope(
    scope: WorkScope, rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], str, str | None]:
    date_values: list[date] = []
    code_values: set[str] = set()
    rows_have_date = bool(rows)
    rows_have_code = bool(rows)
    for row in rows:
        raw_date = next((row.get(name) for name in _DATE_COLUMNS if row.get(name) is not None), None)
        raw_code = next((row.get(name) for name in _CODE_COLUMNS if row.get(name) is not None), None)
        parsed_date = _as_date(raw_date)
        if parsed_date is None:
            rows_have_date = False
        else:
            date_values.append(parsed_date)
        if raw_code is None:
            rows_have_code = False
        else:
            code_values.add(str(raw_code))

    date_from, date_to = scope.date_from, scope.date_to
    codes = set(scope.codes or ())
    precision = "DECLARED"
    if date_values:
        observed_from, observed_to = min(date_values), max(date_values)
        if date_from is not None and observed_from < date_from:
            return _serialize_scope(scope), precision, "written row precedes declared date scope"
        if date_to is not None and observed_to > date_to:
            return _serialize_scope(scope), precision, "written row follows declared date scope"
        if date_from is None:
            date_from = observed_from
            precision = "INFERRED"
        if date_to is None:
            date_to = observed_to
            precision = "INFERRED"
    if scope.codes is not None and code_values and not code_values.issubset(codes):
        return _serialize_scope(scope), precision, "written row is outside declared code scope"
    if scope.codes is None and rows_have_code and len(code_values) <= _MAX_CODES_IN_EVENT:
        codes = code_values
        precision = "INFERRED"
    elif scope.codes is None and rows_have_code and len(code_values) > _MAX_CODES_IN_EVENT:
        codes = set()

    bounded_by_dates = date_from is not None and date_to is not None
    bounded_by_codes = bool(codes) and len(codes) <= _MAX_CODES_IN_EVENT
    if bounded_by_dates:
        span_days = (date_to - date_from).days + 1
        if not bounded_by_codes and span_days > _MAX_DATE_ONLY_SCOPE_DAYS:
            return _serialize_scope(scope), precision, (
                f"date-only impact scope exceeds {_MAX_DATE_ONLY_SCOPE_DAYS} days"
            )
        if (
            bounded_by_codes
            and span_days > _MAX_WIDE_HISTORY_SCOPE_DAYS
            and len(codes) > _MAX_CODES_FOR_WIDE_HISTORY
        ):
            return _serialize_scope(scope), precision, (
                "wide historical impact scope exceeds date/code budget"
            )
    row_impact_known = not rows or rows_have_date or rows_have_code
    if not row_impact_known or (not bounded_by_dates and not bounded_by_codes):
        if rows and not rows_have_date and not rows_have_code:
            reason = "rows have no recognized date or security key"
        elif len(code_values) > _MAX_CODES_IN_EVENT:
            reason = "code-only impact scope exceeds the event size limit"
        else:
            reason = "partition/dimensions alone do not prove a bounded impact scope"
        return _serialize_scope(scope), precision, reason

    result = {
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,
        "codes": sorted(codes) if codes else None,
        "partition": scope.partition,
        "dimensions": dict(scope.dimensions),
    }
    return result, precision, None


def _serialize_scope(scope: WorkScope) -> dict[str, Any]:
    return {
        "date_from": scope.date_from.isoformat() if scope.date_from else None,
        "date_to": scope.date_to.isoformat() if scope.date_to else None,
        "codes": list(scope.codes) if scope.codes else None,
        "partition": scope.partition,
        "dimensions": dict(scope.dimensions),
    }


def _input_hash(rows: Sequence[Mapping[str, Any]], statement_fingerprint: str | None) -> str:
    if statement_fingerprint:
        payload = {"statement_fingerprint": statement_fingerprint}
    else:
        # Retain only one fixed-size digest per row during order-independent
        # canonicalization; do not keep a second copy of potentially large Raw
        # payload strings for an entire write chunk.
        row_hashes = sorted(
            hashlib.sha256(_canonical_json(row).encode("utf-8")).digest()
            for row in rows
        )
        digest = hashlib.sha256()
        for row_hash in row_hashes:
            digest.update(row_hash)
        return digest.hexdigest()
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None
