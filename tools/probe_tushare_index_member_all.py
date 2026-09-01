"""Manually probe Tushare's ``index_member_all`` endpoint.

This is an occasional, read-only verification tool.  It only calls Tushare
through the project's configured client and never opens a database connection
or writes any application data.  The output is deliberately bounded to
metadata and a small number of sample rows.

The endpoint is relevant to the historical industry PIT (point-in-time)
question because its response includes membership effective dates.  A
successful response is not, by itself, sufficient to approve five-year PIT
coverage; callers must still check coverage, continuity, source identity, and
the applicable methodology before promoting it into the data foundation.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_collect.providers.tushare_client import call_api, get_pro


DEFAULT_TS_CODE = "000001.SZ"
DATE_COLUMNS = ("in_date", "out_date")


def _json_value(value: Any) -> Any:
    """Convert pandas/numpy scalar values into JSON-safe values."""
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


def _date_range(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    """Return a bounded, normalized date summary for one response column."""
    if column not in frame.columns:
        return {"present": False, "non_null": 0}
    values = pd.to_datetime(frame[column], format="%Y%m%d", errors="coerce").dropna()
    if values.empty:
        return {"present": True, "non_null": 0}
    return {
        "present": True,
        "non_null": int(values.size),
        "min": values.min().date().isoformat(),
        "max": values.max().date().isoformat(),
    }


def summarize_response(frame: Any, sample_size: int) -> dict[str, Any]:
    """Summarize one Tushare response without emitting an unbounded payload."""
    if frame is None:
        return {"response_type": "None", "row_count": 0, "columns": [], "rows": []}
    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"expected pandas.DataFrame, got {type(frame).__name__}")
    rows = [
        {str(key): _json_value(value) for key, value in row.items()}
        for row in frame.head(sample_size).to_dict("records")
    ]
    return {
        "response_type": "DataFrame",
        "row_count": int(len(frame.index)),
        "columns": [str(column) for column in frame.columns],
        "date_ranges": {column: _date_range(frame, column) for column in DATE_COLUMNS},
        "distinct_industry_codes": (
            sorted(str(value) for value in frame["l3_code"].dropna().unique())
            if "l3_code" in frame.columns
            else []
        ),
        "rows": rows,
    }


def probe(
    ts_code: str | None = None,
    sample_size: int = 10,
    *,
    l3_code: str | None = None,
) -> dict[str, Any]:
    """Call latest and non-latest membership modes for one bounded query.

    The endpoint accepts either a member stock code or a level-three industry
    code.  We intentionally query only one of them per invocation so that this
    remains a small manual probe rather than an ingestion job.
    """
    if (ts_code is None) == (l3_code is None):
        raise ValueError("provide exactly one of ts_code or l3_code")
    query_field, query_value = ("ts_code", ts_code) if ts_code is not None else ("l3_code", l3_code)
    if not query_value or any(character.isspace() for character in query_value):
        raise ValueError(f"{query_field} must be one non-empty Tushare code")
    if not 1 <= sample_size <= 50:
        raise ValueError("sample_size must be between 1 and 50")

    pro = get_pro()
    result: dict[str, Any] = {
        "endpoint": "index_member_all",
        "query": {query_field: query_value},
        "read_only": True,
        "database_access": False,
        "modes": {},
    }
    for mode in ("Y", "N"):
        frame = call_api(
            pro.index_member_all,
            **{query_field: query_value, "is_new": mode},
        )
        result["modes"][f"is_new={mode}"] = summarize_response(frame, sample_size)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only manual probe for Tushare index_member_all"
    )
    query = parser.add_mutually_exclusive_group()
    query.add_argument(
        "--ts-code",
        default=None,
        help=f"one member stock code (default: {DEFAULT_TS_CODE})",
    )
    query.add_argument(
        "--l3-code",
        help="one level-three Shenwan industry code, e.g. 850531.SI",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10,
        help="maximum rows printed per mode (1-50)",
    )
    args = parser.parse_args()
    try:
        report = probe(
            args.ts_code or (None if args.l3_code else DEFAULT_TS_CODE),
            args.sample_size,
            l3_code=args.l3_code,
        )
    except Exception as exc:  # noqa: BLE001 - CLI must report API failures safely.
        print(
            json.dumps(
                {
                    "endpoint": "index_member_all",
                    "query": {"l3_code": args.l3_code}
                    if args.l3_code
                    else {"ts_code": args.ts_code},
                    "status": "ERROR",
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:500],
                    "database_access": False,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1
    report["status"] = "OK"
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
