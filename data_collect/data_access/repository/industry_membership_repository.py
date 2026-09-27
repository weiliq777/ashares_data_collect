"""Repository for daily Tushare industry-name observations.

The source provides today's industry name, not an industry code or an official
effective date.  Rows are therefore kept as DAILY_CAPTURED evidence and are
never promoted to validated PIT evidence by this repository.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
import hashlib
import json
import re
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


_SHANGHAI = ZoneInfo("Asia/Shanghai")
_INDUSTRY_SYSTEM = "TUSHARE_STOCK_BASIC"
_INDUSTRY_LEVEL = 1
_MAX_DAILY_STOCKS = 10_000
_TS_CODE = re.compile(r"\d{6}\.(?:SH|SZ|BJ)\Z")


@dataclass(frozen=True)
class IndustryDailyCaptureResult:
    """Outcome of one bounded, immutable industry capture date."""

    status: str
    trade_date: date | None
    source_rows: int
    inserted_rows: int = 0
    unknown_industry_rows: int = 0
    reason: str | None = None


class IndustryMembershipRepository(BaseRepository):
    """Persist one observed stock-basic industry snapshot through the Kernel."""

    def persist_daily_capture(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        run_date: str | date | None,
        fetched_at: datetime,
        context: RunContext,
    ) -> IndustryDailyCaptureResult:
        """Store a single post-close trading-day observation.

        Interface invariants:
        - ``rows`` are the exact rows returned by the existing stock_basic call;
        - the run date must be the capture's Shanghai calendar date;
        - only an SSE-open date after 17:00 Shanghai time is captured;
        - a date is append-only; identical retries are no-ops, interrupted
          partial batches resume by appending only missing rows, and changed
          same-day classifications fail closed;
        - the read and write are bounded to one date and at most 10,000 codes.
        """
        if not isinstance(context, RunContext):
            raise TypeError("industry capture requires RunContext")
        if not isinstance(fetched_at, datetime) or fetched_at.tzinfo is None:
            raise ValueError("fetched_at must be a timezone-aware datetime")
        if not rows:
            raise ValueError("stock_basic industry capture source is empty")
        if len(rows) > _MAX_DAILY_STOCKS:
            raise ValueError(
                f"stock_basic rows exceed bounded daily limit {_MAX_DAILY_STOCKS}"
            )

        captured_at = fetched_at.astimezone(_SHANGHAI)
        captured_date = captured_at.date()
        if captured_at.time().replace(tzinfo=None) < time(17, 0):
            return IndustryDailyCaptureResult(
                "SKIPPED_BEFORE_CLOSE", None, len(rows),
                reason="daily industry observations are captured after 17:00 Asia/Shanghai",
            )

        requested_date = self._parse_run_date(run_date)
        if requested_date is not None and requested_date != captured_date:
            return IndustryDailyCaptureResult(
                "SKIPPED_NON_CURRENT_DATE", None, len(rows),
                reason="current stock_basic response cannot be backdated to another run date",
            )

        calendar_rows = self.kernel.read._repository_query(
            context=context,
            source="std.trading_calendar",
            sql=(
                "SELECT is_open FROM std.trading_calendar "
                "WHERE exchange=%s AND trade_date=%s AND source=%s"
            ),
            params=("SSE", captured_date, "tushare"),
            scope=WorkScope(
                date_from=captured_date,
                date_to=captured_date,
                partition=f"industry-calendar:{captured_date.isoformat()}",
                dimensions={"exchange": "SSE", "source": "tushare"},
            ),
            max_rows=2,
        )
        if not calendar_rows:
            return IndustryDailyCaptureResult(
                "SKIPPED_CALENDAR_MISSING", None, len(rows),
                reason=f"SSE calendar has no row for {captured_date.isoformat()}",
            )
        if len(calendar_rows) != 1:
            raise RuntimeError(
                f"SSE calendar has ambiguous rows for {captured_date.isoformat()}"
            )
        if not bool(calendar_rows[0]["is_open"]):
            return IndustryDailyCaptureResult(
                "SKIPPED_NON_TRADING_DAY", None, len(rows),
                reason=f"{captured_date.isoformat()} is not an SSE trading day",
            )

        snapshot_rows = self._snapshot_rows(rows, captured_date, captured_at)
        scope = WorkScope(
            date_from=captured_date,
            date_to=captured_date,
            partition=f"industry-membership:{captured_date.isoformat()}",
            dimensions={
                "industry_system": _INDUSTRY_SYSTEM,
                "industry_level": _INDUSTRY_LEVEL,
            },
        )
        lock_scope = f"{captured_date.isoformat()}:{_INDUSTRY_SYSTEM}:{_INDUSTRY_LEVEL}"

        with self.kernel.lock_manager.session_exclusive(
            "pit.industry_membership_daily", lock_scope
        ):
            existing = self._load_snapshot(captured_date, context, scope)
            existing_codes = {str(row["ts_code"]) for row in existing}
            if existing:
                if not self._matches_snapshot_subset(snapshot_rows, existing):
                    raise RuntimeError(
                        "industry membership snapshot already exists with different "
                        f"source content for same trade date {captured_date.isoformat()}"
                    )
                if len(existing) == len(snapshot_rows):
                    return IndustryDailyCaptureResult(
                        "ALREADY_CAPTURED",
                        captured_date,
                        len(rows),
                        unknown_industry_rows=sum(
                            row["quality_status"] == "UNKNOWN" for row in snapshot_rows
                        ),
                    )

            rows_to_insert = [
                row for row in snapshot_rows if row["ts_code"] not in existing_codes
            ]

            attempted, inserted = self.kernel.write.bulk_insert_ignore(
                context,
                scope,
                "pit.industry_membership_daily",
                rows_to_insert,
            )
            if attempted != len(rows_to_insert):
                raise RuntimeError(
                    "industry membership insert attempted an unexpected row count"
                )

            persisted = self._load_snapshot(captured_date, context, scope)
            if not self._same_snapshot(snapshot_rows, persisted):
                raise RuntimeError(
                    "industry membership snapshot coverage/content check failed for "
                    f"{captured_date.isoformat()}"
                )

        return IndustryDailyCaptureResult(
            "RESUMED_PARTIAL_CAPTURE" if existing else "CAPTURED",
            captured_date,
            len(rows),
            inserted_rows=inserted,
            unknown_industry_rows=sum(
                row["quality_status"] == "UNKNOWN" for row in snapshot_rows
            ),
        )

    @staticmethod
    def _parse_run_date(run_date: str | date | None) -> date | None:
        if run_date is None:
            return None
        if isinstance(run_date, datetime):
            return run_date.date()
        if isinstance(run_date, date):
            return run_date
        value = str(run_date).strip()
        if re.fullmatch(r"\d{8}", value):
            return datetime.strptime(value, "%Y%m%d").date()
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"invalid industry capture run_date: {run_date!r}") from exc

    @staticmethod
    def _snapshot_rows(
        source_rows: Sequence[Mapping[str, Any]],
        trade_date: date,
        fetched_at: datetime,
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        seen: set[str] = set()
        for source_row in source_rows:
            code = str(source_row.get("ts_code") or "").strip().upper()
            if not _TS_CODE.fullmatch(code):
                raise ValueError(f"invalid stock_basic ts_code: {code!r}")
            if code in seen:
                raise ValueError(f"duplicate stock_basic ts_code: {code}")
            seen.add(code)
            if "industry" not in source_row:
                raise ValueError(f"stock_basic response is missing industry for {code}")

            industry_value = source_row.get("industry")
            industry_name = (
                str(industry_value).strip()
                if industry_value is not None and str(industry_value).strip()
                else None
            )
            identity = json.dumps(
                {"ts_code": code, "industry_name": industry_name},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
            result.append({
                "trade_date": trade_date,
                "ts_code": code,
                "industry_system": _INDUSTRY_SYSTEM,
                "industry_level": _INDUSTRY_LEVEL,
                "industry_code": None,
                "industry_name": industry_name,
                "source_asof_date": trade_date,
                "data_available_timestamp": fetched_at,
                "source_record_key": f"stock_basic:{code}:{digest}",
                "record_version": 1,
                "quality_status": "DAILY_CAPTURED" if industry_name else "UNKNOWN",
            })
        return sorted(result, key=lambda row: row["ts_code"])

    def _load_snapshot(
        self, trade_date: date, context: RunContext, scope: WorkScope
    ) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context=context,
            source="pit.industry_membership_daily",
            sql=(
                "SELECT ts_code, industry_name, source_record_key, quality_status, "
                "source_asof_date "
                "FROM pit.industry_membership_daily "
                "WHERE trade_date=%s AND industry_system=%s AND industry_level=%s "
                "ORDER BY ts_code"
            ),
            params=(trade_date, _INDUSTRY_SYSTEM, _INDUSTRY_LEVEL),
            scope=scope,
            max_rows=_MAX_DAILY_STOCKS,
        )

    @staticmethod
    def _same_snapshot(
        expected: Sequence[Mapping[str, Any]],
        persisted: Sequence[Mapping[str, Any]],
    ) -> bool:
        if len(expected) != len(persisted):
            return False
        return IndustryMembershipRepository._matches_snapshot_subset(expected, persisted)

    @staticmethod
    def _matches_snapshot_subset(
        expected: Sequence[Mapping[str, Any]],
        persisted: Sequence[Mapping[str, Any]],
    ) -> bool:
        """Check stored rows against the expected day without requiring full coverage.

        This lets an interrupted append-only batch safely resume by adding only
        missing codes. Existing rows with changed names/status/identity still
        fail closed and are never rewritten.
        """
        expected_by_code = {str(row["ts_code"]): row for row in expected}
        persisted_by_code = {str(row["ts_code"]): row for row in persisted}
        if (
            len(expected_by_code) != len(expected)
            or len(persisted_by_code) != len(persisted)
            or not set(persisted_by_code).issubset(expected_by_code)
        ):
            return False
        for code, stored_row in persisted_by_code.items():
            expected_row = expected_by_code[code]
            if any(
                stored_row.get(column) != expected_row.get(column)
                for column in (
                    "industry_name", "source_record_key", "quality_status",
                    "source_asof_date",
                )
            ):
                return False
        return True
