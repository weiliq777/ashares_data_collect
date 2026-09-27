from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from data_collect.data_access import DataAccessKernel
from data_collect.data_access.context import RunContext
from data_collect.data_access.dataset_policy import WriteMode
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.industry_membership_repository import (
    IndustryMembershipRepository,
)
from tests._fake_db import FakeConnection


SHANGHAI = ZoneInfo("Asia/Shanghai")


def context() -> RunContext:
    return RunContext(
        run_id="industry-test-20260925",
        job_name="a_share_instrument_tushare",
        code_version="git:test",
        config_hash="config:test",
        pipeline_version="data-access-v1",
    )


def rows() -> list[dict[str, object]]:
    return [
        {"ts_code": "600000.SH", "industry": "银行"},
        {"ts_code": "000001.SZ", "industry": None},
    ]


def make_repository():
    connection = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(),
        connection_factory=lambda: connection,
    )
    return IndustryMembershipRepository(kernel), kernel, connection


def install_daily_store(repository, calendar_rows, *, initial_rows=None):
    stored = list(initial_rows or [])
    writes = []

    def query(**kwargs):
        sql = kwargs["sql"]
        if "FROM std.trading_calendar" in sql:
            return list(calendar_rows)
        if "FROM pit.industry_membership_daily" in sql:
            return list(stored)
        raise AssertionError(f"unexpected repository read: {sql}")

    def insert(context, scope, target, values):
        assert target == "pit.industry_membership_daily"
        batch = [dict(value) for value in values]
        writes.append(batch)
        stored.extend(batch)
        return len(batch), len(batch)

    repository.kernel.read._repository_query = query
    repository.kernel.write.bulk_insert_ignore = insert
    return stored, writes


def test_persists_name_only_daily_observations_without_fabricating_industry_code():
    repository, kernel, _ = make_repository()
    stored, writes = install_daily_store(
        repository, [{"is_open": True}],
    )

    result = repository.persist_daily_capture(
        rows(),
        run_date="20260925",
        fetched_at=datetime(2026, 9, 25, 18, 5, tzinfo=SHANGHAI),
        context=context(),
    )

    assert result.status == "CAPTURED"
    assert result.trade_date == date(2026, 9, 25)
    assert result.inserted_rows == 2
    assert result.unknown_industry_rows == 1
    assert len(writes) == 1
    by_code = {row["ts_code"]: row for row in stored}
    assert by_code["600000.SH"]["industry_code"] is None
    assert by_code["600000.SH"]["industry_name"] == "银行"
    assert by_code["600000.SH"]["industry_system"] == "TUSHARE_STOCK_BASIC"
    assert by_code["600000.SH"]["quality_status"] == "DAILY_CAPTURED"
    assert by_code["000001.SZ"]["quality_status"] == "UNKNOWN"
    assert all(row["source_asof_date"] == date(2026, 9, 25) for row in stored)
    assert all(row["data_available_timestamp"].tzinfo is not None for row in stored)
    kernel.close()


def test_identical_same_day_retry_is_idempotent_and_keeps_original_capture_time():
    repository, kernel, _ = make_repository()
    stored, writes = install_daily_store(repository, [{"is_open": True}])
    first = datetime(2026, 9, 25, 18, 5, tzinfo=SHANGHAI)
    repository.persist_daily_capture(
        rows(), run_date="20260925", fetched_at=first, context=context()
    )

    result = repository.persist_daily_capture(
        rows(),
        run_date="20260925",
        fetched_at=datetime(2026, 9, 25, 18, 20, tzinfo=SHANGHAI),
        context=context(),
    )

    assert result.status == "ALREADY_CAPTURED"
    assert result.inserted_rows == 0
    assert len(writes) == 1
    assert all(row["data_available_timestamp"] == first for row in stored)
    kernel.close()


def test_retry_completes_an_interrupted_partial_daily_snapshot_without_rewriting_rows():
    repository, kernel, _ = make_repository()
    full_snapshot = repository._snapshot_rows(
        rows(), date(2026, 9, 25), datetime(2026, 9, 25, 18, 5, tzinfo=SHANGHAI)
    )
    stored, writes = install_daily_store(
        repository,
        [{"is_open": True}],
        initial_rows=[dict(full_snapshot[0])],
    )

    result = repository.persist_daily_capture(
        rows(),
        run_date="20260925",
        fetched_at=datetime(2026, 9, 25, 18, 20, tzinfo=SHANGHAI),
        context=context(),
    )

    assert result.status == "RESUMED_PARTIAL_CAPTURE"
    assert result.inserted_rows == 1
    assert len(writes) == 1
    assert len(writes[0]) == 1
    assert writes[0][0]["ts_code"] == "600000.SH"
    assert len(stored) == 2
    kernel.close()


def test_same_day_changed_industry_fails_closed_instead_of_overwriting_snapshot():
    repository, kernel, _ = make_repository()
    stored, writes = install_daily_store(repository, [{"is_open": True}])
    repository.persist_daily_capture(
        rows(), run_date="20260925",
        fetched_at=datetime(2026, 9, 25, 18, 5, tzinfo=SHANGHAI),
        context=context(),
    )

    changed = [{"ts_code": "600000.SH", "industry": "金融"}, rows()[1]]
    with pytest.raises(RuntimeError, match="same trade date"):
        repository.persist_daily_capture(
            changed, run_date="20260925",
            fetched_at=datetime(2026, 9, 25, 18, 20, tzinfo=SHANGHAI),
            context=context(),
        )

    assert len(writes) == 1
    assert next(row for row in stored if row["ts_code"] == "600000.SH")["industry_name"] == "银行"
    kernel.close()


@pytest.mark.parametrize(
    ("input_rows", "message"),
    [([], "empty"), ([rows()[0], rows()[0]], "duplicate")],
)
def test_empty_or_duplicate_source_rows_are_rejected(input_rows, message):
    repository, kernel, _ = make_repository()
    install_daily_store(repository, [{"is_open": True}])

    with pytest.raises(ValueError, match=message):
        repository.persist_daily_capture(
            input_rows, run_date="20260925",
            fetched_at=datetime(2026, 9, 25, 18, 5, tzinfo=SHANGHAI),
            context=context(),
        )
    kernel.close()


def test_daily_capture_policy_allows_append_only_writes():
    policy = build_default_registry().require("pit.industry_membership_daily")
    assert policy.write_modes == frozenset({WriteMode.BULK_INSERT})


@pytest.mark.parametrize(
    ("input_rows", "message"),
    [
        ([{"ts_code": "not-a-code", "industry": "银行"}], "invalid stock_basic ts_code"),
        ([{"ts_code": "600000.SH"}], "missing industry"),
    ],
)
def test_invalid_source_identity_or_missing_field_is_rejected(input_rows, message):
    repository, kernel, _ = make_repository()
    install_daily_store(repository, [{"is_open": True}])

    with pytest.raises(ValueError, match=message):
        repository.persist_daily_capture(
            input_rows, run_date="20260925",
            fetched_at=datetime(2026, 9, 25, 18, 5, tzinfo=SHANGHAI),
            context=context(),
        )
    kernel.close()


@pytest.mark.parametrize(
    ("run_date", "fetched_at", "calendar_rows", "expected_status"),
    [
        ("20260924", datetime(2026, 9, 25, 18, 5, tzinfo=SHANGHAI), [{"is_open": True}], "SKIPPED_NON_CURRENT_DATE"),
        ("20260925", datetime(2026, 9, 25, 16, 50, tzinfo=SHANGHAI), [{"is_open": True}], "SKIPPED_BEFORE_CLOSE"),
        ("20260925", datetime(2026, 9, 25, 18, 5, tzinfo=SHANGHAI), [{"is_open": False}], "SKIPPED_NON_TRADING_DAY"),
        ("20260925", datetime(2026, 9, 25, 18, 5, tzinfo=SHANGHAI), [], "SKIPPED_CALENDAR_MISSING"),
    ],
)
def test_snapshot_is_never_backdated_or_captured_before_close(
    run_date, fetched_at, calendar_rows, expected_status,
):
    repository, kernel, _ = make_repository()
    _, writes = install_daily_store(repository, calendar_rows)

    result = repository.persist_daily_capture(
        rows(), run_date=run_date, fetched_at=fetched_at, context=context()
    )

    assert result.status == expected_status
    assert result.trade_date is None
    assert writes == []
    kernel.close()


def test_ambiguous_calendar_rows_fail_closed_without_writing():
    repository, kernel, _ = make_repository()
    _, writes = install_daily_store(
        repository, [{"is_open": True}, {"is_open": False}]
    )

    with pytest.raises(RuntimeError, match="ambiguous"):
        repository.persist_daily_capture(
            rows(),
            run_date="20260925",
            fetched_at=datetime(2026, 9, 25, 18, 5, tzinfo=SHANGHAI),
            context=context(),
        )

    assert writes == []
    kernel.close()
