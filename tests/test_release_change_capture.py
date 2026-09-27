from __future__ import annotations

from datetime import date

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.release_change_capture import build_change_event
from data_collect.data_access.kernel import DataAccessKernel
from data_collect.data_access.policy_catalog import build_default_registry
from tests._fake_db import FakeConnection, FakeCursor


def context() -> RunContext:
    return RunContext(
        run_id="run-delta-1", job_name="daily", code_version="git:test",
        config_hash="cfg", pipeline_version="v1",
    )


def test_change_event_derives_a_bounded_scope_from_a_single_day_batch():
    event = build_change_event(
        context(), "raw.append", "public.tushare_daily_raw",
        WorkScope(partition="raw:public.tushare_daily_raw"),
        rows=[
            {"ts_code": "000001.SZ", "trade_date": date(2026, 9, 24)},
            {"ts_code": "600000.SH", "trade_date": date(2026, 9, 24)},
        ],
    )

    assert event["delta_eligible"] is True
    assert event["scope"]["date_from"] == "2026-09-24"
    assert event["scope"]["date_to"] == "2026-09-24"
    assert event["scope"]["codes"] == ["000001.SZ", "600000.SH"]
    assert event["input_row_count"] == 2
    assert len(event["event_key"]) == 64


def test_unbounded_partition_only_change_requires_full_audit():
    event = build_change_event(
        context(), "raw.append", "public.tushare_financial_raw_version",
        WorkScope(partition="financial:20260630"),
        rows=[{"dataset": "income", "business_key": "opaque-key", "payload_hash": "a" * 64}],
    )

    assert event["delta_eligible"] is False
    assert event["full_audit_reason"]


def test_event_identity_is_idempotent_under_input_order_changes():
    rows = [
        {"ts_code": "000001.SZ", "trade_date": date(2026, 9, 24), "amount": 10},
        {"ts_code": "600000.SH", "trade_date": date(2026, 9, 24), "amount": 20},
    ]
    first = build_change_event(
        context(), "write.upsert", "std.daily_kline",
        WorkScope(date_from=date(2026, 9, 24), date_to=date(2026, 9, 24)), rows=rows,
    )
    second = build_change_event(
        context(), "write.upsert", "std.daily_kline",
        WorkScope(date_from=date(2026, 9, 24), date_to=date(2026, 9, 24)), rows=list(reversed(rows)),
    )

    assert first["event_key"] == second["event_key"]


def test_change_capture_is_inserted_inside_the_same_kernel_transaction():
    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn,
        capture_release_changes=True,
    )
    event = build_change_event(
        context(), "write.upsert", "std.daily_kline",
        WorkScope(date_from=date(2026, 9, 24), date_to=date(2026, 9, 24)),
        rows=[{"ts_code": "000001.SZ", "trade_date": date(2026, 9, 24)}],
    )

    def write_fact():
        with conn.cursor() as cur:
            cur.execute("FACT_WRITE")
        return 1

    assert kernel._run_chunk(write_fact, change_event=event) == 1

    statements = [item["sql"] for item in conn.executed]
    shared_lock = next(i for i, sql in enumerate(statements) if "pg_advisory_xact_lock_shared" in sql)
    fact_write = statements.index("FACT_WRITE")
    scope_insert = next(i for i, sql in enumerate(statements) if "INSERT INTO ops.release_change_scope" in sql)
    event_insert = next(i for i, sql in enumerate(statements) if "INSERT INTO ops.release_change_event" in sql)
    assert shared_lock < fact_write < scope_insert < event_insert
    assert scope_insert < event_insert
    assert conn.committed == 1
    assert conn.rolled_back == 0


def test_change_event_insert_failure_rolls_back_the_fact_chunk():
    class FailingEventCursor(FakeCursor):
        def execute(self, sql, params=None):
            if "INSERT INTO ops.release_change_event" in sql:
                raise RuntimeError("ledger unavailable")
            return super().execute(sql, params)

    class FailingEventConnection(FakeConnection):
        def cursor(self, name=None, withhold=False):
            self.cursor_names.append(name)
            return FailingEventCursor(self, name, withhold)

    conn = FailingEventConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn,
        capture_release_changes=True,
    )
    event = build_change_event(
        context(), "UPSERT", "std.daily_kline",
        WorkScope(date_from=date(2026, 9, 24), date_to=date(2026, 9, 24)),
        rows=[{"ts_code": "000001.SZ", "trade_date": date(2026, 9, 24)}],
    )

    import pytest

    with pytest.raises(RuntimeError, match="ledger unavailable"):
        kernel._run_chunk(lambda: 1, idempotent=False, change_event=event)
    assert conn.committed == 0
    assert conn.rolled_back == 1


def test_capture_is_opt_in_for_safe_rollout():
    conn = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn,
    )
    event = build_change_event(
        context(), "write.upsert", "std.daily_kline",
        WorkScope(date_from=date(2026, 9, 24), date_to=date(2026, 9, 24)), rows=[],
    )

    assert kernel._run_chunk(lambda: 1, change_event=event) == 1
    assert not any("INSERT INTO ops.release_change_event" in item["sql"] for item in conn.executed)


def test_rows_outside_declared_scope_cannot_be_marked_delta_eligible():
    event = build_change_event(
        context(), "UPSERT", "std.daily_kline",
        WorkScope(date_from=date(2026, 9, 24), date_to=date(2026, 9, 24)),
        rows=[{"ts_code": "000001.SZ", "trade_date": date(2026, 9, 25)}],
    )

    assert event["delta_eligible"] is False
    assert "follows declared date scope" in event["full_audit_reason"]


def test_unbounded_legacy_history_window_is_not_delta_eligible():
    event = build_change_event(
        context(), "REBUILD", "feature.company_financial_v2",
        WorkScope(
            date_from=date(2021, 8, 1), date_to=date(2026, 9, 24),
        ),
        statement_fingerprint="f" * 64,
    )

    assert event["delta_eligible"] is False
    assert "exceeds" in event["full_audit_reason"]


def test_release_delta_schema_has_append_only_control_plane_tables():
    from pathlib import Path

    migration = (
        Path(__file__).resolve().parents[1] / "sql/110_release_change_capture.sql"
    ).read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS ops.release_change_scope" in migration
    assert "CREATE TABLE IF NOT EXISTS ops.release_change_event" in migration
    assert "reject_release_change_mutation" in migration
    assert "REVOKE UPDATE, DELETE, TRUNCATE" in migration


class _ScriptedCaptureCursor(FakeCursor):
    def execute(self, sql, params=None):
        super().execute(sql, params)
        normalized = " ".join(sql.split()).lower()
        if "from ops.tushare_data_release" in normalized and "release_change_watermark" in normalized:
            self._fetch_rows = [(42, 7)]
        elif "coalesce(max(event_id), 0)" in normalized:
            self._fetch_rows = [(9,)]
        elif "from ops.release_delta_package" in normalized:
            self._fetch_rows = []
        elif "select release_id from ops.tushare_data_release" in normalized and "for update" in normalized:
            self._fetch_rows = [(42,)]
        elif "from ops.release_change_event e" in normalized:
            self._fetch_rows = [
                (8, "a" * 64, "run-a", "std.daily_kline", "UPSERT", "b" * 64, True, None, "SUCCEEDED"),
                (9, "c" * 64, "run-b", "feature.price_daily_v2", "REBUILD", "d" * 64, True, None, "SUCCEEDED"),
            ]
        if normalized.startswith("update ops.tushare_data_release") and "set status='validated'" in normalized:
            self.rowcount = 1


class _ScriptedCaptureConnection(FakeConnection):
    def cursor(self, name=None, withhold=False):
        self.cursor_names.append(name)
        return _ScriptedCaptureCursor(self, name, withhold)


def _capture_kernel(conn):
    return DataAccessKernel(
        policy_registry=build_default_registry(), connection_factory=lambda: conn,
        capture_release_changes=True,
    )


def test_release_validation_boundary_uses_exclusive_ledger_lock_and_watermark():
    conn = _ScriptedCaptureConnection()
    kernel = _capture_kernel(conn)
    watermark = kernel.write.begin_release_validation_boundary(
        context(), WorkScope(partition="release:validation-boundary")
    )

    statements = [item["sql"] for item in conn.executed]
    lock_index = next(i for i, sql in enumerate(statements) if "pg_advisory_xact_lock(" in sql)
    watermark_index = next(i for i, sql in enumerate(statements) if "COALESCE(MAX(event_id), 0)" in sql)
    assert watermark == 9
    assert lock_index < watermark_index
    assert conn.committed == 1


def test_delta_candidate_links_exact_events_and_requires_a_valid_watermarked_parent():
    conn = _ScriptedCaptureConnection()
    kernel = _capture_kernel(conn)
    result = kernel.write.capture_release_delta_candidate(
        context(), WorkScope(partition="release-delta:capture"), 42, "RESEARCH"
    )

    statements = [item["sql"] for item in conn.executed]
    lock_index = next(i for i, sql in enumerate(statements) if "pg_advisory_xact_lock(" in sql)
    parent_index = next(i for i, sql in enumerate(statements) if "FROM ops.tushare_data_release" in sql)
    insert_index = next(i for i, sql in enumerate(statements) if "INSERT INTO ops.release_delta_package" in sql)
    assert result["status"] == "CAPTURED"
    assert result["event_count"] == 2
    assert result["event_from_id"] == 8
    assert result["event_through_id"] == 9
    assert lock_index < parent_index < insert_index
    link_writes = [item for item in conn.executed if item["kind"] == "executemany"]
    assert len(link_writes) == 1
    assert len(link_writes[0]["params"]) == 2
    event_scan = next(item for item in conn.executed if "FROM ops.release_change_event e" in item["sql"])
    assert "ORDER BY e.event_id LIMIT %s" in event_scan["sql"]
    assert event_scan["params"] == (7, 9, 1_001)
    assert not any("FROM feature." in item["sql"] or "FROM std." in item["sql"] for item in conn.executed)
    assert conn.committed == 1


def test_delta_candidate_fails_closed_when_any_event_is_not_delta_eligible():
    class IneligibleCursor(_ScriptedCaptureCursor):
        def execute(self, sql, params=None):
                super().execute(sql, params)
                if "from ops.release_change_event e" in " ".join(sql.split()).lower():
                    self._fetch_rows[1] = (*self._fetch_rows[1][:6], False, None, "SUCCEEDED")

    class IneligibleConnection(_ScriptedCaptureConnection):
        def cursor(self, name=None, withhold=False):
            self.cursor_names.append(name)
            return IneligibleCursor(self, name, withhold)

    conn = IneligibleConnection()
    kernel = _capture_kernel(conn)

    import pytest

    with pytest.raises(RuntimeError, match="full audit"):
        kernel.write.capture_release_delta_candidate(
            context(), WorkScope(partition="release-delta:capture"), 42, "RESEARCH"
        )
    assert not any("INSERT INTO ops.release_delta_package" in item["sql"] for item in conn.executed)
    assert conn.committed == 0
    assert conn.rolled_back == 1


def test_delta_candidate_fails_closed_when_a_producer_run_is_not_successful():
    class FailedProducerCursor(_ScriptedCaptureCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if "from ops.release_change_event e" in " ".join(sql.split()).lower():
                self._fetch_rows[0] = (*self._fetch_rows[0][:8], "FAILED")

    class FailedProducerConnection(_ScriptedCaptureConnection):
        def cursor(self, name=None, withhold=False):
            self.cursor_names.append(name)
            return FailedProducerCursor(self, name, withhold)

    conn = FailedProducerConnection()
    kernel = _capture_kernel(conn)
    import pytest

    with pytest.raises(RuntimeError, match="full audit"):
        kernel.write.capture_release_delta_candidate(
            context(), WorkScope(partition="release-delta:capture"), 42, "RESEARCH"
        )
    assert not any("INSERT INTO ops.release_delta_package" in item["sql"] for item in conn.executed)
    assert conn.rolled_back == 1


def test_delta_candidate_rejects_legacy_parent_without_a_trusted_watermark():
    class LegacyParentCursor(_ScriptedCaptureCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if "from ops.tushare_data_release" in " ".join(sql.split()).lower():
                self._fetch_rows = [(42, None)]

    class LegacyParentConnection(_ScriptedCaptureConnection):
        def cursor(self, name=None, withhold=False):
            self.cursor_names.append(name)
            return LegacyParentCursor(self, name, withhold)

    conn = LegacyParentConnection()
    kernel = _capture_kernel(conn)
    import pytest

    with pytest.raises(RuntimeError, match="captured full audit first"):
        kernel.write.capture_release_delta_candidate(
            context(), WorkScope(partition="release-delta:capture"), 42, "RESEARCH"
        )
    assert conn.rolled_back == 1


def test_full_release_commit_rejects_events_written_during_quality_audit():
    class AdvancedWatermarkCursor(_ScriptedCaptureCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if "coalesce(max(event_id), 0)" in " ".join(sql.split()).lower():
                self._fetch_rows = [(10,)]

    class AdvancedWatermarkConnection(_ScriptedCaptureConnection):
        def cursor(self, name=None, withhold=False):
            self.cursor_names.append(name)
            return AdvancedWatermarkCursor(self, name, withhold)

    conn = AdvancedWatermarkConnection()
    kernel = _capture_kernel(conn)
    result = {
        "tag": "release-fenced", "baseline_domain": "RESEARCH",
        "quality_hash": "q" * 64, "gate_json": "{}", "manifest_json": "{}",
        "source_manifest_hash": "m" * 64, "release_change_watermark": 9,
    }

    import pytest

    with pytest.raises(RuntimeError, match="source changes occurred"):
        kernel.write.commit_release_validation(
            context(), WorkScope(partition="release:commit:release-fenced"), result,
        )
    assert not any("SET status='superseded'" in item["sql"] for item in conn.executed)
    assert conn.committed == 0
    assert conn.rolled_back == 1


def test_full_release_commit_persists_the_verified_event_watermark():
    conn = _ScriptedCaptureConnection()
    kernel = _capture_kernel(conn)
    result = {
        "tag": "release-fenced", "baseline_domain": "RESEARCH",
        "quality_hash": "q" * 64, "gate_json": "{}", "manifest_json": "{}",
        "source_manifest_hash": "m" * 64, "release_change_watermark": 9,
    }

    assert kernel.write.commit_release_validation(
        context(), WorkScope(partition="release:commit:release-fenced"), result,
    ) == 1
    update = next(
        item for item in conn.executed
        if item["sql"].lstrip().startswith("UPDATE ops.tushare_data_release")
        and "release_change_watermark=%s" in item["sql"]
    )
    assert update["params"][4] == 9
    assert conn.committed == 1


def test_capture_enabled_release_cannot_bypass_the_full_audit_watermark():
    conn = _ScriptedCaptureConnection()
    kernel = _capture_kernel(conn)
    result = {
        "tag": "release-unfenced", "baseline_domain": "RESEARCH",
        "quality_hash": "q" * 64, "gate_json": "{}", "manifest_json": "{}",
        "source_manifest_hash": "m" * 64,
    }

    import pytest

    with pytest.raises(RuntimeError, match="must include its audited event watermark"):
        kernel.write.commit_release_validation(
            context(), WorkScope(partition="release:commit:release-unfenced"), result,
        )
    assert conn.executed == []


def test_delta_candidate_schema_links_events_once_and_keeps_packages_unvalidated():
    from pathlib import Path

    migration = (
        Path(__file__).resolve().parents[1] / "sql/111_release_delta_packages.sql"
    ).read_text(encoding="utf-8")
    assert "release_change_watermark" in migration
    assert "CREATE TABLE IF NOT EXISTS ops.release_delta_package" in migration
    assert "CREATE TABLE IF NOT EXISTS ops.release_delta_event" in migration
    assert "UNIQUE (baseline_domain, event_id)" in migration
    assert "CAPTURED" in migration
    assert "VALIDATED" not in migration


def test_delta_candidate_reader_is_bounded_and_checks_exact_membership():
    from types import SimpleNamespace
    from data_collect.data_access.repository.release_repository import ReleaseRepository

    calls = []

    class ReadProbe:
        def _repository_query(self, context, source, sql, params, scope, max_rows):
            calls.append((source, sql, params, scope, max_rows))
            if source == "ops.release_delta_package":
                return [{"package_id": "00000000-0000-0000-0000-000000000001",
                         "baseline_domain": "RESEARCH", "event_count": 1}]
            return [{"event_id": 7, "event_key": "a" * 64}]

    repository = ReleaseRepository(SimpleNamespace(read=ReadProbe()))
    candidate = repository.delta_candidate(
        "00000000-0000-0000-0000-000000000001", context()
    )

    assert candidate is not None
    assert len(candidate["events"]) == candidate["event_count"] == 1
    assert calls[0][0] == "ops.release_delta_package" and calls[0][4] == 2
    assert calls[1][0] == "ops.release_delta_event" and calls[1][4] == 1_001
    assert "LIMIT 1001" in calls[1][1]


def test_delta_candidate_reader_rejects_missing_event_links():
    from types import SimpleNamespace
    import pytest
    from data_collect.data_access.repository.release_repository import ReleaseRepository

    class ReadProbe:
        def _repository_query(self, context, source, sql, params, scope, max_rows):
            if source == "ops.release_delta_package":
                return [{"package_id": "p", "baseline_domain": "RESEARCH", "event_count": 2}]
            return [{"event_id": 7}]

    repository = ReleaseRepository(SimpleNamespace(read=ReadProbe()))
    with pytest.raises(RuntimeError, match="membership mismatch"):
        repository.delta_candidate("p", context())
