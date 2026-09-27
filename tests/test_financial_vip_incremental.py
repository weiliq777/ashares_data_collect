from pathlib import Path
from datetime import date

import pandas as pd
import pytest

from data_collect.data_access.context import RunContext, WorkScope

from data_collect.jobs import tushare_financial_vip_incremental as vip


def test_recent_report_periods_never_selects_an_open_future_quarter():
    assert vip._recent_report_periods(date(2026, 8, 14), 3) == [
        "20260630", "20260331", "20251231"
    ]


def test_indicator_vip_keeps_only_current_update_flag_and_has_safe_identity(monkeypatch):
    anomalies = []
    monkeypatch.setattr(
        vip,
        "_record_anomaly",
        lambda *args, **kwargs: anomalies.append((args, kwargs)),
    )
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ", "000002.SZ"],
            "ann_date": ["20250823", "20250823", "20250823"],
            "end_date": ["20250630", "20250630", "20250630"],
            "update_flag": ["0", "1", "1"],
            "roe": [1.0, 2.0, 3.0],
        }
    )

    records = vip._validate_and_prepare(
        frame, "fina_indicator_vip", "fina_indicator", "20250630", "batch-1"
    )

    assert [row["ts_code"] for row in records] == ["000001.SZ", "000002.SZ"]
    assert [row["roe"] for row in records] == [2.0, 3.0]
    assert anomalies == []


def test_indicator_vip_quarantines_ambiguous_current_rows(monkeypatch):
    anomalies = []
    monkeypatch.setattr(
        vip,
        "_record_anomaly",
        lambda *args, **kwargs: anomalies.append((args, kwargs)),
    )
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "ann_date": ["20250823", "20250823"],
            "end_date": ["20250630", "20250630"],
            "update_flag": ["1", "1"],
            "roe": [1.0, 2.0],
        }
    )

    records = vip._validate_and_prepare(
        frame, "fina_indicator_vip", "fina_indicator", "20250630", "batch-2"
    )

    assert records == []
    assert any(args[4] == "INDICATOR_IDENTITY_AMBIGUOUS" for args, _ in anomalies)


def test_report_vip_requires_identity_columns_and_current_flag(monkeypatch):
    anomalies = []
    monkeypatch.setattr(
        vip,
        "_record_anomaly",
        lambda *args, **kwargs: anomalies.append((args, kwargs)),
    )
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "ann_date": ["20250823", "20250823"],
            "f_ann_date": ["20250823", "20250823"],
            "end_date": ["20250630", "20250630"],
            "report_type": [1, 1],
            "comp_type": [1, 1],
            "end_type": [2, 2],
            "update_flag": [0, 1],
            "n_income": [10.0, 10.0],
        }
    )

    records = vip._validate_and_prepare(
        frame, "income_vip", "income", "20250630", "batch-3"
    )

    assert len(records) == 1
    assert records[0]["update_flag"] == 1
    assert anomalies == []


def test_report_vip_missing_identity_is_quarantined(monkeypatch):
    anomalies = []
    monkeypatch.setattr(
        vip,
        "_record_anomaly",
        lambda *args, **kwargs: anomalies.append((args, kwargs)),
    )
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "ann_date": ["20250823"],
            "end_date": ["20250630"],
            "update_flag": [1],
            "n_income": [10.0],
        }
    )

    records = vip._validate_and_prepare(
        frame, "income_vip", "income", "20250630", "batch-4"
    )

    assert records == []
    assert any(args[4] == "MISSING_IDENTITY_COLUMNS" for args, _ in anomalies)


def test_financial_vip_migration_has_bounded_cutover_and_atomic_queue_contract():
    sql = Path("sql/046_financial_vip_incremental.sql").read_text(encoding="utf-8")
    source = Path(
        "data_collect/jobs/tushare_financial_vip_incremental.py"
    ).read_text(encoding="utf-8")
    repository = Path(
        "data_collect/data_access/repository/financial_vip_repository.py"
    ).read_text(encoding="utf-8")
    gateway = Path(
        "data_collect/data_access/write_gateway.py"
    ).read_text(encoding="utf-8")

    assert "financial_change_queue" in sql
    assert "financial_ingest_anomaly" in sql
    assert "financial_version_cutover_watermark" in sql
    assert "available_date DATE" in sql
    assert "processed_through DATE" in sql
    assert "'RESOLVED'" in sql
    assert "legacy-cutover-046" in sql
    assert "tushare_income_raw" not in sql.split("CREATE TABLE IF NOT EXISTS financial_change_queue", 1)[1].split("CREATE TABLE IF NOT EXISTS financial_ingest_anomaly", 1)[0]
    assert "_write_versions_and_queue" in source
    assert "ON CONFLICT DO NOTHING" in gateway
    assert "_db_connection" not in repository
    assert "financial_change_queue" in repository
    assert "limit=PAGE_SIZE, offset=offset" in source
    assert "r.record_id > %s" in repository
    assert "def _legacy_pending_codes" in source
    assert "set(queue_codes) | set(legacy_codes)" in source


def test_financial_vip_page_lookup_has_nonblocking_batch_hash_indexes():
    sql = Path("sql/060_financial_vip_page_lookup_indexes.sql").read_text(encoding="utf-8")
    assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS" in sql
    assert "(batch_id, dataset, payload_hash)" in sql
    assert "(batch_id, payload_hash)" in sql


def test_financial_queue_lease_migration_adds_owner_and_expiry_columns():
    sql = Path("sql/096_financial_change_queue_leases.sql").read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS claim_run_id" in sql
    assert "ADD COLUMN IF NOT EXISTS claim_owner_id" in sql
    assert "ADD COLUMN IF NOT EXISTS lease_expires_at" in sql
    assert "idx_financial_change_queue_lease" in sql


def test_financial_queue_temporal_scope_and_quarantine_migrations_are_explicit():
    temporal = Path("sql/097_financial_change_queue_temporal_scope.sql").read_text(encoding="utf-8")
    quarantine = Path("sql/098_financial_change_queue_quarantine_resolution.sql").read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS available_date" in temporal
    assert "ADD COLUMN IF NOT EXISTS processed_through" in temporal
    assert "COALESCE(v.f_ann_date, v.ann_date, q.report_date)" in temporal
    assert "idx_financial_change_queue_temporal_claim" in temporal
    assert "ADD COLUMN IF NOT EXISTS quarantine_resolved_by" in quarantine
    assert "'RESOLVED'" in quarantine


def test_financial_queue_consumer_drains_multiple_bounded_batches(monkeypatch):
    batches = [([1], ["000001.SZ"]), ([2], ["000002.SZ"]), ([], [])]
    calls = []
    limits = []

    class Repository:
        def renew_queue_lease(self, queue_ids, context, lease_seconds):
            calls.append(("renew", tuple(queue_ids), lease_seconds))

        def finish_queue(self, queue_ids, status, error, context, claim_owner_id, *, processed_through):
            calls.append(("finish", tuple(queue_ids), status, processed_through))
            return len(queue_ids)

        def finish_legacy(self, codes, status, error, context):
            return len(codes)

        def claimed_queue_datasets(self, queue_ids, context, claim_owner_id):
            return {"income"}

        def queue_status_counts(self, context, through_date):
            return {"DONE": 2}

    monkeypatch.setattr(
        vip, "_pending_queue_codes",
        lambda repository, context, *, limit, claim_owner_id, takeover, through_date: limits.append(limit) or batches.pop(0),
    )
    monkeypatch.setattr(vip, "_legacy_pending_codes", lambda repository, context, *, limit: [])

    from data_collect.jobs import tushare_fina_indicator_version as indicator
    from data_collect.jobs import tushare_foundation_features as features

    monkeypatch.setattr(vip, "standard_run", lambda *args: calls.append(("standard", args[2])))
    monkeypatch.setattr(indicator, "_rebuild_standard_unlocked", lambda *args: calls.append(("indicator", args[1])))
    monkeypatch.setattr(features, "rebuild_company_financial_v2", lambda *args, **kwargs: calls.append(("feature", tuple(kwargs["requested_codes"]))) or {"rows": 1})

    summary = vip._consume_pending_changes(Repository(), vip.RunContext("run-1", "job", "code", "cfg", "v1"), date(2026, 9, 5))
    assert summary["processed_batches"] == 2
    assert summary["processed_queue_rows"] == 2
    assert summary["processed_stocks"] == 2
    assert [item for item in calls if item[0] == "finish"] == [
        ("finish", (1,), "DONE", date(2026, 9, 5)),
        ("finish", (2,), "DONE", date(2026, 9, 5)),
    ]
    assert all(item[2] == vip.QUEUE_LEASE_SECONDS for item in calls if item[0] == "renew")
    assert limits == [vip.QUEUE_CLAIM_LIMIT] * 3


def test_financial_consumer_rebuilds_only_typed_changed_datasets(monkeypatch):
    calls = []
    batches = [([1], ["000001.SZ"]), ([], [])]

    class Repository:
        def claimed_queue_datasets(self, queue_ids, context, claim_owner_id):
            return {"cashflow"}

        def finish_queue(self, *args, **kwargs):
            return 1

        def finish_legacy(self, *args, **kwargs):
            return 0

        def queue_status_counts(self, context, through_date):
            return {"DONE": 1}

    monkeypatch.setattr(
        vip, "_pending_queue_codes",
        lambda *args, **kwargs: batches.pop(0),
    )
    monkeypatch.setattr(vip, "_legacy_pending_codes", lambda *args, **kwargs: [])

    from data_collect.jobs import tushare_financial_raw_to_standard as standard
    from data_collect.jobs import tushare_fina_indicator_version as indicator
    from data_collect.jobs import tushare_foundation_features as features

    monkeypatch.setattr(vip, "standard_run", lambda *args: calls.append(("standard", args[0], args[1])))
    monkeypatch.setattr(indicator, "_rebuild_standard_unlocked", lambda *args: calls.append(("indicator", args[0], args[1])))
    monkeypatch.setattr(features, "rebuild_company_financial_v2", lambda *args, **kwargs: {"rows": 1})

    vip._consume_pending_changes(
        Repository(), vip.RunContext("run-1", "job", "code", "cfg", "v1"), date(2026, 9, 5),
    )
    assert calls == [("standard", "cashflow", vip.DERIVATION_PAGE_SIZE)]


def test_financial_consumer_has_a_small_stock_budget_and_aggregate_deadline():
    assert vip.QUEUE_CLAIM_LIMIT <= 100
    assert vip.QUEUE_BATCH_DEADLINE_SECONDS <= 30 * 60
    source = Path("data_collect/jobs/tushare_financial_vip_incremental.py").read_text(encoding="utf-8")
    assert "legacy_limit = max(0, QUEUE_CLAIM_LIMIT - len(queue_ids))" in source
    assert "financial change consumer batch exceeds stock-code budget" in source
    assert 'check_batch_deadline("after_feature")' in source


def test_foundation_feature_partition_uses_the_declared_target_table(monkeypatch):
    from data_collect.jobs import tushare_foundation_features as features

    class Kernel:
        def close(self):
            pass

    class Repository:
        def publish_feature_partition(self, target, rows, codes, output_start, output_end, context):
            assert target == "feature.company_financial_v2"
            return 1

    monkeypatch.setattr(features, "_financial_access", lambda run_id=None: (Kernel(), Repository(), object()))
    assert features._write_partition(
        "feature.company_financial_v2", ["000001.SZ"], [{"ts_code": "000001.SZ"}],
        date(2026, 1, 1), date(2026, 1, 2), "run-1",
    ) == 1


def test_foundation_feature_rebuild_closes_kernel_on_setup_failure(monkeypatch):
    from data_collect.jobs import tushare_foundation_features as features

    class Kernel:
        closed = False

        def close(self):
            self.closed = True

    kernel = Kernel()

    class Repository:
        def stock_codes(self, *args):
            raise RuntimeError("setup failed")

    monkeypatch.setattr(features, "_financial_access", lambda run_id=None: (kernel, Repository(), object()))
    with __import__("pytest").raises(RuntimeError, match="setup failed"):
        features.rebuild_company_financial_v2(
            None, date(2020, 1, 1), date(2021, 1, 1), date(2026, 1, 1),
            requested_codes=["000001.SZ"],
        )
    assert kernel.closed


def test_financial_consumer_heartbeat_is_periodic_and_uses_a_shorter_interval():
    assert vip.QUEUE_HEARTBEAT_SECONDS < vip.QUEUE_LEASE_SECONDS
    assert vip.QUEUE_HEARTBEAT_SECONDS * 3 <= vip.QUEUE_LEASE_SECONDS


def test_financial_page_checkpoint_is_scoped_to_fetch_batch_and_uses_provider_rows():
    source = Path(
        "data_collect/jobs/tushare_financial_vip_incremental.py"
    ).read_text(encoding="utf-8")
    assert 'key = f"{batch_id}:{period}:offset:{offset}"' in source
    assert '_checkpoint(checkpoint_dataset, key, "DONE", raw_rows' in source


def test_financial_queue_consumer_scopes_claim_and_records_processed_cutoff():
    source = Path("data_collect/jobs/tushare_financial_vip_incremental.py").read_text(encoding="utf-8")
    repository = Path("data_collect/data_access/repository/financial_vip_repository.py").read_text(encoding="utf-8")
    gateway = Path("data_collect/data_access/write_gateway.py").read_text(encoding="utf-8")
    assert "through_date=output_end" in source
    assert "processed_through=output_end" in source
    assert "available_date <= %s" in gateway
    assert "processed_through=CASE" in gateway
    assert "queue_status_counts(self, context: RunContext, through_date: date)" in repository


def test_quarantine_transitions_are_bounded_and_audited():
    from data_collect.data_access.write_gateway import WriteGateway

    source = __import__("inspect").getsource(WriteGateway.requeue_quarantined_financial_queue)
    resolve_source = __import__("inspect").getsource(WriteGateway.resolve_quarantined_financial_queue)
    assert "status='QUARANTINED'" in source
    assert "quarantine_resolved_by" in source
    assert "quarantine_resolution_reason" in source
    assert "status='RESOLVED'" in resolve_source
    assert "at most 1000 quarantined queue IDs" in source


def test_quarantine_partial_transition_raises_inside_chunk_transaction():
    from types import SimpleNamespace

    from data_collect.data_access.write_gateway import WriteGateway

    class KernelProbe:
        def __init__(self):
            self.transaction_manager = SimpleNamespace(
                config=SimpleNamespace(max_rows_per_tx=1000),
            )
            self.committed = False
            self.rolled_back = False

        def _reserve_atomic_chunk(self, _rows):
            return None

        def _run_chunk(self, operation, *, idempotent):
            try:
                value = operation()
            except Exception:
                self.rolled_back = True
                raise
            self.committed = True
            return value

    kernel = KernelProbe()
    gateway = WriteGateway.__new__(WriteGateway)
    gateway._kernel = kernel

    with pytest.raises(RuntimeError, match="financial quarantine transition was partial"):
        gateway._transition_quarantined_queue(
            (11, 12),
            WorkScope(partition="queue-admin-test"),
            None,
            lambda _ids: 1,
            RunContext(
                run_id="queue-admin-run", job_name="queue-admin-test",
                code_version="test", config_hash="test", pipeline_version="test",
            ),
            expected_status="PENDING",
        )

    assert kernel.rolled_back
    assert not kernel.committed
