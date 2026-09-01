from datetime import date

import pandas as pd
import pytest

from data_collect.jobs import tushare_daily_incremental
from data_collect.jobs import tushare_derived_rebuild
from data_collect.jobs import tushare_phase2_features
from data_collect.jobs import tushare_phase2_standard
from data_collect.jobs import tushare_phase2_incremental
from data_collect.jobs import tushare_raw_to_standard
from data_collect.jobs import tushare_job_lock
from data_collect.jobs import research_snapshot_series
from data_collect.jobs import tushare_financial_raw_init
from data_collect.jobs import tushare_fina_indicator_version
from data_collect.jobs import tushare_market_incremental_by_date
from data_collect.normalize.tushare_daily import normalize_daily_basic
from data_collect.jobs.research_snapshot import FOUNDATION_SCHEMA_VERSION
from data_collect.data_access import DataAccessKernel, RunContext, WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.feature_repository import FeatureRepository
from data_collect.data_access.repository.pit_repository import PitRepository
from data_collect.data_access.repository.security_repository import SecurityRepository
from tests._fake_db import FakeConnection


def _da_context() -> RunContext:
    return RunContext("test-run", "test-job", "git:test", "cfg", "v1")


def test_normalize_daily_basic_converts_missing_vendor_values_to_sql_null():
    frame = pd.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["20260817", "20260817"],
        "pe_ttm": [5.0, float("nan")],
        "pb": [None, 0.8],
    })

    result = normalize_daily_basic(frame)

    assert result.loc[0, "pe_ttm"] == 5.0
    assert result.loc[1, "pe_ttm"] is None
    assert result.loc[0, "pb"] is None
    assert result.loc[1, "pb"] == 0.8


class _Cursor:
    def __init__(self):
        self.commands = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.commands.append((sql, params))
        self.rowcount = 1

    def executemany(self, sql, rows):
        self.commands.append((sql, list(rows)))
        self.rowcount = len(rows)

    def fetchone(self):
        return (True,)


class _Connection:
    def __init__(self):
        self.cur = _Cursor()
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def test_replace_day_rolls_back_delete_when_insert_fails(monkeypatch):
    conn = _Connection()
    rows = [{"stock_code": "000001.SZ", "trade_date": date(2026, 8, 6)}]

    def fail_insert(*args, **kwargs):
        raise RuntimeError("insert failed")

    monkeypatch.setattr(conn.cur, "executemany", fail_insert)
    kernel = DataAccessKernel(policy_registry=build_default_registry(), connection_factory=lambda: conn)
    with pytest.raises(RuntimeError, match="insert failed"):
        kernel.write.replace_day(
            _da_context(), WorkScope(date_from=date(2026, 8, 6), date_to=date(2026, 8, 6)),
            "std.daily_kline", "trade_date", date(2026, 8, 6), rows,
        )

    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert conn.cur.commands[0][0].startswith("DELETE FROM std.daily_kline")


def test_replace_day_commits_delete_and_insert_once(monkeypatch):
    conn = _Connection()
    rows = [{"stock_code": "000001.SZ", "trade_date": date(2026, 8, 6)}]
    kernel = DataAccessKernel(policy_registry=build_default_registry(), connection_factory=lambda: conn)
    assert kernel.write.replace_day(
        _da_context(), WorkScope(date_from=date(2026, 8, 6), date_to=date(2026, 8, 6)),
        "std.daily_kline", "trade_date", date(2026, 8, 6), rows,
    ) == (1, 1)
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert len(conn.cur.commands) == 2


def test_daily_basic_incremental_preserves_foundation_fields(monkeypatch):
    captured = {}

    def replace(self, table, trade_date, rows, context):
        captured.update({"table": table, "trade_date": trade_date, "frame": pd.DataFrame(rows)})
        return len(rows)

    monkeypatch.setattr(
        tushare_daily_incremental.MarketRepository,
        "replace_daily_partition",
        replace,
    )
    frame = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": ["20260806"],
        "pe_ttm": [6.5],
        "pb": [0.7],
        "ps_ttm": [1.2],
        "dv_ratio": [2.0],
        "dv_ttm": [3.5],
    })
    tushare_daily_incremental._rebuild_market_date(
        frame, "daily_basic", date(2026, 8, 6)
    )
    row = captured["frame"].iloc[0]
    assert captured["table"] == "valuation_daily"
    assert row["dv_ttm"] == 3.5
    assert row["dv_ttm_vendor"] == 3.5
    assert row["pe_ttm_vendor"] == 6.5
    assert row["dv_ttm_source"] == "TUSHARE_DAILY_BASIC"
    assert row["data_available_timestamp"].tzinfo is not None


def test_market_sample_mode_cannot_replace_production_standard():
    with pytest.raises(ValueError, match="隔离 smoke 表"):
        tushare_daily_incremental.run.__wrapped__(limit_stocks=5, dry_run=False)
    with pytest.raises(ValueError, match="隔离 smoke 表"):
        tushare_market_incremental_by_date.run.__wrapped__(
            limit_stocks=5, dry_run=False
        )


def test_legacy_market_incremental_write_entry_is_fail_closed():
    with pytest.raises(RuntimeError, match="已废弃生产写入"):
        tushare_daily_incremental.run.__wrapped__(dry_run=False)


def test_market_raw_writer_reports_actual_immutable_changes(monkeypatch):
    monkeypatch.setattr(
        tushare_daily_incremental.MarketRepository,
        "append_market_versions",
        lambda *args, **kwargs: (0, 0),
    )
    frame = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": ["20260806"],
        "close": [10.0],
    })

    assert tushare_daily_incremental._version_and_snapshot(
        "daily", frame, "repeat"
    ) == (0, 0)


def test_market_derived_retry_is_not_gated_by_new_raw_row_count():
    source = __import__("pathlib").Path(
        "data_collect/jobs/tushare_market_incremental_by_date.py"
    ).read_text(encoding="utf-8")
    condition = source.split("# Re-run the bounded derived interval", 1)[1].split(
        "from data_collect.jobs.tushare_derived_rebuild", 1
    )[0]
    assert "materialized_partitions" in condition
    assert " and total" not in condition


def test_phase2_calendar_refresh_preserves_corrections_in_version_layer(monkeypatch):
    frame = pd.DataFrame({
        "exchange": ["SSE"], "cal_date": ["20260806"],
        "is_open": [1], "pretrade_date": ["20260805"],
    })
    calls = []
    monkeypatch.setattr(tushare_phase2_incremental, "call_api", lambda *a, **k: frame)
    monkeypatch.setattr(
        tushare_daily_incremental,
        "_version_and_snapshot",
        lambda dataset, value, batch: calls.append((dataset, len(value), batch)) or (1, 1),
    )
    monkeypatch.setattr(
        tushare_raw_to_standard,
        "rebuild_calendar",
        lambda **kwargs: calls.append(("rebuild", kwargs)) or (1, 1),
    )

    pro = type("Pro", (), {"trade_cal": object()})()
    assert tushare_phase2_incremental._refresh_trade_calendar(
        pro, "20260805", "20260806", False
    ) == 1
    assert calls[0][0:2] == ("trade_cal", 1)
    assert calls[1] == (
        "rebuild",
        {"start_date": date(2026, 8, 5), "end_date": date(2026, 8, 6)},
    )


def test_raw_standard_full_projection_uses_bounded_upserts(monkeypatch):
    frames = [pd.DataFrame({
        "stock_code": ["000001.SZ"],
        "trade_date": [date(2026, 8, 6)],
    })]
    calls = []

    def transform(frame):
        return frame

    transform.table_name = "daily_kline"
    monkeypatch.setattr(tushare_raw_to_standard, "_payload_frames", lambda *a, **k: iter(frames))
    monkeypatch.setattr(
        tushare_raw_to_standard,
        "_upsert_frame",
        lambda frame, target, keys: calls.append((target, keys, len(frame))) or len(frame),
    )

    assert tushare_raw_to_standard._write_chunks(
        "tushare_daily_raw", transform,
        conflict_columns=("stock_code", "trade_date"),
    ) == (1, 1)
    assert calls == [("daily_kline", ("stock_code", "trade_date"), 1)]
    source = __import__("pathlib").Path(
        "data_collect/jobs/tushare_raw_to_standard.py"
    ).read_text(encoding="utf-8")
    block = source.split("def _write_chunks", 1)[1].split("def rebuild_daily", 1)[0]
    assert "TRUNCATE TABLE" not in block
    assert "CREATE UNLOGGED" not in block


def test_adj_factor_projection_accepts_a_bounded_range(monkeypatch):
    calls = []

    monkeypatch.setattr(
        tushare_raw_to_standard,
        "_write_chunks",
        lambda *args, **kwargs: calls.append((args, kwargs)) or (5, 5),
    )

    result = tushare_raw_to_standard.rebuild_adj_factor(
        start_date=date(2026, 8, 17), end_date=date(2026, 8, 21),
    )

    assert result == (5, 5)
    assert calls[0][0][0] == "tushare_adj_factor_raw"
    assert calls[0][1]["start_date"] == date(2026, 8, 17)
    assert calls[0][1]["end_date"] == date(2026, 8, 21)
    assert calls[0][1]["conflict_columns"] == (
        "stock_code", "trade_date", "source",
    )


def test_bounded_standard_rebuild_deletes_only_requested_dates():
    conn = FakeConnection()
    kernel = DataAccessKernel(policy_registry=build_default_registry(), connection_factory=lambda: conn)
    repo = SecurityRepository(kernel)
    bounds = (date(2026, 8, 5), date(2026, 8, 6))
    repo.rebuild_status_daily(WorkScope(date_from=bounds[0], date_to=bounds[1]), _da_context())
    captured = conn.executed[0]

    assert "TRUNCATE TABLE std.security_status_daily" not in captured["sql"]
    assert "DELETE FROM std.security_status_daily" in captured["sql"]
    assert "cal_date BETWEEN %s AND %s" in captured["sql"]
    assert captured["params"] == (*bounds, *bounds, *bounds)
    assert "FROM tushare_stk_limit_raw" in captured["sql"]
    assert "WHERE trade_date BETWEEN %s AND %s" in captured["sql"]


def test_tradeability_blocks_limit_direction():
    conn = FakeConnection()
    kernel = DataAccessKernel(policy_registry=build_default_registry(), connection_factory=lambda: conn)
    PitRepository(kernel).rebuild_tradeability(
        WorkScope(date_from=date(2026, 8, 5), date_to=date(2026, 8, 6)), _da_context()
    )
    captured = conn.executed[0]

    assert "is_tradeable AND limit_status <> 'UP_LIMIT'" in captured["sql"]
    assert "is_tradeable AND limit_status <> 'DOWN_LIMIT'" in captured["sql"]
    assert "BUY_BLOCKED_UP_LIMIT" in captured["sql"]
    assert "SELL_BLOCKED_DOWN_LIMIT" in captured["sql"]


def test_dividend_business_key_survives_corrected_event_dates():
    conn = FakeConnection()
    kernel = DataAccessKernel(policy_registry=build_default_registry(), connection_factory=lambda: conn)
    SecurityRepository(kernel).rebuild_dividend(_da_context())
    captured = {"sql": conn.executed[0]["sql"]}

    sql = captured["sql"]
    assert "DISTINCT ON (ts_code, end_date, div_proc)" in sql
    business_key = sql.split("AS business_key", 1)[0].rsplit("ts_code ||", 1)[1]
    assert "record_date" not in business_key
    assert "ex_date" not in business_key


def test_bounded_price_rebuild_preserves_other_dates():
    conn = FakeConnection()
    kernel = DataAccessKernel(policy_registry=build_default_registry(), connection_factory=lambda: conn)
    repo = FeatureRepository(kernel)
    bounds = (date(2026, 8, 5), date(2026, 8, 6))
    repo.rebuild_price(WorkScope(date_from=bounds[0], date_to=bounds[1]), _da_context(), ["000001.SZ"])
    captured = {"sql": conn.executed[0]["sql"], "params": conn.executed[0]["params"]}

    assert "TRUNCATE TABLE feature.price_daily_v1" not in captured["sql"]
    assert "DELETE FROM feature.price_daily_v1" in captured["sql"]
    assert "FROM metrics\n    WHERE trade_date BETWEEN %s AND %s" in captured["sql"]
    assert captured["params"] == (
        ["000001.SZ"], *bounds, ["000001.SZ"], bounds[1], *bounds
    )


def test_market_derived_orchestrator_propagates_date_bounds(monkeypatch):
    standard_calls = []
    feature_calls = []
    bounds = (date(2026, 8, 5), date(2026, 8, 6))

    monkeypatch.setattr(
        tushare_phase2_standard,
        "run",
        lambda task, start_date=None, end_date=None: (
            standard_calls.append((task, start_date, end_date)) or {task: 1}
        ),
    )
    monkeypatch.setattr(
        tushare_phase2_features,
        "run",
        lambda task, start_date=None, end_date=None: (
            feature_calls.append((task, start_date, end_date)) or {task: 1}
        ),
    )

    tushare_derived_rebuild.rebuild_after_market(*bounds)

    assert standard_calls == [
        ("status", *bounds),
        ("universe", *bounds),
        ("tradeability", *bounds),
    ]
    assert feature_calls == [
        ("price_v2", *bounds),
        ("valuation", *bounds),
    ]


def test_phase2_incremental_keeps_legacy_price_v1_out_of_daily_path(monkeypatch):
    feature_calls = []
    monkeypatch.setattr(
        tushare_phase2_standard,
        "run",
        lambda task, start_date=None, end_date=None: {task: 1},
    )
    monkeypatch.setattr(
        tushare_phase2_features,
        "run",
        lambda task, start_date=None, end_date=None: (
            feature_calls.append((task, start_date, end_date)) or {task: 1}
        ),
    )

    tushare_phase2_incremental._rebuild_standard(
        {"stock_st"}, date(2026, 8, 5), date(2026, 8, 6)
    )

    assert [task for task, _, _ in feature_calls] == ["price_v2", "valuation"]


def test_financial_derived_orchestrator_requires_changed_stock_scope(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "data_collect.jobs.tushare_financial_incremental._rebuild_financial_derived",
        lambda codes, output_end: calls.append((codes, output_end)),
    )

    with pytest.raises(ValueError, match="changed stock_codes"):
        tushare_derived_rebuild.rebuild_after_financial([], "2026-08-06")
    result = tushare_derived_rebuild.rebuild_after_financial(
        ["000002.SZ", "000001.SZ", "000001.SZ"], "2026-08-06"
    )
    assert calls == [
        (["000001.SZ", "000002.SZ"], date(2026, 8, 6))
    ]
    assert result["stocks"] == 2


def test_bounded_price_v2_rebuild_never_truncates():
    conn = FakeConnection()
    kernel = DataAccessKernel(policy_registry=build_default_registry(), connection_factory=lambda: conn)
    bounds = (date(2026, 8, 5), date(2026, 8, 6))
    FeatureRepository(kernel).rebuild_price_v2(
        WorkScope(date_from=bounds[0], date_to=bounds[1]), _da_context(), ["000001.SZ"]
    )
    sql = "\n".join(entry["sql"] for entry in conn.executed)
    assert "TRUNCATE TABLE" not in sql
    assert "DELETE FROM feature.price_daily_v2 WHERE ts_code=ANY(%s)" in sql
    assert "DELETE FROM feature.price_adjustment_factor_asof WHERE ts_code=ANY(%s)" in sql
    assert conn.committed == 2


def test_bounded_shareholder_return_preserves_other_dates():
    conn = FakeConnection()
    kernel = DataAccessKernel(policy_registry=build_default_registry(), connection_factory=lambda: conn)
    bounds = (date(2026, 8, 5), date(2026, 8, 6))
    FeatureRepository(kernel).rebuild_shareholder_return(
        WorkScope(date_from=bounds[0], date_to=bounds[1]), _da_context(), ["000001.SZ"]
    )
    captured = {"sql": conn.executed[0]["sql"], "params": conn.executed[0]["params"]}

    assert "TRUNCATE TABLE feature.shareholder_return_v1" not in captured["sql"]
    assert "DELETE FROM feature.shareholder_return_v1" in captured["sql"]
    assert "WHERE d.stock_code=ANY(%s) AND d.trade_date BETWEEN %s AND %s" in captured["sql"]
    assert captured["params"] == (
        ["000001.SZ"], *bounds, ["000001.SZ"], *bounds
    )


def test_heavy_warehouse_lock_commits_acquire_and_always_unlocks(monkeypatch):
    conn = _Connection()
    monkeypatch.setattr(
        tushare_job_lock,
        "DataAccessKernel",
        lambda **kwargs: DataAccessKernel(
            policy_registry=kwargs["policy_registry"],
            connection_factory=lambda: conn,
        ),
    )

    with pytest.raises(RuntimeError, match="worker failed"):
        with tushare_job_lock.warehouse_write_lock():
            raise RuntimeError("worker failed")

    assert conn.commits == 2
    assert "pg_try_advisory_lock" in conn.cur.commands[0][0]
    assert "pg_advisory_unlock" in conn.cur.commands[1][0]


def test_implicit_full_market_rebuilds_fail_closed():
    with pytest.raises(RuntimeError, match="隐式全市场 Standard/PIT"):
        tushare_phase2_standard.run("status")
    with pytest.raises(RuntimeError, match="隐式全市场 Feature"):
        tushare_phase2_features.run("price_v2")
    with pytest.raises(RuntimeError, match="兼容层"):
        tushare_phase2_features.run("company_financial")


def test_foundation_snapshot_warmup_uses_v2_financial_source(monkeypatch):
    captured = {}

    class _Repository:
        def financial_warmup_count(self, first_date, schema_version, context):
            captured.update({"first_date": first_date, "schema_version": schema_version})
            return 1

    class _Kernel:
        def close(self):
            pass

    monkeypatch.setattr(
        research_snapshot_series,
        "_service",
        lambda label: (_Kernel(), _Repository(), object()),
    )

    research_snapshot_series.assert_financial_pit_warmup(
        [date(2026, 8, 6)], FOUNDATION_SCHEMA_VERSION
    )

    assert captured == {
        "first_date": date(2026, 8, 6),
        "schema_version": FOUNDATION_SCHEMA_VERSION,
    }


def test_financial_raw_save_reports_actual_version_changes(monkeypatch):
    monkeypatch.setattr(
        tushare_financial_raw_init.FinancialRepository,
        "append_api_rows",
        lambda *args, **kwargs: 0,
    )
    frame = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "ann_date": ["20260430"],
        "end_date": ["20260331"],
        "roe": [1.2],
    })

    changed = tushare_financial_raw_init._save_raw(
        "tushare_fina_indicator_raw", frame, batch_id="repeat"
    )

    assert changed == 0


def test_financial_derived_work_is_durable_across_process_retries():
    source = __import__("pathlib").Path(
        "data_collect/jobs/tushare_financial_incremental.py"
    ).read_text(encoding="utf-8")
    assert "def _pending_derived_codes" in source
    assert '"financial_derived_v2", code, "pending"' in source
    assert "if rebuild_derived and not dry_run and pending_codes" in source
    assert '"financial_derived_v2", code, "done"' in source
    assert '"financial_derived_v2", code, "failed"' in source
