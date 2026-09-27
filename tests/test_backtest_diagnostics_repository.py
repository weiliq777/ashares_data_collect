from datetime import date, timedelta

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.backtest_run_repository import BacktestRunRepository
from tests._fake_db import FakeConnection


def _context():
    return RunContext(
        run_id="diagnostic-test",
        job_name="backtest_diagnostics",
        code_version="git:test",
        config_hash="cfg",
        pipeline_version="data-access-v1",
    )


def test_diagnostic_order_query_accepts_postgresql_reserved_identifier():
    connection = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(),
        connection_factory=lambda: connection,
    )
    try:
        rows = BacktestRunRepository(kernel).diagnostic_orders("run-1", _context())
        assert rows == []
        query = connection.executed[-1]["sql"]
        assert 'FROM backtest."order"' in query
    finally:
        kernel.close()


def test_diagnostic_run_by_version_is_bounded_and_scoped():
    connection = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(),
        connection_factory=lambda: connection,
    )
    try:
        rows = BacktestRunRepository(kernel).diagnostic_run_by_version(
            "dataset-1", "strategy-1", "capital-sensitivity-30k-v1", _context()
        )
        assert rows is None
        query = connection.executed[-1]["sql"]
        assert "dataset_id=%s" in query
        assert "strategy_id=%s" in query
        assert "run_version=%s" in query
        assert "execution_model_version" in query
        assert "code_archive_sha256" in query
        assert "LIMIT 2" in query
    finally:
        kernel.close()


def test_diagnostic_capital_runs_uses_fixed_scope_and_small_limit():
    connection = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(),
        connection_factory=lambda: connection,
    )
    try:
        rows = BacktestRunRepository(kernel).diagnostic_capital_runs(
            "dataset-1", "strategy-1", [10_000, 30_000], _context()
        )
        assert rows == []
        query = connection.executed[-1]["sql"]
        assert "dataset_id=%s" in query
        assert "strategy_id=%s" in query
        assert "initial_cash = ANY(%s::numeric[])" in query
        assert "capital-sensitivity-%%" in query
        assert "LIMIT 10" in query
    finally:
        kernel.close()


def test_diagnostic_future_closes_is_bounded_to_dataset_and_date_window():
    connection = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(),
        connection_factory=lambda: connection,
    )
    try:
        rows = BacktestRunRepository(kernel).diagnostic_future_closes(
            "dataset-1",
            [("000001.SZ", date(2024, 1, 8))],
            date(2024, 1, 8),
            _context(),
        )
        assert rows == []
        statement = connection.executed[-1]
        query = statement["sql"]
        assert "FROM backtest.dataset_component" in query
        assert "composite_dataset_id=%s" in query
        assert "b.dataset_id=c.dataset_id" in query
        assert "b.trade_date BETWEEN c.effective_from AND c.effective_to" in query
        assert "INTERVAL '120 days'" in query
        assert "row_number() OVER" in query
        assert query.count("%s") == len(statement["params"])
    finally:
        kernel.close()


def test_diagnostic_future_closes_batches_and_deduplicates_entries():
    connection = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(),
        connection_factory=lambda: connection,
    )
    try:
        entries = [
            (f"00000{index}.SZ", date(2024, 1, 8) + timedelta(days=index))
            for index in range(11)
        ]
        entries.append(entries[0])
        rows = BacktestRunRepository(kernel).diagnostic_future_closes(
            "dataset-1", entries, date(2024, 1, 8), _context()
        )
        assert rows == []
        statements = connection.executed
        queries = [item["sql"] for item in statements]
        assert len(queries) == 2
        assert all("FROM backtest.dataset_component" in query for query in queries)
        assert all("b.dataset_id=c.dataset_id" in query for query in queries)
        assert all("INTERVAL '120 days'" in query for query in queries)
        assert all(query.count("%s") == len(item["params"]) for query, item in zip(queries, statements))
    finally:
        kernel.close()


def test_diagnostic_trade_chart_binds_to_dataset_members():
    from datetime import date

    connection = FakeConnection()
    kernel = DataAccessKernel(
        policy_registry=build_default_registry(),
        connection_factory=lambda: connection,
    )
    try:
        rows = BacktestRunRepository(kernel).diagnostic_trade_chart(
            "dataset-1", "000001.SZ", date(2024, 1, 1), date(2024, 3, 31), _context()
        )
        assert rows == []
        statement = connection.executed[-1]
        query = statement["sql"]
        assert "FROM backtest.dataset_component" in query
        assert "m.dataset_id=c.dataset_id" in query
        assert "m.research_date BETWEEN c.effective_from AND c.effective_to" in query
        assert "JOIN research.stock_snapshot_daily s" in query
        assert "s.snapshot_id=m.research_snapshot_id" in query
        assert "s.research_date BETWEEN %s AND %s" in query
        assert query.count("%s") == len(statement["params"])
    finally:
        kernel.close()
