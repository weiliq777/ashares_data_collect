from datetime import date
from types import SimpleNamespace

from data_collect.data_access.repository.release_repository import ReleaseRepository
from data_collect.data_access.release_delta_impact import DeltaImpactDisposition


def test_repository_plans_delta_using_only_bounded_control_plane_reads():
    calls = []

    class ReadProbe:
        def _repository_query(self, context, source, sql, params, scope, max_rows):
            calls.append((source, sql, params, max_rows))
            if source == "ops.release_delta_package":
                return [{
                    "package_id": "00000000-0000-0000-0000-000000000001",
                    "baseline_release_id": 12,
                    "baseline_domain": "MARKET_ACTIVITY",
                    "event_count": 1,
                    "package_status": "CAPTURED",
                }]
            if source == "ops.release_delta_event":
                return [{
                    "event_id": 33,
                    "target": "std.daily_kline",
                    "operation": "UPSERT",
                    "scope": {"date_from": "2026-09-24", "date_to": "2026-09-24"},
                    "delta_eligible": True,
                    "producer_run_status": "SUCCEEDED",
                }]
            if source == "ops.tushare_data_release":
                return [{"baseline_cutoff_date": date(2026, 9, 25)}]
            raise AssertionError(f"unexpected source read: {source}")

    repository = ReleaseRepository(SimpleNamespace(read=ReadProbe()))
    plan = repository.plan_delta_impact("00000000-0000-0000-0000-000000000001", SimpleNamespace())

    assert plan.disposition is DeltaImpactDisposition.BOUNDED_RECHECK
    assert plan.source_start == date(2026, 8, 11)
    assert all(call[3] <= 1_001 for call in calls)
    assert all("FROM std." not in call[1] and "FROM feature." not in call[1] for call in calls)
    assert [call[0] for call in calls] == [
        "ops.release_delta_package",
        "ops.release_delta_event",
        "ops.tushare_data_release",
    ]
