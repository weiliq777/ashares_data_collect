from datetime import date
from pathlib import Path

import pytest

from data_collect.jobs.backtest_dataset import (
    FEATURE_VERSION,
    SUPPORTED_RESEARCH_SCHEMA_VERSIONS,
    _assert_dataset_snapshot_identity,
    _assert_build_code_current,
    _foundation_contract_audit_is_present,
    _foundation_contract_issues,
    _check_baseline,
    missing_snapshot_dates,
)
from data_collect.jobs.backtest_run import _scenario_id_for_capital
from data_collect.jobs.research_snapshot import FOUNDATION_SCHEMA_VERSION, SCHEMA_VERSION
from data_collect.data_access.research_contract import (
    INDEPENDENT_FINANCIAL_FACTOR_SCHEMA_VERSION,
    STRATEGY_NEUTRAL_SCHEMA_VERSION,
)


def test_backtest_scenario_identity_tracks_initial_capital():
    assert _scenario_id_for_capital(10_000) == "scen_a_share_10k_v1"
    assert _scenario_id_for_capital(20_000) == "scen_a_share_20k_v1"


def test_backtest_migration_has_frozen_dataset_and_run_contract():
    sql = Path("sql/028_create_backtest_model.sql").read_text(encoding="utf-8")
    for token in (
        "backtest.dataset_manifest",
        "backtest.dataset_member",
        "backtest.dataset_execution_bar",
        "backtest.dataset_dividend_event",
        "backtest.dataset_rule_snapshot",
        "backtest.market_rule_effective",
        "backtest.run_manifest",
        "backtest.signal",
        "backtest.order",
        "backtest.fill",
        "backtest.position_lot",
        "backtest.cash_ledger",
        "backtest.performance_daily",
        "backtest.assert_dataset_manifest_state",
        "backtest.assert_run_manifest_state",
        "VALIDATED backtest dataset is immutable before freeze",
        "validated/frozen backtest dataset child is immutable",
        "backtest run child rows are writable only while RUNNING",
        "formal backtest run requires FROZEN Dataset",
        "formal backtest run requires input and parameters hashes",
        "dirty code requires an explicit code archive before running",
        "baseline_quality_report_hash",
        "backtest_dataset_baseline_quality_hash_check",
        "backtest.assert_dataset_baseline_binding",
    ):
        assert token in sql
    assert "CREATE TABLE IF NOT EXISTS tushare_" not in sql
    assert "DROP TABLE" not in sql
    assert "code_archive_path" in sql
    assert "cash_settlement_days" in sql
    assert "market_rule_version" in sql
    assert "fee_rule_version" in sql
    source = Path("data_collect/jobs/backtest_dataset.py").read_text(encoding="utf-8")
    assert "def _manifest(conn, dataset_id: str, *, for_update: bool = False)" in source
    assert "_manifest(conn, dataset_id, for_update=True)" in source


def test_backtest_dataset_job_rejects_non_frozen_snapshot_in_source():
    source = Path("data_collect/jobs/backtest_dataset.py").read_text(encoding="utf-8")
    assert "只能引用 FROZEN Research Snapshot" in source
    assert "只有 FROZEN Dataset 可以用于正式回测" in source
    assert "tushare_daily_raw" in source
    assert "COALESCE(v.payload, r.payload)" in source
    assert "COALESCE(v.payload, r.payload)->>'open'" in source
    assert "COALESCE(v.payload, r.payload)->>'vol'" in source
    assert "LEFT JOIN daily_kline" not in source
    # Research snapshot quality is an aggregate over financial/feature inputs;
    # execution-source availability must be decided from the selected OHLC Raw
    # payload and the limit Raw record, not from that aggregate status.
    assert "s.quality_status IN ('VALID', 'WARNING')" not in source
    assert "lim.record_id IS NOT NULL" in source
    assert "limit_source_payload_hash" in source
    assert "suspend_source_payload_hash" in source
    assert "status_source_present" in source
    assert "rule_audit_rows" in source
    assert "approval_status, approved_by, approved_at" in source
    assert "assert_market_coverage_range_publishable" in source
    assert 'manifest["warmup_start"]' in source
    assert 'manifest["evaluation_end"]' in source
    assert "Evaluation 边界必须是 FROZEN Dataset 开市日" in source
    assert "NULL source_record_version" in source
    assert "dataset_execution_bar" in source
    assert "mv.dataset='trade_cal'" in source
    assert "legacy.fetched_at <= %s::timestamp" in source
    assert "ORDER BY d.trade_date" in source
    assert "effective_announce_date > m.research_date" in source
    assert "corporate_action_hash_for_day" in source
    assert "m.corporate_action_hash" in source
    assert "RESEARCH_SCHEMA_VERSION" in source
    assert "DISTINCT ON (event_key)" in source
    assert "status='FAILED', failed_at=CURRENT_TIMESTAMP" in source
    assert source.count("_check_baseline(") >= 4
    assert "baseline_quality_report_hash" in source
    assert '"baseline_quality_report"' in source
    assert "数据基线 quality_report_hash 已变化" in source
    assert "Dataset 边界必须是 SSE 开市日" in source
    final_sql = Path("sql/031_phase3_final_audit_contract.sql").read_text(encoding="utf-8")
    for token in (
        "signal_score",
        "planned_shares",
        "Run input_hash must equal Dataset input_manifest_hash",
        "Run rule versions must equal Dataset rule versions",
        "baseline_quality_report_hash",
        "backtest_dataset_baseline_quality_hash_check",
    ):
        assert token in final_sql


def test_backtest_run_binds_dataset_rule_versions_and_immutable_strategy():
    source = Path("data_collect/jobs/backtest_run.py").read_text(encoding="utf-8")
    assert "market_rule_version, fee_rule_version" in source
    assert "config.max_portfolio_risk_pct,\n             config.reserve_cash_pct" in source
    assert "parameters_hash" in source
    assert "策略定义已存在且不可覆盖" in source
    assert "ON CONFLICT (strategy_id) DO UPDATE" not in source
    assert "正式回测必须显式设置 max_portfolio_risk_pct" in source


def test_quality_strategy_default_matches_frozen_gap_stress_rule():
    source = Path("data_collect/jobs/backtest_run.py").read_text(encoding="utf-8")
    assert "gap_stress_pct=0.05" in source


def test_backtest_strategy_version_binding_migration_cuts_over_legacy_fk():
    source = Path("sql/095_backtest_strategy_version_binding.sql").read_text(encoding="utf-8")
    assert "DROP CONSTRAINT IF EXISTS run_manifest_strategy_id_fkey" in source
    assert "run_manifest_strategy_version_fkey" in source
    assert "REFERENCES backtest.app_strategy_version(strategy_id, strategy_version)" in source
    assert "assert_run_strategy_binding" in source
    assert "requires an APPROVED application strategy" in source
    assert "formal Backtest Run requires strategy_version" in source


def test_snapshot_series_keeps_one_snapshot_per_cutoff_date():
    source = Path("data_collect/jobs/research_snapshot_series.py").read_text(encoding="utf-8")
    assert "normalize_cutoff_dates" in source
    assert "Research Snapshot 截止日不能重复" in source
    assert "create_snapshot" in source
    assert "build_snapshot" in source
    assert "validate_snapshot" in source
    assert "freeze_snapshot" in source


def test_dataset_requires_a_snapshot_for_each_open_day():
    assert missing_snapshot_dates(
        [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5)],
        [date(2026, 8, 3), date(2026, 8, 5)],
    ) == (date(2026, 8, 4),)
    assert missing_snapshot_dates(
        [date(2026, 8, 3), date(2026, 8, 4)],
        [date(2026, 8, 3), date(2026, 8, 4)],
    ) == ()


def test_dataset_accepts_current_and_foundation_research_contracts():
    assert SUPPORTED_RESEARCH_SCHEMA_VERSIONS == {
        SCHEMA_VERSION,
        FOUNDATION_SCHEMA_VERSION,
        STRATEGY_NEUTRAL_SCHEMA_VERSION,
        INDEPENDENT_FINANCIAL_FACTOR_SCHEMA_VERSION,
    }
    source = Path("data_collect/jobs/backtest_dataset.py").read_text(encoding="utf-8")
    assert "financial_data_available_timestamp" in source
    assert "net_profit_consolidated_ttm" in source
    assert "ocf_to_net_profit_consolidated_ttm" in source
    assert "financial_source_fingerprint" in source
    assert len(FEATURE_VERSION) <= 80


def test_materialized_dataset_requires_one_snapshot_schema_and_exact_baseline():
    baseline = {
        "quality_report_hash": "quality-hash",
        "source_manifest_hash": "source-hash",
    }
    rows = [
        {
            "schema_version": "research-v2.1",
            "data_baseline_tag": "foundation_v2_20260821",
            "baseline_quality_report_hash": "quality-hash",
            "baseline_source_manifest_hash": "source-hash",
        },
        {
            "schema_version": "research-v2.1",
            "data_baseline_tag": "foundation_v2_20260821",
            "baseline_quality_report_hash": "quality-hash",
            "baseline_source_manifest_hash": "source-hash",
        },
    ]

    assert _assert_dataset_snapshot_identity(
        rows, "foundation_v2_20260821", baseline,
    ) == "research-v2.1"

    with pytest.raises(ValueError, match="exactly one Research Snapshot schema"):
        _assert_dataset_snapshot_identity(
            [rows[0], {**rows[1], "schema_version": "research-v2.0"}],
            "foundation_v2_20260821",
            baseline,
        )
    with pytest.raises(ValueError, match="Every Research Snapshot"):
        _assert_dataset_snapshot_identity(
            [rows[0], {**rows[1], "schema_version": None}],
            "foundation_v2_20260821",
            baseline,
        )
    with pytest.raises(ValueError, match="data_baseline_tag"):
        _assert_dataset_snapshot_identity(
            [rows[0], {**rows[1], "data_baseline_tag": "other-baseline"}],
            "foundation_v2_20260821",
            baseline,
        )
    with pytest.raises(ValueError, match="质量哈希"):
        _assert_dataset_snapshot_identity(
            [rows[0], {**rows[1], "baseline_quality_report_hash": "other-hash"}],
            "foundation_v2_20260821",
            baseline,
        )
    with pytest.raises(ValueError, match="Raw 来源边界"):
        _assert_dataset_snapshot_identity(
            [rows[0], {**rows[1], "baseline_source_manifest_hash": "other-hash"}],
            "foundation_v2_20260821",
            baseline,
        )


def test_new_foundation_dataset_rejects_release_without_current_corporate_action_evidence(
    monkeypatch,
):
    from data_collect.jobs import backtest_dataset

    class FakeKernel:
        def close(self):
            pass

    class FakeRepository:
        def baseline(self, baseline_tag, context):
            return {
                "status": "validated",
                "schema_version": "feature-contract-v2.0",
                "quality_report": {"failures": {}},
            }

    monkeypatch.setattr(
        backtest_dataset,
        "_read_service",
        lambda label: (FakeKernel(), FakeRepository(), object()),
    )

    with pytest.raises(ValueError, match="当前版本、完整的除权日范围审计"):
        _check_baseline("foundation", date(2026, 8, 21))


def test_dataset_foundation_gate_is_bounded_and_ignores_legacy_schema(monkeypatch):
    calls = []

    def fake_gate(self, manifests, context):
        calls.append(manifests)
        return {"snapshot-1": {"non_v2_source_rows": 1}}

    from data_collect.data_access.repository.research_snapshot_repository import (
        ResearchSnapshotRepository,
    )

    monkeypatch.setattr(
        ResearchSnapshotRepository,
        "foundation_financial_contract_issues",
        fake_gate,
    )

    class FakeDatasetRepository:
        kernel = object()

    rows = [
        {
            "snapshot_id": "snapshot-1",
            "research_cutoff_date": date(2026, 8, 20),
            "schema_version": FOUNDATION_SCHEMA_VERSION,
        }
    ]
    issues = _foundation_contract_issues(rows, FakeDatasetRepository(), object())
    assert issues == {"snapshot-1": {"non_v2_source_rows": 1}}
    assert len(calls) == 1
    assert calls[0][0]["cutoff"] == date(2026, 8, 20)

    legacy = [{**rows[0], "schema_version": SCHEMA_VERSION}]
    assert _foundation_contract_issues(legacy, FakeDatasetRepository(), object()) == {}
    assert len(calls) == 1


def test_foundation_contract_pass_marker_is_required_for_formal_use():
    assert _foundation_contract_audit_is_present({
        "quality_report": {
            "foundation_financial_contract": {
                "status": "PASS",
                "audit_version": "foundation-financial-contract-v1",
            }
        }
    })
    assert not _foundation_contract_audit_is_present({"quality_report": "{}"})


def test_dataset_handoff_selects_only_requested_snapshot_range():
    source = Path(
        "data_collect/jobs/backtest_dataset_handoff.py"
    ).read_text(encoding="utf-8")
    assert "research_cutoff_date BETWEEN %s AND %s" in source
    assert "warmup_start, evaluation_end" in source


def test_dataset_validation_mentions_full_warmup_and_evaluation_coverage():
    source = Path("data_collect/jobs/backtest_dataset.py").read_text(encoding="utf-8")
    assert "warmup/evaluation 全区间缺少 Dataset 成员日期" in source
    assert "Dataset 成员包含交易日历范围外日期" in source
    assert "expected_role =" in source


def test_formal_run_rejects_dirty_code_before_strategy_registration():
    source = Path("data_collect/jobs/backtest_run.py").read_text(encoding="utf-8")
    assert "Fail before registering an append-only strategy definition" in source
    assert 'if metadata["dirty"] and not code_archive_path' in source


def test_dataset_freeze_requires_same_code_identity_as_build():
    manifest = {
        "code_version": "abc+dirty",
        "git_commit": "abc",
        "git_dirty": True,
        "diff_hash": "diff-a",
    }
    _assert_build_code_current(manifest, dict(commit="abc", dirty=True, diff_hash="diff-a", code_version="abc+dirty"))
    try:
        _assert_build_code_current(manifest, dict(commit="abc", dirty=True, diff_hash="diff-b", code_version="abc+dirty"))
    except RuntimeError as exc:
        assert "重新 build" in str(exc)
    else:
        raise AssertionError("changed build/freeze code identity was not rejected")


def test_phase2_dividend_standard_declares_backtest_lineage_columns():
    sql = Path("sql/018_create_phase2_model.sql").read_text(encoding="utf-8")
    assert "business_key VARCHAR(180)" in sql
    assert "payload_hash CHAR(64)" in sql


def test_market_rules_have_database_level_non_overlap_guard():
    sql = Path("sql/030_create_phase3_ops_gate.sql").read_text(encoding="utf-8")
    assert "CREATE EXTENSION IF NOT EXISTS btree_gist" in sql
    assert "market_rule_effective_no_overlap" in sql
    assert "EXCLUDE USING gist" in sql
