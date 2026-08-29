from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from data_collect.features.financial_v2 import (
    DIVIDEND_PERCENTILE_MIN_OBSERVATIONS,
    annual_window_stats,
    ocf_to_profit,
    period_kind,
    quarter_bridge,
    roe_ttm,
    safe_cagr,
    safe_growth,
    ttm_bridge,
)
from data_collect.jobs.tushare_foundation_features import (
    _dependent_report_dates,
    _latest,
    _pit_evidence_level,
    calculate_valuation_features,
)
import data_collect.jobs.tushare_foundation_features as foundation_features
from data_collect.jobs.tushare_financial_raw_to_standard import transform


def test_period_kind_requires_end_type_to_match_calendar_period():
    assert period_kind(date(2026, 3, 31), "1") == "Q1"
    assert period_kind(date(2026, 12, 31), "4") == "FY"
    assert period_kind(date(2026, 3, 31), "4") is None


def test_quarter_and_ttm_bridges_have_basis_specific_component_counts():
    quarter = quarter_bridge(60, 25, "H1")
    assert (quarter.value, quarter.basis, quarter.required, quarter.actual, quarter.complete) == (
        35, "YTD_DIFFERENCE", 2, 2, True,
    )
    direct = ttm_bridge(100, None, None, "FY")
    assert (direct.value, direct.basis, direct.required, direct.actual, direct.complete) == (
        100, "DIRECT_FY", 1, 1, True,
    )
    bridged = ttm_bridge(30, 100, 20, "Q1")
    assert bridged.value == 110
    assert (bridged.required, bridged.actual, bridged.complete) == (3, 3, True)


def test_incomplete_ttm_never_exposes_partial_value():
    result = ttm_bridge(30, 100, None, "Q1")
    assert result.value is None
    assert (result.required, result.actual, result.complete) == (3, 2, False)


def test_ocf_ratio_fuses_nonpositive_profit_but_keeps_negative_ocf():
    assert ocf_to_profit(-5, -10) == (None, "NEGATIVE_OR_ZERO_PROFIT")
    assert ocf_to_profit(-5, 10) == (-0.5, "VALID")
    assert ocf_to_profit(None, 10) == (None, "MISSING_OPERATING_CASHFLOW")


def test_roe_ttm_uses_parent_profit_and_average_parent_equity():
    value, status = roe_ttm(20, 90, 110)
    assert value == pytest.approx(0.2)
    assert status == "VALID"
    assert roe_ttm(20, -120, 100) == (None, "NEGATIVE_OR_ZERO_EQUITY")


def test_five_year_facts_are_null_when_window_is_incomplete():
    stats = annual_window_stats(
        [(date(year, 12, 31), value) for year, value in ((2022, -1), (2023, 2), (2024, 3))],
        [(date(year, 12, 31), 1) for year in (2022, 2023, 2024)],
        [],
    )
    assert stats["profit_window_complete"] is False
    assert stats["profit_loss_event_5y"] is None
    assert stats["loss_year_count_5y"] is None


def test_five_observations_with_a_missing_fiscal_year_are_not_a_complete_window():
    years = (2019, 2020, 2022, 2023, 2024)
    stats = annual_window_stats(
        [(date(year, 12, 31), 1) for year in years],
        [(date(year, 12, 31), 1) for year in years],
        [],
    )
    assert stats["profit_observation_years_5y"] == 4
    assert stats["profit_window_complete"] is False
    assert stats["cashflow_window_complete"] is False
    assert stats["profit_loss_event_5y"] is None


def test_five_year_cashflow_window_uses_profit_year_as_common_anchor():
    stats = annual_window_stats(
        [(date(year, 12, 31), 1) for year in range(2021, 2026)],
        [(date(year, 12, 31), 1) for year in range(2019, 2024)],
        [(date(year, 12, 31), 0.1) for year in range(2021, 2026)],
    )
    assert stats["profit_window_complete"] is True
    assert stats["cashflow_observation_years_5y"] == 3
    assert stats["cashflow_window_complete"] is False


def test_cagr_rejects_nonpositive_endpoints():
    assert safe_cagr(200, 100, 5) == pytest.approx(2 ** 0.2 - 1)
    assert safe_cagr(200, -100, 5) is None


def test_growth_rejects_nonpositive_base_and_uses_recovery_facts_instead():
    assert safe_growth(10, -5) is None
    assert safe_growth(10, 0) is None
    assert safe_growth(-5, 10) == pytest.approx(-1.5)


def test_financial_pit_evidence_is_explicit_about_source_date_assumption():
    assert _pit_evidence_level([
        {"vintage_quality": "SOURCE_ANNOUNCEMENT_ASSUMED"},
    ]) == "SOURCE_ANNOUNCEMENT_ASSUMED"
    assert _pit_evidence_level([
        {"vintage_quality": "SOURCE_ANNOUNCEMENT_ASSUMED"},
        {"vintage_quality": "WAREHOUSE_OBSERVED_ONLY"},
    ]) == "MIXED_SOURCE_AND_WAREHOUSE_OBSERVED"


def test_gross_margin_never_falls_back_to_total_operating_cost():
    source = Path("data_collect/jobs/tushare_foundation_features.py").read_text(
        encoding="utf-8"
    )
    block = source.split('oper_cost_ttm = bridges["oper_cost"]', 1)[1].split(
        "gross_margin = None", 1
    )[0]
    assert 'bridges["total_cogs"]' not in block


def test_debt_ratio_never_mixes_vendor_percent_with_decimal_ratio():
    source = Path("data_collect/jobs/tushare_foundation_features.py").read_text(
        encoding="utf-8"
    )
    block = source.split("total_assets, total_liab", 1)[1].split(
        'return {', 1
    )[0]
    assert '_value(indicator, "debt_to_assets")' not in block


def test_dividend_percentile_counts_zero_as_a_valid_observation():
    rows = []
    start = date(2025, 1, 1)
    for offset in range(DIVIDEND_PERCENTILE_MIN_OBSERVATIONS):
        day = date.fromordinal(start.toordinal() + offset)
        rows.append({
            "stock_code": "000001.SZ", "trade_date": day,
            "pe_ttm": 10, "pb": 1, "ps_ttm": 1, "dv_ratio": 0,
            "total_mv": 1, "circ_mv": 1, "pe_ttm_vendor": 10,
            "ps_ttm_vendor": 1, "pb_vendor": 1, "dv_ttm_vendor": 0,
            "dv_ttm_source": "TUSHARE_DAILY_BASIC",
            "source_observed_at": None, "data_available_timestamp": None,
        })
    result = calculate_valuation_features(rows, rows[-1]["trade_date"], rows[-1]["trade_date"])
    assert result[0]["dv_observation_days"] == DIVIDEND_PERCENTILE_MIN_OBSERVATIONS
    assert result[0]["dv_ttm_percentile_5y"] == 1.0


def test_income_standard_maps_total_and_parent_profit_explicitly():
    result = transform(pd.DataFrame([{
        "ts_code": "000001.SZ", "end_date": "20251231",
        "ann_date": "20260331", "f_ann_date": "20260331",
        "report_type": "1", "comp_type": "1", "end_type": "4",
        "n_income": 120.0, "n_income_attr_p": 100.0,
        "_record_id": 1, "_logical_business_key": "income-key",
        "_payload_hash": "a" * 64, "_fetched_at": "2026-04-01 01:00:00",
    }]), "income")
    row = result.iloc[0]
    assert row["net_profit_consolidated"] == 120
    assert row["net_profit_parent"] == 100
    assert row["net_profit"] == 120
    assert str(row["data_available_timestamp"].tzinfo) == "Asia/Shanghai"


def test_cashflow_standard_does_not_use_reconciliation_profit_as_primary_profit():
    result = transform(pd.DataFrame([{
        "ts_code": "000001.SZ", "end_date": "20251231",
        "ann_date": "20260331", "report_type": "1", "comp_type": "1",
        "end_type": "4", "n_cashflow_act": 80.0, "net_profit": 999.0,
        "_record_id": 2, "_logical_business_key": "cash-key",
        "_payload_hash": "b" * 64, "_fetched_at": "2026-04-01 01:00:00",
    }]), "cashflow")
    row = result.iloc[0]
    assert row["operating_cashflow_ytd"] == 80
    assert row["cashflow_net_profit_reconciliation"] == 999


def test_financial_feature_input_enforces_one_cutoff_for_all_four_sources():
    source = Path("data_collect/data_access/repository/financial_repository.py").read_text(encoding="utf-8")
    loader = source.split("_EVENTS_SQL", 1)[1].split("def load_events", 1)[0]
    assert loader.count("data_available_timestamp <") == 4
    assert loader.count(
        "(%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'"
    ) == 4


def test_indicator_company_type_link_is_asof_bounded():
    source = Path("data_collect/data_access/repository/fina_indicator_repository.py").read_text(
        encoding="utf-8"
    )
    lateral = source.split("LEFT JOIN LATERAL", 1)[1].split(
        ") linked ON TRUE", 1
    )[0]
    assert "i.data_available_timestamp <=" in lateral
    assert "COALESCE(p.f_ann_date, p.ann_date," in lateral
    assert "p.fetched_at::date" in lateral
    quality = Path("data_collect/data_access/repository/foundation_quality_repository.py").read_text(
        encoding="utf-8"
    )
    assert '"indicator_future_comp_type_link"' in quality
    assert "i.data_available_timestamp" in quality


def test_foundation_pipeline_has_session_lock_and_same_run_resume():
    source = Path("data_collect/data_access/lock_manager.py").read_text(encoding="utf-8")
    pipeline = Path("data_collect/jobs/tushare_foundation_pipeline.py").read_text(encoding="utf-8")
    assert "pg_try_advisory_lock" in source
    assert "pg_advisory_unlock" in source
    repository = Path("data_collect/data_access/repository/financial_repository.py").read_text(encoding="utf-8")
    assert 'foundation-rebuild' in repository
    assert "def resume_run" in pipeline
    assert "def resume_run" in repository
    assert 'not in {"FAILED", "RUNNING"}' in repository
    assert 'parser.add_argument("--run-id")' in pipeline


def test_foundation_run_transitions_do_not_upsert_jsonb_readback():
    source = Path(
        "data_collect/data_access/repository/financial_repository.py"
    ).read_text(encoding="utf-8")
    mark_block = source.split("def mark_run", 1)[1].split("def resume_run", 1)[0]
    assert "_adapt_run_record(self._run_record(run_id, context))" in mark_block
    assert "rows=[row]" in mark_block
    assert "json.dumps(" in source
    resume_block = source.split("def resume_run", 1)[1].split("def create_run", 1)[0]
    assert 'resume_row = {' in resume_block
    assert 'rows=[resume_row]' in resume_block


def test_company_checkpoint_hash_covers_complete_feature_output():
    source = Path(
        "data_collect/jobs/tushare_foundation_features.py"
    ).read_text(encoding="utf-8")
    block = source.split("result_hash = hashlib.sha256", 1)[1].split(
        ").hexdigest()", 1
    )[0]
    assert "json.dumps(" in block
    assert "rows," in block
    assert "sort_keys=True" in block
    assert "allow_nan=False" in block
    assert 'ZoneInfo("Asia/Shanghai")' in source


def test_v2_release_gate_requires_feature_quality_and_validated_run():
    source = Path("data_collect/data_access/repository/release_repository.py").read_text(encoding="utf-8")
    gateway = Path("data_collect/data_access/write_gateway.py").read_text(encoding="utf-8")
    assert "FoundationQualityRepository" in source
    assert "inspect_foundation_v2" in source
    assert "foundation_v2:validated_production_run" in source
    assert "def commit_validation" in source
    assert "commit_release_validation" in gateway
    assert "_run_chunk" in gateway
    assert 'schema_version == "feature-contract-v2.0"' in source
    assert "output_end>=%s" in source


def test_v2_release_does_not_block_on_deliberately_stale_v1_compatibility():
    from data_collect.jobs.tushare_release import _invariant_blocks_release

    legacy_check = "financial_feature_missing_revenue_mapping"
    assert _invariant_blocks_release(
        legacy_check, include_foundation_v2=False
    )
    assert not _invariant_blocks_release(
        legacy_check, include_foundation_v2=True
    )
    assert _invariant_blocks_release(
        "financial_pit_missing_lineage", include_foundation_v2=True
    )


def test_v2_quality_skips_legacy_revenue_scan():
    source = Path(
        "data_collect/data_access/repository/release_repository.py"
    ).read_text(encoding="utf-8")
    assert "skipped_invariants" in source
    assert 'financial_feature_missing_revenue_mapping' in source
    assert "phase2_invariants(context, cutoff, skipped_invariants)" in source


def test_foundation_quality_gate_rejects_partial_outputs():
    source = Path(
        "data_collect/data_access/repository/foundation_quality_repository.py"
    ).read_text(encoding="utf-8")
    for check in (
        "company_missing_report_periods",
        "company_contract_mismatch",
        "valuation_missing_rows",
        "valuation_extra_rows",
        "valuation_contract_mismatch",
    ):
        assert f'"{check}"' in source


def test_foundation_quality_treats_post_cutoff_feature_rows_as_audit_info():
    source = Path(
        "data_collect/data_access/repository/foundation_quality_repository.py"
    ).read_text(encoding="utf-8")
    required = source.split("zero_required = {", 1)[1].split("}", 1)[0]
    assert '"company_future_rows"' not in required
    assert 'report["informational"]' in source


def test_foundation_quality_batches_same_table_metrics_without_losing_rules():
    from data_collect.data_access.repository.foundation_quality_repository import (
        FoundationQualityRepository,
    )

    calls = []

    class ReadProbe:
        def _repository_query(
            self, context, source, sql, params=(), scope=None, max_rows=None,
        ):
            calls.append((source, sql, params))
            if "AS company_rows" in sql:
                return [{
                    "company_rows": 10,
                    "company_contract_mismatch": 0,
                    "invalid_complete_ttm": 0,
                    "company_future_rows": 2,
                    "invalid_ocf_ratio": 0,
                    "invalid_comp_type_cashflow": 0,
                    "missing_pit_evidence": 0,
                }]
            if "AS valuation_rows" in sql:
                return [{
                    "valuation_rows": 20,
                    "valuation_contract_mismatch": 0,
                    "invalid_dividend_percentile": 0,
                    "valuation_future_rows": 0,
                }]
            if "WITH feature_by_date" in sql:
                return []
            return [{"value": 0}]

    class KernelProbe:
        read = ReadProbe()

    report = FoundationQualityRepository(KernelProbe()).inspect_foundation_v2(
        date(2026, 8, 1), date(2026, 8, 21), context=None,
    )

    company_calls = [sql for source, sql, _ in calls if source == "feature.company_financial_v2"]
    valuation_calls = [sql for source, sql, _ in calls if source == "feature.valuation_daily_v1"]
    assert len(company_calls) == 2  # missing-period anti-join + one combined scan
    assert len(valuation_calls) == 2  # one combined scan + one bounded coverage fingerprint
    assert "FILTER (WHERE" in next(sql for sql in company_calls if "AS company_rows" in sql)
    assert "FILTER (WHERE" in next(sql for sql in valuation_calls if "AS valuation_rows" in sql)
    assert report["checks"]["company_rows"] == 10
    assert report["checks"]["company_future_rows"] == 2
    assert report["checks"]["valuation_rows"] == 20
    assert report["passed"] is True


def test_valuation_availability_gate_rejects_both_early_and_next_day_values():
    source = Path("data_collect/data_access/repository/foundation_quality_repository.py").read_text(
        encoding="utf-8"
    )
    block = source.split("AS valuation_future_rows", 1)[0].rsplit(
        "COUNT(*) FILTER", 1
    )[1]
    assert "data_available_timestamp IS NULL" in block
    assert "data_available_timestamp <" in block
    assert "data_available_timestamp >=" in block
    assert "(trade_date + 1)::timestamp" in block
    assert "dv_ttm_percentile_5y<0" in source
    assert "dv_ttm_percentile_5y>1" in source


def test_company_financial_cutoffs_do_not_depend_on_database_session_timezone():
    snapshot = Path(
        "data_collect/data_access/repository/research_snapshot_repository.py"
    ).read_text(encoding="utf-8")
    quality = Path(
        "data_collect/data_access/repository/foundation_quality_repository.py"
    ).read_text(encoding="utf-8")
    explicit_cutoff = (
        "(%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'"
    )

    assert explicit_cutoff in snapshot
    assert explicit_cutoff in quality


def test_unfiltered_market_projection_uses_sequential_candidate_merge():
    source = Path("data_collect/data_access/repository/raw_standard_repository.py").read_text(
        encoding="utf-8"
    )
    assert "WITH candidates AS" in source
    assert "source_priority" in source
    assert "FROM candidates" in source


def test_standard_upsert_uses_bounded_pages_and_skips_noop_updates():
    source = Path("data_collect/data_access/repository/raw_standard_repository.py").read_text(
        encoding="utf-8"
    )
    assert "page_size: int = 5000" in source
    assert "skip_noop=True" in source
    gateway = Path("data_collect/data_access/write_gateway.py").read_text(encoding="utf-8")
    assert "IS DISTINCT FROM EXCLUDED" in gateway
    financial_job = Path(
        "data_collect/jobs/tushare_financial_raw_to_standard.py"
    ).read_text(encoding="utf-8")
    financial_repository = Path(
        "data_collect/data_access/repository/financial_standard_repository.py"
    ).read_text(encoding="utf-8")
    financial_upsert = financial_repository.split("def upsert_standard_rows", 1)[1]
    assert "page_size: int = 2000" in financial_upsert
    assert "skip_noop=True" in financial_upsert
    assert "FinancialStandardRepository" in financial_job
    assert "get_connection" not in financial_job
    indicator = Path(
        "data_collect/data_access/repository/fina_indicator_repository.py"
    ).read_text(encoding="utf-8")
    indicator_upsert = indicator.split("def rebuild_standard", 1)[1]
    assert "fina_indicator_standard.{column} IS DISTINCT FROM" in indicator_upsert


def test_full_market_valuation_projection_uses_throttled_daily_sql_batches():
    source = Path("data_collect/data_access/repository/raw_standard_repository.py").read_text(
        encoding="utf-8"
    )
    block = source.split("def rebuild_valuation_set_based", 1)[1]
    assert "def rebuild_valuation_set_based" in source
    assert "batch_trade_dates: int = 1" in source
    assert "throttle_seconds: float = 0.1" in block
    assert "WITH candidates AS" in block
    assert "NULLIF(NULLIF(BTRIM(payload->>'" in block
    assert "'NaN')::double precision" in block
    assert "ON CONFLICT (stock_code,trade_date,source) DO UPDATE" in block
    assert "run_scoped_merge_counted" in block


def test_v2_snapshot_preserves_explicit_missing_lineage():
    source = Path(
        "data_collect/data_access/repository/research_snapshot_repository.py"
    ).read_text(encoding="utf-8")
    v2 = source.split("def foundation_snapshot_sql", 1)[1]
    assert "LEFT JOIN feature.valuation_daily_v1 v" in v2
    assert "s.ts_code || ':' || s.research_date::text || ':missing'" in v2
    assert "data_available_timestamp <" in v2
    validation = Path(
        "data_collect/data_access/repository/foundation_quality_repository.py"
    ).read_text(encoding="utf-8")
    assert "f.data_available_timestamp" in v2
    assert "f.report_date <= s.research_date" in v2
    assert "dv_ttm_percentile_5y<0" in Path(
        "data_collect/data_access/repository/foundation_quality_repository.py"
    ).read_text(encoding="utf-8")
    assert "dv_ttm_percentile_5y>1" in Path(
        "data_collect/data_access/repository/foundation_quality_repository.py"
    ).read_text(encoding="utf-8")


def test_financial_restatement_rebuilds_dependent_visible_periods():
    def event(dataset, report_date, end_type):
        return {
            "dataset": dataset,
            "report_date": report_date,
            "end_type": end_type,
        }

    state = {"income": {
        (date(2025, 9, 30),): event("income", date(2025, 9, 30), "3"),
        (date(2025, 12, 31),): event("income", date(2025, 12, 31), "4"),
        (date(2026, 9, 30),): event("income", date(2026, 9, 30), "3"),
    }}
    q3_change = event("cashflow", date(2025, 9, 30), "3")
    assert _dependent_report_dates(state, q3_change) == {
        date(2025, 9, 30), date(2025, 12, 31), date(2026, 9, 30),
    }
    fy_change = event("balance", date(2025, 12, 31), "4")
    assert _dependent_report_dates(state, fy_change) == {
        date(2025, 12, 31), date(2026, 9, 30),
    }


def test_q1_restatement_propagates_through_later_same_year_bridges():
    def event(dataset, report_date, end_type):
        return {"dataset": dataset, "report_date": report_date, "end_type": end_type}

    state = {"income": {
        (date(2025, 3, 31),): event("income", date(2025, 3, 31), "1"),
        (date(2025, 6, 30),): event("income", date(2025, 6, 30), "2"),
        (date(2025, 9, 30),): event("income", date(2025, 9, 30), "3"),
        (date(2025, 12, 31),): event("income", date(2025, 12, 31), "4"),
        (date(2026, 3, 31),): event("income", date(2026, 3, 31), "1"),
    }}
    assert _dependent_report_dates(
        state, event("income", date(2025, 3, 31), "1")
    ) == {
        date(2025, 3, 31), date(2025, 6, 30), date(2025, 9, 30),
        date(2025, 12, 31), date(2026, 3, 31),
    }


def test_h1_restatement_propagates_to_q3_and_fy_but_not_prior_q1():
    def event(dataset, report_date, end_type):
        return {"dataset": dataset, "report_date": report_date, "end_type": end_type}

    state = {"income": {
        (date(2025, 3, 31),): event("income", date(2025, 3, 31), "1"),
        (date(2025, 6, 30),): event("income", date(2025, 6, 30), "2"),
        (date(2025, 9, 30),): event("income", date(2025, 9, 30), "3"),
        (date(2025, 12, 31),): event("income", date(2025, 12, 31), "4"),
        (date(2026, 6, 30),): event("income", date(2026, 6, 30), "2"),
    }}
    assert _dependent_report_dates(
        state, event("cashflow", date(2025, 6, 30), "2")
    ) == {
        date(2025, 6, 30), date(2025, 9, 30), date(2025, 12, 31),
        date(2026, 6, 30),
    }


def test_q1_restatement_does_not_mark_next_year_bridges_without_raw_ytd_dependency():
    def event(dataset, report_date, end_type):
        return {"dataset": dataset, "report_date": report_date, "end_type": end_type}

    state = {"income": {
        (date(2025, 3, 31),): event("income", date(2025, 3, 31), "1"),
        (date(2025, 6, 30),): event("income", date(2025, 6, 30), "2"),
        (date(2025, 9, 30),): event("income", date(2025, 9, 30), "3"),
        (date(2025, 12, 31),): event("income", date(2025, 12, 31), "4"),
        (date(2026, 3, 31),): event("income", date(2026, 3, 31), "1"),
        (date(2026, 6, 30),): event("income", date(2026, 6, 30), "2"),
        (date(2026, 9, 30),): event("income", date(2026, 9, 30), "3"),
        (date(2026, 12, 31),): event("income", date(2026, 12, 31), "4"),
    }}
    assert _dependent_report_dates(
        state, event("income", date(2025, 3, 31), "1")
    ) == {
        date(2025, 3, 31), date(2025, 6, 30), date(2025, 9, 30),
        date(2025, 12, 31), date(2026, 3, 31),
    }


def test_cross_statement_selection_fails_closed_on_end_and_company_type():
    period = date(2025, 12, 31)
    state = {"cashflow": {
        (1,): {
            "report_date": period, "end_type": "3", "comp_type": "1",
            "data_available_timestamp": pd.Timestamp("2026-03-01", tz="UTC"),
            "record_version": 1, "payload_hash": "a",
        },
        (2,): {
            "report_date": period, "end_type": "4", "comp_type": "2",
            "data_available_timestamp": pd.Timestamp("2026-03-02", tz="UTC"),
            "record_version": 2, "payload_hash": "b",
        },
    }}
    assert _latest(
        state, "cashflow", period, "1", "FY", require_comp_type_match=True
    ) is None
    selected = _latest(
        state, "cashflow", period, "2", "FY", require_comp_type_match=True
    )
    assert selected["record_version"] == 2


def test_nonindustrial_cashflow_ratio_is_not_exposed_as_comparable():
    source = Path(
        "data_collect/jobs/tushare_foundation_features.py"
    ).read_text(encoding="utf-8")
    block = source.split("ocf_ratio, ocf_reason = ocf_to_profit", 1)[1].split(
        'required = bridges["consolidated"]', 1
    )[0]
    assert 'str(comp_type) != "1"' in block
    assert 'ocf_ratio, ocf_reason = None, "NOT_APPLICABLE"' in block


def test_vendor_indicator_requires_income_company_type_match():
    period = date(2025, 12, 31)
    state = {"indicator": {}}
    for comp_type, roe, version in (("1", 18.0, 1), ("2", 99.0, 2)):
        state["indicator"][(comp_type, version)] = {
            "report_date": period,
            "end_type": "4",
            "comp_type": comp_type,
            "data_available_timestamp": pd.Timestamp("2026-03-31", tz="UTC"),
            "record_version": version,
            "payload_hash": str(version),
            "values": {"roe_reported": roe},
        }

    selected = _latest(
        state, "indicator", period, "1", "FY",
        allow_missing_end_type=True,
        require_comp_type_match=True,
    )
    assert selected["values"]["roe_reported"] == 18.0


def test_valuation_rebuild_filters_completed_codes_before_loading_inputs(monkeypatch):
    class FakeKernel:
        def close(self):
            pass

    class FakeRepository:
        def __init__(self):
            self.load_calls = []
            self.published_codes = []
            self.checkpoints = []

        def valuation_codes(self, *args, **kwargs):
            return ["A", "B", "C", "D", "E", "F"]

        def completed_checkpoint_keys(self, run_id, stage, context):
            assert (run_id, stage) == ("run", "valuation_daily_v2")
            return {"A", "D", "E"}

        def load_valuation_inputs(self, codes, *args):
            codes = tuple(codes)
            self.load_calls.append(codes)
            return [
                {"stock_code": code, "trade_date": date(2021, 8, 2)}
                for code in codes
            ]

        def publish_valuation_partitions(self, target, partitions, *args, **kwargs):
            items = list(partitions)
            self.published_codes.extend(code for code, _rows in items)
            return sum(len(rows) for _code, rows in items)

        def save_checkpoint(self, run_id, stage, key, status, *args):
            self.checkpoints.append((run_id, stage, key, status))

    repository = FakeRepository()
    kernel = FakeKernel()
    monkeypatch.setattr(
        foundation_features,
        "_financial_access",
        lambda run_id=None: (kernel, repository, object()),
    )
    monkeypatch.setattr(
        foundation_features,
        "calculate_valuation_features",
        lambda rows, _output_start, _output_end: [dict(rows[0])],
    )
    monkeypatch.setattr(foundation_features, "VALUATION_CODE_BATCH_SIZE", 3)

    result = foundation_features.rebuild_valuation_features(
        "run", date(2016, 8, 1), date(2021, 8, 2), date(2026, 8, 21)
    )

    assert repository.load_calls == [("B", "C"), ("F",)]
    assert repository.published_codes == ["B", "C", "F"]
    assert result["partitions"] == 6
