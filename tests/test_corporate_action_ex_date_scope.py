from datetime import date
from types import SimpleNamespace

import pytest

from data_collect.data_access.context import RunContext
from data_collect.data_access.corporate_action_evidence import (
    DIVIDEND_EVENT_SUPPORTED_COLUMNS,
    EX_DATE_EVIDENCE_VERSION,
    classify_missing_ex_date,
    quality_report_has_current_ex_date_evidence,
    summarize_missing_ex_date_scope,
)
from data_collect.data_access.repository.quality_inspection_repository import (
    QualityInspectionRepository,
)
from data_collect.data_access.repository.release_repository import ReleaseRepository


def _row(**overrides):
    row = {
        "ts_code": "000001.SZ",
        "source_record_key": "000001.SZ:2021-01-01:实施",
        "record_version": 1,
        "implementation_announce_date": date(2021, 4, 1),
        "record_date": date(2021, 4, 8),
        "pay_date": None,
        "div_list_date": date(2021, 4, 9),
        "cash_div": 0.0,
        "cash_div_tax": 0.0,
        "stock_div": 0.0,
        "stock_bonus_rate": None,
        "stock_conversion_rate": None,
    }
    row.update(overrides)
    return row


def test_official_restructuring_evidence_is_version_bound_and_never_fills_ex_date():
    row = _row(
        ts_code="000523.SZ",
        source_record_key="000523.SZ:2021-10-23:实施",
        record_version=3062,
        implementation_announce_date=date(2021, 12, 3),
        record_date=date(2021, 12, 8),
        div_list_date=date(2021, 12, 9),
        stock_div=1.569,
        stock_conversion_rate=1.569,
    )

    result = classify_missing_ex_date(
        row, scope_start=date(2021, 8, 2), scope_end=date(2026, 8, 21),
    )

    assert result["classification"] == "NOT_APPLICABLE_NO_PRICE_ADJUSTMENT"
    assert result["source_record_version"] == 3062
    assert result["evidence_version"] == EX_DATE_EVIDENCE_VERSION
    assert result["notice_url"].startswith("https://static.cninfo.com.cn/")
    assert "ex_date" not in result

    changed_version = classify_missing_ex_date(
        {**row, "record_version": 3063},
        scope_start=date(2021, 8, 2),
        scope_end=date(2026, 8, 21),
    )
    assert changed_version["classification"] == "IN_SCOPE_UNRESOLVED"


def test_unreviewed_equity_effect_column_invalidates_even_exact_official_evidence():
    row = _row(
        ts_code="000523.SZ",
        source_record_key="000523.SZ:2021-10-23:实施",
        record_version=3062,
        stock_div=1.569,
        stock_conversion_rate=1.569,
        rights_issue_rate=0.2,
    )

    result = classify_missing_ex_date(
        row, scope_start=date(2021, 8, 2), scope_end=date(2026, 8, 21),
    )

    assert result["classification"] == "UNRESOLVED_SCHEMA_EXTENSION"
    assert result["unexpected_fields"] == ["rights_issue_rate"]

    summary = summarize_missing_ex_date_scope(
        [row], scope_start=date(2021, 8, 2), scope_end=date(2026, 8, 21),
    )
    assert summary["schema_extension_unresolved_count"] == 1
    assert summary["unresolved_count"] == 1


@pytest.mark.parametrize(
    "ts_code,source_record_key,record_version,announce_date,stock_div",
    [
        (
            "000980.SZ",
            "000980.SZ:2021-10-23:实施",
            11380,
            date(2021, 12, 9),
            1.5,
        ),
        (
            "002072.SZ",
            "002072.SZ:2021-11-19:实施",
            14556,
            date(2021, 12, 18),
            1.08909,
        ),
        (
            "002175.SZ",
            "002175.SZ:2021-10-12:实施",
            16986,
            date(2021, 12, 21),
            0.69384138,
        ),
    ],
)
def test_each_official_no_ex_date_case_requires_exact_version(
    ts_code, source_record_key, record_version, announce_date, stock_div,
):
    row = _row(
        ts_code=ts_code,
        source_record_key=source_record_key,
        record_version=record_version,
        implementation_announce_date=announce_date,
        stock_div=stock_div,
        stock_conversion_rate=stock_div,
    )

    result = classify_missing_ex_date(
        row, scope_start=date(2021, 8, 21), scope_end=date(2026, 8, 21),
    )
    changed_version = classify_missing_ex_date(
        {**row, "record_version": record_version + 1},
        scope_start=date(2021, 8, 21),
        scope_end=date(2026, 8, 21),
    )

    assert result["classification"] == "NOT_APPLICABLE_NO_PRICE_ADJUSTMENT"
    assert result["notice_url"].startswith("https://static.cninfo.com.cn/")
    assert changed_version["classification"] == "IN_SCOPE_UNRESOLVED"


def test_scope_diagnostic_separates_old_completed_events_and_unresolved_events():
    out_of_scope = classify_missing_ex_date(
        _row(stock_div=0.25, stock_conversion_rate=0.25),
        scope_start=date(2021, 8, 2),
        scope_end=date(2026, 8, 21),
    )
    in_scope = classify_missing_ex_date(
        _row(
            implementation_announce_date=date(2022, 1, 3),
            record_date=date(2022, 1, 10),
            div_list_date=date(2022, 1, 11),
            stock_div=0.25,
            stock_conversion_rate=0.25,
        ),
        scope_start=date(2021, 8, 2),
        scope_end=date(2026, 8, 21),
    )
    no_date = classify_missing_ex_date(
        _row(
            implementation_announce_date=None,
            record_date=None,
            div_list_date=None,
            stock_div=0.25,
            stock_conversion_rate=0.25,
        ),
        scope_start=date(2021, 8, 2),
        scope_end=date(2026, 8, 21),
    )
    zero_action = classify_missing_ex_date(
        _row(implementation_announce_date=date(1994, 4, 2), record_date=None, div_list_date=None),
        scope_start=date(2021, 8, 2),
        scope_end=date(2026, 8, 21),
    )

    assert out_of_scope["classification"] == "OUT_OF_SCOPE_BY_LIFECYCLE_MILESTONE"
    assert in_scope["classification"] == "IN_SCOPE_UNRESOLVED"
    assert no_date["classification"] == "UNRESOLVED"
    assert zero_action["classification"] == "NO_MODELED_CASH_OR_SHARE_EFFECT"


def test_scope_summary_preserves_global_total_and_counts_every_classification():
    rows = [
        _row(stock_div=0.25, stock_conversion_rate=0.25),
        _row(
            ts_code="000523.SZ",
            source_record_key="000523.SZ:2021-10-23:实施",
            record_version=3062,
            implementation_announce_date=date(2021, 12, 3),
            record_date=date(2021, 12, 8),
            div_list_date=date(2021, 12, 9),
            stock_div=1.569,
            stock_conversion_rate=1.569,
        ),
        _row(
            ts_code="000002.SZ",
            source_record_key="unknown-action",
            record_version=99,
            implementation_announce_date=date(2022, 1, 3),
            record_date=date(2022, 1, 10),
            div_list_date=date(2022, 1, 11),
            stock_div=0.25,
            stock_conversion_rate=0.25,
        ),
    ]

    report = summarize_missing_ex_date_scope(
        rows, scope_start=date(2021, 8, 2), scope_end=date(2026, 8, 21),
    )

    assert report["global_missing_count"] == 3
    assert report["out_of_scope_count"] == 1
    assert report["documented_not_applicable_count"] == 1
    assert report["in_scope_unresolved_count"] == 1
    assert report["unresolved_count"] == 1
    assert sum(report["classification_counts"].values()) == 3
    with pytest.raises(ValueError, match="scope_start"):
        summarize_missing_ex_date_scope(
            rows, scope_start=date(2026, 8, 22), scope_end=date(2026, 8, 21),
        )


def test_release_evidence_currentness_requires_matching_versions_and_full_counts():
    scope = summarize_missing_ex_date_scope(
        [_row()],
        scope_start=date(2021, 8, 2),
        scope_end=date(2026, 8, 21),
        schema_columns=set(DIVIDEND_EVENT_SUPPORTED_COLUMNS),
    )
    report = {
        "corporate_action_evidence_version": EX_DATE_EVIDENCE_VERSION,
        "implementation_without_ex_date": 1,
        "implementation_without_ex_date_scope": scope,
    }

    assert quality_report_has_current_ex_date_evidence(report)
    assert not quality_report_has_current_ex_date_evidence(
        {**report, "corporate_action_evidence_version": "old-version"}
    )
    old_scope = {**scope, "audit_version": "corporate-action-ex-date-evidence-v1"}
    assert not quality_report_has_current_ex_date_evidence(
        {**report, "implementation_without_ex_date_scope": old_scope}
    )
    assert not quality_report_has_current_ex_date_evidence(
        {**report, "implementation_without_ex_date": 2}
    )
    assert not quality_report_has_current_ex_date_evidence(
        {**report, "implementation_without_ex_date": 1.5}
    )
    corrupted_scope = {
        **scope,
        "records": [
            {**scope["records"][0], "classification": "UNRESOLVED"}
        ],
    }
    assert not quality_report_has_current_ex_date_evidence(
        {**report, "implementation_without_ex_date_scope": corrupted_scope}
    )


def test_schema_contract_rejects_missing_columns_and_unknown_columns_even_without_rows():
    supported = set(DIVIDEND_EVENT_SUPPORTED_COLUMNS)
    missing = summarize_missing_ex_date_scope(
        [_row()],
        scope_start=date(2021, 8, 2),
        scope_end=date(2026, 8, 21),
        schema_columns=supported - {"stock_conversion_rate"},
    )
    unexpected = summarize_missing_ex_date_scope(
        [],
        scope_start=date(2021, 8, 2),
        scope_end=date(2026, 8, 21),
        schema_columns=supported | {"rights_issue_rate"},
    )

    assert missing["schema_contract_status"] == "FAIL"
    assert missing["schema_contract_issues"]["missing_columns"] == [
        "stock_conversion_rate",
    ]
    assert unexpected["schema_contract_status"] == "FAIL"
    assert unexpected["schema_contract_issues"]["unexpected_columns"] == [
        "rights_issue_rate",
    ]


def test_repository_scoped_gate_preserves_global_count_and_fails_only_on_unresolved_scope(monkeypatch):
    monkeypatch.setattr(
        QualityInspectionRepository,
        "phase2_invariants",
        lambda self, context, cutoff, skip_names: {},
    )
    monkeypatch.setattr(
        QualityInspectionRepository,
        "phase2_checkpoint_completeness",
        lambda self, context, cutoff: {},
    )
    values = {
        "release:lifecycle": 0,
        "release:dividend-duplicates": 0,
        "release:dividend-ex-date": 1,
    }
    repository = ReleaseRepository(object())
    monkeypatch.setattr(
        repository,
        "_scalar",
        lambda table, sql, context, partition, params=(): values[partition],
    )
    monkeypatch.setattr(
        repository,
        "_missing_ex_date_scope",
        lambda scope_start, scope_end, context: {
            "global_missing_count": 1,
            "scope_start_date": scope_start.isoformat(),
            "scope_end_date": scope_end.isoformat(),
            "out_of_scope_count": 0,
            "documented_not_applicable_count": 1,
            "zero_modeled_effect_count": 0,
            "in_scope_unresolved_count": 0,
            "unresolved_count": 0,
            "classification_counts": {"NOT_APPLICABLE_NO_PRICE_ADJUSTMENT": 1},
            "schema_contract_status": "PASS",
            "schema_contract_issues": {},
            "records": [],
        },
    )

    report = repository.quality_gate(
        date(2026, 8, 21), False, RunContext(
            run_id="test", job_name="test", code_version="test",
            config_hash="test", pipeline_version="test",
        ), scope_start=date(2021, 8, 2),
    )

    assert report["implementation_without_ex_date"] == 1
    assert report["implementation_without_ex_date_scope"]["global_missing_count"] == 1
    assert "implementation_without_ex_date" not in report["failures"]
    assert report["failures"] == {}


def test_repository_scope_reader_marks_global_population_and_preserves_global_rows():
    observed = {}
    queries = []
    rows = [{"event_record": _row(stock_div=0.25, stock_conversion_rate=0.25)}]

    def repository_query(context, target, sql, params, scope, max_rows):
        queries.append(sql)
        if "jsonb_object_keys" in sql:
            return [
                {"column_name": column}
                for column in sorted(DIVIDEND_EVENT_SUPPORTED_COLUMNS)
            ]
        observed.update(
            target=target,
            sql=sql,
            params=params,
            scope=scope,
            max_rows=max_rows,
        )
        return rows

    repository = ReleaseRepository(
        SimpleNamespace(read=SimpleNamespace(_repository_query=repository_query))
    )
    scope_start = date(2021, 8, 2)
    scope_end = date(2026, 8, 21)
    report = repository._missing_ex_date_scope(
        scope_start,
        scope_end,
        RunContext(
            run_id="test",
            job_name="test",
            code_version="test",
            config_hash="test",
            pipeline_version="test",
        ),
    )

    assert observed["target"] == "std.dividend_event"
    assert "to_jsonb(event) AS event_record" in observed["sql"]
    assert any("jsonb_object_keys" in query for query in queries)
    assert "implementation_status='实施' AND ex_date IS NULL" in observed["sql"]
    assert observed["scope"].date_from is None
    assert observed["scope"].date_to is None
    assert observed["scope"].dimensions == {
        "scope_start_date": "2021-08-02",
        "scope_end_date": "2026-08-21",
        "query_population": "all_global_missing_ex_date_rows",
    }
    assert observed["max_rows"] == 100_000
    assert report["global_missing_count"] == 1
    assert report["out_of_scope_count"] == 1
    assert report["schema_contract_status"] == "PASS"


def test_scoped_gate_fails_closed_when_classification_count_differs_from_global_count(monkeypatch):
    monkeypatch.setattr(
        QualityInspectionRepository,
        "phase2_invariants",
        lambda self, context, cutoff, skip_names: {},
    )
    monkeypatch.setattr(
        QualityInspectionRepository,
        "phase2_checkpoint_completeness",
        lambda self, context, cutoff: {},
    )
    repository = ReleaseRepository(object())
    monkeypatch.setattr(
        repository,
        "_scalar",
        lambda table, sql, context, partition, params=(): (
            1 if partition == "release:dividend-ex-date" else 0
        ),
    )
    monkeypatch.setattr(
        repository,
        "_missing_ex_date_scope",
        lambda scope_start, scope_end, context: {
            "global_missing_count": 0,
            "unresolved_count": 0,
        },
    )

    report = repository.quality_gate(
        date(2026, 8, 21),
        False,
        RunContext(
            run_id="test",
            job_name="test",
            code_version="test",
            config_hash="test",
            pipeline_version="test",
        ),
        scope_start=date(2021, 8, 2),
    )

    assert report["failures"]["implementation_without_ex_date_scope_count_mismatch"] == {
        "global_count": 1,
        "classified_count": 0,
    }


def test_five_year_window_start_handles_leap_day_cutoff():
    assert ReleaseRepository._five_year_window_start(date(2032, 2, 29)) == date(
        2027, 2, 28,
    )
