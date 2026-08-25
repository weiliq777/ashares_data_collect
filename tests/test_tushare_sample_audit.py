from decimal import Decimal

from tools.audit_tushare_sample import _canonical, _equal


def test_source_audit_normalizes_dates_and_missing_values():
    assert _canonical("trade_date", "2026-08-21") == "20260821"
    assert _canonical("trade_date", "20260821") == "20260821"
    assert _canonical("close", "") is None


def test_source_audit_compares_numeric_values_with_provider_tolerance():
    assert _equal("close", Decimal("11.4100000000"), 11.41)
    assert not _equal("close", 11.41, 11.42)


def test_source_audit_dataset_fields_are_bounded():
    from tools.audit_tushare_sample import DATASET_FIELDS

    assert set(DATASET_FIELDS) == {"daily", "daily_basic", "adj_factor"}
    assert len(DATASET_FIELDS["daily"]) <= 20
