from pathlib import Path

from data_collect.jobs.research_watchlist import RULE_VERSION, _candidate_sql


def test_watchlist_rule_version_is_explicit():
    assert RULE_VERSION == "watchlist-v1"


def test_watchlist_query_is_snapshot_scoped_and_explainable():
    sql = _candidate_sql()
    assert "s.snapshot_id=%s" in sql
    assert "s.can_buy IS TRUE" in sql
    assert "value_quality" in sql
    assert "growth_trend" in sql
    assert "jsonb_build_object" in sql
    assert "ROW_NUMBER()" in sql


def test_watchlist_query_deduplicates_a_stock_across_tracks():
    sql = _candidate_sql()
    assert "PARTITION BY ts_code" in sql
    assert "WHERE track_no=1" in sql
    assert "WHERE rank_no <= %s" in sql


def test_audit_migration_makes_active_records_append_only():
    sql = Path("sql/027_research_audit_immutability.sql").read_text(encoding="utf-8")
    assert "ACTIVE/ARCHIVED watchlist cannot be deleted" in sql
    assert "ACTIVE/ARCHIVED watchlist members are append-only" in sql
    assert "ACTIVE/ARCHIVED evidence is append-only" in sql
    assert "evidence stock is not a member of watchlist" in sql


def test_snapshot_lineage_evidence_is_explicitly_not_external_news():
    from data_collect.jobs.research_watchlist import add_snapshot_lineage_evidence

    assert "快照血缘" in add_snapshot_lineage_evidence.__doc__
    assert "新闻" in add_snapshot_lineage_evidence.__doc__
