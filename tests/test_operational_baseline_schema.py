from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_operational_baselines_are_canonical_ops_tables():
    migration = (ROOT / "sql" / "076_operational_baselines_to_ops.sql").read_text(encoding="utf-8")
    assert "ALTER TABLE public.tushare_history_checkpoint SET SCHEMA ops" in migration
    assert "ALTER TABLE public.tushare_data_release SET SCHEMA ops" in migration
    assert "ops.tushare_history_checkpoint" in migration
    assert "ops.tushare_data_release" in migration


def test_runtime_repositories_do_not_use_unqualified_operational_baselines():
    roots = [ROOT / "data_collect" / "data_access", ROOT / "data_collect" / "jobs"]
    for root in roots:
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            assert '"tushare_history_checkpoint"' not in text
            assert '"tushare_data_release"' not in text
            assert "FROM tushare_history_checkpoint" not in text
            assert "FROM tushare_data_release" not in text


def test_legacy_public_app_privileges_are_revoked():
    revoke = (ROOT / "sql" / "077_revoke_legacy_public_app_privileges.sql").read_text(encoding="utf-8")
    assert "REVOKE ALL ON TABLE" in revoke
    assert "FROM ashares_app" in revoke
    assert "public.fin_income" in revoke
    assert "public.tushare_daily_raw_corrupt_20260811" in revoke
