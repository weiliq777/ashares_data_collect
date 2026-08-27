from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_single_snapshot_pit_visibility_check_is_snapshot_scoped():
    source = (ROOT / "data_collect" / "data_access" / "repository" / "pit_validation_repository.py").read_text(
        encoding="utf-8"
    )
    assert "WHERE snapshot_id=%s AND research_date=%s AND effective_announce_date>research_date" in source
    assert "Falling back to research_date would scan an entire yearly" in source
    assert '"snapshot_id_required"' in source
