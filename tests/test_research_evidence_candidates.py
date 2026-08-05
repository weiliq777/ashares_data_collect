from datetime import date

from data_collect.jobs import research_evidence_candidates as rec


class _Cursor:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params):
        self.params = params

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows):
        self.cursor_obj = _Cursor(rows)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self.cursor_obj


def test_candidates_are_read_only_and_cut_off_at_snapshot_date(monkeypatch):
    conn = _Connection([("snapshot-1", "ACTIVE", date(2026, 8, 3), "600519.SH", "贵州茅台")])
    monkeypatch.setattr(rec, "get_connection", lambda: conn)
    calls = []

    def fake_search(query, **kwargs):
        calls.append((query, kwargs))
        return {"total": 1, "hits": [{
            "document_index": "news-2026", "document_id": "doc-1",
            "url": "https://example.test/news/1", "title": "候选",
            "source": "官方", "channel": "announcement",
            "pub_time": "2026-08-02 10:00:00", "score": 1.0,
            "highlight": None,
        }]}

    monkeypatch.setattr(rec, "search_news", fake_search)
    out = rec.search_candidates("watchlist-1", top_k=3)

    assert len(out) == 1
    assert out[0]["review_status"] == "REVIEW_REQUIRED"
    assert out[0]["document_id"] == "doc-1"
    assert calls[0][0] == "贵州茅台"
    assert calls[0][1]["stocks"] == ["600519.SH"]
    assert calls[0][1]["date_range"] == (None, "2026-08-03")
    assert calls[0][1]["include_identity"] is True
