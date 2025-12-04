from datetime import datetime
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.news_service import _build_news_sql, fetch_news_items  # noqa: E402


def _fake_conn(rows_out):
    class FakeCursor:
        def __init__(self, rows):
            self.rows = rows

        def execute(self, sql, binds):
            self.sql = sql
            self.binds = binds

        def fetchall(self):
            return self.rows

    class FakeConn:
        def __init__(self, rows):
            self.rows = rows
            self.cursor_obj = FakeCursor(rows)

        def cursor(self):
            return self.cursor_obj

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    return FakeConn(rows_out)


def test_build_news_sql_with_filters():
    sql, binds = _build_news_sql(days=30, source="ESG", tags=["ai", "ethics"], ticker="MSFT", limit=5)
    assert "v_news_recent" in sql
    assert "dt_pub >= (SYSDATE - :days)" in sql
    assert "UPPER(source_name) = UPPER(:source)" in sql
    assert "UPPER(tags) LIKE '%' || UPPER(:tag_0) || '%'" in sql
    assert "UPPER(tickers) LIKE '%' || UPPER(:ticker) || '%'" in sql
    assert binds["days"] == 30
    assert binds["source"] == "ESG"
    assert binds["tag_0"] == "ai"
    assert binds["tag_1"] == "ethics"
    assert binds["ticker"] == "MSFT"
    assert binds["limit_plus_one"] == 6


def test_fetch_news_items_maps_rows(monkeypatch):
    dt = datetime(2025, 10, 1, 10, 30, 20)
    rows = [
        (
            "ESG_NEWS",
            123,
            dt,
            "MSFT",
            "Example title",
            "https://example.com",
            "Example Source",
            "Body text",
            "E,G",
            "AI Governance;Compliance",
            "tag1, tag2",
            "MSFT;AAPL",
        ),
        (
            "ESG_NEWS",
            124,
            dt,
            "AAPL",
            "Overflow",
            "https://example.com/2",
            "Example Source",
            "Body 2",
            "",
            "",
            "",
            "",
        ),
    ]

    monkeypatch.setattr("app.news_service.get_connection", lambda: _fake_conn(rows))
    items, has_more, effective_limit = fetch_news_items(limit=1, days=None, source=None, tags=None, ticker=None)

    assert effective_limit == 1
    assert has_more is True
    assert len(items) == 1
    first = items[0]
    assert first["id"] == "ESG_NEWS:123"
    assert first["source"] == "Example Source"
    assert first["tags"] == ["tag1", "tag2"]
    assert first["categories"] == ["AI Governance", "Compliance"]
    assert first["pillar_tags"] == ["E", "G"]
    assert first["ticker"] == "MSFT, AAPL"
    assert first["published_at"].startswith("2025-10-01T10:30:20")
