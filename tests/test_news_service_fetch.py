import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.news_service as news_service


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed_sql = None
        self.binds = None
        self.description = [
            ("ITEM_TABLE", None, None, None, None, None, None),
            ("ITEM_ID", None, None, None, None, None, None),
            ("DT_PUB", None, None, None, None, None, None),
            ("TICKER", None, None, None, None, None, None),
            ("TITLE", None, None, None, None, None, None),
            ("URL", None, None, None, None, None, None),
            ("SOURCE_NAME", None, None, None, None, None, None),
            ("BODY", None, None, None, None, None, None),
            ("PILLAR_TAGS", None, None, None, None, None, None),
            ("CATEGORIES", None, None, None, None, None, None),
            ("TAGS", None, None, None, None, None, None),
            ("TICKERS", None, None, None, None, None, None),
        ]

    def execute(self, sql, binds=None):
        self.executed_sql = sql
        self.binds = binds or {}

    def fetchall(self):
        return list(self._rows)


class _FakeDetailCursor:
    def __init__(self, rows_per_call):
        self._rows_per_call = list(rows_per_call)
        self.executed_sql = []
        self.binds = []

    def execute(self, sql, binds=None):
        self.executed_sql.append(sql)
        self.binds.append(binds or {})

    def fetchone(self):
        if not self._rows_per_call:
            return None
        return self._rows_per_call.pop(0)


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_fetch_news_items_maps_rows_and_meta(monkeypatch):
    rows = [
        (
            "NEWS_ITEMS",
            123,
            datetime(2025, 1, 2, 15, 4, 5, tzinfo=timezone.utc),
            "MSFT",
            "Example headline",
            "https://example.com/article",
            "Example News",
            "Summary text",
            "E, G",
            "Markets, Technology",
            "esg, tech",
            "MSFT",
        ),
        (
            "ESG_NEWS",
            456,
            datetime(2024, 12, 31, 0, 0, 0, tzinfo=timezone.utc),
            "AAPL",
            "Another headline",
            "https://example.com/other",
            "Example Source",
            "Body text",
            None,
            None,
            None,
            "AAPL",
        ),
    ]

    cursor = _FakeCursor(rows)
    fake_conn = _FakeConnection(cursor)

    monkeypatch.setattr(news_service, "get_connection", lambda: fake_conn)

    items, has_more, effective_limit = news_service.fetch_news_items(
        limit=1, days=30, source="Example News", tags=["tech"], ticker=None
    )

    assert effective_limit == 1
    assert has_more is True
    assert len(items) == 1

    item = items[0]
    assert item["id"] == "NEWS_ITEMS:123"
    assert item["title"] == "Example headline"
    assert item["source"] == "Example News"
    assert item["url"] == "https://example.com/article"
    assert item["summary"] == "Summary text"
    assert item["tags"] == ["esg", "tech"]
    assert item["categories"] == ["Markets", "Technology"]
    assert item["pillar_tags"] == ["E", "G"]
    assert item["ticker"] == "MSFT"
    assert item["published_at"].endswith("Z")

    assert "FROM v_news_recent" in cursor.executed_sql
    assert cursor.binds["limit_plus_one"] == 2
    assert cursor.binds["days"] == 30
    assert cursor.binds["source"] == "Example News"
    assert any(key.startswith("tag_") for key in cursor.binds)


def test_fetch_news_item_detail_prefers_full_text(monkeypatch):
    row = (
        "NEWS_ITEMS",
        101,
        datetime(2025, 1, 2, 15, 4, 5, tzinfo=timezone.utc),
        "MSFT",
        "Example headline",
        "https://example.com/article",
        "Example Source",
        "Snippet body",
        "Full article content",
        "E, G",
        "Markets, Technology",
        "esg, tech",
        "MSFT",
    )
    cursor = _FakeDetailCursor([row])
    fake_conn = _FakeConnection(cursor)
    monkeypatch.setattr(news_service, "get_connection", lambda: fake_conn)

    item = news_service.fetch_news_item_detail("NEWS_ITEMS:101")
    assert item is not None
    assert item["body"] == "Full article content"
    assert item["summary"] == "Snippet body"


def test_fetch_news_item_detail_falls_back_to_body(monkeypatch):
    row = (
        "NEWS_ITEMS",
        202,
        datetime(2025, 1, 2, 15, 4, 5, tzinfo=timezone.utc),
        "MSFT",
        "Example headline",
        "https://example.com/article",
        "Example Source",
        "Body-only content",
        None,
        "E, G",
        "Markets, Technology",
        "esg, tech",
        "MSFT",
    )
    cursor = _FakeDetailCursor([None, row])
    fake_conn = _FakeConnection(cursor)
    monkeypatch.setattr(news_service, "get_connection", lambda: fake_conn)

    item = news_service.fetch_news_item_detail("NEWS_ITEMS:202")
    assert item is not None
    assert item["body"] == "Body-only content"
