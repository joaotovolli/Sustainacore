from unittest import mock

from django.test import SimpleTestCase

from core import news_data


class NewsDataTests(SimpleTestCase):
    @mock.patch("core.news_data._get_sources")
    @mock.patch("core.news_data.get_connection")
    def test_fetch_news_list_normalizes_rows(self, get_connection, get_sources):
        get_sources.return_value = {
            "list": {
                "name": "V_NEWS_RECENT",
                "mapping": {
                    "item_id": "ITEM_ID",
                    "item_table": "ITEM_TABLE",
                    "published_at": "DT_PUB",
                    "title": "TITLE",
                    "url": "URL",
                    "source": "SOURCE_NAME",
                    "body": "BODY",
                    "summary": "SUMMARY",
                    "pillar_tags": "PILLAR_TAGS",
                    "categories": "CATEGORIES",
                    "tags": "TAGS",
                    "tickers": "TICKERS",
                    "ticker": "TICKER",
                    "full_text": "FULL_TEXT",
                },
            },
            "schema": "WKSP_ESGAPEX",
        }
        cursor = mock.MagicMock()
        cursor.fetchone.return_value = (2,)
        cursor.fetchall.return_value = [
            (
                "NEWS_ITEMS",
                101,
                "2025-01-02T12:30:00Z",
                "MSFT",
                "Sample headline",
                "https://example.com/story",
                "Example News",
                "Short body",
                "Summary text",
                "Transparency",
                "AI",
                "Regulation",
                "MSFT",
                1,
            ),
        ]
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = None
        get_connection.return_value = conn

        result = news_data.fetch_news_list(limit=10, offset=0, days=7)
        self.assertIsNone(result["error"])
        self.assertEqual(len(result["items"]), 1)
        item = result["items"][0]
        self.assertEqual(item["id"], "NEWS_ITEMS:101")
        self.assertTrue(item["has_full_body"])
        self.assertEqual(result["meta"]["total"], 2)

    @mock.patch("core.news_data._get_sources")
    @mock.patch("core.news_data.get_connection")
    def test_fetch_news_detail_prefers_full_text(self, get_connection, get_sources):
        get_sources.return_value = {
            "detail": {
                "name": "V_NEWS_ENRICHED",
                "mapping": {
                    "item_id": "ITEM_ID",
                    "item_table": "ITEM_TABLE",
                    "published_at": "DT_PUB",
                    "title": "TITLE",
                    "url": "URL",
                    "source": "SOURCE_NAME",
                    "body": "BODY",
                    "full_text": "FULL_TEXT",
                    "content": "CONTENT",
                    "article_text": "ARTICLE_TEXT",
                    "text": "TEXT",
                    "summary": "SUMMARY",
                    "pillar_tags": "PILLAR_TAGS",
                    "categories": "CATEGORIES",
                    "tags": "TAGS",
                    "tickers": "TICKERS",
                    "ticker": "TICKER",
                },
            },
            "schema": "WKSP_ESGAPEX",
        }
        cursor = mock.MagicMock()
        cursor.fetchone.return_value = (
            "NEWS_ITEMS",
            202,
            "2025-01-03T09:00:00Z",
            "AAPL",
            "Detail headline",
            "https://example.com/detail",
            "Example News",
            "Short body",
            "Full body text",
            None,
            None,
            None,
            "Summary text",
            "Transparency",
            "AI",
            "Regulation",
            "AAPL",
        )
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = None
        get_connection.return_value = conn

        result = news_data.fetch_news_detail(news_id="NEWS_ITEMS:202")
        item = result["item"]
        self.assertEqual(item["id"], "NEWS_ITEMS:202")
        self.assertEqual(item["full_text"], "Full body text")

    @mock.patch("core.news_data._discover_objects")
    def test_resolve_source_prefers_full_text(self, discover_objects):
        discover_objects.return_value = {
            "V_NEWS_RECENT": {
                "owner": "WKSP_ESGAPEX",
                "columns": ["ITEM_ID", "TITLE", "URL", "DT_PUB", "BODY"],
            },
            "V_NEWS_ALL": {
                "owner": "WKSP_ESGAPEX",
                "columns": ["ITEM_ID", "TITLE", "URL", "DT_PUB", "FULL_TEXT"],
            },
        }
        result = news_data._resolve_source(mock.MagicMock(), prefer_full=True)
        self.assertEqual(result["name"], "V_NEWS_ALL")

    @mock.patch("core.news_data._get_sources")
    @mock.patch("core.news_data.get_connection")
    def test_fetch_filter_options(self, get_connection, get_sources):
        get_sources.return_value = {
            "list": {
                "name": "V_NEWS_RECENT",
                "mapping": {"source": "SOURCE_NAME", "tags": "TAGS", "tickers": "TICKERS"},
            },
            "schema": "WKSP_ESGAPEX",
        }
        cursor = mock.MagicMock()
        cursor.fetchall.side_effect = [
            [("Source A",), ("Source B",)],
            [("Tag1,Tag2",), ("Tag2",)],
        ]
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = None
        get_connection.return_value = conn

        result = news_data.fetch_filter_options()
        self.assertIn("Source A", result["source_options"])
        self.assertIn("Tag1", result["tag_options"])
