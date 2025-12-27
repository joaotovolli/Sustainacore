from unittest import mock

from django.test import SimpleTestCase

from core import news_data


class NewsDataTests(SimpleTestCase):
    @mock.patch("core.news_data.get_connection")
    def test_fetch_news_list_normalizes_rows(self, get_connection):
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

    @mock.patch("core.news_data.get_connection")
    def test_fetch_news_detail_prefers_full_text(self, get_connection):
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
        self.assertEqual(item["body"], "Full body text")
