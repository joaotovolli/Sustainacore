from datetime import datetime
from unittest import mock

from django.test import SimpleTestCase

from sc_admin_portal.news_storage import delete_news_item, update_news_item, NewsStorageError


class _FakeCursor:
    def __init__(self):
        self.commands = []
        self.rowcount = 0
        self._fetchone = None
        self._fetchall = []

    def setinputsizes(self, **kwargs):  # pragma: no cover - no-op for test
        return None

    def execute(self, sql, params=None):
        statement = sql.strip()
        self.commands.append((statement, params))
        if statement.startswith("SELECT id, title, dt_pub"):
            self._fetchone = (44, "Sample title", datetime(2026, 1, 1))
        elif statement.startswith("SELECT asset_id"):
            self._fetchall = [(10,), (11,)]
        elif statement.startswith("UPDATE news_items"):
            self.rowcount = 1
        elif statement.startswith("UPDATE news_assets"):
            self.rowcount = 1
        elif statement.startswith("DELETE") and "news_item_tags" in statement:
            self.rowcount = 2
        elif statement.startswith("DELETE") and "news_assets" in statement:
            self.rowcount = 2
        elif statement.startswith("DELETE") and "news_items" in statement:
            self.rowcount = 1

    def fetchone(self):
        return self._fetchone

    def fetchall(self):
        return self._fetchall


class _FakeConn:
    def __init__(self):
        self.cursor_obj = _FakeCursor()
        self.committed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class NewsStorageManageTests(SimpleTestCase):
    def test_update_news_item_links_assets(self):
        fake_conn = _FakeConn()
        with mock.patch("sc_admin_portal.news_storage.get_connection", return_value=fake_conn):
            update_news_item(
                news_id=44,
                headline="Updated title",
                body_html='<p>Body</p><img src="/news/assets/10/"><img src="/news/assets/11/">',
            )

        update_calls = [
            stmt for stmt, _ in fake_conn.cursor_obj.commands if stmt.startswith("UPDATE news_assets")
        ]
        self.assertEqual(len(update_calls), 2)
        self.assertTrue(fake_conn.committed)

    def test_delete_news_item_removes_assets_and_tags(self):
        fake_conn = _FakeConn()
        with mock.patch("sc_admin_portal.news_storage.get_connection", return_value=fake_conn):
            result = delete_news_item(news_id=44)

        delete_statements = [stmt for stmt, _ in fake_conn.cursor_obj.commands if stmt.startswith("DELETE")]
        self.assertTrue(any("news_item_tags" in stmt for stmt in delete_statements))
        self.assertTrue(any("news_assets" in stmt for stmt in delete_statements))
        self.assertTrue(any("news_items" in stmt for stmt in delete_statements))
        self.assertEqual(result["assets_deleted"], 2)
        self.assertTrue(fake_conn.committed)

    def test_delete_news_item_handles_lock_errors(self):
        with mock.patch(
            "sc_admin_portal.news_storage.get_connection",
            side_effect=RuntimeError("ORA-12860: deadlock detected while waiting for a sibling row lock"),
        ):
            with self.assertRaises(NewsStorageError) as ctx:
                delete_news_item(news_id="NEWS_ITEMS:44")
        self.assertIn("Delete blocked by database locks", str(ctx.exception))
