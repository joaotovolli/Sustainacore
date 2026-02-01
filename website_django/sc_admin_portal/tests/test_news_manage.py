import os
from contextlib import ExitStack
from unittest import mock

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse

ADMIN_EMAIL = "admin@example.com"


class AdminPortalNewsManageTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.authorized_user = self.user_model.objects.create_user(
            username="authorized",
            email=ADMIN_EMAIL,
            password="pass",
        )

    def _portal_mocks(self):
        return [
            mock.patch("sc_admin_portal.views.oracle_proc.list_recent_jobs", return_value=[]),
            mock.patch("sc_admin_portal.views.oracle_proc.list_pending_approvals", return_value=[]),
            mock.patch("sc_admin_portal.views.oracle_proc.list_recent_decisions", return_value=[]),
            mock.patch("sc_admin_portal.views.oracle_proc.list_recent_research_requests", return_value=[]),
            mock.patch("sc_admin_portal.views.oracle_proc.get_research_settings", return_value={}),
        ]

    def _portal_mocks_ctx(self):
        stack = ExitStack()
        for patcher in self._portal_mocks():
            stack.enter_context(patcher)
        return stack

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    def test_manage_news_requires_auth(self):
        response = self.client.post(
            reverse("sc_admin_portal:dashboard"),
            {
                "action": "edit_news_item",
                "manage_news_id": "NEWS_ITEMS:44",
                "confirm_edit": "on",
            },
        )
        self.assertEqual(response.status_code, 404)

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    def test_lookup_news_item_shows_preview(self):
        with mock.patch("sc_admin_portal.views.get_news_item_preview") as preview_mock:
            preview_mock.return_value = {
                "id": "NEWS_ITEMS:44",
                "item_id": 44,
                "title": "Sample title",
                "published_at": "2026-01-31T00:00:00Z",
                "summary": "Short excerpt",
                "asset_count": 2,
                "asset_ids": [10, 11],
            }
            with self._portal_mocks_ctx():
                self.client.force_login(self.authorized_user)
                response = self.client.post(
                    reverse("sc_admin_portal:dashboard"),
                    {
                        "action": "lookup_news_item",
                        "manage_news_id": "NEWS_ITEMS:44",
                    },
                )
        content = response.content.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("NEWS_ITEMS:44", content)
        self.assertIn("Sample title", content)
        preview_mock.assert_called_once_with(news_id=44)

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    def test_lookup_news_item_invalid_id(self):
        with mock.patch("sc_admin_portal.views.get_news_item_preview") as preview_mock:
            with self._portal_mocks_ctx():
                self.client.force_login(self.authorized_user)
                response = self.client.post(
                    reverse("sc_admin_portal:dashboard"),
                    {
                        "action": "lookup_news_item",
                        "manage_news_id": "NEWS_ITEMS:XYZ",
                    },
                )
        content = response.content.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Enter a valid news id.", content)
        preview_mock.assert_not_called()

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    def test_edit_news_requires_confirmation(self):
        with mock.patch("sc_admin_portal.views.update_news_item") as update_mock:
            with self._portal_mocks_ctx():
                self.client.force_login(self.authorized_user)
                response = self.client.post(
                    reverse("sc_admin_portal:dashboard"),
                    {
                        "action": "edit_news_item",
                        "manage_news_id": "NEWS_ITEMS:44",
                        "manage_news_title": "Updated title",
                    },
                )
        content = response.content.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Confirm you want to overwrite", content)
        update_mock.assert_not_called()

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    def test_edit_news_docx_calls_update(self):
        def docx_side_effect(path, asset_uploader, stats):
            stats["images_found"] = 1
            stats["images_uploaded"] = 1
            return ("DOCX headline", "<p>DOCX body</p>")

        with mock.patch("sc_admin_portal.views.build_news_body_from_docx", side_effect=docx_side_effect):
            with mock.patch("sc_admin_portal.views.update_news_item") as update_mock:
                with self._portal_mocks_ctx():
                    self.client.force_login(self.authorized_user)
                    upload = SimpleUploadedFile(
                        "news.docx",
                        b"fake-docx",
                        content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    )
                    response = self.client.post(
                        reverse("sc_admin_portal:dashboard"),
                        {
                            "action": "edit_news_item",
                            "manage_news_id": "NEWS_ITEMS:44",
                            "confirm_edit": "on",
                            "manage_docx_file": upload,
                        },
                    )
        self.assertEqual(response.status_code, 200)
        update_mock.assert_called_once()

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    def test_delete_news_requires_confirm_match(self):
        with mock.patch("sc_admin_portal.views.delete_news_item") as delete_mock:
            with self._portal_mocks_ctx():
                self.client.force_login(self.authorized_user)
                response = self.client.post(
                    reverse("sc_admin_portal:dashboard"),
                    {
                        "action": "delete_news_item",
                        "manage_news_id": "NEWS_ITEMS:44",
                        "confirm_news_id": "NEWS_ITEMS:45",
                        "confirm_delete": "on",
                    },
                )
        content = response.content.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Confirmation id must match", content)
        delete_mock.assert_not_called()

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    def test_delete_news_not_found(self):
        with mock.patch("sc_admin_portal.views.get_news_item_preview", return_value=None):
            with mock.patch("sc_admin_portal.views.delete_news_item") as delete_mock:
                with self._portal_mocks_ctx():
                    self.client.force_login(self.authorized_user)
                    response = self.client.post(
                        reverse("sc_admin_portal:dashboard"),
                        {
                            "action": "delete_news_item",
                            "manage_news_id": "NEWS_ITEMS:44",
                            "confirm_news_id": "NEWS_ITEMS:44",
                            "confirm_delete": "on",
                        },
                    )
        content = response.content.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("News item not found.", content)
        delete_mock.assert_not_called()
