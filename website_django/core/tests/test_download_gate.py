import json
from unittest import mock
from urllib.parse import parse_qs, urlparse

from django.test import TestCase
from django.urls import reverse


class DownloadGateTests(TestCase):
    @mock.patch("core.downloads.log_event")
    def test_download_redirects_when_logged_out(self, log_event_mock):
        response = self.client.get(reverse("tech100_export"), HTTP_REFERER="/tech100/scores/")
        self.assertEqual(response.status_code, 302)
        location = response["Location"]
        parsed = urlparse(location)
        params = parse_qs(parsed.query)
        self.assertEqual(params.get("download_login"), ["1"])
        self.assertIn("download_url", params)
        log_event_mock.assert_any_call(
            "download_blocked",
            mock.ANY,
            {"download": "/tech100/export/", "reason": "login_required"},
        )

    @mock.patch("core.downloads.log_event")
    def test_download_returns_401_for_ajax(self, log_event_mock):
        response = self.client.get(
            reverse("tech100_export"),
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json().get("login_required"), True)
        log_event_mock.assert_any_call(
            "download_blocked",
            mock.ANY,
            {"download": "/tech100/export/", "reason": "login_required"},
        )

    @mock.patch("core.views.fetch_tech100")
    @mock.patch("core.downloads.log_event")
    def test_download_succeeds_when_logged_in(self, log_event_mock, fetch_mock):
        fetch_mock.return_value = {"items": [], "error": None, "meta": {}}
        session = self.client.session
        session["auth_email"] = "user@example.com"
        session.save()
        self.client.cookies["sc_session"] = "token"

        response = self.client.get(reverse("tech100_export"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv")
        log_event_mock.assert_any_call(
            "download_ok",
            mock.ANY,
            {"download": "/tech100/export/"},
        )

    @mock.patch("core.views.log_event")
    def test_ux_event_endpoint_logs(self, log_event_mock):
        payload = {"event_type": "download_click", "metadata": {"page": "/tech100/scores/"}}
        response = self.client.post(
            reverse("ux_event"),
            data=json.dumps(payload),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        log_event_mock.assert_called_once()
