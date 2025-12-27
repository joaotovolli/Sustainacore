import base64
import json
from unittest import mock

import requests
from django.test import TestCase
from django.urls import reverse


class LoginViewsTests(TestCase):
    @mock.patch("core.views.requests.post")
    def test_login_email_sets_session_and_redirects(self, post_mock):
        post_mock.return_value.status_code = 200
        response = self.client.post(reverse("login"), {"email": "User@Example.com"})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], reverse("login_code"))
        self.assertEqual(self.client.session.get("login_email"), "user@example.com")

    @mock.patch("core.views.get_profile", return_value=None)
    @mock.patch("core.views.requests.post")
    def test_login_code_sets_cookie_on_success(self, post_mock, profile_mock):
        post_mock.return_value.status_code = 200
        post_mock.return_value.json.return_value = {"token": "abc123", "expires_in_seconds": 3600}
        session = self.client.session
        session["login_email"] = "user@example.com"
        session.save()

        response = self.client.post(reverse("login_code"), {"code": "123456"})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], reverse("home"))
        self.assertIn("sc_session", response.cookies)
        self.assertEqual(self.client.session.get("auth_email"), "user@example.com")

    @mock.patch("core.views.requests.post", side_effect=requests.exceptions.Timeout)
    def test_login_code_handles_timeout(self, post_mock):
        session = self.client.session
        session["login_email"] = "user@example.com"
        session.save()

        response = self.client.post(reverse("login_code"), {"code": "123456"})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "We could not verify the code right now. Please try again.")

    def test_logout_clears_cookie(self):
        self.client.cookies["sc_session"] = "token"
        response = self.client.post(reverse("logout"))
        self.assertEqual(response.status_code, 302)
        self.assertIn("sc_session", response.cookies)

    def test_header_shows_login_when_logged_out(self):
        self.client.cookies.clear()
        session = self.client.session
        session.flush()
        response = self.client.get(reverse("login"))
        self.assertContains(response, "Login")
        self.assertNotContains(response, "auth-menu")

    def test_header_shows_account_when_logged_in(self):
        session = self.client.session
        session["auth_email"] = "user@example.com"
        session.save()
        self.client.cookies["sc_session"] = "token"
        response = self.client.get(reverse("login"))
        self.assertContains(response, "auth-menu")
        self.assertContains(response, "user")
        self.assertContains(response, "Connected")
        self.assertContains(response, "user@example.com")

    def test_account_redirects_when_logged_out(self):
        response = self.client.get(reverse("account"))
        self.assertEqual(response.status_code, 302)
        self.assertIn(reverse("login"), response["Location"])

    @mock.patch("core.views.upsert_profile")
    @mock.patch("core.views.get_profile", return_value=None)
    def test_account_upserts_profile(self, profile_mock, upsert_mock):
        session = self.client.session
        session["auth_email"] = "user@example.com"
        session.save()
        self.client.cookies["sc_session"] = "token"
        response = self.client.post(
            reverse("account"),
            {"name": "Alex", "country": "United States", "company": "Acme", "phone": "555"},
        )
        self.assertEqual(response.status_code, 200)
        upsert_mock.assert_called_once_with(
            "user@example.com",
            "Alex",
            "United States",
            "Acme",
            "555",
        )

    @mock.patch("core.views.upsert_profile")
    @mock.patch("core.views.get_profile", return_value=None)
    def test_account_allows_empty_profile(self, profile_mock, upsert_mock):
        session = self.client.session
        session["auth_email"] = "user@example.com"
        session.save()
        self.client.cookies["sc_session"] = "token"
        response = self.client.post(
            reverse("account"),
            {"name": "", "country": "", "company": "", "phone": ""},
        )
        self.assertEqual(response.status_code, 200)
        upsert_mock.assert_called_once_with("user@example.com", "", "", "", "")

    @mock.patch("core.views.upsert_profile")
    @mock.patch("core.views.get_profile", return_value=None)
    def test_account_saves_country_only(self, profile_mock, upsert_mock):
        session = self.client.session
        session["auth_email"] = "user@example.com"
        session.save()
        self.client.cookies["sc_session"] = "token"
        response = self.client.post(
            reverse("account"),
            {"name": "", "country": "Canada", "company": "", "phone": ""},
        )
        self.assertEqual(response.status_code, 200)
        upsert_mock.assert_called_once_with("user@example.com", "", "Canada", "", "")

    def test_account_country_groups_render(self):
        session = self.client.session
        session["auth_email"] = "user@example.com"
        session.save()
        self.client.cookies["sc_session"] = "token"
        response = self.client.get(reverse("account"))
        content = response.content.decode("utf-8")
        self.assertIn('optgroup label="Most used"', content)
        self.assertIn('optgroup label="All countries"', content)
        most_used_block = content.split('optgroup label="Most used"', 1)[-1]
        most_used_block = most_used_block.split('optgroup label="All countries"', 1)[0]
        self.assertIn("United Kingdom", most_used_block)
        self.assertIn("India", most_used_block)
        self.assertIn("China", most_used_block)
        option_count = content.count("<option")
        self.assertGreaterEqual(option_count, 200)

    def test_header_ignores_invalid_token(self):
        self.client.cookies["sc_session"] = "not-a-token"
        response = self.client.get(reverse("login"))
        self.assertContains(response, "Login")
        self.assertNotContains(response, "auth-menu")

    def test_header_accepts_token_with_email_claim(self):
        payload = {"email": "alice@example.com"}
        payload_json = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        payload_b64 = base64.urlsafe_b64encode(payload_json).decode("utf-8").rstrip("=")
        token = f"header.{payload_b64}.sig"
        self.client.cookies["sc_session"] = token
        response = self.client.get(reverse("login"))
        self.assertContains(response, "alice")
        self.assertContains(response, "alice@example.com")
        self.assertContains(response, "auth-menu")
