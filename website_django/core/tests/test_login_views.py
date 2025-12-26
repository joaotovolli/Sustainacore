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

    @mock.patch("core.views.requests.post")
    def test_login_code_sets_cookie_on_success(self, post_mock):
        post_mock.return_value.status_code = 200
        post_mock.return_value.json.return_value = {"token": "abc123", "expires_in_seconds": 3600}
        session = self.client.session
        session["login_email"] = "user@example.com"
        session.save()

        response = self.client.post(reverse("login_code"), {"code": "123456"})
        self.assertEqual(response.status_code, 302)
        self.assertIn("sc_session", response.cookies)

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
