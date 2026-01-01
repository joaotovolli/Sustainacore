import os
from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.auth import COOKIE_NAME


ADMIN_EMAIL = "admin@example.com"


class AdminPortalAccessTests(TestCase):
    def setUp(self):
        self.portal_url = reverse("sc_admin_portal:dashboard")
        self.approve_url_name = "sc_admin_portal:approve"
        self.reject_url_name = "sc_admin_portal:reject"
        self.user_model = get_user_model()
        self.authorized_user = self.user_model.objects.create_user(
            username="authorized",
            email=ADMIN_EMAIL,
            password="pass",
        )
        self.other_user = self.user_model.objects.create_user(
            username="other",
            email="other@example.com",
            password="pass",
        )

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    def test_portal_returns_404_when_not_logged_in(self):
        response = self.client.get(self.portal_url)
        self.assertEqual(response.status_code, 404)

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    def test_portal_returns_404_for_wrong_email(self):
        self.client.force_login(self.other_user)
        response = self.client.get(self.portal_url)
        self.assertEqual(response.status_code, 404)

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.list_recent_jobs", return_value=[])
    @mock.patch("sc_admin_portal.views.oracle_proc.list_pending_approvals", return_value=[])
    @mock.patch("sc_admin_portal.views.oracle_proc.list_recent_decisions", return_value=[])
    def test_portal_returns_200_for_authorized_email(
        self, decisions_mock, approvals_mock, jobs_mock
    ):
        self.client.force_login(self.authorized_user)
        response = self.client.get(self.portal_url)
        self.assertEqual(response.status_code, 200)
        jobs_mock.assert_called_once()
        approvals_mock.assert_called_once()
        decisions_mock.assert_called_once()

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.list_recent_jobs", return_value=[])
    @mock.patch("sc_admin_portal.views.oracle_proc.list_pending_approvals", return_value=[])
    @mock.patch("sc_admin_portal.views.oracle_proc.list_recent_decisions", return_value=[])
    def test_portal_allows_session_auth_email(
        self, decisions_mock, approvals_mock, jobs_mock
    ):
        session = self.client.session
        session["auth_email"] = ADMIN_EMAIL
        session.save()
        self.client.cookies[COOKIE_NAME] = "token"
        response = self.client.get(self.portal_url)
        self.assertEqual(response.status_code, 200)
        jobs_mock.assert_called_once()
        approvals_mock.assert_called_once()
        decisions_mock.assert_called_once()

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.insert_job")
    @mock.patch("sc_admin_portal.views.oracle_proc.list_recent_jobs", return_value=[])
    @mock.patch("sc_admin_portal.views.oracle_proc.list_pending_approvals", return_value=[])
    @mock.patch("sc_admin_portal.views.oracle_proc.list_recent_decisions", return_value=[])
    def test_submit_job_calls_oracle_insert(
        self, decisions_mock, approvals_mock, jobs_mock, insert_mock
    ):
        self.client.force_login(self.authorized_user)
        response = self.client.post(
            self.portal_url,
            {
                "action": "submit_job",
                "routine_code": "NEWS_PUBLISH",
                "content_text": "Some text",
                "instructions": "Do the thing",
            },
        )
        self.assertEqual(response.status_code, 200)
        insert_mock.assert_called_once()

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.decide_approval")
    @mock.patch("sc_admin_portal.views.oracle_proc.get_approval", return_value=None)
    def test_approve_requires_authorized_user(self, approval_mock, decide_mock):
        approve_url = reverse(self.approve_url_name, args=[123])
        response = self.client.post(approve_url, {"decision_notes": "ok"})
        self.assertEqual(response.status_code, 404)
        decide_mock.assert_not_called()

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.decide_approval")
    @mock.patch("sc_admin_portal.views.oracle_proc.get_approval", return_value={"approval_id": 123})
    def test_reject_calls_oracle_update(self, approval_mock, decide_mock):
        self.client.force_login(self.authorized_user)
        reject_url = reverse(self.reject_url_name, args=[123])
        response = self.client.post(reject_url, {"decision_notes": "no"})
        self.assertEqual(response.status_code, 302)
        decide_mock.assert_called_once()

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.decide_approval")
    @mock.patch("sc_admin_portal.views.oracle_proc.get_approval", return_value={"approval_id": 456})
    def test_approve_calls_oracle_update(self, approval_mock, decide_mock):
        self.client.force_login(self.authorized_user)
        approve_url = reverse(self.approve_url_name, args=[456])
        response = self.client.post(approve_url, {"decision_notes": "yes"})
        self.assertEqual(response.status_code, 302)
        decide_mock.assert_called_once()
