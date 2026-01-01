import os
from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.auth import COOKIE_NAME
from sc_admin_portal import oracle_proc


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
    @mock.patch("sc_admin_portal.views.oracle_proc.insert_job", return_value=42)
    @mock.patch("sc_admin_portal.views.oracle_proc.list_recent_jobs", side_effect=RuntimeError("boom"))
    @mock.patch("sc_admin_portal.views.oracle_proc.list_pending_approvals", return_value=[])
    @mock.patch("sc_admin_portal.views.oracle_proc.list_recent_decisions", return_value=[])
    def test_submit_job_still_shows_success_on_refresh_error(
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
        content = response.content.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Job submitted.", content)
        self.assertIn("jobs refresh failed", content)
        self.assertNotIn("Could not submit the job", content)

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.list_recent_jobs", return_value=[])
    @mock.patch("sc_admin_portal.views.oracle_proc.list_recent_decisions", return_value=[])
    def test_approval_list_shows_attachment_link(self, decisions_mock, jobs_mock):
        self.client.force_login(self.authorized_user)
        with mock.patch(
            "sc_admin_portal.views.oracle_proc.list_pending_approvals",
            return_value=[
                {
                    "approval_id": 7,
                    "source_job_id": None,
                    "request_type": "PUBLISH_NEWS",
                    "title": "Title",
                    "created_at": None,
                    "summary": "Summary",
                    "file_name": "brief.pdf",
                    "file_mime": "application/pdf",
                    "proposed_text_preview": "Preview",
                    "details_preview": "",
                    "gemini_comments_preview": "",
                }
            ],
        ):
            response = self.client.get(self.portal_url)
        content = response.content.decode("utf-8")
        self.assertIn("brief.pdf", content)
        self.assertIn(reverse("sc_admin_portal:approval_file", args=[7]), content)
        self.assertIn(reverse("sc_admin_portal:approve", args=[7]), content)
        self.assertIn(reverse("sc_admin_portal:reject", args=[7]), content)

    def test_materialize_value_reads_lob(self):
        class FakeLob:
            def __init__(self):
                self.read_count = 0

            def read(self):
                self.read_count += 1
                return "payload"

        lob = FakeLob()
        self.assertEqual(oracle_proc._materialize_value(lob), "payload")
        self.assertEqual(lob.read_count, 1)

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.get_approval_file")
    def test_approval_file_requires_admin(self, get_file_mock):
        url = reverse("sc_admin_portal:approval_file", args=[8])
        response = self.client.get(url)
        self.assertEqual(response.status_code, 404)
        get_file_mock.assert_not_called()

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.get_approval_file")
    def test_approval_file_download(self, get_file_mock):
        self.client.force_login(self.authorized_user)
        get_file_mock.return_value = {
            "file_name": "note.txt",
            "file_mime": "text/plain",
            "file_blob": b"hello",
        }
        url = reverse("sc_admin_portal:approval_file", args=[9])
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/plain")
        self.assertIn("note.txt", response["Content-Disposition"])

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.decide_approval")
    def test_approve_requires_authorized_user(self, decide_mock):
        approve_url = reverse(self.approve_url_name, args=[123])
        response = self.client.post(approve_url, {"decision_notes": "ok"})
        self.assertEqual(response.status_code, 404)
        decide_mock.assert_not_called()

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.decide_approval")
    def test_reject_calls_oracle_update(self, decide_mock):
        self.client.force_login(self.authorized_user)
        decide_mock.return_value = 1
        reject_url = reverse(self.reject_url_name, args=[123])
        response = self.client.post(reject_url, {"decision_notes": "no"})
        self.assertEqual(response.status_code, 302)
        decide_mock.assert_called_once_with(
            approval_id=123,
            status="REJECTED",
            decided_by=ADMIN_EMAIL,
            decision_notes="no",
        )

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.decide_approval")
    def test_approve_calls_oracle_update(self, decide_mock):
        self.client.force_login(self.authorized_user)
        decide_mock.return_value = 1
        approve_url = reverse(self.approve_url_name, args=[456])
        response = self.client.post(approve_url, {"decision_notes": "yes"})
        self.assertEqual(response.status_code, 302)
        decide_mock.assert_called_once_with(
            approval_id=456,
            status="APPROVED",
            decided_by=ADMIN_EMAIL,
            decision_notes="yes",
        )

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    @mock.patch("sc_admin_portal.views.oracle_proc.decide_approval", return_value=0)
    def test_decision_already_decided_message(self, decide_mock):
        self.client.force_login(self.authorized_user)
        approve_url = reverse(self.approve_url_name, args=[999])
        response = self.client.post(approve_url, {"decision_notes": "ok"}, follow=True)
        content = response.content.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("already decided or not found", content)
