import os
from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from sc_admin_portal.models import SocialDraftPost


ADMIN_EMAIL = "admin@example.com"


class AdminPortalAccessTests(TestCase):
    def setUp(self):
        self.portal_url = reverse("sc_admin_portal:dashboard")
        self.approve_url_name = "sc_admin_portal:approve"
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
    def test_portal_returns_200_for_authorized_email(self):
        self.client.force_login(self.authorized_user)
        response = self.client.get(self.portal_url)
        self.assertEqual(response.status_code, 200)

    @mock.patch.dict(os.environ, {"SC_ADMIN_EMAIL": ADMIN_EMAIL})
    def test_approve_requires_authorized_user(self):
        draft = SocialDraftPost.objects.create(body_text="Draft body")
        approve_url = reverse(self.approve_url_name, args=[draft.id])

        response = self.client.post(approve_url)
        self.assertEqual(response.status_code, 404)
        draft.refresh_from_db()
        self.assertEqual(draft.status, SocialDraftPost.STATUS_DRAFT)
        self.assertIsNone(draft.approved_at)

        self.client.force_login(self.authorized_user)
        response = self.client.post(approve_url)
        self.assertEqual(response.status_code, 302)
        draft.refresh_from_db()
        self.assertEqual(draft.status, SocialDraftPost.STATUS_APPROVED)
        self.assertIsNotNone(draft.approved_at)
