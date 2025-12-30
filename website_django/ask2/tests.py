from __future__ import annotations

import json
from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase

from telemetry.models import WebAsk2Conversation, WebAsk2Message


class TestAsk2Telemetry(TestCase):
    def _post_ask2(self):
        return self.client.post(
            "/ask2/api/",
            '{"message": "Hello"}',
            content_type="application/json",
        )

    @mock.patch("ask2.views.client.ask2_query")
    def test_ask2_inserts_conversation_and_messages(self, ask2_query_mock):
        ask2_query_mock.return_value = {"reply": "Hi there"}
        response = self._post_ask2()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(WebAsk2Conversation.objects.count(), 1)
        self.assertEqual(WebAsk2Message.objects.count(), 2)
        roles = list(WebAsk2Message.objects.values_list("role", flat=True))
        self.assertIn("user", roles)
        self.assertIn("assistant", roles)

    @mock.patch("ask2.views.client.ask2_query")
    def test_authenticated_user_is_stored(self, ask2_query_mock):
        ask2_query_mock.return_value = {"reply": "Hi there"}
        user = get_user_model().objects.create_user(username="ask2user", password="pass")
        self.client.force_login(user)
        response = self._post_ask2()
        self.assertEqual(response.status_code, 200)
        conversation = WebAsk2Conversation.objects.first()
        self.assertEqual(conversation.user_id, user.id)

    @mock.patch("ask2.views.client.ask2_query")
    def test_content_truncation(self, ask2_query_mock):
        ask2_query_mock.return_value = {"reply": "R" * 25000}
        long_message = "X" * 25000
        response = self.client.post(
            "/ask2/api/",
            json.dumps({"message": long_message}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        msg = WebAsk2Message.objects.filter(role="user").first()
        self.assertEqual(msg.content_len, len(long_message))
        self.assertLessEqual(len(msg.content), 20000)

    @mock.patch("ask2.views.log_ask2_exchange")
    @mock.patch("ask2.views.client.ask2_query")
    def test_logging_failure_does_not_break_response(self, ask2_query_mock, log_mock):
        ask2_query_mock.return_value = {"reply": "Hi there"}
        log_mock.side_effect = RuntimeError("fail")
        response = self._post_ask2()
        self.assertEqual(response.status_code, 200)
