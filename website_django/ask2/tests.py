from __future__ import annotations

from unittest import mock

from django.test import TestCase, override_settings


class TestAsk2Telemetry(TestCase):
    def _post_ask2(self):
        return self.client.post(
            "/ask2/api/",
            '{"message": "Hello"}',
            content_type="application/json",
        )

    @mock.patch("ask2.views.client.ask2_query")
    @mock.patch("ask2.views.record_event")
    def test_metadata_only_by_default(self, record_event_mock, ask2_query_mock):
        ask2_query_mock.return_value = {"reply": "Hi there"}
        response = self._post_ask2()
        self.assertEqual(response.status_code, 200)
        event_types = [call.kwargs.get("event_type") for call in record_event_mock.call_args_list]
        self.assertIn("ask2_chat", event_types)
        self.assertNotIn("ask2_message", event_types)

    @override_settings(ASK2_STORE_CONVERSATIONS=True)
    @mock.patch("ask2.views.client.ask2_query")
    @mock.patch("ask2.views.record_event")
    def test_message_events_when_enabled(self, record_event_mock, ask2_query_mock):
        ask2_query_mock.return_value = {"reply": "Hi there"}
        response = self._post_ask2()
        self.assertEqual(response.status_code, 200)
        event_types = [call.kwargs.get("event_type") for call in record_event_mock.call_args_list]
        self.assertIn("ask2_chat", event_types)
        self.assertGreaterEqual(event_types.count("ask2_message"), 2)
