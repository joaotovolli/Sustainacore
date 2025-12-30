import json

from django.conf import settings
from django.http import HttpResponse
from django.test import RequestFactory, TestCase

from telemetry.consent import CONSENT_COOKIE, ConsentState, parse_consent_cookie, serialize_consent
from telemetry.middleware import TelemetryMiddleware
from telemetry.models import WebEvent


class TestConsentCookie(TestCase):
    def test_parse_consent_cookie(self):
        consent = ConsentState(
            analytics=True,
            functional=False,
            policy_version=settings.TELEMETRY_POLICY_VERSION,
            source="banner",
        )
        raw = serialize_consent(consent)
        parsed = parse_consent_cookie(raw)
        self.assertIsNotNone(parsed)
        self.assertTrue(parsed.analytics)
        self.assertFalse(parsed.functional)


class TestTelemetryMiddleware(TestCase):
    def test_logs_page_view_event(self):
        factory = RequestFactory()
        request = factory.get("/telemetry-test/")
        response = HttpResponse("ok", content_type="text/html")
        middleware = TelemetryMiddleware(lambda _request: response)
        middleware(request)

        event = WebEvent.objects.get()
        self.assertEqual(event.event_type, "page_view")
        self.assertEqual(event.path, "/telemetry-test/")


class TestTelemetryEventEndpoint(TestCase):
    def test_event_endpoint_ignored_without_consent(self):
        payload = json.dumps({"event_name": "download_click", "metadata": {"page": "/"}})
        response = self.client.post("/telemetry/event/", payload, content_type="application/json")
        self.assertEqual(response.status_code, 204)
        self.assertEqual(WebEvent.objects.count(), 0)

    def test_event_endpoint_records_with_consent(self):
        consent = ConsentState(
            analytics=True,
            functional=False,
            policy_version=settings.TELEMETRY_POLICY_VERSION,
            source="banner",
        )
        cookie_value = serialize_consent(consent)
        self.client.cookies[CONSENT_COOKIE] = cookie_value
        payload = json.dumps({"event_name": "download_click", "metadata": {"page": "/"}})
        response = self.client.post("/telemetry/event/", payload, content_type="application/json")
        self.assertEqual(response.status_code, 204)
        self.assertEqual(WebEvent.objects.count(), 1)
