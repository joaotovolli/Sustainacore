import json

from django.conf import settings
from django.contrib.sessions.middleware import SessionMiddleware
from django.http import HttpResponse
from django.test import RequestFactory, TestCase, TransactionTestCase

from core.analytics import ANON_COOKIE
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

    def test_parse_quoted_consent_cookie(self):
        consent = ConsentState(
            analytics=False,
            functional=True,
            policy_version=settings.TELEMETRY_POLICY_VERSION,
            source="banner",
        )
        raw = f"\"{serialize_consent(consent)}\""
        parsed = parse_consent_cookie(raw)
        self.assertIsNotNone(parsed)
        self.assertFalse(parsed.analytics)
        self.assertTrue(parsed.functional)


class TestTelemetryMiddleware(TransactionTestCase):
    def test_logs_page_view_event(self):
        factory = RequestFactory()
        request = factory.get("/telemetry-test/")
        response = HttpResponse("ok", content_type="text/html")
        middleware = TelemetryMiddleware(lambda _request: response)
        middleware(request)

        event = WebEvent.objects.get()
        self.assertEqual(event.event_type, "page_view")
        self.assertEqual(event.path, "/telemetry-test/")

    def test_session_user_id_and_geo_set_with_consent(self):
        factory = RequestFactory()
        consent = ConsentState(
            analytics=True,
            functional=False,
            policy_version=settings.TELEMETRY_POLICY_VERSION,
            source="banner",
        )
        request = factory.get(
            "/telemetry-test/",
            HTTP_X_COUNTRY_CODE="US",
            HTTP_X_REGION_CODE="CA",
        )
        request.COOKIES[CONSENT_COOKIE] = serialize_consent(consent)
        session_middleware = SessionMiddleware(lambda req: None)
        session_middleware.process_request(request)

        response = HttpResponse("ok", content_type="text/html")
        middleware = TelemetryMiddleware(lambda _request: response)
        response = middleware(request)
        response = session_middleware.process_response(request, response)

        event = WebEvent.objects.get()
        self.assertIsNotNone(event.session_key)
        self.assertIsNotNone(event.user_id)
        self.assertEqual(event.country_code, "US")
        self.assertEqual(event.region_code, "CA")
        self.assertIn("sessionid", response.cookies)
        self.assertIn(ANON_COOKIE, response.cookies)

    def test_identifiers_not_set_without_consent(self):
        factory = RequestFactory()
        request = factory.get(
            "/telemetry-test/",
            HTTP_X_COUNTRY_CODE="US",
            HTTP_X_REGION_CODE="CA",
        )
        session_middleware = SessionMiddleware(lambda req: None)
        session_middleware.process_request(request)

        response = HttpResponse("ok", content_type="text/html")
        middleware = TelemetryMiddleware(lambda _request: response)
        response = middleware(request)
        response = session_middleware.process_response(request, response)

        event = WebEvent.objects.get()
        self.assertIsNone(event.session_key)
        self.assertIsNone(event.user_id)
        self.assertIsNone(event.country_code)
        self.assertIsNone(event.region_code)
        self.assertNotIn("sessionid", response.cookies)
        self.assertNotIn(ANON_COOKIE, response.cookies)


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
