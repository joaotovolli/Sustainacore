import json

from django.conf import settings
from django.contrib.sessions.middleware import SessionMiddleware
from django.http import HttpResponse
from types import SimpleNamespace

from django.test import RequestFactory, SimpleTestCase, TestCase, TransactionTestCase
from django.test.utils import override_settings

from core.analytics import ANON_COOKIE
from telemetry.consent import CONSENT_COOKIE, ConsentState, parse_consent_cookie, serialize_consent
from telemetry.middleware import TelemetryMiddleware
from telemetry.models import WebEvent
from telemetry.utils import get_client_ip, get_geo_fields


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


class TestTelemetryIpSelection(SimpleTestCase):
    def test_prefers_public_ip_from_forwarded_chain(self):
        request = SimpleNamespace(
            META={
                "HTTP_X_FORWARDED_FOR": "10.0.0.1, 1.2.3.4",
                "REMOTE_ADDR": "127.0.0.1",
            }
        )
        self.assertEqual(get_client_ip(request), "1.2.3.4")

    def test_falls_back_to_real_ip_when_forwarded_private(self):
        request = SimpleNamespace(
            META={
                "HTTP_X_FORWARDED_FOR": "10.0.0.1, 192.168.1.5",
                "HTTP_X_REAL_IP": "1.2.3.4",
                "REMOTE_ADDR": "127.0.0.1",
            }
        )
        self.assertEqual(get_client_ip(request), "1.2.3.4")

    def test_falls_back_to_remote_addr_when_no_public(self):
        request = SimpleNamespace(
            META={
                "HTTP_X_FORWARDED_FOR": "10.0.0.1, 192.168.1.5",
                "HTTP_X_REAL_IP": "172.16.0.9",
                "REMOTE_ADDR": "127.0.0.1",
            }
        )
        self.assertEqual(get_client_ip(request), "127.0.0.1")


class TestTelemetryDebugHeaders(TestCase):
    def test_debug_headers_disabled_by_default(self):
        response = self.client.get("/telemetry/debug/headers/")
        self.assertEqual(response.status_code, 404)

    @override_settings(TELEMETRY_DEBUG_HEADERS=True)
    def test_debug_headers_returns_presence(self):
        response = self.client.get(
            "/telemetry/debug/headers/",
            REMOTE_ADDR="127.0.0.1",
            HTTP_X_REAL_IP="1.2.3.4",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("ip_headers", payload)
        self.assertTrue(payload["ip_headers"]["REMOTE_ADDR"])
        self.assertTrue(payload["ip_headers"]["HTTP_X_REAL_IP"])

    @override_settings(TELEMETRY_TRUST_X_FORWARDED_FOR=False)
    def test_ignores_forwarded_when_untrusted(self):
        request = SimpleNamespace(
            META={
                "HTTP_X_FORWARDED_FOR": "1.2.3.4",
                "HTTP_X_REAL_IP": "1.2.3.4",
                "REMOTE_ADDR": "127.0.0.1",
            }
        )
        self.assertEqual(get_client_ip(request), "127.0.0.1")


class TestTelemetryGeoParsing(SimpleTestCase):
    @override_settings(
        TELEMETRY_GEO_COUNTRY_HEADERS=["X-Country-Code"],
        TELEMETRY_GEO_REGION_HEADERS=["X-Region-Code"],
    )
    def test_geo_headers_normalize_and_split(self):
        request = SimpleNamespace(
            META={
                "HTTP_X_COUNTRY_CODE": " us, ca ",
                "HTTP_X_REGION_CODE": "  ny, us ",
            }
        )
        country, region = get_geo_fields(request)
        self.assertEqual(country, "US")
        self.assertEqual(region, "NY")
