import json
from datetime import datetime, timezone

from django.conf import settings
from django.core.management import call_command
from django.contrib.sessions.middleware import SessionMiddleware
from django.http import HttpResponse
from types import SimpleNamespace

from django.test import RequestFactory, SimpleTestCase, TestCase, TransactionTestCase
from django.test.utils import override_settings
from core.analytics import ANON_COOKIE
from telemetry.consent import CONSENT_COOKIE, ConsentState, parse_consent_cookie, serialize_consent
from telemetry.middleware import TelemetryMiddleware
from telemetry.models import WebEvent, WebEventDaily
from unittest import mock

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
        self.assertEqual(event.is_bot, "N")
        self.assertIsNone(event.query_string)
        self.assertIsNone(event.payload_json)
        self.assertIsNone(event.user_agent)

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
        self.assertIsNone(event.session_key)
        self.assertIsNotNone(event.user_id)
        self.assertEqual(event.country_code, "US")
        self.assertEqual(event.region_code, "CA")
        self.assertIn(ANON_COOKIE, response.cookies)

    def test_existing_session_key_is_preserved(self):
        factory = RequestFactory()
        consent = ConsentState(
            analytics=True,
            functional=False,
            policy_version=settings.TELEMETRY_POLICY_VERSION,
            source="banner",
        )
        request = factory.get("/telemetry-test/")
        request.COOKIES[CONSENT_COOKIE] = serialize_consent(consent)
        session_middleware = SessionMiddleware(lambda req: None)
        session_middleware.process_request(request)
        request.session["auth_email"] = "user@example.com"
        request.session.save()
        existing_session_key = request.session.session_key

        response = HttpResponse("ok", content_type="text/html")
        middleware = TelemetryMiddleware(lambda _request: response)
        response = middleware(request)
        response = session_middleware.process_response(request, response)

        event = WebEvent.objects.get(path="/telemetry-test/")
        self.assertEqual(event.session_key, existing_session_key)
        self.assertIn("sessionid", response.cookies)

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
        self.assertEqual(event.country_code, "US")
        self.assertEqual(event.region_code, "CA")
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
        event = WebEvent.objects.get()
        self.assertEqual(event.event_name, "download_click")
        self.assertIsNone(event.payload_json)
        self.assertIsNone(event.debug_json)

    def test_event_endpoint_stores_host_not_full_referrer(self):
        consent = ConsentState(
            analytics=True,
            functional=False,
            policy_version=settings.TELEMETRY_POLICY_VERSION,
            source="banner",
        )
        self.client.cookies[CONSENT_COOKIE] = serialize_consent(consent)
        payload = json.dumps({"event_name": "download_click", "metadata": {"page": "https://sustainacore.org/"}})
        self.client.post(
            "/telemetry/event/",
            payload,
            content_type="application/json",
            HTTP_HOST="sustainacore.org",
            HTTP_ORIGIN="https://sustainacore.org",
            HTTP_REFERER="https://sustainacore.org/tech100/",
            HTTP_USER_AGENT="Mozilla/5.0",
        )
        event = WebEvent.objects.get()
        self.assertEqual(event.referrer, "sustainacore.org")
        self.assertEqual(event.referrer_host, "sustainacore.org")


class TestTelemetryAggregationCommand(TestCase):
    def test_aggregate_web_telemetry_groups_daily_counts(self):
        day = datetime(2026, 3, 5, 10, 0, tzinfo=timezone.utc)
        for session_key, user_id, ip_hash in (
            ("s1", 1, "ip-a"),
            ("s1", 1, "ip-a"),
            ("s2", 2, "ip-b"),
        ):
            WebEvent.objects.create(
                event_ts=day,
                event_type="page_view",
                event_name=None,
                path="/tech100/",
                referrer="www.google.com",
                referrer_host="www.google.com",
                user_agent="browser:chrome",
                is_bot="N",
                status_code=200,
                response_ms=120,
                session_key=session_key,
                user_id=user_id,
                ip_hash=ip_hash,
                consent_analytics_effective="Y",
            )

        call_command("aggregate_web_telemetry", "--date", "2026-03-05")

        aggregate = WebEventDaily.objects.get()
        self.assertEqual(aggregate.bucket_date.isoformat(), "2026-03-05")
        self.assertEqual(aggregate.event_count, 3)
        self.assertEqual(aggregate.unique_sessions, 2)
        self.assertEqual(aggregate.unique_users, 2)
        self.assertEqual(aggregate.unique_visitors, 2)
        self.assertEqual(aggregate.referrer_host, "www.google.com")
        self.assertEqual(aggregate.total_response_ms, 360)
        self.assertEqual(aggregate.max_response_ms, 120)


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

    @override_settings(TELEMETRY_GEOIP_ENABLED=True)
    def test_geoip_fallback_used_when_headers_missing(self):
        request = SimpleNamespace(META={"REMOTE_ADDR": "127.0.0.1"})
        with mock.patch("telemetry.utils._lookup_geoip_fields", return_value=("US", "CA")):
            country, region = get_geo_fields(request)
        self.assertEqual(country, "US")
        self.assertEqual(region, "CA")
