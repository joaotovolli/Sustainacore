from datetime import datetime, date, timedelta
import html
import os
import uuid
import csv
import json
import re
import hashlib
from typing import Dict, Iterable, List, Optional, Tuple
import logging
import time
from urllib.parse import unquote, urlparse, urlencode
from pathlib import Path
import sys

import requests
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.staticfiles.storage import staticfiles_storage
from django.core.cache import cache
from django.http import FileResponse, HttpResponse, JsonResponse, Http404
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.cache import cache_page
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_POST

from core.api_client import create_news_item_admin, fetch_tech100
from core.news_data import NewsDataError, fetch_filter_options
from core.news_data import fetch_news_detail as fetch_news_detail_oracle
from core.news_data import fetch_news_list
from core.news_html import render_news_html, summarize_html
from core.news_snippets import build_news_snippet
from core.auth import apply_auth_cookie, clear_auth_cookie, is_logged_in
from core.analytics import EVENT_TYPES, log_event
from core.downloads import require_login_for_download
from core.profile_data import get_profile, upsert_profile
from core.terms_acceptance import (
    has_terms_acceptance,
    record_terms_acceptance_with_error,
)
from core.countries import get_country_lists, resolve_country_name
from core.tech100_index_data import (
    get_data_mode,
    get_attribution_table,
    get_index_levels,
    get_latest_trade_date,
    get_max_drawdown,
    get_quality_counts,
    get_return_between,
    get_rolling_vol,
    get_ytd_return,
)
from core.tech100_company_data import (
    METRIC_COLUMNS as TECH100_COMPANY_METRICS,
    get_company_bundle,
    get_company_list,
    get_company_history,
    get_company_series,
    get_company_summary,
)
from ai_reg import data as ai_reg_data
from core import sitemaps
from sc_admin_portal.news_storage import get_news_asset
from telemetry.consent import get_consent_from_request
from telemetry.logger import record_event


logger = logging.getLogger(__name__)
TECH100_PREVIEW_LIMIT = 25

PAGE_CACHE_SECONDS = 60


def _backend_url(path: str) -> str:
    base = settings.SUSTAINACORE_BACKEND_URL.rstrip("/")
    return f"{base}{path}"


def _normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def _mask_email(value: str) -> str:
    if not value or "@" not in value:
        return "***"
    name, domain = value.split("@", 1)
    if len(name) <= 1:
        masked = "*"
    else:
        masked = f"{name[0]}***"
    return f"{masked}@{domain}"


def _client_ip(request) -> str:
    forwarded = request.META.get("HTTP_X_FORWARDED_FOR")
    if forwarded:
        return forwarded.split(",", 1)[0].strip()
    return request.META.get("REMOTE_ADDR", "") or ""


def _redact_email(value: str) -> str:
    if not value or "@" not in value:
        return "***"
    name, domain = value.split("@", 1)
    prefix = name[:2] if len(name) >= 2 else name[:1]
    return f"{prefix}***@{domain}"


def _email_hash(value: str) -> str:
    if not value:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _log_login_code_event(
    correlation_id: str,
    email: str,
    step: str,
    outcome: str,
    error_class: Optional[str] = None,
    backend_status: Optional[int] = None,
    backend_url: Optional[str] = None,
    backend_body: Optional[str] = None,
) -> None:
    trimmed_body = (backend_body or "").strip()
    if len(trimmed_body) > 200:
        trimmed_body = trimmed_body[:200]
    logger.warning(
        "[LOGIN_CODE] corr_id=%s email_hash=%s email_redacted=%s step=%s outcome=%s error_class=%s backend_status=%s backend_url=%s backend_body=%s timestamp=%s",
        correlation_id,
        _email_hash(email),
        _redact_email(email),
        step,
        outcome,
        error_class or "",
        backend_status if backend_status is not None else "",
        backend_url or "",
        trimmed_body,
        datetime.utcnow().isoformat(),
    )


def _news_fixture_items() -> List[Dict[str, object]]:
    return [
        {
            "id": "NEWS_ITEMS:99",
            "title": "Responsible AI governance updates for Q3",
            "summary": "A concise overview of the quarter’s policy signals, disclosure trends, and board-level oversight moves.",
            "source": "SustainaCore",
            "published_at": "2025-01-02",
            "tags": ["Governance", "Policy"],
            "has_full_body": True,
        },
        {
            "id": "NEWS_ITEMS:44",
            "title": "Transparency report highlights from leading firms",
            "summary": "Key themes across transparency reports including audit scope, model risk tracking, and incident response.",
            "source": "SustainaCore",
            "published_at": "2025-01-01",
            "tags": ["Transparency"],
            "has_full_body": True,
        },
        {
            "id": "NEWS_ITEMS:77",
            "title": "Regulatory signals shaping AI disclosure",
            "summary": "A snapshot of new regulatory guidance and what it means for public accountability commitments.",
            "source": "SustainaCore",
            "published_at": "2024-12-28",
            "tags": ["Regulation"],
            "has_full_body": True,
        },
    ]


def _news_fixture_detail(news_id: str) -> Optional[Dict[str, object]]:
    fixtures = {
        "NEWS_ITEMS:99": "This full article expands on governance updates, covering policy milestones, stakeholder engagement, and risk review practices in depth.",
        "NEWS_ITEMS:44": "This full article unpacks transparency report coverage including model risk controls, audit outcomes, and disclosure cadence.",
        "NEWS_ITEMS:77": "This full article reviews regulatory guidance, highlighting disclosure expectations and oversight requirements.",
    }
    body = fixtures.get(news_id)
    if not body:
        return None
    title = next((item["title"] for item in _news_fixture_items() if item["id"] == news_id), "News update")
    return {
        "id": news_id,
        "title": title,
        "summary": body[:200],
        "full_text": body,
        "source": "SustainaCore",
        "published_at": "2025-01-02T12:30:00Z",
        "url": "",
        "tags": ["Governance"],
        "categories": [],
        "pillar_tags": [],
        "ticker": None,
        "has_full_body": True,
    }


def _backend_host() -> str:
    base = settings.SUSTAINACORE_BACKEND_URL.rstrip("/")
    parsed = urlparse(base)
    return parsed.netloc or parsed.path or base


def login_email(request):
    notice = request.session.pop("login_notice", "")
    error = ""
    next_url = request.GET.get("next") or request.POST.get("next") or ""
    if request.method == "POST":
        email = _normalize_email(request.POST.get("email", ""))
        accepted = request.POST.get("terms_accept") in {"on", "true", "1"}
        correlation_id = uuid.uuid4().hex
        if not accepted:
            error = "You must agree to the Terms and Privacy Policy to receive a login code."
            return render(
                request,
                "login_email.html",
                {"notice": notice, "error": error, "next": next_url},
            )
        if email:
            recorded, acceptance_error = record_terms_acceptance_with_error(
                email, request, "request_code"
            )
            _log_login_code_event(
                correlation_id,
                email,
                "acceptance_insert",
                "ok" if recorded else "fail",
                acceptance_error,
            )
            if not recorded:
                error = "We could not record your acceptance. Please try again."
                return render(
                    request,
                    "login_email.html",
                    {"notice": notice, "error": error, "next": next_url},
                )
            payload = {"email": email}
            headers = {"X-Request-ID": correlation_id}
            client_ip = _client_ip(request)
            if client_ip:
                headers["X-Forwarded-For"] = client_ip
            start = time.monotonic()
            try:
                response = requests.post(
                    _backend_url("/api/auth/request-code"),
                    json=payload,
                    headers=headers,
                    timeout=settings.SUSTAINACORE_BACKEND_TIMEOUT,
                )
                duration_ms = int((time.monotonic() - start) * 1000)
                logger.warning(
                    "auth.request_code backend=%s email=%s status=%s duration_ms=%s",
                    _backend_host(),
                    _mask_email(email),
                    response.status_code,
                    duration_ms,
                )
                log_event("auth_request_code", request, {"source": "login_page"})
                _log_login_code_event(
                    correlation_id,
                    email,
                    "email_send",
                    "ok" if response.ok else "fail",
                    None if response.ok else "BackendHTTPError",
                    response.status_code,
                    _backend_url("/api/auth/request-code"),
                    response.text,
                )
                data = None
                try:
                    data = response.json()
                except ValueError:
                    data = None
                if not response.ok or (isinstance(data, dict) and data.get("ok") is False):
                    error = "We couldn't send the email right now. Please try again later."
                    return render(
                        request,
                        "login_email.html",
                        {"notice": notice, "error": error, "next": next_url},
                    )
            except requests.RequestException as exc:
                duration_ms = int((time.monotonic() - start) * 1000)
                logger.warning(
                    "auth.request_code backend=%s email=%s error=%s duration_ms=%s",
                    _backend_host(),
                    _mask_email(email),
                    type(exc).__name__,
                    duration_ms,
                )
                log_event("auth_request_code", request, {"source": "login_page", "status": "error"})
                _log_login_code_event(
                    correlation_id,
                    email,
                    "email_send",
                    "fail",
                    type(exc).__name__,
                    None,
                    _backend_url("/api/auth/request-code"),
                )
                error = "We couldn't send the email right now. Please try again later."
                return render(
                    request,
                    "login_email.html",
                    {"notice": notice, "error": error, "next": next_url},
                )
        request.session["login_email"] = email
        request.session["login_notice"] = "If that email is eligible, we sent a code."
        if next_url:
            return redirect(f"{reverse('login_code')}?{urlencode({'next': next_url})}")
        return redirect("login_code")
    return render(
        request,
        "login_email.html",
        {"notice": notice, "error": error, "next": next_url},
    )


def login_code(request):
    email = request.session.get("login_email", "")
    if not email:
        return redirect("login")
    notice = request.session.pop("login_notice", "")
    error = ""
    next_url = request.GET.get("next") or request.POST.get("next") or ""
    if request.method == "POST":
        if not has_terms_acceptance(email):
            error = "Please accept the Terms and Privacy Policy before signing in."
            return render(
                request,
                "login_code.html",
                {"email": email, "error": error, "notice": notice, "next": next_url},
            )
        code = (request.POST.get("code") or "").strip()
        payload = {"email": email, "code": code}
        try:
            resp = requests.post(
                _backend_url("/api/auth/verify-code"),
                json=payload,
                timeout=settings.SUSTAINACORE_BACKEND_TIMEOUT,
            )
            if resp.status_code == 200:
                data = resp.json() or {}
                token = data.get("token")
                expires = data.get("expires_in_seconds")
                if token:
                    request.session["auth_email"] = email
                    messages.success(request, "Login successful.")
                    target = "/"
                    if next_url and url_has_allowed_host_and_scheme(
                        next_url,
                        allowed_hosts={request.get_host()},
                        require_https=request.is_secure(),
                    ):
                        target = next_url
                    response = redirect(target)
                    apply_auth_cookie(response, token, expires)
                    log_event("auth_verify_ok", request, {"source": "login_page"})
                    return response
            error = "Invalid or expired code. Please try again."
            log_event("auth_verify_fail", request, {"source": "login_page"})
        except requests.RequestException:
            error = "We could not verify the code right now. Please try again."
            log_event("auth_verify_fail", request, {"source": "login_page", "status": "error"})
    return render(
        request,
        "login_code.html",
        {"email": email, "error": error, "notice": notice, "next": next_url},
    )


@require_POST
def auth_request_code(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {}
    email = _normalize_email(payload.get("email", ""))
    accepted = bool(payload.get("terms_accepted"))
    correlation_id = uuid.uuid4().hex
    if not email:
        return JsonResponse({"ok": False, "error": "invalid"}, status=400)
    if not accepted:
        return JsonResponse({"ok": False, "error": "terms_required"}, status=400)
    recorded, acceptance_error = record_terms_acceptance_with_error(
        email, request, "request_code"
    )
    _log_login_code_event(
        correlation_id,
        email,
        "acceptance_insert",
        "ok" if recorded else "fail",
        acceptance_error,
    )
    if not recorded:
        return JsonResponse(
            {
                "ok": False,
                "error": "terms_acceptance_failed",
                "message": "We could not record your acceptance. Please try again.",
            },
            status=502,
        )
    if email:
        headers = {"X-Request-ID": correlation_id}
        client_ip = _client_ip(request)
        if client_ip:
            headers["X-Forwarded-For"] = client_ip
        start = time.monotonic()
        try:
            response = requests.post(
                _backend_url("/api/auth/request-code"),
                json={"email": email},
                headers=headers,
                timeout=settings.SUSTAINACORE_BACKEND_TIMEOUT,
            )
            duration_ms = int((time.monotonic() - start) * 1000)
            logger.warning(
                "auth.request_code backend=%s email=%s status=%s duration_ms=%s",
                _backend_host(),
                _mask_email(email),
                response.status_code,
                duration_ms,
            )
            log_event("auth_request_code", request, {"source": "modal"})
            _log_login_code_event(
                correlation_id,
                email,
                "email_send",
                "ok" if response.ok else "fail",
                None if response.ok else "BackendHTTPError",
                response.status_code,
                _backend_url("/api/auth/request-code"),
                response.text,
            )
            data = None
            try:
                data = response.json()
            except ValueError:
                data = None
            if not response.ok or (isinstance(data, dict) and data.get("ok") is False):
                error_code = None
                error_message = None
                if isinstance(data, dict):
                    error_code = data.get("error")
                    error_message = data.get("message")
                return JsonResponse(
                    {
                        "ok": False,
                        "error": error_code or "send_failed",
                        "message": error_message
                        or "We couldn't send the email right now. Please try again later.",
                    },
                    status=response.status_code if response.status_code >= 400 else 502,
                )
        except requests.RequestException as exc:
            duration_ms = int((time.monotonic() - start) * 1000)
            logger.warning(
                "auth.request_code backend=%s email=%s error=%s duration_ms=%s",
                _backend_host(),
                _mask_email(email),
                type(exc).__name__,
                duration_ms,
            )
            log_event("auth_request_code", request, {"source": "modal", "status": "error"})
            _log_login_code_event(
                correlation_id,
                email,
                "email_send",
                "fail",
                type(exc).__name__,
                None,
                _backend_url("/api/auth/request-code"),
            )
            return JsonResponse(
                {
                    "ok": False,
                    "error": "send_failed",
                    "message": "We couldn't send the email right now. Please try again later.",
                },
                status=502,
            )
    request.session["login_email"] = email
    return JsonResponse({"ok": True})


@require_POST
def auth_verify_code(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {}
    email = _normalize_email(payload.get("email", ""))
    code = (payload.get("code") or "").strip()
    if not email or not code:
        log_event("auth_verify_fail", request, {"source": "modal", "status": "missing"})
        return JsonResponse({"ok": False, "error": "invalid"}, status=400)
    if not has_terms_acceptance(email):
        log_event("auth_verify_fail", request, {"source": "modal", "status": "terms_required"})
        return JsonResponse({"ok": False, "error": "terms_required"}, status=403)
    try:
        resp = requests.post(
            _backend_url("/api/auth/verify-code"),
            json={"email": email, "code": code},
            timeout=settings.SUSTAINACORE_BACKEND_TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json() or {}
            token = data.get("token")
            expires = data.get("expires_in_seconds")
            if token:
                request.session["auth_email"] = email
                response = JsonResponse({"ok": True})
                apply_auth_cookie(response, token, expires)
                log_event("auth_verify_ok", request, {"source": "modal"})
                return response
        log_event("auth_verify_fail", request, {"source": "modal"})
        return JsonResponse({"ok": False, "error": "invalid"}, status=400)
    except requests.RequestException:
        log_event("auth_verify_fail", request, {"source": "modal", "status": "error"})
        return JsonResponse({"ok": False, "error": "error"}, status=502)


@require_POST
def ux_event(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {}
    event_type = payload.get("event_type")
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    if event_type not in EVENT_TYPES:
        return JsonResponse({"ok": False}, status=400)
    consent = get_consent_from_request(request)
    if not consent.analytics:
        return JsonResponse({"ok": True}, status=204)
    log_event(event_type, request, metadata)
    try:
        record_event(
            event_type="ui_event",
            request=request,
            consent=consent,
            path=request.path,
            query_string=None,
            http_method=request.method,
            status_code=204,
            payload={"event_name": event_type, "metadata": metadata},
        )
    except Exception:
        pass
    return JsonResponse({"ok": True})


@csrf_protect
@require_POST
def logout(request):
    request.session.pop("login_email", None)
    request.session.pop("login_notice", None)
    request.session.pop("auth_email", None)
    request.session.pop("auth_state", None)
    response = redirect("home")
    clear_auth_cookie(response)
    return response


def account(request):
    if not is_logged_in(request):
        return redirect(f"{reverse('login')}?next={reverse('account')}")

    email = (request.session.get("auth_email") or "").strip()
    missing_email = not email
    profile = None
    error = ""
    profile_empty = False
    if email:
        try:
            profile = get_profile(email)
            if profile:
                profile_empty = not any(
                    (profile.get("name"), profile.get("country"), profile.get("company"), profile.get("phone"))
                )
            else:
                profile_empty = True
        except Exception as exc:
            logger.warning("account.profile_load_failed", exc_info=exc)
            error = "We could not load your profile details right now."

    if request.method == "POST":
        if not email:
            return redirect(f"{reverse('login')}?next={reverse('account')}")
        name = (request.POST.get("name") or "").strip()
        country = resolve_country_name((request.POST.get("country") or "").strip())
        company = (request.POST.get("company") or "").strip()
        phone = (request.POST.get("phone") or "").strip()
        try:
            upsert_profile(email, name, country, company, phone)
            messages.success(request, "Profile saved.")
            profile = {
                "name": name,
                "country": country,
                "company": company,
                "phone": phone,
            }
            profile_empty = not any((name, country, company, phone))
        except Exception as exc:
            logger.warning("account.profile_save_failed", exc_info=exc)
            error = "We could not save your profile right now."

    return render(
        request,
        "account.html",
        {
            "profile": profile
            or {"name": "", "country": "", "company": "", "phone": ""},
            "missing_email": missing_email,
            "error": error,
            "profile_empty": profile_empty,
            "country_lists": get_country_lists(),
        },
    )


def _format_port_weight(value) -> str:
    if value is None or value == "":
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    decimals = 1 if abs(num) >= 10 else 2
    return f"{num:.{decimals}f}%"


def _format_port_weight_whole(value) -> str:
    if value in (None, ""):
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    rounded = int(round(num))
    return f"{rounded}%"


def _format_score(value) -> str:
    if value is None or value == "":
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    formatted = f"{num:.2f}".rstrip("0").rstrip(".")
    return formatted


def _port_date_to_string(value) -> str:
    parsed = _parse_port_date(value)
    if parsed:
        return parsed.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "").strip()


def _format_port_date_display(value) -> str:
    parsed = _parse_port_date(value)
    if parsed:
        return parsed.strftime("%b-%Y")
    if isinstance(value, (datetime, date)):
        return datetime.combine(value, datetime.min.time()).strftime("%b-%Y")
    text = str(value or "").strip()
    return text


def _filter_companies(companies: Iterable[Dict], filters: Dict[str, str]) -> List[Dict]:
    filtered: List[Dict] = []
    port_date_filter = (filters.get("port_date") or "").strip()
    sector_filter = (filters.get("sector") or "").strip()
    search_term = (filters.get("q") or filters.get("search") or "").lower().strip()

    for company in companies:
        port_date_value = _port_date_to_string(
            company.get("port_date")
            or company.get("port_date_str")
            or company.get("updated_at")
            or company.get("as_of_date")
        )
        if port_date_filter and port_date_value != port_date_filter:
            continue

        sector_value = str(company.get("sector") or company.get("gics_sector") or "").strip()
        if sector_filter and sector_value != sector_filter:
            continue

        if search_term:
            company_name = (company.get("company_name") or "").lower()
            ticker = (company.get("ticker") or "").lower()
            if search_term not in company_name and search_term not in ticker:
                continue

        filtered.append(company)

    return filtered


def _parse_news_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _parse_news_list(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        candidates = value
    else:
        candidates = str(value).replace(";", ",").split(",")
    parsed: List[str] = []
    for candidate in candidates:
        text = str(candidate).strip()
        if text:
            parsed.append(text)
    return parsed


def _split_news_paragraphs(text: str) -> List[str]:
    cleaned = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not cleaned:
        return []
    chunks = [chunk.strip() for chunk in re.split(r"\n{2,}", cleaned) if chunk.strip()]
    if len(chunks) == 1:
        chunks = [chunk.strip() for chunk in cleaned.split("\n") if chunk.strip()]
    return chunks


def _resolve_news_full_text(item: Dict[str, object]) -> Tuple[str, str]:
    """Return the best available article body and the chosen source key."""
    candidates = [
        "full_text",
        "content",
        "article_text",
        "body",
        "text",
        "summary",
    ]
    values: Dict[str, str] = {}
    for key in candidates:
        value = item.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        values[key] = text

    if not values:
        return "", ""

    best_key = None
    for key in candidates:
        if key in values:
            best_key = key
            break

    best_key = best_key or next(iter(values.keys()))
    best_text = values[best_key]

    longest_key = max(values, key=lambda k: len(values[k]))
    longest_text = values[longest_key]

    if best_key == "summary" and len(longest_text) >= max(len(best_text) * 3, 800):
        return longest_text, longest_key

    if len(best_text) < 800 and len(longest_text) > len(best_text):
        return longest_text, longest_key

    return best_text, best_key


def _is_external_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    if host.endswith("sustainacore.org"):
        return False
    return True


def _fetch_news_detail_from_oracle(item_key: str) -> Optional[Dict[str, object]]:
    if not item_key:
        return None
    table = None
    raw_id = item_key
    if ":" in item_key:
        table, raw_id = item_key.split(":", 1)
        table = table.strip().upper() or None
    raw_id = (raw_id or "").strip()
    if not raw_id:
        return None
    try:
        item_id: object = int(raw_id)
    except ValueError:
        item_id = raw_id

    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.append(str(repo_root))

    try:
        import db_helper
    except Exception as exc:
        logger.warning("Oracle helper import failed: %s", exc)
        return None

    sql_enriched = (
        "SELECT e.item_table, e.item_id, e.dt_pub, e.ticker, e.title, e.url, "
        "e.source_name, e.body, a.full_text, e.pillar_tags, e.categories, e.tags, e.tickers "
        "FROM v_news_enriched e "
        "LEFT JOIN v_news_all a ON a.item_table = e.item_table AND a.item_id = e.item_id "
        "WHERE e.item_id = :item_id "
    )
    sql_recent = (
        "SELECT r.item_table, r.item_id, r.dt_pub, r.ticker, r.title, r.url, "
        "r.source_name, r.body, a.full_text, r.pillar_tags, r.categories, r.tags, r.tickers "
        "FROM v_news_recent r "
        "LEFT JOIN v_news_all a ON a.item_table = r.item_table AND a.item_id = r.item_id "
        "WHERE r.item_id = :item_id "
    )
    binds: Dict[str, object] = {"item_id": item_id}
    if table:
        sql_enriched += "AND e.item_table = :item_table "
        sql_recent += "AND r.item_table = :item_table "
        binds["item_table"] = table

    try:
        with db_helper.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql_enriched, binds)
            row = cur.fetchone()
            if not row:
                cur.execute(sql_recent, binds)
                row = cur.fetchone()
            if not row:
                return None
            (
                item_table,
                item_id_value,
                dt_pub,
                ticker_single,
                title,
                url,
                source_name,
                body,
                full_text,
                pillar_tags,
                categories,
                tags_raw,
                tickers_raw,
            ) = [db_helper._to_plain(val) for val in row]
    except Exception as exc:
        logger.warning("Oracle news detail fetch failed: %s", exc)
        return None

    def _parse_oracle_list(value: object) -> List[str]:
        return _parse_news_list(value)

    parsed_tags = _parse_oracle_list(tags_raw)
    parsed_categories = _parse_oracle_list(categories)
    parsed_pillar_tags = _parse_oracle_list(pillar_tags)
    parsed_tickers = _parse_oracle_list(tickers_raw)

    ticker_value = ", ".join(parsed_tickers) if parsed_tickers else (
        ticker_single or tickers_raw or ""
    )

    item_id_str = None
    if item_table and item_id_value:
        item_id_str = f"{item_table}:{item_id_value}"
    elif item_id_value is not None:
        item_id_str = str(item_id_value)

    full_body = full_text or body or None
    summary_value = body or full_body or ""

    return {
        "id": item_id_str,
        "title": title,
        "source": source_name,
        "url": url,
        "summary": summary_value,
        "body": full_body,
        "tags": parsed_tags,
        "categories": parsed_categories,
        "pillar_tags": parsed_pillar_tags,
        "ticker": ticker_value or None,
        "published_at": dt_pub,
        "has_full_body": bool(full_body),
    }


def _parse_port_date(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    text = str(value).strip()
    if not text:
        return None
    for candidate in (text, text.replace("Z", ""), text.split("T")[0]):
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo:
                parsed = parsed.replace(tzinfo=None)
            return parsed
        except (TypeError, ValueError):
            continue
    return None


def _safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_weight_value(value, default=None):
    """Parse weight values that may arrive as raw numbers or strings with a % suffix."""
    if isinstance(value, str):
        candidate = value.strip().rstrip("%").strip()
    else:
        candidate = value
    return _safe_float(candidate, default)


def _lower_key_map(row: Dict) -> Dict[str, any]:
    return {str(key).lower(): value for key, value in row.items()}


def _get_value(row_map: Dict[str, any], keys) -> any:
    for key in keys:
        if key is None:
            continue
        candidate = row_map.get(str(key).lower())
        if candidate in ("", None):
            continue
        if isinstance(candidate, str):
            candidate_stripped = candidate.strip()
            if candidate_stripped == "":
                continue
            return candidate_stripped
        return candidate
    return None


def _company_history_key(company: Dict) -> str:
    ticker = str(company.get("ticker") or "").strip().lower()
    name = str(company.get("company_name") or company.get("company") or "").strip().lower()
    if ticker and name:
        return f"{ticker}::{name}"
    if ticker:
        return ticker
    if name:
        return name
    return ""


def _build_company_history(companies: Iterable[Dict]) -> Dict[str, List[Dict]]:
    history: Dict[str, List[Dict]] = {}
    for company in companies:
        key = _company_history_key(company)
        if not key:
            continue
        history.setdefault(key, []).append(dict(company))

    def _history_sort(item: Dict):
        parsed_date = _parse_port_date(item.get("port_date"))
        return parsed_date or datetime.min

    for entries in history.values():
        entries.sort(key=_history_sort, reverse=True)

    return history


def _extract_aiges_score(row: Dict):
    for key in (
        "aiges_composite",
        "aiges_composite_average",
        "aiges_composite_score",
        "aiges_composite_index",
        "aiges_score",
        "aiges",
        "overall",
    ):
        value = row.get(key)
        if value not in ("", None):
            return value
    return None


def _first_value(row: Dict, keys):
    for key in keys:
        value = row.get(key)
        if value not in ("", None):
            return value
    return None


def _assign_rank_indexes(rows: List[Dict]) -> None:
    """Backfill rank_index per port_date based on AIGES composite if backend omits rank."""
    per_date: Dict[str, List[Dict]] = {}
    for row in rows:
        date_key = row.get("port_date_str") or _port_date_to_string(row.get("port_date")) or "undated"
        score = _safe_float(row.get("aiges_composite"))
        per_date.setdefault(date_key, []).append({"score": score, "row": row})

    for date_key, items in per_date.items():
        items.sort(key=lambda item: (-_safe_float(item["score"], float("-inf")), str(item["row"].get("company_name") or "")))
        for idx, item in enumerate(items, start=1):
            row = item["row"]
            if row.get("rank_index") in ("", None):
                row["rank_index"] = idx


def _normalize_row(raw: Dict) -> Dict:
    # Normalize varied API field names into a canonical TECH100 schema.
    row = dict(raw)
    lower_map = _lower_key_map(row)

    row["company_name"] = _get_value(lower_map, ["company_name", "company"])
    row["ticker"] = _get_value(lower_map, ["ticker", "symbol"])

    sector_value = _get_value(lower_map, ["gics_sector", "gics_sector_name", "sector", "industry_group"])
    row["gics_sector"] = sector_value
    row["sector"] = sector_value or row.get("sector")

    raw_port_date = _get_value(lower_map, ["port_date", "rebalance_date", "as_of_date", "updated_at", "dt", "date"])
    parsed_port_date = _parse_port_date(raw_port_date)
    row["port_date"] = parsed_port_date
    row["port_date_str"] = parsed_port_date.date().isoformat() if parsed_port_date else _port_date_to_string(raw_port_date)

    rank_val = _get_value(lower_map, ["rank_index", "rank", "index_rank", "rnk"])
    row["rank_index"] = _safe_float(rank_val, rank_val)
    port_weight_value = _parse_weight_value(
        _get_value(
            lower_map,
            ["port_weight", "weight", "portfolio_weight", "index_weight", "port_wt", "weight_percent"],
        )
    )
    row["port_weight"] = port_weight_value
    row["weight"] = _parse_weight_value(_get_value(lower_map, ["weight"]), port_weight_value)

    row["transparency"] = _safe_float(
        _get_value(lower_map, ["transparency", "transparency_score", "trs"]), _get_value(lower_map, ["transparency"])
    )
    row["ethical_principles"] = _safe_float(
        _get_value(lower_map, ["ethical_principles", "ethics", "ethics_score", "ethical_score"]),
        _get_value(lower_map, ["ethical_principles"]),
    )
    row["governance_structure"] = _safe_float(
        _get_value(lower_map, ["governance_structure", "governance", "accountability", "accountability_score", "gov_score"])
    )
    row["regulatory_alignment"] = _safe_float(
        _get_value(lower_map, ["regulatory_alignment", "regulation_alignment", "regulatory", "regulation", "regulatory_score"])
    )
    row["stakeholder_engagement"] = _safe_float(
        _get_value(lower_map, ["stakeholder_engagement", "stakeholder", "stakeholder_score"])
    )
    row["aiges_composite"] = _safe_float(
        _get_value(
            lower_map,
            [
                "aiges_composite",
                "aiges_composite_average",
                "aiges_composite_score",
                "aiges_composite_index",
                "aiges_score",
                "aiges",
                "overall",
                "composite",
            ],
        )
    )
    row["aiges_composite_average"] = row["aiges_composite"]
    row["summary"] = _get_value(
        lower_map, ["summary", "company_summary", "aiges_summary", "overall_summary", "description"]
    )
    return row


@cache_page(PAGE_CACHE_SECONDS)
def home(request):
    def _format_number(value, decimals=2):
        if value is None:
            return None
        try:
            return f"{float(value):.{decimals}f}"
        except (TypeError, ValueError):
            return None

    def _format_percent(value):
        if value is None:
            return None
        try:
            pct = float(value) * 100.0
        except (TypeError, ValueError):
            return None
        abs_pct = abs(pct)
        if abs_pct < 0.005:
            decimals = 4
        elif abs_pct < 0.05:
            decimals = 3
        else:
            decimals = 2
        return f"{pct:.{decimals}f}%"

    try:
        tech100_response = fetch_tech100()
    except Exception:
        logger.exception("TECH100 preview unavailable for home.")
        tech100_response = {}

    try:
        news_response = fetch_news_list(limit=3, offset=0, date_range="all")
    except NewsDataError:
        logger.exception("News preview unavailable for home.")
        news_response = {}

    raw_tech100_items = tech100_response.get("items", []) or []
    tech100_items = [_normalize_row(item) for item in raw_tech100_items if isinstance(item, dict)]
    tech100_preview = []
    for item in tech100_items[:25]:
        name = item.get("company_name") or item.get("company") or ""
        ticker = item.get("ticker") or ""
        company_display = f"{name} ({ticker})" if name and ticker else (name or ticker or "—")
        tech100_preview.append(
            {
                "company": name or ticker or "—",
                "ticker": ticker or "",
                "company_display": company_display,
                "sector": item.get("sector") or item.get("gics_sector"),
                "transparency": _format_score(item.get("transparency")),
                "ethical_principles": _format_score(item.get("ethical_principles")),
                "governance_structure": _format_score(item.get("governance_structure")),
                "regulatory_alignment": _format_score(item.get("regulatory_alignment")),
                "stakeholder_engagement": _format_score(item.get("stakeholder_engagement")),
                "composite": _format_score(item.get("aiges_composite") or item.get("overall")),
            }
        )

    news_items = [item for item in (news_response.get("items", []) or []) if isinstance(item, dict)]
    news_items.sort(
        key=lambda item: _parse_news_datetime(item.get("published_at")) or datetime.min,
        reverse=True,
    )
    news_preview = []
    for item in news_items[:3]:
        if not isinstance(item, dict):
            continue
        item_id = item.get("id")
        has_full_body = item.get("has_full_body")
        external_url = item.get("url")
        if item_id and has_full_body:
            detail_url = reverse("news_detail", args=[item_id])
        elif external_url:
            detail_url = external_url
        else:
            detail_url = reverse("news")
        raw_text = (
            item.get("summary")
            or item.get("body")
            or item.get("content")
            or item.get("full_text")
            or ""
        )
        snippet = build_news_snippet(raw_text, max_len=260)
        if not snippet:
            snippet = "Open the full story for details."
        news_preview.append(
            {
                "id": item_id,
                "title": item.get("title"),
                "source": item.get("source"),
                "published_at": item.get("published_at"),
                "tags": item.get("tags") or [],
                "snippet": snippet,
                "detail_url": detail_url,
                "external": bool(external_url and not (item_id and has_full_body)),
            }
        )

    tech100_snapshot = {
        "has_data": False,
        "data_error": False,
        "data_mode": get_data_mode(),
        "levels": [],
        "levels_count": 0,
        "quality_counts": {},
    }
    if settings.SUSTAINACORE_ENV != "preview":
        try:
            latest_date = get_latest_trade_date()
            if latest_date:
                start_date = latest_date - timedelta(days=90)
                levels = get_index_levels(start_date, latest_date)
                stats = get_quality_counts(latest_date)
                month_start = date(latest_date.year, latest_date.month, 1)
                ytd_start = date(latest_date.year, 1, 1)
                drawdown_ytd = get_max_drawdown(ytd_start, latest_date)
                latest_level = levels[-1][1] if levels else None
                ret_1d = None
                if len(levels) >= 2:
                    prev_level = levels[-2][1]
                    if prev_level not in (None, 0) and latest_level is not None:
                        ret_1d = (latest_level / prev_level) - 1.0
                ret_mtd = get_return_between(latest_date, month_start)
                ret_ytd, _ = get_ytd_return(latest_date)
                vol_30d = get_rolling_vol(latest_date, window=30)
                ret_ytd_display = _format_percent(ret_ytd)
                tech100_snapshot = {
                    "has_data": True,
                    "data_error": False,
                    "as_of": latest_date.isoformat(),
                    "levels": [
                        {"date": trade_date.isoformat(), "level": level}
                        for trade_date, level in levels
                    ],
                    "latest_level": latest_level,
                    "latest_level_display": _format_number(latest_level),
                    "ret_1d": ret_1d,
                    "ret_1d_display": _format_percent(ret_1d),
                    "ret_mtd": ret_mtd,
                    "ret_mtd_display": _format_percent(ret_mtd),
                    "ret_ytd": ret_ytd,
                    "ret_ytd_display": ret_ytd_display,
                    "vol_30d": vol_30d,
                    "vol_30d_display": _format_percent(vol_30d),
                    "drawdown_ytd": drawdown_ytd.drawdown if drawdown_ytd else None,
                    "drawdown_ytd_display": _format_percent(drawdown_ytd.drawdown if drawdown_ytd else None),
                    "quality_counts": stats,
                    "data_mode": get_data_mode(),
                    "levels_count": len(levels),
                }
        except Exception:
            logger.exception("TECH100 snapshot unavailable for home.")
            tech100_snapshot = {
                "has_data": False,
                "data_error": True,
                "data_mode": get_data_mode(),
                "levels": [],
                "levels_count": 0,
                "quality_counts": {},
            }

    attribution_snapshot = {
        "has_data": False,
        "data_mode": get_data_mode(),
        "as_of": None,
        "ranges": {"1d": {"top": [], "worst": []}, "mtd": {"top": [], "worst": []}, "ytd": {"top": [], "worst": []}},
    }

    def _safe_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _build_contrib_rows(rows, key: str):
        items = []
        for row in rows:
            value = _safe_float(row.get(key))
            if value is None:
                continue
            items.append(
                {
                    "ticker": row.get("ticker"),
                    "name": row.get("name") or row.get("company_name") or row.get("ticker") or "—",
                    "value": value,
                    "value_display": _format_percent(value),
                }
            )
        items.sort(key=lambda item: item["value"], reverse=True)
        top = items[:5]
        worst = list(reversed(items[-5:])) if items else []
        return {"top": top, "worst": worst}

    try:
        if get_data_mode() == "fixture":
            as_of = date.today()
        else:
            as_of = get_latest_trade_date()
        if as_of:
            mtd_start = date(as_of.year, as_of.month, 1)
            ytd_start = date(as_of.year, 1, 1)
            attr_rows = get_attribution_table(as_of, mtd_start, ytd_start)
            attribution_snapshot = {
                "has_data": bool(attr_rows),
                "data_mode": get_data_mode(),
                "as_of": as_of.isoformat(),
                "ranges": {
                    "1d": _build_contrib_rows(attr_rows, "contribution"),
                    "mtd": _build_contrib_rows(attr_rows, "contrib_mtd"),
                    "ytd": _build_contrib_rows(attr_rows, "contrib_ytd"),
                },
            }
    except Exception:
        logger.exception("TECH100 attribution snapshot unavailable for home.")

    attribution_default = attribution_snapshot.get("ranges", {}).get("1d", {"top": [], "worst": []})

    try:
        as_of_dates = ai_reg_data.fetch_as_of_dates()
    except Exception:
        logger.exception("AI regulation dates unavailable for home.")
        as_of_dates = []

    context = {
        "year": datetime.now().year,
        "tech100_preview": tech100_preview,
        "news_preview": news_preview,
        "tech100_snapshot": tech100_snapshot,
        "attribution_snapshot": attribution_snapshot,
        "attribution_default": attribution_default,
        "as_of_dates": as_of_dates,
        "latest_as_of": as_of_dates[0] if as_of_dates else None,
    }
    try:
        return render(request, "home.html", context)
    except Exception:
        logger.exception("Home render failed")
        raise


def tech100(request):
    search_term = (request.GET.get("q") or request.GET.get("search") or "").strip()
    filters = {
        "port_date": (request.GET.get("port_date") or "").strip(),
        "sector": (request.GET.get("sector") or "").strip(),
        "q": search_term,
        "search": search_term,
    }

    tech100_response = fetch_tech100(
        port_date=filters["port_date"] or None,
        sector=filters["sector"] or None,
        search=search_term or None,
    )
    raw_companies = tech100_response.get("items", []) or []
    companies = [_normalize_row(c) for c in raw_companies if isinstance(c, dict)]
    _assign_rank_indexes(companies)

    def history_sort(item):
        parsed_date = item.get("port_date") or _parse_port_date(item.get("port_date_str"))
        return parsed_date or datetime.min

    def matches_filters(rows: List[Dict]) -> bool:
        if filters["port_date"]:
            if not any((_port_date_to_string(r.get("port_date") or r.get("port_date_str")) == filters["port_date"]) for r in rows):
                return False
        if filters["sector"]:
            if not any(((r.get("sector") or r.get("gics_sector")) == filters["sector"]) for r in rows):
                return False
        if filters["search"]:
            term = filters["search"].lower()
            row = rows[-1]
            name = (row.get("company_name") or "").lower()
            ticker = (row.get("ticker") or "").lower()
            if term not in name and term not in ticker:
                return False
        return True

    grouped_companies: Dict[str, List[Dict]] = {}
    for idx, row in enumerate(companies):
        key = _company_history_key(row) or f"row-{idx}"
        grouped_companies.setdefault(key, []).append(row)

    display_companies: List[Dict] = []
    for rows in grouped_companies.values():
        rows_sorted = sorted(rows, key=history_sort)
        if not matches_filters(rows_sorted):
            continue
        latest = rows_sorted[-1]
        summary_value = next((r.get("summary") for r in reversed(rows_sorted) if r.get("summary")), "")
        weight_value = _safe_float(latest.get("weight"), None)
        port_weight = _safe_float(latest.get("port_weight"), None)
        if weight_value is None:
            weight_value = port_weight
        formatted_port_weight = _format_port_weight(weight_value) or None
        formatted_port_weight_whole = _format_port_weight_whole(weight_value) or None
        port_date_display = _format_port_date_display(latest.get("port_date") or latest.get("port_date_str"))
        history_list = [
            {
                "port_date": entry.get("port_date"),
                "port_date_str": entry.get("port_date_str"),
                "transparency": entry.get("transparency"),
                "ethical_principles": entry.get("ethical_principles"),
                "governance_structure": entry.get("governance_structure"),
                "regulatory_alignment": entry.get("regulatory_alignment"),
                "stakeholder_engagement": entry.get("stakeholder_engagement"),
                "aiges_composite": entry.get("aiges_composite") or _extract_aiges_score(entry),
            }
            for entry in rows_sorted
        ]
        display_companies.append(
            {
                "company_name": latest.get("company_name"),
                "ticker": latest.get("ticker"),
                "port_date": latest.get("port_date"),
                "port_date_str": latest.get("port_date_str"),
                "port_date_display": port_date_display,
                "rank_index": latest.get("rank_index"),
                "gics_sector": latest.get("gics_sector"),
                "sector": latest.get("sector"),
                "summary": summary_value,
                "transparency": latest.get("transparency"),
                "ethical_principles": latest.get("ethical_principles"),
                "governance_structure": latest.get("governance_structure"),
                "regulatory_alignment": latest.get("regulatory_alignment"),
                "stakeholder_engagement": latest.get("stakeholder_engagement"),
                "aiges_composite": latest.get("aiges_composite") or _extract_aiges_score(latest),
                "port_weight": port_weight,
                "weight": weight_value,
                "port_weight_display": formatted_port_weight,
                "weight_display": formatted_port_weight,
                "weight_display_whole": formatted_port_weight_whole,
                "history": history_list,
            }
        )

    def company_sort(item: Dict):
        parsed_date = item.get("port_date") or _parse_port_date(item.get("port_date_str"))
        date_sort = -(parsed_date.timestamp()) if parsed_date else float("inf")
        rank_sort = _safe_float(item.get("rank_index"), float("inf"))
        name_sort = (item.get("company_name") or "").lower()
        return (date_sort, rank_sort, name_sort)

    display_companies = sorted(display_companies, key=company_sort)
    if display_companies:
        sample = display_companies[0]
        logger.info(
            "TECH100 sample company: name=%s ticker=%s port_date=%s rank=%s aiges=%s history_len=%s",
            sample.get("company_name"),
            sample.get("ticker"),
            sample.get("port_date_str") or sample.get("port_date"),
            sample.get("rank_index"),
            sample.get("aiges_composite"),
            len(sample.get("history") or []),
        )

    port_date_options = sorted(
        {item.get("port_date_str") for item in companies if item.get("port_date_str")}, reverse=True
    )
    sector_options = sorted({item.get("sector") for item in companies if item.get("sector")})

    total_count = len(display_companies)
    logged_in = is_logged_in(request)
    is_preview = not logged_in
    if is_preview:
        display_companies = display_companies[:TECH100_PREVIEW_LIMIT]
        log_event(
            "table_preview_rendered",
            request,
            {
                "page": "/tech100/",
                "port_date": filters.get("port_date", ""),
                "sector": filters.get("sector", ""),
                "q": filters.get("q", ""),
                "preview_limit": TECH100_PREVIEW_LIMIT,
            },
        )

    context = {
        "year": datetime.now().year,
        "companies": display_companies,
        "all_companies": display_companies if is_preview else companies,
        "tech100_error": tech100_response.get("error"),
        "port_date_options": port_date_options,
        "sector_options": sector_options,
        "filters": filters,
        "visible_count": len(display_companies),
        "total_count": total_count,
        "preview_limit": TECH100_PREVIEW_LIMIT,
        "is_preview": is_preview,
    }
    return render(request, "tech100.html", context)


@require_login_for_download
def tech100_export(request):
    search_term = (request.GET.get("q") or request.GET.get("search") or "").strip()
    filters = {
        "port_date": (request.GET.get("port_date") or "").strip(),
        "sector": (request.GET.get("sector") or "").strip(),
        "q": search_term,
        "search": search_term,
    }

    tech100_response = fetch_tech100()
    if tech100_response.get("error"):
        return HttpResponse("Unable to export TECH100 data right now.", status=502)

    companies = _filter_companies(tech100_response.get("items", []) or [], filters)

    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = "attachment; filename=tech100.csv"

    headers = [
        "RANK_INDEX",
        "WEIGHT",
        "COMPANY_NAME",
        "TICKER",
        "GICS_SECTOR",
        "AIGES_COMPOSITE_AVERAGE",
        "TRANSPARENCY",
        "ETHICAL_PRINCIPLES",
        "GOVERNANCE_STRUCTURE",
        "REGULATORY_ALIGNMENT",
        "STAKEHOLDER_ENGAGEMENT",
        "SUMMARY",
    ]

    writer = csv.writer(response)
    writer.writerow(headers)

    for company in companies:
        weight_value = _safe_float(company.get("weight"), None)
        if weight_value is None:
            weight_value = _safe_float(company.get("port_weight"), None)

        writer.writerow(
            [
                company.get("rank_index") or "",
                _format_port_weight(weight_value),
                company.get("company_name") or "",
                company.get("ticker") or "",
                (company.get("gics_sector") or company.get("sector") or ""),
                _format_score(company.get("aiges_composite_average")),
                _format_score(company.get("transparency")),
                _format_score(company.get("ethical_principles")),
                _format_score(company.get("governance_structure")),
                _format_score(company.get("regulatory_alignment")),
                _format_score(company.get("stakeholder_engagement")),
                company.get("summary") or "",
            ]
        )

    return response


def tech100_company(request, ticker: str):
    summary = get_company_summary(ticker)
    if not summary:
        raise Http404("Company not found.")

    latest_scores = summary.get("latest_scores") or {}
    latest_scores_display = {key: _format_score(value) for key, value in latest_scores.items()}

    context = {
        "year": datetime.now().year,
        "company_name": summary.get("company_name") or summary.get("ticker"),
        "ticker": summary.get("ticker"),
        "sector": summary.get("sector"),
        "latest_date": summary.get("latest_date"),
        "latest_rank": summary.get("latest_rank"),
        "latest_weight": summary.get("latest_weight"),
        "latest_scores": latest_scores,
        "latest_scores_display": latest_scores_display,
        "metric_options": list(TECH100_COMPANY_METRICS.keys()),
    }
    return render(request, "tech100_company.html", context)


def api_tech100_company_summary(request, ticker: str):
    summary = get_company_summary(ticker)
    if not summary:
        return JsonResponse({"error": "not_found"}, status=404)
    return JsonResponse(summary)


def api_tech100_company_series(request, ticker: str):
    metric = (request.GET.get("metric") or "composite").lower()
    baseline = (request.GET.get("baseline") or "top25_avg").lower()
    range_key = (request.GET.get("range") or "6m").lower()

    if baseline != "top25_avg":
        return JsonResponse({"error": "invalid_baseline"}, status=400)
    if metric not in TECH100_COMPANY_METRICS:
        return JsonResponse({"error": "invalid_metric"}, status=400)

    series = get_company_series(ticker, metric, range_key)
    if series is None:
        return JsonResponse({"error": "not_found"}, status=404)

    payload = {
        "ticker": (ticker or "").upper(),
        "metric": metric,
        "baseline": baseline,
        "range": range_key,
        "series": series,
    }
    return JsonResponse(payload)


def api_tech100_company_history(request, ticker: str):
    history = get_company_history(ticker)
    if history is None:
        return JsonResponse({"error": "not_found"}, status=404)
    return JsonResponse({"ticker": (ticker or "").upper(), "history": history})


def api_tech100_company_bundle(request, ticker: str):
    metric = (request.GET.get("metric") or "composite").lower()
    range_key = (request.GET.get("range") or "6m").lower()
    compare = (request.GET.get("compare") or "").strip()
    include_companies = (request.GET.get("companies") or "").strip() == "1"

    if metric not in TECH100_COMPANY_METRICS:
        return JsonResponse({"error": "invalid_metric"}, status=400)

    bundle = get_company_bundle(
        ticker=ticker,
        metric=metric,
        range_key=range_key,
        compare_ticker=compare or None,
        include_companies=include_companies,
    )
    if not bundle:
        return JsonResponse({"error": "not_found"}, status=404)
    bundle["ticker"] = (ticker or "").upper()
    bundle["compare"] = (compare or "").upper() if compare else None
    return JsonResponse(bundle)


def api_tech100_companies(request):
    return JsonResponse({"companies": get_company_list()})


@require_login_for_download
def tech100_company_download(request, ticker: str):
    history = get_company_history(ticker)
    if history is None:
        raise Http404("Company not found.")

    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = f"attachment; filename=tech100_{(ticker or '').upper()}.csv"

    headers = [
        "PORT_DATE",
        "RANK_INDEX",
        "WEIGHT",
        "COMPOSITE",
        "TRANSPARENCY",
        "ETHICAL_PRINCIPLES",
        "GOVERNANCE_STRUCTURE",
        "REGULATORY_ALIGNMENT",
        "STAKEHOLDER_ENGAGEMENT",
    ]
    writer = csv.writer(response)
    writer.writerow(headers)

    for row in history:
        writer.writerow(
            [
                row.get("date") or "",
                row.get("rank") or "",
                _format_port_weight(row.get("weight")),
                _format_score(row.get("composite")),
                _format_score(row.get("transparency")),
                _format_score(row.get("ethical_principles")),
                _format_score(row.get("governance_structure")),
                _format_score(row.get("regulatory_alignment")),
                _format_score(row.get("stakeholder_engagement")),
            ]
        )

    return response


def news(request):
    if os.getenv("NEWS_UI_DATA_MODE") == "fixture":
        news_items = _news_fixture_items()
        back_query = request.GET.urlencode()
        context = {
            "year": datetime.now().year,
            "articles": news_items,
            "news_error": None,
            "news_error_ref": None,
            "news_meta": {"count": len(news_items), "has_more": False},
            "news_json_ld": "",
            "filters": {"source": "", "tag": "", "ticker": "", "date_range": "all"},
            "source_options": ["SustainaCore"],
            "tag_options": sorted({tag for item in news_items for tag in item.get("tags", [])}),
            "source_supported": True,
            "tag_supported": True,
            "ticker_supported": False,
            "back_query": back_query,
            "page": 1,
            "prev_url": None,
            "next_url": None,
            "date_range_options": [
                {"value": "all", "label": "All time"},
                {"value": "7", "label": "Last 7 days"},
                {"value": "30", "label": "Last 30 days"},
                {"value": "90", "label": "Last 90 days"},
                {"value": "365", "label": "Last 12 months"},
            ],
        }
        return render(request, "news.html", context)
    raw_date_range = (request.GET.get("date_range") or "all").strip()
    date_range = raw_date_range if raw_date_range in {"all", "7", "30", "90"} else "all"
    filters = {
        "source": (request.GET.get("source") or "").strip(),
        "tag": (request.GET.get("tag") or "").strip(),
        "ticker": (request.GET.get("ticker") or "").strip(),
        "date_range": date_range,
    }

    date_mapping = {"7": 7, "30": 30, "90": 90, "365": 365}
    days = date_mapping.get(date_range)

    page_raw = request.GET.get("page", "1")
    try:
        page = max(1, int(page_raw))
    except (TypeError, ValueError):
        page = 1
    limit = 20
    offset = (page - 1) * limit

    news_error = None
    news_error_ref = None
    try:
        news_response = fetch_news_list(
            source=filters["source"] or None,
            tag=filters["tag"] or None,
            ticker=filters["ticker"] or None,
            days=days,
            limit=limit,
            offset=offset,
            date_range=date_range,
        )
    except NewsDataError:
        ref = f"news-{uuid.uuid4().hex[:8]}"
        news_error_ref = ref
        news_error = f"News is temporarily unavailable. Reference ID: {ref}"
        logger.exception("news_oracle_error list ref=%s", ref)
        log_event("news_list_error", request, metadata={"ref": ref})
        news_response = {"items": [], "meta": {}, "error": news_error}

    filter_options = {
        "source_options": [],
        "tag_options": [],
        "supports_source": False,
        "supports_tag": False,
        "supports_ticker": False,
    }
    try:
        filter_options = fetch_filter_options()
    except NewsDataError:
        ref = f"news-filter-{uuid.uuid4().hex[:8]}"
        logger.exception("news_oracle_error filters ref=%s", ref)

    news_items = news_response.get("items", [])
    for item in news_items:
        if "has_full_body" not in item:
            item_id = str(item.get("id") or "")
            item["has_full_body"] = item_id.startswith("ESG_NEWS:")
        summary_value = item.get("summary")
        if summary_value:
            summary_text = str(summary_value)
            if "&lt;" in summary_text or "&gt;" in summary_text:
                summary_text = html.unescape(summary_text)
            if re.search(r"<[a-zA-Z][^>]*>", summary_text):
                summary_text = summarize_html(summary_text)
            item["summary"] = summary_text
    sources = filter_options["source_options"] or sorted(
        {item.get("source") for item in news_items if item.get("source")}
    )
    tags = filter_options["tag_options"] or sorted(
        {tag for item in news_items for tag in item.get("tags", [])}
    )

    news_structured = []
    for item in news_items:
        headline = (item.get("title") or "").strip()
        url = (item.get("url") or "").strip()
        if not headline or not url:
            continue
        article = {
            "@context": "https://schema.org",
            "@type": "NewsArticle",
            "headline": headline,
            "url": url,
            "publisher": {
                "@type": "Organization",
                "name": (item.get("source") or "SustainaCore").strip(),
            },
            "datePublished": item.get("published_at") or None,
            "description": (item.get("summary") or "").strip() or None,
        }
        cleaned = {key: value for key, value in article.items() if value is not None}
        news_structured.append(cleaned)

    has_more = bool(news_response.get("meta", {}).get("has_more"))
    prev_url = None
    next_url = None
    if page > 1:
        prev_params = request.GET.copy()
        prev_params["page"] = page - 1
        prev_url = f"?{prev_params.urlencode()}"
    if has_more:
        next_params = request.GET.copy()
        next_params["page"] = page + 1
        next_url = f"?{next_params.urlencode()}"

    back_query = request.GET.urlencode()
    context = {
        "year": datetime.now().year,
        "articles": news_items,
        "news_error": news_error or news_response.get("error"),
        "news_error_ref": news_error_ref,
        "news_meta": news_response.get("meta", {}),
        "news_json_ld": json.dumps(news_structured, ensure_ascii=True) if news_structured else "",
        "filters": filters,
        "source_options": sources,
        "tag_options": tags,
        "source_supported": filter_options["supports_source"],
        "tag_supported": filter_options["supports_tag"],
        "ticker_supported": filter_options["supports_ticker"],
        "back_query": back_query,
        "page": page,
        "prev_url": prev_url,
        "next_url": next_url,
        "date_range_options": [
            {"value": "all", "label": "All time"},
            {"value": "7", "label": "Last 7 days"},
            {"value": "30", "label": "Last 30 days"},
            {"value": "90", "label": "Last 90 days"},
            {"value": "365", "label": "Last 12 months"},
        ],
    }
    return render(request, "news.html", context)


def news_detail(request, news_id: str):
    resolved_id = (news_id or "").strip()
    decoded_id = unquote(resolved_id) if resolved_id else resolved_id

    detail_response = {"item": None, "error": None}
    selected_item = None
    news_error_ref = None
    if os.getenv("NEWS_UI_DATA_MODE") == "fixture":
        selected_item = _news_fixture_detail(decoded_id or resolved_id)
        detail_response = {"item": selected_item, "error": None if selected_item else "News item not found."}
    else:
        try:
            detail_response = fetch_news_detail_oracle(news_id=decoded_id or resolved_id)
            selected_item = detail_response.get("item")
        except NewsDataError:
            ref = f"news-detail-{uuid.uuid4().hex[:8]}"
            news_error_ref = ref
            logger.exception("news_oracle_error detail ref=%s", ref)
            log_event("news_detail_error", request, metadata={"ref": ref, "news_id": decoded_id or resolved_id})
            detail_response = {"item": None, "error": f"News details are temporarily unavailable. Reference ID: {ref}"}

    published_at = _parse_news_datetime(selected_item.get("published_at")) if selected_item else None
    body_text = ""
    body_source = ""
    if selected_item:
        body_text, body_source = _resolve_news_full_text(selected_item)
    body_text = str(body_text) if body_text is not None else ""
    body_is_fallback = body_source in {"summary", "text"}

    body_html = ""
    body_paragraphs: List[str] = []
    if body_text and re.search(r"<[a-zA-Z][^>]*>", body_text):
        body_html = render_news_html(body_text)
    else:
        body_paragraphs = _split_news_paragraphs(body_text)

    tags = _parse_news_list(selected_item.get("tags") if selected_item else None)
    categories = _parse_news_list(selected_item.get("categories") if selected_item else None)
    pillar_tags = _parse_news_list(selected_item.get("pillar_tags") if selected_item else None)
    tickers = _parse_news_list(selected_item.get("tickers") if selected_item else None)
    ticker_single = selected_item.get("ticker") if selected_item else None
    if ticker_single:
        tickers.extend(_parse_news_list(ticker_single))
    tickers = list(dict.fromkeys([ticker for ticker in tickers if ticker]))

    external_url = None
    if selected_item:
        url_value = (selected_item.get("url") or "").strip()
        if url_value and _is_external_url(url_value):
            external_url = url_value

    back_query = request.GET.get("back", "").strip()
    back_url = reverse("news")
    if back_query and "http" not in back_query and "//" not in back_query:
        back_url = f"{back_url}?{back_query}"

    context = {
        "year": datetime.now().year,
        "news_error": detail_response.get("error"),
        "news_error_ref": news_error_ref,
        "article": selected_item,
        "published_at": published_at,
        "body_html": body_html,
        "body_paragraphs": body_paragraphs,
        "body_is_fallback": body_is_fallback,
        "body_source": body_source,
        "body_length": len(body_text),
        "debug": settings.DEBUG,
        "tags": tags,
        "categories": categories,
        "pillar_tags": pillar_tags,
        "tickers": tickers,
        "external_url": external_url,
        "back_url": back_url,
    }

    status = 200 if selected_item else 404
    if detail_response.get("error"):
        status = 503 if not selected_item else 200
    return render(request, "news_detail.html", context, status=status)


def news_asset(request, asset_id: int):
    if asset_id <= 0:
        raise Http404("Asset not found.")
    asset = get_news_asset(asset_id)
    if not asset:
        raise Http404("Asset not found.")
    content_type = asset.get("mime_type") or "application/octet-stream"
    response = HttpResponse(asset.get("file_blob") or b"", content_type=content_type)
    file_name = asset.get("file_name")
    if file_name:
        safe_name = str(file_name).replace('"', "")
        response["Content-Disposition"] = f'inline; filename="{safe_name}"'
    return response


@login_required
def news_admin(request):
    default_form_values = {
        "title": "",
        "url": "",
        "source": "",
        "summary": "",
        "published_at": "",
        "pillar_tags": [],
        "categories": [],
        "tags": [],
        "tickers": [],
    }

    context = {
        "year": datetime.now().year,
        "form_values": default_form_values,
        "created_item": None,
        "admin_error": None,
    }

    if request.method == "POST":
        def parse_list(field_name: str):
            raw_value = (request.POST.get(field_name) or "").strip()
            return [part.strip() for part in raw_value.split(",") if part.strip()]

        form_values = {
            "title": (request.POST.get("title") or "").strip(),
            "url": (request.POST.get("url") or "").strip(),
            "source": (request.POST.get("source") or "").strip(),
            "summary": (request.POST.get("summary") or "").strip(),
            "published_at": (request.POST.get("dt_pub") or "").strip(),
            "pillar_tags": parse_list("pillar_tags"),
            "categories": parse_list("categories"),
            "tags": parse_list("tags"),
            "tickers": parse_list("tickers"),
        }

        context["form_values"] = form_values

        if not form_values["title"] or not form_values["url"]:
            context["admin_error"] = "Title and URL are required."
        else:
            create_response = create_news_item_admin(
                title=form_values["title"],
                url=form_values["url"],
                source=form_values["source"] or None,
                summary=form_values["summary"] or None,
                published_at=form_values["published_at"] or None,
                pillar_tags=form_values["pillar_tags"],
                categories=form_values["categories"],
                tags=form_values["tags"],
                tickers=form_values["tickers"],
            )

            context["created_item"] = create_response.get("item")
            context["admin_error"] = create_response.get("error")

        if context["created_item"]:
            context["form_values"] = default_form_values

    return render(request, "news_admin.html", context)


def robots_txt(request):
    if _is_preview_request(request):
        lines = [
            "User-agent: *",
            "Disallow: /",
            "",
        ]
    else:
        sitemap_url = f"{settings.SITE_URL.rstrip('/')}/sitemap.xml"
        lines = [
            "User-agent: *",
            "Allow: /",
            "Disallow: /admin/",
            "Disallow: /news/admin/",
            "Disallow: /api/",
            "Disallow: /ask2/api/",
            f"Sitemap: {sitemap_url}",
            "",
        ]
    return HttpResponse("\n".join(lines), content_type="text/plain")


def favicon(request):
    try:
        favicon_file = staticfiles_storage.open("favicon.ico")
    except FileNotFoundError as exc:
        raise Http404("favicon not found") from exc
    response = FileResponse(favicon_file, content_type="image/x-icon")
    response.headers["Cache-Control"] = "public, max-age=604800, immutable"
    return response


def _is_preview_request(request) -> bool:
    host = request.get_host().split(":")[0].lower()
    prod_hosts = {"sustainacore.org", "www.sustainacore.org"}
    preview_hosts = {h.lower() for h in settings.PREVIEW_HOSTS}
    host_is_preview = host in preview_hosts or host.startswith("preview.") or ".preview." in host
    env_is_preview = settings.SUSTAINACORE_ENV == "preview" or settings.PREVIEW_MODE
    return (host not in prod_hosts) and (env_is_preview or host_is_preview)


def sitemap_index(request):
    cache_key = "sitemap_index_xml"
    cached = cache.get(cache_key)
    if cached:
        response = HttpResponse(cached["content"], content_type=cached["content_type"])
        if cached.get("last_modified"):
            response.headers["Last-Modified"] = cached["last_modified"]
        return response

    section_entries = sitemaps.get_section_index_entries()
    content = sitemaps.render_sitemap_index(section_entries)
    response = HttpResponse(content, content_type="application/xml")
    last_modified = max(
        [entry.get("lastmod") for entry in section_entries if entry.get("lastmod")],
        default=None,
    )
    if last_modified:
        response.headers["Last-Modified"] = last_modified
    cache.set(
        cache_key,
        {
            "content": response.content,
            "content_type": response.get("Content-Type", "application/xml"),
            "last_modified": response.headers.get("Last-Modified"),
        },
        timeout=settings.SITEMAP_CACHE_SECONDS,
    )
    return response


def sitemap_section(request, section: str):
    cache_key = f"sitemap_section_{section}"
    cached = cache.get(cache_key)
    if cached:
        response = HttpResponse(cached["content"], content_type=cached["content_type"])
        if cached.get("last_modified"):
            response.headers["Last-Modified"] = cached["last_modified"]
        return response

    entries = sitemaps.get_section_entries(section)
    if not entries:
        raise Http404("Sitemap section not found")
    content = sitemaps.render_urlset(entries)
    response = HttpResponse(content, content_type="application/xml")
    last_modified = sitemaps.get_section_lastmod(section)
    if last_modified:
        response.headers["Last-Modified"] = last_modified
    cache.set(
        cache_key,
        {
            "content": response.content,
            "content_type": response.get("Content-Type", "application/xml"),
            "last_modified": response.headers.get("Last-Modified"),
        },
        timeout=settings.SITEMAP_CACHE_SECONDS,
    )
    return response


def press_index(request):
    context = {
        "year": datetime.now().year,
    }
    return render(request, "press_index.html", context)


def privacy(request):
    context = {
        "year": datetime.now().year,
    }
    return render(request, "privacy.html", context)


def terms(request):
    context = {
        "year": datetime.now().year,
    }
    return render(request, "terms.html", context)


def corrections(request):
    context = {
        "year": datetime.now().year,
    }
    return render(request, "corrections.html", context)


def tech100_methodology(request):
    context = {
        "year": datetime.now().year,
    }
    return render(request, "tech100_methodology.html", context)


@require_login_for_download
def tech100_methodology_download(request):
    pdf_path = Path(settings.BASE_DIR) / "static" / "docs" / "TECH100_AI_Governance_Methodology_v1.1.pdf"
    if not pdf_path.exists():
        raise Http404("Methodology PDF not found.")
    response = FileResponse(pdf_path.open("rb"), content_type="application/pdf")
    response["Content-Disposition"] = 'attachment; filename="TECH100_AI_Governance_Methodology_v1.1.pdf"'
    return response


def press_tech100(request):
    context = {
        "year": datetime.now().year,
    }
    return render(request, "press_tech100.html", context)
