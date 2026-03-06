from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from functools import lru_cache
from typing import Iterable, Optional, Tuple
from urllib.parse import urlparse

from django.conf import settings


def _parse_ip(value: str) -> Optional[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        return ipaddress.ip_address(value)
    except ValueError:
        return None


def _first_public_ip(candidates: Iterable[str]) -> Optional[str]:
    for candidate in candidates:
        if not candidate:
            continue
        ip_obj = _parse_ip(candidate)
        if ip_obj and ip_obj.is_global:
            return str(ip_obj)
    return None


def get_client_ip(request) -> str:
    trust_forwarded = getattr(settings, "TELEMETRY_TRUST_X_FORWARDED_FOR", True)
    forwarded_raw = request.META.get("HTTP_X_FORWARDED_FOR") if trust_forwarded else None
    real_ip_raw = request.META.get("HTTP_X_REAL_IP") if trust_forwarded else None
    remote_addr = request.META.get("REMOTE_ADDR", "") or ""

    if forwarded_raw:
        forwarded_ips = [part.strip() for part in forwarded_raw.split(",")]
        public_ip = _first_public_ip(forwarded_ips)
        if public_ip:
            return public_ip

    if real_ip_raw:
        public_ip = _first_public_ip([real_ip_raw.strip()])
        if public_ip:
            return public_ip

    ip_obj = _parse_ip(remote_addr.strip()) if remote_addr else None
    if ip_obj:
        return str(ip_obj)
    return remote_addr


def truncate_ip(raw_ip: str) -> Optional[str]:
    if not raw_ip:
        return None
    try:
        ip_obj = ipaddress.ip_address(raw_ip)
    except ValueError:
        return None
    if ip_obj.version == 4:
        network = ipaddress.ip_network(f"{raw_ip}/24", strict=False)
    else:
        network = ipaddress.ip_network(f"{raw_ip}/48", strict=False)
    return str(network)


def hash_ip(raw_ip: str) -> Optional[str]:
    if not raw_ip:
        return None
    salt = getattr(settings, "TELEMETRY_HASH_SALT", settings.SECRET_KEY)
    payload = f"{salt}:{raw_ip}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def get_ip_fields(request) -> Tuple[Optional[str], Optional[str]]:
    raw_ip = get_client_ip(request)
    return truncate_ip(raw_ip), hash_ip(raw_ip)


def normalize_path(value: Optional[str], *, max_len: int = 512) -> str:
    raw = (value or "").strip() or "/"
    parsed = urlparse(raw)
    if parsed.scheme or parsed.netloc:
        raw = parsed.path or "/"
    if not raw.startswith("/"):
        raw = f"/{raw.lstrip('/')}"
    return raw[:max_len]


def referrer_host(value: Optional[str], *, max_len: int = 255) -> Optional[str]:
    raw = (value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    host = parsed.hostname or parsed.netloc or raw
    host = host.split("/", 1)[0].strip().lower()
    return host[:max_len] if host else None


def classify_user_agent(user_agent: Optional[str], *, max_len: int = 128) -> Optional[str]:
    ua = (user_agent or "").strip().lower()
    if not ua:
        return None
    labels = [
        ("googlebot", "bot:googlebot"),
        ("bingbot", "bot:bingbot"),
        ("linkedinbot", "bot:linkedinbot"),
        ("facebookexternalhit", "bot:facebook"),
        ("slackbot", "bot:slack"),
        ("curl/", "bot:curl"),
        ("python-requests", "bot:python-requests"),
        ("playwright", "bot:playwright"),
        ("headless", "bot:headless"),
        ("bot", "bot:generic"),
        ("crawler", "bot:crawler"),
        ("spider", "bot:spider"),
        ("edg/", "browser:edge"),
        ("chrome/", "browser:chrome"),
        ("firefox/", "browser:firefox"),
        ("safari/", "browser:safari"),
    ]
    for token, label in labels:
        if token in ua:
            return label[:max_len]
    return "browser:other"


def status_class(status_code: Optional[int]) -> Optional[str]:
    if status_code is None:
        return None
    try:
        value = int(status_code)
    except (TypeError, ValueError):
        return None
    if value < 100:
        return None
    return f"{value // 100}xx"


def stable_token(value: Optional[str]) -> Optional[str]:
    raw = (value or "").strip()
    if not raw:
        return None
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def compact_json(payload: Optional[dict], *, max_len: int = 2000) -> Optional[str]:
    if payload is None:
        return None
    try:
        encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError):
        return None
    return encoded[:max_len]


def ensure_session_key(request) -> Optional[str]:
    try:
        # Never create anonymous DB-backed sessions just for telemetry.
        # When the session store is unhealthy, a forced save can turn
        # an otherwise successful page view into a 400 response.
        return request.session.session_key
    except Exception:
        return None


def anonymize_user_id(value: str) -> int:
    salt = getattr(settings, "TELEMETRY_HASH_SALT", settings.SECRET_KEY)
    payload = f"{salt}:{value}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    user_id = int.from_bytes(digest[:8], "big") % 2147483647
    return user_id or 1


def resolve_user_id(request, *, consent_analytics: bool, anon_id: Optional[str]) -> Optional[int]:
    if not consent_analytics:
        return None
    if getattr(request, "user", None) and getattr(request.user, "is_authenticated", False):
        try:
            return int(request.user.id)
        except (TypeError, ValueError):
            return None
    if anon_id:
        return anonymize_user_id(anon_id)
    return None


def is_bot_user_agent(user_agent: Optional[str]) -> bool:
    ua = (user_agent or "").lower()
    if not ua:
        return False
    return any(token in ua for token in ("bot", "spider", "crawler", "crawl", "scrape", "scanner"))


_GEO_VALUE_RE = re.compile(r"^[A-Z0-9_-]+$")


def _normalize_header_name(name: str) -> str:
    key = name.strip().upper().replace("-", "_")
    if not key:
        return ""
    if key.startswith("HTTP_") or key in {"CONTENT_TYPE", "CONTENT_LENGTH"}:
        return key
    return f"HTTP_{key}"


def _get_header_value(request, headers: Iterable[str]) -> Optional[str]:
    for header in headers:
        key = _normalize_header_name(header)
        if not key:
            continue
        value = request.META.get(key)
        if value:
            return value
    return None


def _clean_geo_value(value: Optional[str], max_len: int) -> Optional[str]:
    if not value:
        return None
    cleaned = value.split(",", 1)[0].strip().upper()
    if not cleaned:
        return None
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len]
    if not _GEO_VALUE_RE.match(cleaned):
        return None
    return cleaned


def get_geo_fields(request) -> Tuple[Optional[str], Optional[str]]:
    country_headers = getattr(settings, "TELEMETRY_GEO_COUNTRY_HEADERS", [])
    region_headers = getattr(settings, "TELEMETRY_GEO_REGION_HEADERS", [])
    country = _clean_geo_value(_get_header_value(request, country_headers), 8)
    region = _clean_geo_value(_get_header_value(request, region_headers), 16)
    if country or region:
        return country, region
    if not getattr(settings, "TELEMETRY_GEOIP_ENABLED", False):
        return country, region
    raw_ip = get_client_ip(request)
    if not raw_ip:
        return country, region
    geoip_country, geoip_region = _lookup_geoip_fields(raw_ip)
    return geoip_country, geoip_region


@lru_cache(maxsize=1)
def _get_geoip_reader():
    db_path = getattr(settings, "TELEMETRY_GEOIP_DB_PATH", "") or ""
    if not db_path:
        return None
    try:
        import geoip2.database  # type: ignore
    except Exception:
        return None
    try:
        return geoip2.database.Reader(db_path)
    except Exception:
        return None


def _lookup_geoip_fields(raw_ip: str) -> Tuple[Optional[str], Optional[str]]:
    reader = _get_geoip_reader()
    if not reader:
        return None, None
    try:
        response = reader.city(raw_ip)
    except Exception:
        return None, None
    country = _clean_geo_value(getattr(response.country, "iso_code", None), 8)
    subdivision = getattr(response.subdivisions, "most_specific", None)
    region_code = getattr(subdivision, "iso_code", None)
    region = _clean_geo_value(region_code, 16)
    return country, region
