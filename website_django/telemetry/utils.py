from __future__ import annotations

import hashlib
import ipaddress
from typing import Optional, Tuple

from django.conf import settings


def get_client_ip(request) -> str:
    trust_forwarded = getattr(settings, "TELEMETRY_TRUST_X_FORWARDED_FOR", False)
    forwarded = request.META.get("HTTP_X_FORWARDED_FOR") if trust_forwarded else None
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR", "") or ""


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
