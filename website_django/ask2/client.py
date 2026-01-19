"""Lightweight HTTP client for VM1 Ask2 APIs."""

from __future__ import annotations

import json
from typing import Any, Dict

import logging
import requests
from django.conf import settings

logger = logging.getLogger(__name__)

PRIMARY_TIMEOUT_SECONDS = 6.0
CONNECT_TIMEOUT_SECONDS = 3.0


def _build_headers() -> Dict[str, str]:
    """Build headers for VM1 requests."""
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    token = getattr(settings, "BACKEND_API_TOKEN", "") or ""
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _base_url() -> str:
    return (getattr(settings, "BACKEND_API_BASE", "") or "").rstrip("/")


def check_backend_health(timeout: float = 8.0) -> Dict[str, Any]:
    """Check the VM1 health endpoint."""
    url = f"{_base_url()}/api/health"
    try:
        response = requests.get(url, headers=_build_headers(), timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        return {"error": "backend_health_failed", "message": str(exc)}
    except json.JSONDecodeError:
        return {"error": "backend_health_failed", "message": "Invalid JSON from backend"}


def _post_json(path: str, payload: Dict[str, Any], timeout: float) -> Dict[str, Any]:
    url = f"{_base_url()}{path}"
    try:
        response = requests.post(
            url,
            headers=_build_headers(),
            json=payload,
            timeout=(CONNECT_TIMEOUT_SECONDS, timeout),
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        logger.warning("Ask2 backend request failed for %s: %s", path, exc)
        return {"error": "backend_failure", "message": str(exc)}
    except json.JSONDecodeError:
        logger.warning("Invalid JSON response from Ask2 backend for %s", path)
        return {"error": "backend_failure", "message": "Invalid JSON from backend"}


def ask2_query(user_message: str, timeout: float = 12.0) -> Dict[str, Any]:
    """Send a question to the VM1 Ask2 endpoint."""
    payload = {"user_message": user_message}
    primary_timeout = min(timeout, PRIMARY_TIMEOUT_SECONDS)
    result = _post_json("/api/ask2", payload, timeout=primary_timeout)
    if "error" not in result:
        return result

    fallback_payload = {"q": user_message, "k": 4}
    fallback = _post_json("/ask2_direct", fallback_payload, timeout=timeout)
    if "error" not in fallback:
        if not isinstance(fallback.get("meta"), dict):
            fallback["meta"] = {}
        fallback["meta"]["ask2_fallback"] = "ask2_direct"
        return fallback

    return result
