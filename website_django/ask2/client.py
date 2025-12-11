"""Lightweight HTTP client for VM1 Ask2 APIs."""

from __future__ import annotations

import json
from typing import Any, Dict

import requests
from django.conf import settings


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


def ask2_query(user_message: str, timeout: float = 12.0) -> Dict[str, Any]:
    """Send a question to the VM1 Ask2 endpoint."""
    url = f"{_base_url()}/api/ask2"
    payload = {"user_message": user_message}
    try:
        response = requests.post(url, headers=_build_headers(), json=payload, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        return {"error": "backend_failure", "message": str(exc)}
    except json.JSONDecodeError:
        return {"error": "backend_failure", "message": "Invalid JSON from backend"}
