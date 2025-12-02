"""HTTP client helpers for VM1 APIs used by the website."""

from __future__ import annotations

import json
from typing import Any, Dict, List

import requests
from django.conf import settings


def _build_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    token = getattr(settings, "BACKEND_API_TOKEN", "") or ""
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _base_url() -> str:
    return (getattr(settings, "BACKEND_API_BASE", "") or "").rstrip("/")


def _get_json(path: str, timeout: float) -> Dict[str, Any] | List[Dict[str, Any]]:
    url = f"{_base_url()}{path}"
    try:
        response = requests.get(url, headers=_build_headers(), timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        return {"error": "backend_failure", "message": str(exc)}
    except json.JSONDecodeError:
        return {"error": "backend_failure", "message": "Invalid JSON from backend"}


def _extract_items(payload: Dict[str, Any] | List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            return payload["data"]
        if "items" in payload and isinstance(payload["items"], list):
            return payload["items"]
        # When payload looks like an error object
        if "error" in payload:
            return []
    return []


def fetch_tech100(timeout: float = 8.0) -> Dict[str, Any]:
    payload = _get_json("/api/tech100", timeout=timeout)
    if isinstance(payload, dict) and "error" in payload:
        return {"items": [], "error": payload.get("message", "Unable to load TECH100 data.")}

    return {"items": _extract_items(payload), "error": None}


def fetch_news(timeout: float = 8.0) -> Dict[str, Any]:
    payload = _get_json("/api/news", timeout=timeout)
    if isinstance(payload, dict) and "error" in payload:
        return {"items": [], "error": payload.get("message", "Unable to load news data.")}

    return {"items": _extract_items(payload), "error": None}
