"""HTTP client helpers for VM1 APIs used by the website."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
import logging

import requests
from django.conf import settings

logger = logging.getLogger(__name__)


def _build_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    token = getattr(settings, "BACKEND_API_TOKEN", "") or ""
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _base_url() -> str:
    return (getattr(settings, "BACKEND_API_BASE", "") or "").rstrip("/")


def _get_json(
    path: str, *, timeout: float, params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any] | List[Dict[str, Any]]:
    url = f"{_base_url()}{path}"
    try:
        response = requests.get(
            url,
            headers=_build_headers(),
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        logger.warning("Backend request failed for %s: %s", path, exc)
        return {"error": "backend_failure", "message": str(exc)}
    except json.JSONDecodeError:
        logger.warning("Invalid JSON response from backend for %s", path)
        return {"error": "backend_failure", "message": "Invalid JSON from backend"}


def _extract_items(payload: Dict[str, Any] | List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data

        items = payload.get("items")
        if isinstance(items, list):
            return items

        if "error" in payload:
            return []

    return []


def _extract_meta(payload: Dict[str, Any] | List[Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(payload, dict):
        meta = payload.get("meta")
        if isinstance(meta, dict):
            return meta
    return {}


def fetch_tech100(timeout: float = 8.0) -> Dict[str, Any]:
    payload = _get_json("/api/tech100", timeout=timeout)
    if isinstance(payload, dict) and "error" in payload:
        return {
            "items": [],
            "error": payload.get("message", "Unable to load TECH100 data."),
        }

    return {"items": _extract_items(payload), "error": None}


def fetch_news(
    *,
    source: Optional[str] = None,
    tag: Optional[str] = None,
    days: Optional[int] = None,
    limit: int = 20,
    timeout: float = 8.0,
) -> Dict[str, Any]:
    """Fetch news items from VM1 `/api/news` endpoint."""

    # VM1 `/api/news` accepts limit, days, source, and tag (multi) query params
    # and responds with {"items": [...], "meta": {...}}. Default backend days is 30
    # when the param is omitted. Direct call from this VM returned 403 "Domain forbidden"
    # without the correct auth/host headers.

    params: Dict[str, Any] = {"limit": limit}
    if source:
        params["source"] = source
    if tag:
        params["tag"] = tag
    if days is not None:
        params["days"] = days

    payload = _get_json("/api/news", timeout=timeout, params=params)
    if isinstance(payload, dict) and "error" in payload:
        return {
            "items": [],
            "meta": {},
            "error": payload.get("message", "Unable to load news data."),
        }

    return {
        "items": _extract_items(payload),
        "meta": _extract_meta(payload),
        "error": None,
    }
