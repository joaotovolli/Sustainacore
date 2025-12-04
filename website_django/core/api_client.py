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
    ticker: Optional[str] = None,
    days: Optional[int] = None,
    limit: int = 20,
    timeout: float = 8.0,
) -> Dict[str, Any]:
    """Fetch news items from VM1 `/api/news` endpoint."""

    # VM1 `/api/news` accepts limit, days, source, and tag (multi) query params
    # and responds with {"items": [...], "meta": {...}}. Default backend days is 30
    # when the param is omitted. The backend requires `Authorization: Bearer` with the
    # shared API token when API auth is enabled, so `BACKEND_API_TOKEN` must be present
    # in the Django settings/environment for production calls to succeed.

    params: Dict[str, Any] = {"limit": limit}
    if source:
        params["source"] = source
    if tag:
        params["tag"] = tag
    if ticker:
        params["ticker"] = ticker
    if days is not None:
        params["days"] = days

    payload = _get_json("/api/news", timeout=timeout, params=params)

    if not isinstance(payload, dict):
        logger.warning("Unexpected news payload shape: %s", payload)
        return {"items": [], "meta": {}, "error": "Unable to load news data."}

    if "error" in payload:
        return {
            "items": [],
            "meta": {},
            "error": payload.get("message", "Unable to load news data."),
        }

    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}

    return {
        "items": items,
        "meta": meta,
        "error": None,
    }


def _post_json(path: str, *, timeout: float, json_body: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{_base_url()}{path}"
    try:
        response = requests.post(
            url,
            headers=_build_headers(),
            json=json_body,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        logger.warning("Backend POST failed for %s: %s", path, exc)
        return {"error": "backend_failure", "message": str(exc)}
    except json.JSONDecodeError:
        logger.warning("Invalid JSON response from backend for %s", path)
        return {"error": "backend_failure", "message": "Invalid JSON from backend"}


def create_news_item_admin(
    *,
    title: str,
    url: str,
    source: Optional[str] = None,
    summary: Optional[str] = None,
    published_at: Optional[str] = None,
    pillar_tags: Optional[List[str]] = None,
    categories: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    tickers: Optional[List[str]] = None,
    timeout: float = 8.0,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "title": title,
        "url": url,
    }
    if source:
        payload["source"] = source
    if summary is not None:
        payload["summary"] = summary
    if published_at:
        payload["published_at"] = published_at
    if pillar_tags is not None:
        payload["pillar_tags"] = pillar_tags
    if categories is not None:
        payload["categories"] = categories
    if tags is not None:
        payload["tags"] = tags
    if tickers is not None:
        payload["tickers"] = tickers

    response_payload = _post_json("/api/news/admin/items", timeout=timeout, json_body=payload)

    if isinstance(response_payload, dict) and "error" in response_payload:
        return {"item": None, "error": response_payload.get("message", "Unable to create news item.")}

    created_item: Optional[Dict[str, Any]] = None
    if isinstance(response_payload, dict):
        if isinstance(response_payload.get("item"), dict):
            created_item = response_payload.get("item")
        elif isinstance(response_payload.get("data"), dict):
            created_item = response_payload.get("data")
        else:
            created_item = response_payload

    return {"item": created_item, "error": None}
