"""HTTP client helpers for VM1 APIs used by the website."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from typing import Any, Dict, List, Optional
import logging

import requests
from django.conf import settings
from django.core.cache import cache

logger = logging.getLogger(__name__)

CACHE_TTLS = {
    "tech100": 60,
    "tech100_error": 15,
    "news": 60,
    "news_error": 15,
    "news_detail": 60,
    "news_detail_error": 15,
}


def _cache_key(prefix: str, params: Dict[str, Any]) -> str:
    encoded = json.dumps(params, sort_keys=True, default=str)
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:12]
    return f"backend:{prefix}:{digest}"


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


def _tech100_fixture_items() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    sectors = ["Software", "Semiconductors", "Internet", "Hardware", "Services"]
    regions = ["North America", "Europe", "Asia-Pacific"]
    for idx in range(25):
        rank = idx + 1
        score = round(86.5 - idx * 0.6, 2)
        items.append(
            {
                "company_name": f"Tech Company {rank}",
                "ticker": f"TCH{rank:02d}",
                "gics_sector": sectors[idx % len(sectors)],
                "region": regions[idx % len(regions)],
                "port_date": "2025-01-31",
                "rank_index": rank,
                "weight": round(0.04 - idx * 0.001, 4),
                "aiges_composite_average": score,
                "aiges_composite": score,
                "transparency": round(80 + (idx % 5) * 2.1, 2),
                "ethical_principles": round(78 + (idx % 4) * 2.4, 2),
                "governance_structure": round(82 + (idx % 3) * 1.8, 2),
                "regulatory_alignment": round(76 + (idx % 6) * 1.7, 2),
                "stakeholder_engagement": round(79 + (idx % 5) * 1.9, 2),
                "summary": "Fixture data for VRT stability.",
            }
        )
    return items


def fetch_tech100(
    *,
    port_date: Optional[str] = None,
    sector: Optional[str] = None,
    search: Optional[str] = None,
    query: Optional[str] = None,
    timeout: float = 8.0,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if port_date:
        params["port_date"] = port_date
    if sector:
        params["sector"] = sector
    search_param = query or search
    if search_param:
        # Support both historic "search" and "q" query params depending on backend implementation
        params["q"] = search_param
        params["search"] = search_param
    if search:
        params["search"] = search

    if os.getenv("TECH100_UI_DATA_MODE") == "fixture":
        items = _tech100_fixture_items()
        if sector:
            items = [item for item in items if item.get("gics_sector") == sector]
        if search_param:
            search_lower = search_param.lower()
            items = [
                item
                for item in items
                if search_lower in (item.get("company_name") or "").lower()
                or search_lower in (item.get("ticker") or "").lower()
            ]
        result = {"items": items, "meta": {"count": len(items)}, "error": None}
        return result

    cache_key = _cache_key("tech100", params)
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    payload = _get_json("/api/tech100", timeout=timeout, params=params or None)
    if isinstance(payload, dict) and "error" in payload:
        result = {
            "items": [],
            "meta": {},
            "error": payload.get("message", "Unable to load TECH100 data."),
        }
        cache.set(cache_key, result, CACHE_TTLS["tech100_error"])
        return result

    result = {"items": _extract_items(payload), "meta": _extract_meta(payload), "error": None}
    cache.set(cache_key, result, CACHE_TTLS["tech100"])
    return result


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

    cache_key = _cache_key("news", params)
    cached = None
    if not os.getenv("PYTEST_CURRENT_TEST") and "test" not in sys.argv:
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

    payload = _get_json("/api/news", timeout=timeout, params=params)

    if not isinstance(payload, dict):
        logger.warning("Unexpected news payload shape: %s", payload)
        result = {"items": [], "meta": {}, "error": "Unable to load news data."}
        cache.set(cache_key, result, CACHE_TTLS["news_error"])
        return result

    if "error" in payload:
        result = {
            "items": [],
            "meta": {},
            "error": payload.get("message", "Unable to load news data."),
        }
        cache.set(cache_key, result, CACHE_TTLS["news_error"])
        return result

    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}

    result = {
        "items": items,
        "meta": meta,
        "error": None,
    }
    cache.set(cache_key, result, CACHE_TTLS["news"])
    return result


def fetch_news_detail(*, news_id: str, timeout: float = 8.0) -> Dict[str, Any]:
    """Fetch a single news item with full body text from VM1."""

    if not news_id:
        return {"item": None, "error": "Invalid news identifier."}

    cache_key = _cache_key("news_detail", {"news_id": news_id})
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    safe_id = requests.utils.quote(str(news_id))
    payload = _get_json(f"/api/news/{safe_id}", timeout=timeout)

    if not isinstance(payload, dict):
        logger.warning("Unexpected news detail payload shape: %s", payload)
        result = {"item": None, "error": "Unable to load news data."}
        cache.set(cache_key, result, CACHE_TTLS["news_detail_error"])
        return result

    if "error" in payload:
        result = {"item": None, "error": payload.get("message", "Unable to load news data.")}
        cache.set(cache_key, result, CACHE_TTLS["news_detail_error"])
        return result

    item = payload.get("item") if isinstance(payload.get("item"), dict) else None
    result = {"item": item, "error": None}
    cache.set(cache_key, result, CACHE_TTLS["news_detail"])
    return result


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
