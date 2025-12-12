"""Twelve Data EOD price fetcher."""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request

API_URL = "https://api.twelvedata.com/time_series"


def _build_url(tickers: list[str], start: str, end: str, api_key: str) -> str:
    symbols = ",".join(sorted(set(tickers)))
    params = {
        "symbol": symbols,
        "interval": "1day",
        "start_date": start,
        "end_date": end,
        "apikey": api_key,
        "order": "ASC",
        "outputsize": 5000,
    }
    return f"{API_URL}?{urllib.parse.urlencode(params)}"


def _parse_rows(payload: dict) -> list[dict]:
    rows: list[dict] = []
    if not payload:
        return rows

    if "data" in payload:
        # Multi-symbol response
        entries = payload.get("data", {}) or {}
        for ticker, body in entries.items():
            values = body.get("values") or []
            for item in values:
                rows.append(
                    {
                        "ticker": ticker,
                        "trade_date": item.get("datetime", "")[:10],
                        "close": _to_float(item.get("close")),
                        "adj_close": _to_float(item.get("adjusted_close")),
                        "volume": _to_int(item.get("volume")),
                        "currency": body.get("meta", {}).get("currency"),
                    }
                )
        return rows

    values = payload.get("values") or []
    ticker = (payload.get("meta") or {}).get("symbol")
    for item in values:
        rows.append(
            {
                "ticker": ticker,
                "trade_date": item.get("datetime", "")[:10],
                "close": _to_float(item.get("close")),
                "adj_close": _to_float(item.get("adjusted_close")),
                "volume": _to_int(item.get("volume")),
                "currency": (payload.get("meta") or {}).get("currency"),
            }
        )
    return rows


def _to_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def fetch_eod_prices(tickers: list[str], start: str, end: str) -> list[dict]:
    """
    Return rows with:
      ticker, trade_date (YYYY-MM-DD), close, adj_close, volume, currency
    """

    if not tickers:
        return []

    api_key = os.getenv("TWELVEDATA_API_KEY")
    if not api_key:
        raise RuntimeError("TWELVEDATA_API_KEY is not set")

    url = _build_url(tickers, start, end, api_key)
    request = urllib.request.Request(url, headers={"User-Agent": "sustainacore-index-engine"})

    try:
        with urllib.request.urlopen(request, timeout=30) as resp:  # nosec: B310
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:  # pragma: no cover - network specific
        raise RuntimeError(f"twelvedata_http_error:{exc.code}") from exc
    except urllib.error.URLError as exc:  # pragma: no cover - network specific
        raise RuntimeError(f"twelvedata_url_error:{exc.reason}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("twelvedata_invalid_json") from exc

    if isinstance(payload, dict) and payload.get("status") == "error":
        message = payload.get("message") or "twelvedata_error"
        raise RuntimeError(message)

    return _parse_rows(payload)
