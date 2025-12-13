"""Twelve Data EOD price fetcher (throttled for free tier limits)."""
from __future__ import annotations

import json
import os
import time
import http.client  # preload stdlib http to avoid app.http shadowing
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterable, List

API_URL = "https://api.twelvedata.com/time_series"
RATE_LIMIT_PER_MIN = 8
MAX_RETRIES = 5
_TOKENS = RATE_LIMIT_PER_MIN
_RESET_AT = time.time() + 60.0


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


def _build_url(ticker: str, start: str, end: str, api_key: str) -> str:
    params = {
        "symbol": ticker,
        "interval": "1day",
        "start_date": start,
        "end_date": end,
        "apikey": api_key,
        "order": "ASC",
        "outputsize": 5000,
        "timezone": "Exchange",
        "adjust": "all",
    }
    return f"{API_URL}?{urllib.parse.urlencode(params)}"


def _parse_rows(payload: dict, ticker: str | None) -> list[dict]:
    rows: list[dict] = []
    if not payload:
        return rows

    values = payload.get("values") or []
    meta = payload.get("meta") or {}
    ticker_value = ticker or meta.get("symbol")
    currency = meta.get("currency")
    for item in values:
        close_px = _to_float(item.get("close"))
        rows.append(
            {
                "ticker": ticker_value,
                "trade_date": (item.get("datetime") or "")[:10],
                "close": close_px,
                # With adjust=all the close is already adjusted; keep a copy.
                "adj_close": close_px,
                "volume": _to_int(item.get("volume")),
                "currency": currency,
            }
        )
    return rows


def _refresh_tokens(now: float) -> None:
    global _TOKENS, _RESET_AT
    if now >= _RESET_AT:
        _TOKENS = RATE_LIMIT_PER_MIN
        _RESET_AT = now + 60.0


def _acquire_token() -> None:
    """Blocking token reservation for the per-minute quota."""

    global _TOKENS, _RESET_AT
    while True:
        now = time.time()
        _refresh_tokens(now)
        if _TOKENS > 0:
            _TOKENS -= 1
            return
        sleep_for = max(0.0, _RESET_AT - now)
        time.sleep(sleep_for or 0.1)


def _sleep_until_reset() -> None:
    now = time.time()
    _refresh_tokens(now)
    sleep_for = max(0.0, _RESET_AT - now)
    time.sleep(sleep_for or 0.1)


def _should_retry_rate_limit(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("status") != "error":
        return False
    message = (payload.get("message") or "").lower()
    return "credit" in message or "limit" in message


def _fetch_ticker(
    ticker: str,
    start: str,
    end: str,
    api_key: str,
    max_retries: int = MAX_RETRIES,
) -> list[dict]:
    url = _build_url(ticker, start, end, api_key)
    request = urllib.request.Request(url, headers={"User-Agent": "sustainacore-index-engine"})

    for attempt in range(1, max_retries + 1):
        _acquire_token()
        try:
            with urllib.request.urlopen(request, timeout=30) as resp:  # nosec: B310
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:  # pragma: no cover - network specific
            if exc.code == 429 and attempt < max_retries:
                _sleep_until_reset()
                continue
            raise RuntimeError(f"twelvedata_http_error:{exc.code}") from exc
        except urllib.error.URLError as exc:  # pragma: no cover - network specific
            raise RuntimeError(f"twelvedata_url_error:{exc.reason}") from exc

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            if attempt < max_retries:
                _sleep_until_reset()
                continue
            raise RuntimeError("twelvedata_invalid_json") from exc

        if _should_retry_rate_limit(payload) and attempt < max_retries:
            _sleep_until_reset()
            continue

        if isinstance(payload, dict) and payload.get("status") == "error":
            message = payload.get("message") or "twelvedata_error"
            raise RuntimeError(message)

        return _parse_rows(payload, ticker)

    return []


def fetch_eod_prices(tickers: Iterable[str], start: str, end: str) -> list[dict]:
    """
    Return rows with:
      ticker, trade_date (YYYY-MM-DD), close, adj_close, volume, currency
    """

    tickers_list = [t.strip().upper() for t in tickers if t and str(t).strip()]
    if not tickers_list:
        return []

    api_key = os.getenv("TWELVEDATA_API_KEY")
    if not api_key:
        raise RuntimeError("TWELVEDATA_API_KEY is not set")

    rows: List[dict] = []
    for ticker in sorted(set(tickers_list)):
        rows.extend(_fetch_ticker(ticker, start, end, api_key))
    return rows
