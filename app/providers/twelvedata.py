"""Twelve Data EOD price fetcher (throttled for free tier limits)."""
from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import time
import http.client  # preload stdlib http to avoid app.http shadowing
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List, Optional

API_URL = "https://api.twelvedata.com/time_series"
_API_BASE = "https://api.twelvedata.com"
_API_KEY_ENV_VARS = ("SC_TWELVEDATA_API_KEY", "TWELVEDATA_API_KEY")
RATE_LIMIT_PER_MIN = 8
MAX_RETRIES = 5
_TOKENS = RATE_LIMIT_PER_MIN
_RESET_AT = time.time() + 60.0
_LOGGER = logging.getLogger(__name__)
_urlopen = urllib.request.urlopen


class TwelveDataError(RuntimeError):
    """Raised when Twelve Data returns an unexpected error."""


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


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_date(raw: Any) -> Optional[_dt.date]:
    if raw is None:
        return None
    text = str(raw)
    try:
        return _dt.date.fromisoformat(text)
    except ValueError:
        try:
            return _dt.datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError:
            return None


def _extract_trade_date(entry: Dict[str, Any]) -> Optional[_dt.date]:
    for key in ("trade_date", "datetime", "date"):
        if key in entry:
            return _coerce_date(entry.get(key))
    return None


def _build_time_series_url(ticker: str, start: str, end: str, api_key: str) -> str:
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
    url = _build_time_series_url(ticker, start, end, api_key)
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

    api_key = os.getenv("TWELVEDATA_API_KEY") or os.getenv("SC_TWELVEDATA_API_KEY")
    if not api_key:
        raise RuntimeError("TWELVEDATA_API_KEY is not set")

    rows: List[dict] = []
    for ticker in sorted(set(tickers_list)):
        rows.extend(_fetch_ticker(ticker, start, end, api_key))
    return rows


def _build_api_url(path: str, params: Dict[str, Any]) -> str:
    safe_params = {k: v for k, v in params.items() if v is not None}
    return f"{_API_BASE}/{path}?{urllib.parse.urlencode(safe_params)}"


def _get_api_key(explicit: Optional[str] = None) -> str:
    if explicit:
        return explicit
    for env_var in _API_KEY_ENV_VARS:
        raw = os.getenv(env_var)
        if raw:
            return raw.strip()
    raise TwelveDataError("TWELVEDATA_API_KEY is not configured")


def _request_json(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = _build_api_url(path, params)
    req = urllib.request.Request(url)
    try:
        with _urlopen(req, timeout=30) as resp:
            payload = resp.read()
    except urllib.error.HTTPError as exc:  # pragma: no cover - exercised via URLError branch
        payload = exc.read()
    except urllib.error.URLError as exc:
        raise TwelveDataError(f"Unable to reach Twelve Data: {exc.reason}") from exc

    try:
        return json.loads(payload.decode("utf-8"))
    except Exception as exc:
        raise TwelveDataError("Malformed response from Twelve Data") from exc


def fetch_api_usage(api_key: Optional[str] = None) -> Dict[str, Any]:
    """Fetch the current API usage/plan details."""

    key = _get_api_key(api_key)
    payload = _request_json("api_usage", {"apikey": key})

    if isinstance(payload, dict) and payload.get("status") == "error":
        message = payload.get("message") or "unknown error"
        raise TwelveDataError(f"Twelve Data api_usage error: {message}")

    return {
        "timestamp": payload.get("timestamp") or payload.get("datetime"),
        "current_usage": _coerce_int(payload.get("current_usage")),
        "plan_limit": _coerce_int(payload.get("plan_limit") or payload.get("api_limit")),
        "plan_category": payload.get("plan_category") or payload.get("plan") or payload.get("plan_id"),
    }


def remaining_credits(usage: Dict[str, Any]) -> int:
    """Compute remaining credits from an api_usage payload."""

    plan_limit = _coerce_int(usage.get("plan_limit"))
    current_usage = _coerce_int(usage.get("current_usage"))

    if plan_limit is None or current_usage is None:
        return 0

    remaining = plan_limit - current_usage
    return remaining if remaining > 0 else 0


def fetch_time_series(
    ticker: str,
    start_date: _dt.date,
    end_date: _dt.date,
    *,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch daily time series data for the given ticker."""

    key = _get_api_key(api_key)
    payload = _request_json(
        "time_series",
        {
            "symbol": ticker,
            "interval": "1day",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "order": "ASC",
            "apikey": key,
        },
    )

    if payload.get("status") == "error":
        message = payload.get("message") or "unknown error"
        if "no data is available" in message.lower():
            return []
        raise TwelveDataError(f"Twelve Data time_series error for {ticker}: {message}")

    values = payload.get("values") or payload.get("data") or []
    if not isinstance(values, list):
        return []
    return values


def has_eod_for_date(ticker: str, date: _dt.date, *, api_key: Optional[str] = None) -> bool:
    """Return True if Twelve Data already exposes an end-of-day bar for the date."""

    key = _get_api_key(api_key)
    payload = _request_json(
        "time_series",
        {
            "symbol": ticker,
            "interval": "1day",
            "start_date": date.isoformat(),
            "end_date": date.isoformat(),
            "outputsize": 1,
            "apikey": key,
        },
    )

    message = str(payload.get("message") or "").lower()
    if payload.get("status") == "error":
        if "no data is available" in message:
            return False
        raise TwelveDataError(f"Twelve Data time_series error for {ticker}: {payload.get('message')}")

    values: Iterable[Dict[str, Any]] = payload.get("values") or payload.get("data") or []
    for entry in values:
        trade_date = _extract_trade_date(entry)
        if trade_date == date:
            return True
    return False


__all__ = [
    "fetch_api_usage",
    "fetch_eod_prices",
    "fetch_time_series",
    "has_eod_for_date",
    "remaining_credits",
    "TwelveDataError",
]
