"""Market data EOD price fetcher (throttled for tier limits)."""
from __future__ import annotations

import datetime as _dt
import fcntl
import json
import logging
import os
import random
import time
import http.client  # preload stdlib http to avoid app.http shadowing
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Tuple

_API_TIME_SERIES_PATH = "time_series"
_API_KEY_ENV_VARS = ("SC_MARKET_DATA_API_KEY", "MARKET_DATA_API_KEY")
_API_BASE_ENV = "MARKET_DATA_API_BASE_URL"
DEFAULT_CALLS_PER_WINDOW = 6
DEFAULT_WINDOW_SECONDS = 120
MAX_RETRIES = 5
_LOCK_PATH = "/tmp/sc_idx_market_data.lock"
_CALLS_PER_WINDOW: int
_WINDOW_SECONDS: int
_TOKENS: int
_RESET_AT: float
_LOGGER = logging.getLogger(__name__)
_urlopen = urllib.request.urlopen
_CALLS_PER_WINDOW_ENV = "SC_IDX_MARKET_DATA_CALLS_PER_WINDOW"
_WINDOW_SECONDS_ENV = "SC_IDX_MARKET_DATA_WINDOW_SECONDS"


class MarketDataProviderError(RuntimeError):
    """Raised when the market data provider returns an unexpected error."""


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


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _load_throttle_config() -> Tuple[int, int]:
    global _CALLS_PER_WINDOW, _WINDOW_SECONDS, _TOKENS, _RESET_AT
    _CALLS_PER_WINDOW = _env_int(_CALLS_PER_WINDOW_ENV, DEFAULT_CALLS_PER_WINDOW)
    _WINDOW_SECONDS = _env_int(_WINDOW_SECONDS_ENV, DEFAULT_WINDOW_SECONDS)
    _TOKENS = _CALLS_PER_WINDOW
    _RESET_AT = time.monotonic() + _WINDOW_SECONDS
    return _CALLS_PER_WINDOW, _WINDOW_SECONDS


def get_throttle_config(*, refresh: bool = False) -> Dict[str, int]:
    """Return the active throttling config."""

    if refresh:
        _load_throttle_config()
    return {"calls_per_window": _CALLS_PER_WINDOW, "window_seconds": _WINDOW_SECONDS}


@contextmanager
def _provider_lock():
    """Cross-process lock to serialize provider calls."""

    handle = open(_LOCK_PATH, "a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            handle.close()


def _get_api_base(explicit: Optional[str] = None) -> str:
    if explicit:
        return explicit.strip().rstrip("/")
    raw = os.getenv(_API_BASE_ENV)
    if raw:
        return raw.strip().rstrip("/")
    raise MarketDataProviderError(f"{_API_BASE_ENV} is not configured")


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
    base = _get_api_base()
    return f"{base}/{_API_TIME_SERIES_PATH}?{urllib.parse.urlencode(params)}"


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
        _TOKENS = _CALLS_PER_WINDOW
        _RESET_AT = now + _WINDOW_SECONDS


def _acquire_token_blocking() -> None:
    """Blocking token reservation for the current window."""

    global _TOKENS, _RESET_AT
    while True:
        now = time.monotonic()
        _refresh_tokens(now)
        if _TOKENS > 0:
            _TOKENS -= 1
            return
        sleep_for = max(0.0, _RESET_AT - now)
        time.sleep(sleep_for or 0.1)


def _sleep_until_window_reset() -> None:
    now = time.monotonic()
    _refresh_tokens(now)
    sleep_for = max(0.0, _RESET_AT - now)
    time.sleep(sleep_for or 0.1)


def _sleep_backoff(attempt: int, *, base: float = 1.0, cap: float = 30.0) -> None:
    delay = min(cap, base * (2 ** max(0, attempt - 1)))
    jitter = random.random() * 0.5
    time.sleep(delay + jitter)


def _should_retry_rate_limit(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("status") != "error":
        return False
    message = (payload.get("message") or "").lower()
    return "credit" in message or "limit" in message


def _throttled_json_request(
    request: urllib.request.Request,
    *,
    timeout: int = 30,
    max_retries: int = MAX_RETRIES,
    error_cls=RuntimeError,
) -> Dict[str, Any]:
    """Perform a provider HTTP request with shared throttling + locking."""

    for attempt in range(1, max_retries + 1):
        with _provider_lock():
            _acquire_token_blocking()
            try:
                with _urlopen(request, timeout=timeout) as resp:  # nosec: B310
                    body = resp.read()
            except urllib.error.HTTPError as exc:  # pragma: no cover - network specific
                body = exc.read()
                if attempt < max_retries and exc.code in (429, 500, 502, 503, 504):
                    if exc.code == 429:
                        _sleep_until_window_reset()
                    else:
                        _sleep_backoff(attempt)
                    continue
                raise error_cls(f"market_data_http_error:{exc.code}") from exc
            except urllib.error.URLError as exc:  # pragma: no cover - network specific
                if attempt < max_retries:
                    _sleep_backoff(attempt)
                    continue
                raise error_cls(f"market_data_url_error:{exc.reason}") from exc

        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            if attempt < max_retries:
                _sleep_backoff(attempt)
                continue
            raise error_cls("market_data_invalid_json") from exc

        if _should_retry_rate_limit(payload) and attempt < max_retries:
            _sleep_until_window_reset()
            continue

        return payload

    raise error_cls("market_data_max_retries_exceeded")


def _fetch_ticker(
    ticker: str,
    start: str,
    end: str,
    api_key: str,
    max_retries: int = MAX_RETRIES,
) -> list[dict]:
    url = _build_time_series_url(ticker, start, end, api_key)
    request = urllib.request.Request(url, headers={"User-Agent": "sustainacore-index-engine"})

    payload = _throttled_json_request(request, max_retries=max_retries, error_cls=RuntimeError)

    if isinstance(payload, dict) and payload.get("status") == "error":
        message = payload.get("message") or "market_data_error"
        raise RuntimeError(message)

    return _parse_rows(payload, ticker)


def fetch_eod_prices(tickers: Iterable[str], start: str, end: str) -> list[dict]:
    """
    Return rows with:
      ticker, trade_date (YYYY-MM-DD), close, adj_close, volume, currency
    """

    tickers_list = [t.strip().upper() for t in tickers if t and str(t).strip()]
    if not tickers_list:
        return []

    api_key = _get_api_key()
    if not api_key:
        raise RuntimeError("MARKET_DATA_API_KEY is not set")

    rows: List[dict] = []
    if start == end:
        trade_date = _dt.date.fromisoformat(start)
        for ticker in sorted(set(tickers_list)):
            rows.extend(fetch_single_day_bar(ticker, trade_date, api_key=api_key))
        return rows

    for ticker in sorted(set(tickers_list)):
        rows.extend(_fetch_ticker(ticker, start, end, api_key))
    return rows


def _build_api_url(path: str, params: Dict[str, Any]) -> str:
    safe_params = {k: v for k, v in params.items() if v is not None}
    base = _get_api_base()
    return f"{base}/{path}?{urllib.parse.urlencode(safe_params)}"


def _get_api_key(explicit: Optional[str] = None) -> str:
    if explicit:
        return explicit
    for env_var in _API_KEY_ENV_VARS:
        raw = os.getenv(env_var)
        if raw:
            return raw.strip()
    raise MarketDataProviderError("MARKET_DATA_API_KEY is not configured")


def _request_json(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = _build_api_url(path, params)
    req = urllib.request.Request(url)
    return _throttled_json_request(req, error_cls=MarketDataProviderError)


def fetch_api_usage(api_key: Optional[str] = None) -> Dict[str, Any]:
    """Fetch the current API usage/plan details."""

    key = _get_api_key(api_key)
    payload = _request_json("api_usage", {"apikey": key})

    if isinstance(payload, dict) and payload.get("status") == "error":
        message = payload.get("message") or "unknown error"
        raise MarketDataProviderError(f"Market data api_usage error: {message}")

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
        raise MarketDataProviderError(f"Market data time_series error for {ticker}: {message}")

    values = payload.get("values") or payload.get("data") or []
    if not isinstance(values, list):
        return []
    return values


def fetch_latest_bar(
    ticker: str,
    *,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch the most recent daily bar for a ticker."""

    key = _get_api_key(api_key)
    payload = _request_json(
        "time_series",
        {
            "symbol": ticker,
            "interval": "1day",
            "outputsize": 1,
            "order": "DESC",
            "timezone": "Exchange",
            "apikey": key,
        },
    )

    message = str(payload.get("message") or "").lower()
    if payload.get("status") == "error":
        if "no data is available" in message:
            return []
        raise MarketDataProviderError(
            f"Market data time_series error for {ticker}: {payload.get('message')}"
        )

    values = payload.get("values") or payload.get("data") or []
    if not isinstance(values, list):
        return []
    return values


def fetch_daily_window_desc(
    ticker: str,
    *,
    window: int = 10,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch a small descending window of daily bars for robustness."""

    key = _get_api_key(api_key)
    payload = _request_json(
        "time_series",
        {
            "symbol": ticker,
            "interval": "1day",
            "outputsize": window,
            "order": "DESC",
            "timezone": "Exchange",
            "adjust": "all",
            "apikey": key,
        },
    )

    message = str(payload.get("message") or "").lower()
    if payload.get("status") == "error":
        if "no data is available" in message:
            return []
        raise MarketDataProviderError(
            f"Market data time_series error for {ticker}: {payload.get('message')}"
        )

    values = payload.get("values") or payload.get("data") or []
    if not isinstance(values, list):
        return []
    return values


def fetch_single_day_bar(
    ticker: str,
    trade_date: _dt.date,
    *,
    api_key: Optional[str] = None,
    window: int = 10,
) -> List[Dict[str, Any]]:
    """Fetch a specific trade_date by scanning a descending window."""

    values = fetch_daily_window_desc(ticker, window=window, api_key=api_key)
    for entry in values:
        if _extract_trade_date(entry) == trade_date:
            payload = {"values": [entry]}
            return _parse_rows(payload, ticker)
    return []


def fetch_latest_eod_date(ticker: str, *, api_key: Optional[str] = None) -> _dt.date:
    """Return the latest available EOD trade date for the ticker."""

    key = _get_api_key(api_key)
    payload = _request_json(
        "time_series",
        {
            "symbol": ticker,
            "interval": "1day",
            "outputsize": 1,
            "order": "DESC",
            "timezone": "Exchange",
            "adjust": "all",
            "apikey": key,
        },
    )

    message = str(payload.get("message") or "").lower()
    if payload.get("status") == "error":
        if "no data is available" in message:
            raise MarketDataProviderError(f"Market data time_series empty for {ticker}")
        raise MarketDataProviderError(
            f"Market data time_series error for {ticker}: {payload.get('message')}"
        )

    values = payload.get("values") or payload.get("data") or []
    if not isinstance(values, list) or not values:
        raise MarketDataProviderError(f"Market data time_series empty for {ticker}")

    trade_date = _extract_trade_date(values[0])
    if trade_date is None:
        raise MarketDataProviderError(f"Market data time_series missing date for {ticker}")
    return trade_date


def has_eod_for_date(ticker: str, date: _dt.date, *, api_key: Optional[str] = None) -> bool:
    """Return True if provider already exposes an end-of-day bar for the date."""

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
        raise MarketDataProviderError(
            f"Market data time_series error for {ticker}: {payload.get('message')}"
        )

    values: Iterable[Dict[str, Any]] = payload.get("values") or payload.get("data") or []
    for entry in values:
        trade_date = _extract_trade_date(entry)
        if trade_date == date:
            return True
    return False


_load_throttle_config()


__all__ = [
    "get_throttle_config",
    "fetch_api_usage",
    "fetch_eod_prices",
    "fetch_latest_bar",
    "fetch_daily_window_desc",
    "fetch_single_day_bar",
    "fetch_latest_eod_date",
    "fetch_time_series",
    "has_eod_for_date",
    "remaining_credits",
    "MarketDataProviderError",
]
