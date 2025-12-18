"""Twelve Data EOD price fetcher (throttled for free tier limits)."""
from __future__ import annotations

import datetime as _dt
import fcntl
import importlib.util
import json
import logging
import os
import sys
import sysconfig
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Tuple

API_URL = "https://api.twelvedata.com/time_series"
_API_BASE = "https://api.twelvedata.com"
_API_KEY_ENV_VARS = ("SC_TWELVEDATA_API_KEY", "TWELVEDATA_API_KEY")
DEFAULT_CALLS_PER_WINDOW = 6
DEFAULT_WINDOW_SECONDS = 120
MAX_RETRIES = 5
_LOCK_PATH = "/tmp/sc_idx_twelvedata.lock"
_CALLS_PER_WINDOW: int
_WINDOW_SECONDS: int
_TOKENS: int
_RESET_AT: float
_CALLS_PER_WINDOW_ENV = "SC_IDX_TWELVEDATA_CALLS_PER_WINDOW"
_WINDOW_SECONDS_ENV = "SC_IDX_TWELVEDATA_WINDOW_SECONDS"


def _ensure_stdlib_http_client() -> None:
    """Load stdlib http.client even if app.http shadows the stdlib package."""

    if "http.client" in sys.modules:
        return
    stdlib = sysconfig.get_paths().get("stdlib")
    if not stdlib:
        return
    http_init = os.path.join(stdlib, "http", "__init__.py")
    http_client = os.path.join(stdlib, "http", "client.py")
    if os.path.isfile(http_init) and "http" not in sys.modules:
        spec = importlib.util.spec_from_file_location("http", http_init)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            sys.modules["http"] = module
            spec.loader.exec_module(module)
    if os.path.isfile(http_client):
        spec = importlib.util.spec_from_file_location("http.client", http_client)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            sys.modules["http.client"] = module
            spec.loader.exec_module(module)
            parent = sys.modules.get("http")
            if parent is not None and not hasattr(parent, "client"):
                setattr(parent, "client", module)


_ensure_stdlib_http_client()
import http.client  # noqa: E402  # preload stdlib http to avoid app.http shadowing
import urllib.error  # noqa: E402
import urllib.parse  # noqa: E402
import urllib.request  # noqa: E402

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
    """Cross-process lock to serialize Twelve Data calls."""

    handle = open(_LOCK_PATH, "a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            handle.close()


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
    """Perform a Twelve Data HTTP request with shared throttling + locking."""

    for attempt in range(1, max_retries + 1):
        with _provider_lock():
            _acquire_token_blocking()
            try:
                with _urlopen(request, timeout=timeout) as resp:  # nosec: B310
                    body = resp.read()
            except urllib.error.HTTPError as exc:  # pragma: no cover - network specific
                body = exc.read()
                if exc.code == 429 and attempt < max_retries:
                    _sleep_until_window_reset()
                    continue
                raise error_cls(f"twelvedata_http_error:{exc.code}") from exc
            except urllib.error.URLError as exc:  # pragma: no cover - network specific
                raise error_cls(f"twelvedata_url_error:{exc.reason}") from exc

        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            if attempt < max_retries:
                _sleep_until_window_reset()
                continue
            raise error_cls("twelvedata_invalid_json") from exc

        if _should_retry_rate_limit(payload) and attempt < max_retries:
            _sleep_until_window_reset()
            continue

        return payload

    raise error_cls("twelvedata_max_retries_exceeded")


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
        message = payload.get("message") or "twelvedata_error"
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
    return _throttled_json_request(req, error_cls=TwelveDataError)


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
        raise TwelveDataError(f"Twelve Data time_series error for {ticker}: {payload.get('message')}")

    values = payload.get("values") or payload.get("data") or []
    if not isinstance(values, list):
        return []
    return values


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
            raise TwelveDataError(f"Twelve Data time_series empty for {ticker}")
        raise TwelveDataError(f"Twelve Data time_series error for {ticker}: {payload.get('message')}")

    values = payload.get("values") or payload.get("data") or []
    if not isinstance(values, list) or not values:
        raise TwelveDataError(f"Twelve Data time_series empty for {ticker}")

    trade_date = _extract_trade_date(values[0])
    if trade_date is None:
        raise TwelveDataError(f"Twelve Data time_series missing date for {ticker}")
    return trade_date


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


_load_throttle_config()


__all__ = [
    "get_throttle_config",
    "fetch_api_usage",
    "fetch_eod_prices",
    "fetch_latest_eod_date",
    "fetch_latest_bar",
    "fetch_time_series",
    "has_eod_for_date",
    "remaining_credits",
    "TwelveDataError",
]
