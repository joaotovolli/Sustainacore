"""Alpha Vantage daily adjusted price fetcher with simple throttling."""
from __future__ import annotations

import fcntl
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from typing import Any, Tuple

API_URL = "https://www.alphavantage.co/query"
FUNCTION = "TIME_SERIES_DAILY_ADJUSTED"
API_KEY_ENV = "ALPHAVANTAGE_API_KEY"
DEFAULT_CALLS_PER_WINDOW = 4
DEFAULT_WINDOW_SECONDS = 70
MAX_RETRIES = 5
LOCK_PATH = "/tmp/sc_idx_alphavantage.lock"

_CALLS_PER_WINDOW: int
_WINDOW_SECONDS: int
_TOKENS: int
_RESET_AT: float
_LOGGER = logging.getLogger(__name__)
_urlopen = urllib.request.urlopen
CALLS_PER_WINDOW_ENV = "SC_IDX_ALPHAVANTAGE_CALLS_PER_WINDOW"
WINDOW_SECONDS_ENV = "SC_IDX_ALPHAVANTAGE_WINDOW_SECONDS"


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
    _CALLS_PER_WINDOW = _env_int(CALLS_PER_WINDOW_ENV, DEFAULT_CALLS_PER_WINDOW)
    _WINDOW_SECONDS = _env_int(WINDOW_SECONDS_ENV, DEFAULT_WINDOW_SECONDS)
    _TOKENS = _CALLS_PER_WINDOW
    _RESET_AT = time.monotonic() + _WINDOW_SECONDS
    return _CALLS_PER_WINDOW, _WINDOW_SECONDS


_load_throttle_config()


@contextmanager
def _provider_lock():
    """Cross-process lock to serialize Alpha Vantage calls."""

    handle = open(LOCK_PATH, "a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            handle.close()


def _refresh_tokens(now: float) -> None:
    global _TOKENS, _RESET_AT
    if now >= _RESET_AT:
        _TOKENS = _CALLS_PER_WINDOW
        _RESET_AT = now + _WINDOW_SECONDS


def _acquire_token_blocking() -> None:
    global _TOKENS
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


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _get_api_key(explicit: str | None = None) -> str:
    if explicit:
        return explicit.strip()
    raw = os.getenv(API_KEY_ENV)
    if not raw:
        raise RuntimeError("ALPHAVANTAGE_API_KEY is not set")
    return raw.strip()


def _build_url(ticker: str, outputsize: str, api_key: str) -> str:
    params = {
        "function": FUNCTION,
        "symbol": ticker,
        "outputsize": outputsize,
        "apikey": api_key,
        "datatype": "json",
    }
    return f"{API_URL}?{urllib.parse.urlencode(params)}"


def _extract_error_message(payload: dict[str, Any]) -> tuple[str | None, bool]:
    for key in ("Note", "Information"):
        if key in payload:
            value = payload.get(key)
            return (str(value).strip() if value is not None else "rate limit"), True

    if "Error Message" in payload:
        value = payload.get("Error Message")
        return (str(value).strip() if value is not None else "unknown error"), False

    return None, False


def _throttled_json_request(
    request: urllib.request.Request,
    *,
    timeout: int = 30,
    max_retries: int = MAX_RETRIES,
) -> dict[str, Any]:
    for attempt in range(1, max_retries + 1):
        with _provider_lock():
            _acquire_token_blocking()
            try:
                with _urlopen(request, timeout=timeout) as resp:  # nosec: B310
                    body = resp.read()
            except urllib.error.HTTPError as exc:
                body = exc.read()
                if exc.code == 429 and attempt < max_retries:
                    _sleep_until_window_reset()
                    continue
                raise RuntimeError(f"alphavantage_http_error:{exc.code}") from exc
            except urllib.error.URLError as exc:
                raise RuntimeError(f"alphavantage_url_error:{exc.reason}") from exc

        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            if attempt < max_retries:
                _sleep_until_window_reset()
                continue
            raise RuntimeError("alphavantage_invalid_json") from exc

        if not isinstance(payload, dict):
            if attempt < max_retries:
                _sleep_until_window_reset()
                continue
            raise RuntimeError("alphavantage_invalid_payload")

        message, is_rate_limit = _extract_error_message(payload)
        if message:
            if is_rate_limit and attempt < max_retries:
                _sleep_until_window_reset()
                continue
            raise RuntimeError(message)

        return payload

    raise RuntimeError("alphavantage_max_retries_exceeded")


def _parse_rows(payload: dict[str, Any], ticker: str) -> list[dict]:
    time_series = payload.get("Time Series (Daily)")
    if not isinstance(time_series, dict):
        return []

    rows: list[dict] = []
    for trade_date in sorted(time_series.keys()):
        entry = time_series.get(trade_date)
        if not isinstance(entry, dict):
            continue
        close = _to_float(entry.get("4. close"))
        adj_close = _to_float(entry.get("5. adjusted close"))
        volume = _to_int(entry.get("6. volume"))
        rows.append(
            {
                "ticker": ticker,
                "trade_date": trade_date,
                "close": close,
                "adj_close": adj_close,
                "volume": volume,
                "currency": None,
            }
        )
    return rows


def fetch_daily_adjusted(ticker: str, *, outputsize: str) -> list[dict]:
    """
    Returns rows with ticker, trade_date (YYYY-MM-DD), close, adj_close, volume, currency=None.
    """

    normalized_ticker = ticker.strip().upper()
    if not normalized_ticker:
        return []

    if outputsize not in {"compact", "full"}:
        raise ValueError("outputsize must be 'compact' or 'full'")

    api_key = _get_api_key()
    url = _build_url(normalized_ticker, outputsize, api_key)
    request = urllib.request.Request(url, headers={"User-Agent": "sustainacore-index-engine"})
    payload = _throttled_json_request(request)
    return _parse_rows(payload, normalized_ticker)
