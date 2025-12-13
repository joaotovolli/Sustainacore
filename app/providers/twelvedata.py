import datetime as _dt
import json
import logging
import os
from typing import Any, Dict, Iterable, List, Optional
from urllib import error as _url_error
from urllib import parse as _url_parse
from urllib.request import Request as _Request
from urllib.request import urlopen as _urlopen


_LOGGER = logging.getLogger(__name__)
_API_BASE = "https://api.twelvedata.com"
_API_KEY_ENV_VARS = ("SC_TWELVEDATA_API_KEY", "TWELVEDATA_API_KEY")


class TwelveDataError(RuntimeError):
    """Raised when Twelve Data returns an unexpected error."""


def _get_api_key(explicit: Optional[str] = None) -> str:
    if explicit:
        return explicit
    for env_var in _API_KEY_ENV_VARS:
        raw = os.getenv(env_var)
        if raw:
            return raw.strip()
    raise TwelveDataError("TWELVEDATA_API_KEY is not configured")


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


def _build_url(path: str, params: Dict[str, Any]) -> str:
    safe_params = {k: v for k, v in params.items() if v is not None}
    return f"{_API_BASE}/{path}?{_url_parse.urlencode(safe_params)}"


def _request_json(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = _build_url(path, params)
    req = _Request(url)
    try:
        with _urlopen(req, timeout=30) as resp:
            payload = resp.read()
    except _url_error.HTTPError as exc:  # pragma: no cover - exercised via URLError branch
        payload = exc.read()
    except _url_error.URLError as exc:
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
    "fetch_time_series",
    "has_eod_for_date",
    "remaining_credits",
    "TwelveDataError",
]
