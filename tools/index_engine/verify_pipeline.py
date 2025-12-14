"""Verification helper for the SC_IDX ingest pipeline."""
from __future__ import annotations

import datetime as _dt
import os
import pathlib
import sys
from typing import Any, Dict, List

ROOT_PATH = pathlib.Path(__file__).resolve().parents[2]
APP_PATH = ROOT_PATH / "app"
for path in (ROOT_PATH, APP_PATH):
    if str(path) not in sys.path:
        sys.path.append(str(path))

from db_helper import get_connection
from providers.twelvedata import TwelveDataError, fetch_api_usage, fetch_latest_bar

ENV_FILES = ("/etc/sustainacore-ai/secrets.env", "/etc/sustainacore/db.env")


def _load_env_files() -> None:
    """Load key=value pairs from known env files without printing secrets."""

    for path in ENV_FILES:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                for line in handle:
                    text = line.strip()
                    if not text or text.startswith("#"):
                        continue
                    if text.startswith("export "):
                        text = text[len("export ") :].strip()
                    name, sep, value = text.partition("=")
                    if sep != "=":
                        continue
                    key = name.strip()
                    if not key or key in os.environ:
                        continue
                    os.environ[key] = value.strip().strip("\"'")
        except (FileNotFoundError, PermissionError):
            continue
        except Exception:
            continue


def _fetch_latest_bar_for_symbol(symbol: str) -> tuple[list[dict], str | None, str | None]:
    try:
        rows = fetch_latest_bar(symbol)
    except Exception as exc:  # pragma: no cover - network / env driven
        return [], None, f"provider_error: {exc}"
    if not rows:
        return [], None, "provider_empty: no rows returned"

    first = rows[0] if isinstance(rows[0], dict) else {}
    latest_dt = str(first.get("datetime") or first.get("date") or first.get("trade_date") or "").strip() or None
    return rows, latest_dt, None


def _query_oracle() -> Dict[str, Any]:
    with get_connection() as conn:
        cur = conn.cursor()

        cur.execute("SELECT USER FROM dual")
        user_row = cur.fetchone()
        oracle_user = str(user_row[0]).strip() if user_row and user_row[0] is not None else None

        cur.execute(
            "SELECT MAX(ingested_at) FROM SC_IDX_PRICES_RAW WHERE provider = 'TWELVEDATA'"
        )
        ingested_row = cur.fetchone()
        max_ingested = ingested_row[0] if ingested_row else None

        cur.execute(
            "SELECT MAX(trade_date) FROM SC_IDX_PRICES_RAW "
            "WHERE status = 'OK' AND provider = 'TWELVEDATA'"
        )
        max_ok_row = cur.fetchone()
        max_ok_trade_date = max_ok_row[0] if max_ok_row else None

        cur.execute(
            "SELECT trade_date, status, COUNT(*) "
            "FROM SC_IDX_PRICES_RAW "
            "WHERE trade_date >= TRUNC(SYSDATE) - 3 "
            "AND provider = 'TWELVEDATA' "
            "GROUP BY trade_date, status "
            "ORDER BY trade_date DESC, status"
        )
        daily_counts = [
            {"trade_date": row[0], "status": row[1], "count": row[2]}
            for row in cur.fetchall()
        ]

        cur.execute("SELECT COUNT(*) FROM SC_IDX_PRICES_RAW WHERE provider = 'TWELVEDATA'")
        raw_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM SC_IDX_PRICES_CANON")
        canon_count = cur.fetchone()[0]

    return {
        "oracle_user": oracle_user,
        "max_ingested_at": max_ingested,
        "max_ok_trade_date": max_ok_trade_date,
        "daily_counts": daily_counts,
        "raw_count": raw_count,
        "canon_count": canon_count,
    }


def _coerce_datetime(value: Any) -> _dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value
    if isinstance(value, _dt.date):
        return _dt.datetime.combine(value, _dt.time.min)
    return None


def _print_usage(label: str, usage: Dict[str, Any]) -> None:
    current = usage.get("current_usage")
    limit = usage.get("plan_limit")
    category = usage.get("plan_category")
    ts = usage.get("timestamp")
    print(f"{label}: current_usage={current} plan_limit={limit} plan_category={category} as_of={ts}")


def main() -> int:
    _load_env_files()

    print("== Twelve Data usage check ==")
    try:
        usage_before = fetch_api_usage()
    except TwelveDataError as exc:
        print(f"FAIL unable to fetch Twelve Data usage before probe: {exc}")
        return 1
    _print_usage("usage_before", usage_before)

    provider_rows, latest_dt, provider_error = _fetch_latest_bar_for_symbol("AAPL")

    try:
        usage_after = fetch_api_usage()
    except TwelveDataError as exc:
        print(f"FAIL unable to fetch Twelve Data usage after probe: {exc}")
        return 1
    _print_usage("usage_after", usage_after)

    print(
        "provider_call: symbol=AAPL provider_rows={rows} latest_datetime={latest_dt} provider_error={error}".format(
            rows=len(provider_rows),
            latest_dt=latest_dt,
            error=provider_error,
        )
    )

    before_value = usage_before.get("current_usage") or 0
    after_value = usage_after.get("current_usage") or 0
    delta = after_value - before_value
    if delta > 0:
        print(f"PASS credits delta observed: delta={delta} latest_datetime={latest_dt}")
    else:
        print(f"FAIL credits did not change: delta={delta} latest_datetime={latest_dt} provider_error={provider_error}")

    print("\n== Oracle verification ==")
    try:
        oracle_data = _query_oracle()
    except Exception as exc:  # pragma: no cover - env specific
        print(f"FAIL oracle query failed: {exc}")
        return 1

    print(f"oracle_user={oracle_data.get('oracle_user')}")
    print(f"max_ingested_at={oracle_data.get('max_ingested_at')}")
    print(f"max_ok_trade_date={oracle_data.get('max_ok_trade_date')}")
    print("raw_counts_last_3_days:")
    for row in oracle_data.get("daily_counts", []):
        td = row.get("trade_date")
        status = row.get("status")
        count = row.get("count")
        print(f"  {td} status={status} count={count}")
    print(f"raw_row_count={oracle_data.get('raw_count')} canon_row_count={oracle_data.get('canon_count')}")

    now = _dt.datetime.now(_dt.timezone.utc)
    ingested_at = _coerce_datetime(oracle_data.get("max_ingested_at"))
    if ingested_at is not None:
        ingested_at = ingested_at.replace(tzinfo=_dt.timezone.utc) if ingested_at.tzinfo is None else ingested_at.astimezone(_dt.timezone.utc)
    ingested_recent = ingested_at is not None and (now - ingested_at).total_seconds() <= 3 * 24 * 3600
    if ingested_recent:
        print("PASS oracle ingested_at updated recently")
    else:
        print("FAIL oracle ingested_at not recent")

    raw_ok = (oracle_data.get("raw_count") or 0) > 0
    canon_ok = (oracle_data.get("canon_count") or 0) > 0
    if raw_ok and canon_ok:
        print("PASS raw/canon rows exist")
    else:
        print("FAIL raw/canon rows missing or zero")

    if delta > 0 and ingested_recent and raw_ok and canon_ok:
        print("\nOVERALL: PASS")
        return 0

    print("\nOVERALL: FAIL")
    return 2


if __name__ == "__main__":
    sys.exit(main())
