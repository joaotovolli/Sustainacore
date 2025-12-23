from __future__ import annotations

import argparse
import datetime as _dt
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.index_engine.env_loader import load_default_env

load_default_env()

import app.providers.market_data_provider as provider  # noqa: E402
from tools.index_engine import run_daily  # noqa: E402

DEFAULT_START = _dt.date(2025, 1, 2)


def _probe(date_value: _dt.date) -> str:
    try:
        rows = provider.fetch_single_day_bar("SPY", date_value)
        return "OK" if rows else "NO_DATA"
    except Exception:
        return "ERROR"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Debug provider availability for SC_IDX ingest")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args(argv)

    today_utc = _dt.date.today()
    provider_latest = provider.fetch_latest_eod_date("SPY")
    trading_days = run_daily.fetch_trading_days(DEFAULT_START, provider_latest)
    candidate = run_daily.compute_eligible_end_date(
        provider_latest=provider_latest, today_utc=today_utc, trading_days=trading_days
    )

    print(f"provider_latest_eod_spy={provider_latest}")
    if candidate is None:
        print("candidate_end=None (no trading days)")
        return 0

    # probe candidate + 2 prior trading days if available
    probes = []
    idx = trading_days.index(candidate)
    for offset in range(0, 3):
        probe_idx = idx - offset
        if probe_idx < 0:
            break
        probes.append(trading_days[probe_idx])

    for date_value in probes:
        result = _probe(date_value)
        print(f"probe date={date_value} result={result}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
