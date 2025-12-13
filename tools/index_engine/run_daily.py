import datetime as _dt
import importlib.util
import os
import sys
from pathlib import Path


DEFAULT_START = _dt.date(2025, 1, 2)
DEFAULT_BUFFER = 25
BUFFER_ENV = "SC_IDX_TWELVEDATA_CREDIT_BUFFER"
PROBE_SYMBOL_ENV = "SC_IDX_PROBE_SYMBOL"


def _load_provider_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "providers" / "twelvedata.py"
    spec = importlib.util.spec_from_file_location("twelvedata_provider", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load Twelve Data provider module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _load_ingest_module():
    module_path = Path(__file__).resolve().parent / "ingest_prices.py"
    spec = importlib.util.spec_from_file_location("index_engine_ingest", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load ingest_prices module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _select_end_date(provider, probe_symbol: str, today_utc: _dt.date) -> _dt.date:
    try:
        if provider.has_eod_for_date(probe_symbol, today_utc):
            return today_utc
    except Exception as exc:
        print(
            f"warning: falling back to yesterday; could not probe {probe_symbol}: {exc}",
            file=sys.stderr,
        )
    return today_utc - _dt.timedelta(days=1)


def main() -> int:
    provider = _load_provider_module()
    ingest_module = _load_ingest_module()
    try:
        usage = provider.fetch_api_usage()
    except Exception as exc:
        print(f"error fetching Twelve Data usage: {exc}", file=sys.stderr)
        return 1

    remaining = provider.remaining_credits(usage)
    buffer = _env_int(BUFFER_ENV, DEFAULT_BUFFER)
    max_provider_calls = max(0, remaining - buffer - 1)

    today_utc = _dt.datetime.now(_dt.timezone.utc).date()
    probe_symbol = os.getenv(PROBE_SYMBOL_ENV, "AAPL")
    end_date = _select_end_date(provider, probe_symbol, today_utc)

    print(
        f"index_engine_daily: end={end_date.isoformat()} remaining={remaining} "
        f"buffer={buffer} max_provider_calls={max_provider_calls}"
    )

    if max_provider_calls <= 0:
        print("budget_stop: provider_calls_used=0 max_provider_calls=0")
        return 0

    ingest_args = [
        "--backfill",
        "--start",
        DEFAULT_START.isoformat(),
        "--end",
        end_date.isoformat(),
        "--max-provider-calls",
        str(max_provider_calls),
    ]

    tickers_env = os.getenv("SC_IDX_TICKERS")
    if tickers_env:
        ingest_args.extend(["--tickers", tickers_env])

    return ingest_module.main(ingest_args)


if __name__ == "__main__":
    sys.exit(main())
