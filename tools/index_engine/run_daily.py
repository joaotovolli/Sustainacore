import datetime as _dt
import importlib.util
import os
import sys
from pathlib import Path


DEFAULT_START = _dt.date(2025, 1, 2)
DEFAULT_BUFFER = 25
DEFAULT_DAILY_LIMIT = 800
BUFFER_ENV = "SC_IDX_TWELVEDATA_CREDIT_BUFFER"
DAILY_LIMIT_ENV = "SC_IDX_TWELVEDATA_DAILY_LIMIT"
DAILY_BUFFER_ENV = "SC_IDX_TWELVEDATA_DAILY_BUFFER"
PROBE_SYMBOL_ENV = "SC_IDX_PROBE_SYMBOL"


def _load_provider_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "providers" / "twelvedata.py"
    spec = importlib.util.spec_from_file_location("twelvedata_provider", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load Twelve Data provider module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _load_run_log_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "index_engine" / "run_log.py"
    spec = importlib.util.spec_from_file_location("index_engine_run_log", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load run_log module")
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


def _compute_daily_budget(daily_limit: int, daily_buffer: int, calls_used_today: int) -> tuple[int, int]:
    remaining_daily = max(0, daily_limit - calls_used_today)
    max_provider_calls = max(0, remaining_daily - daily_buffer)
    return remaining_daily, max_provider_calls


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
    run_log = _load_run_log_module()
    ingest_module = _load_ingest_module()
    try:
        usage = provider.fetch_api_usage()
    except Exception as exc:
        usage = {}
        current_usage = None
        plan_limit = None
        print(f"warning: unable to fetch Twelve Data usage (per-minute probe only): {exc}", file=sys.stderr)
    else:
        current_usage = usage.get("current_usage")
        plan_limit = usage.get("plan_limit")

    calls_used_today = run_log.fetch_calls_used_today("TWELVEDATA")
    daily_limit = _env_int(DAILY_LIMIT_ENV, DEFAULT_DAILY_LIMIT)
    daily_buffer = _env_int(DAILY_BUFFER_ENV, _env_int(BUFFER_ENV, DEFAULT_BUFFER))
    remaining_daily, max_provider_calls = _compute_daily_budget(daily_limit, daily_buffer, calls_used_today)

    today_utc = _dt.datetime.now(_dt.timezone.utc).date()
    probe_symbol = os.getenv(PROBE_SYMBOL_ENV, "AAPL")
    end_date = _select_end_date(provider, probe_symbol, today_utc)

    print(
        "index_engine_daily: end={end} calls_used_today={used} remaining_daily={remaining_daily} "
        "daily_limit={daily_limit} daily_buffer={daily_buffer} max_provider_calls={max_calls} "
        "minute_limit={minute_limit} minute_used={minute_used}".format(
            end=end_date.isoformat(),
            used=calls_used_today,
            remaining_daily=remaining_daily,
            daily_limit=daily_limit,
            daily_buffer=daily_buffer,
            max_calls=max_provider_calls,
            minute_limit=plan_limit,
            minute_used=current_usage,
        )
    )

    if max_provider_calls <= 0:
        print(
            "daily_budget_stop: provider_calls_used=0 max_provider_calls=0 "
            f"calls_used_today={calls_used_today} daily_limit={daily_limit} daily_buffer={daily_buffer}"
        )
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
