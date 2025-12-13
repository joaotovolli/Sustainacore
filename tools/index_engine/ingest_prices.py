import argparse
import datetime as _dt
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple


_FETCH_TIME_SERIES: Optional[Callable[[str, _dt.date, _dt.date], List[Dict[str, Any]]]] = None


DEFAULT_TICKERS = ("AAPL",)
DEFAULT_CALL_INTERVAL = float(os.getenv("SC_IDX_PROVIDER_INTERVAL_SECONDS", "7.5"))
DEFAULT_DATA_DIR = Path(__file__).resolve().parent / "data"


def _load_provider_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "providers" / "twelvedata.py"
    spec = importlib.util.spec_from_file_location("twelvedata_provider", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load Twelve Data provider module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _default_fetcher() -> Callable[[str, _dt.date, _dt.date], List[Dict[str, Any]]]:
    global _FETCH_TIME_SERIES
    if _FETCH_TIME_SERIES is not None:
        return _FETCH_TIME_SERIES

    try:
        from app.providers import twelvedata as _provider
    except Exception:
        _provider = _load_provider_module()

    _FETCH_TIME_SERIES = _provider.fetch_time_series
    return _FETCH_TIME_SERIES


def _parse_date(raw: str) -> _dt.date:
    try:
        return _dt.date.fromisoformat(raw)
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {raw}") from exc


def _parse_tickers(raw: Optional[str]) -> List[str]:
    if not raw:
        return list(DEFAULT_TICKERS)
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def _record_trade_date(entry: Dict[str, Any]) -> Optional[_dt.date]:
    for key in ("trade_date", "datetime", "date"):
        raw = entry.get(key)
        if raw is None:
            continue
        text = str(raw)
        try:
            return _dt.date.fromisoformat(text)
        except ValueError:
            try:
                return _dt.datetime.fromisoformat(text.replace("Z", "+00:00")).date()
            except ValueError:
                continue
    return None


def _load_existing(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    records: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def _write_records(path: Path, records: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")


def _latest_date(records: Iterable[Dict[str, Any]]) -> Optional[_dt.date]:
    latest: Optional[_dt.date] = None
    for record in records:
        trade_date = _record_trade_date(record)
        if trade_date and (latest is None or trade_date > latest):
            latest = trade_date
    return latest


def _merge_records(
    ticker: str,
    existing: List[Dict[str, Any]],
    new_values: List[Dict[str, Any]],
    start_date: _dt.date,
    end_date: _dt.date,
) -> Tuple[List[Dict[str, Any]], int]:
    merged: Dict[_dt.date, Dict[str, Any]] = {}
    for record in existing:
        trade_date = _record_trade_date(record)
        if trade_date:
            merged[trade_date] = record

    new_count = 0
    for entry in new_values:
        trade_date = _record_trade_date(entry)
        if trade_date is None:
            continue
        if trade_date < start_date or trade_date > end_date:
            continue

        record = dict(entry)
        record["ticker"] = ticker
        record["trade_date"] = trade_date.isoformat()
        if trade_date not in merged:
            new_count += 1
        merged[trade_date] = record

    ordered = [merged[date] for date in sorted(merged)]
    return ordered, new_count


def backfill_prices(
    tickers: Iterable[str],
    start_date: _dt.date,
    end_date: _dt.date,
    *,
    max_provider_calls: Optional[int] = None,
    fetcher: Optional[Callable[[str, _dt.date, _dt.date], List[Dict[str, Any]]]] = None,
    data_dir: Path = DEFAULT_DATA_DIR,
    sleep_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    fetch_fn = fetcher or _default_fetcher()
    tickers_list = [ticker.strip().upper() for ticker in tickers if ticker.strip()]
    call_interval = DEFAULT_CALL_INTERVAL if sleep_seconds is None else sleep_seconds

    provider_calls_used = 0
    tickers_completed = 0
    records_written = 0

    for index, ticker in enumerate(tickers_list):
        if max_provider_calls is not None and provider_calls_used >= max_provider_calls:
            print(
                f"budget_stop: provider_calls_used={provider_calls_used} "
                f"max_provider_calls={max_provider_calls}"
            )
            break

        existing = _load_existing(data_dir / f"{ticker}.jsonl")
        latest_existing = _latest_date(existing)
        effective_start = start_date
        if latest_existing and latest_existing >= start_date:
            effective_start = latest_existing + _dt.timedelta(days=1)

        if effective_start > end_date:
            continue

        provider_calls_used += 1
        try:
            values = fetch_fn(ticker, effective_start, end_date)
        except Exception as exc:  # pragma: no cover - transports provider errors
            print(f"error fetching {ticker}: {exc}", file=sys.stderr)
            continue

        merged, new_count = _merge_records(ticker, existing, values, effective_start, end_date)
        if new_count:
            _write_records(data_dir / f"{ticker}.jsonl", merged)
            records_written += new_count

        tickers_completed += 1

        if call_interval > 0 and index < len(tickers_list) - 1:
            time.sleep(call_interval)

    return {
        "provider_calls_used": provider_calls_used,
        "tickers_requested": len(tickers_list),
        "tickers_completed": tickers_completed,
        "records_written": records_written,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill prices from Twelve Data.")
    parser.add_argument("--backfill", action="store_true", help="Run a backfill between start/end dates.")
    parser.add_argument("--start", type=_parse_date, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=_parse_date, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--tickers",
        type=str,
        help="Comma-separated list of tickers. Defaults to SC_IDX_TICKERS or a minimal built-in list.",
    )
    parser.add_argument(
        "--max-provider-calls",
        type=int,
        default=None,
        help="Maximum Twelve Data requests to issue this run.",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=str(DEFAULT_DATA_DIR),
        help="Directory for cached price files.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.backfill:
        parser.error("Only backfill mode is supported at this time.")

    if not args.start or not args.end:
        parser.error("--start and --end are required for backfill")

    tickers = _parse_tickers(args.tickers or os.getenv("SC_IDX_TICKERS"))
    summary = backfill_prices(
        tickers,
        args.start,
        args.end,
        max_provider_calls=args.max_provider_calls,
        data_dir=Path(args.data_dir),
    )

    print(
        f"ingest_complete: tickers={summary['tickers_completed']}/"
        f"{summary['tickers_requested']} provider_calls_used={summary['provider_calls_used']} "
        f"records_written={summary['records_written']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
