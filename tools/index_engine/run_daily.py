from __future__ import annotations

import argparse
import datetime as _dt
import importlib.util
import os
import subprocess
import sys
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.index_engine.env_loader import load_default_env
from tools.index_engine.oracle_preflight import (
    collect_wallet_diagnostics,
    format_wallet_diagnostics,
    probe_oracle_user,
)

DEFAULT_START = _dt.date(2025, 1, 2)
DEFAULT_BUFFER = 25
DEFAULT_DAILY_LIMIT = 800
BUFFER_ENV = "SC_IDX_MARKET_DATA_CREDIT_BUFFER"
DAILY_LIMIT_ENV = "SC_IDX_MARKET_DATA_DAILY_LIMIT"
DAILY_BUFFER_ENV = "SC_IDX_MARKET_DATA_DAILY_BUFFER"
PROBE_SYMBOL_ENV = "SC_IDX_PROBE_SYMBOL"
EMAIL_ON_BUDGET_STOP_ENV = "SC_IDX_EMAIL_ON_BUDGET_STOP"
TRADING_DAYS_RETRY_ENV = "SC_IDX_TRADING_DAYS_RETRY_ATTEMPTS"
TRADING_DAYS_RETRY_BASE_ENV = "SC_IDX_TRADING_DAYS_RETRY_BASE_SEC"


def _load_provider_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "providers" / "market_data_provider.py"
    spec = importlib.util.spec_from_file_location("market_data_provider", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load market data provider module")
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


def _load_trading_days_module():
    module_path = Path(__file__).resolve().parent / "update_trading_days.py"
    spec = importlib.util.spec_from_file_location("index_engine_trading_days", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load update_trading_days module")
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


def _load_run_report_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "index_engine" / "run_report.py"
    spec = importlib.util.spec_from_file_location("index_engine_run_report", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load run_report module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _load_alerts_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "index_engine" / "alerts.py"
    spec = importlib.util.spec_from_file_location("index_engine_alerts", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load alerts module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _load_alert_state_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "index_engine" / "alert_state.py"
    spec = importlib.util.spec_from_file_location("index_engine_alert_state", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load alert_state module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _load_db_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "index_engine" / "db.py"
    spec = importlib.util.spec_from_file_location("index_engine_db", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load db module")
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


def compute_eligible_end_date(
    *,
    provider_latest: _dt.date,
    today_utc: _dt.date,
    trading_days: list[_dt.date],
) -> _dt.date | None:
    guard_date = min(provider_latest, today_utc - _dt.timedelta(days=1))
    eligible = [day for day in trading_days if day <= guard_date]
    if not eligible:
        return None
    return max(eligible)


def select_effective_end_date(
    provider_latest: _dt.date,
    trading_days: list[_dt.date],
) -> _dt.date | None:
    eligible = [day for day in trading_days if day <= provider_latest]
    if not eligible:
        return None
    return max(eligible)


def select_next_missing_trading_day(
    trading_days: list[_dt.date],
    max_canon_trade_date: _dt.date | None,
) -> _dt.date | None:
    for day in trading_days:
        if max_canon_trade_date is None or day > max_canon_trade_date:
            return day
    return None


def _resolve_end_date(
    provider,
    probe_symbol: str,
    today_utc: _dt.date,
) -> tuple[_dt.date, list[_dt.date], _dt.date | None]:
    try:
        provider_latest = provider.fetch_latest_eod_date(probe_symbol)
    except Exception as exc:
        print(
            f"warning: falling back to yesterday; could not fetch latest EOD for {probe_symbol}: {exc}",
            file=sys.stderr,
        )
        provider_latest = today_utc - _dt.timedelta(days=1)

    trading_days = _db_module.fetch_trading_days(DEFAULT_START, today_utc)
    if not trading_days:
        return provider_latest, trading_days, None
    end_date = compute_eligible_end_date(
        provider_latest=provider_latest,
        today_utc=today_utc,
        trading_days=trading_days,
    )
    return provider_latest, trading_days, end_date


def _probe_with_fallback(
    start_date: _dt.date,
    trading_days: list[_dt.date],
    *,
    probe_fn,
) -> tuple[_dt.date | None, list[_dt.date]]:
    tried: list[_dt.date] = []
    for day in sorted(trading_days, reverse=True):
        if day > start_date:
            continue
        tried.append(day)
        status = probe_fn(day)
        if status == "NO_DATA":
            continue
        if status == "OK":
            return day, tried
        return None, tried
    return None, tried


def _select_end_date(provider, probe_symbol: str, today_utc: _dt.date) -> _dt.date | None:
    _provider_latest, _trading_days, end_date = _resolve_end_date(provider, probe_symbol, today_utc)
    return end_date


def _safe_journal_tail() -> str | None:
    try:
        proc = subprocess.run(
            ["journalctl", "-u", "sc-idx-price-ingest.service", "-n", "120", "--no-pager"],
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            return proc.stdout
    except Exception:
        return None
    return None


def _send_once_per_day(alert_name: str, subject: str, body: str, status: str) -> None:
    try:
        should_send = should_send_alert_once_per_day(alert_name, detail=body, status=status)
    except Exception:
        should_send = True
    if should_send:
        send_email(subject, body)


def _maybe_send_alert(status: str, summary: dict, run_id: str, email_on_budget_stop: bool) -> None:
    try:
        if status == "ERROR":
            tail_log = _safe_journal_tail()
            body = format_run_report(run_id, summary, tail_log)
            _send_once_per_day(
                "sc_idx_ingest_error",
                f"SC_IDX ingest ERROR on VM1 (run_id={run_id})",
                body,
                "ERROR",
            )
        elif status == "DAILY_BUDGET_STOP" and email_on_budget_stop:
            body = format_run_report(run_id, summary, None)
            _send_once_per_day(
                "sc_idx_budget_stop",
                f"SC_IDX ingest DAILY_BUDGET_STOP on VM1 (run_id={run_id})",
                body,
                "DAILY_BUDGET_STOP",
            )
    except Exception as exc:  # pragma: no cover - defensive
        print(f"warning: failed to send alert: {exc}", file=sys.stderr)


def _oracle_preflight_or_exit(*, run_id: str, today_utc: _dt.date, email_on_budget_stop: bool) -> str | None:
    try:
        oracle_user = probe_oracle_user()
        if oracle_user:
            return oracle_user
    except Exception as exc:
        diag = collect_wallet_diagnostics()
        print(f"oracle_preflight_failed: {exc}", file=sys.stderr)
        print(format_wallet_diagnostics(diag), file=sys.stderr)

    try:
        start_run(
            "sc_idx_price_ingest",
            end_date=today_utc,
            provider="MARKET_DATA",
            max_provider_calls=0,
            meta={
                "run_id": run_id,
                "start_date": DEFAULT_START,
                "oracle_user": None,
            },
        )
        finish_run(
            run_id,
            status="ERROR",
            provider_calls_used=0,
            raw_upserts=0,
            canon_upserts=0,
            raw_ok=0,
            raw_missing=0,
            raw_error=0,
            max_provider_calls=0,
            usage_current=None,
            usage_limit=None,
            usage_remaining=None,
            oracle_user=None,
            error="oracle_preflight_failed",
        )
    except Exception:
        pass

    try:
        _maybe_send_alert(
            "ERROR",
            {"status": "ERROR", "error_msg": "oracle_preflight_failed"},
            run_id,
            email_on_budget_stop,
        )
    except Exception:
        pass
    return None


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SC_IDX daily ingest runner")
    parser.add_argument("--debug", action="store_true", help="Enable verbose ingest diagnostics")
    parser.add_argument("--dry-run", action="store_true", help="Print latest EOD date and exit")
    parser.add_argument("--force-backfill-tickers", help="Comma-separated tickers to backfill regardless of env")
    if argv is None and os.getenv("PYTEST_CURRENT_TEST"):
        argv = []
    return parser.parse_args(argv)


_alerts_module = _load_alerts_module()
_run_log_module = _load_run_log_module()
_run_report_module = _load_run_report_module()
_alert_state_module = _load_alert_state_module()
_db_module = _load_db_module()

send_email = _alerts_module.send_email
start_run = _run_log_module.start_run
finish_run = _run_log_module.finish_run
fetch_calls_used_today = _run_log_module.fetch_calls_used_today
format_run_report = _run_report_module.format_run_report
should_send_alert_once_per_day = _alert_state_module.should_send_alert_once_per_day


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    load_default_env()

    run_id = str(uuid.uuid4())
    email_on_budget_stop = os.getenv(EMAIL_ON_BUDGET_STOP_ENV) == "1"
    today_utc = _dt.datetime.now(_dt.timezone.utc).date()
    force_fail = os.getenv("SC_IDX_FORCE_FAIL") == "1"

    oracle_user = None
    if not force_fail:
        oracle_user = _oracle_preflight_or_exit(
            run_id=run_id,
            today_utc=today_utc,
            email_on_budget_stop=email_on_budget_stop,
        )
        if oracle_user is None:
            return 2

    provider = _load_provider_module()
    ingest_module = _load_ingest_module()
    trading_days_module = _load_trading_days_module()

    try:
        usage = provider.fetch_api_usage()
        current_usage = usage.get("current_usage")
        plan_limit = usage.get("plan_limit")
    except Exception as exc:
        current_usage = None
        plan_limit = None
        print(f"warning: unable to fetch provider usage (per-minute probe only): {exc}", file=sys.stderr)

    calls_used_today = fetch_calls_used_today("MARKET_DATA")
    daily_limit = _env_int(DAILY_LIMIT_ENV, DEFAULT_DAILY_LIMIT)
    daily_buffer = _env_int(DAILY_BUFFER_ENV, _env_int(BUFFER_ENV, DEFAULT_BUFFER))
    remaining_daily, max_provider_calls = _compute_daily_budget(daily_limit, daily_buffer, calls_used_today)

    summary = {
        "run_id": run_id,
        "status": "STARTED",
        "error_msg": None,
        "provider": "MARKET_DATA",
        "start_date": DEFAULT_START,
        "end_date": today_utc,
        "max_provider_calls": max_provider_calls,
        "provider_calls_used": 0,
        "raw_upserts": 0,
        "canon_upserts": 0,
        "raw_ok": 0,
        "raw_missing": 0,
        "raw_error": 0,
        "max_ok_trade_date": None,
        "oracle_user": oracle_user,
        "usage_current": current_usage,
        "usage_limit": plan_limit,
        "usage_remaining": None,
    }

    if force_fail:
        start_run(
            "sc_idx_price_ingest",
            end_date=today_utc,
            provider="MARKET_DATA",
            max_provider_calls=0,
            meta={
                "run_id": run_id,
                "start_date": DEFAULT_START,
                "oracle_user": oracle_user,
                "usage_current": current_usage,
                "usage_limit": plan_limit,
                "usage_remaining": None,
                "credit_buffer": None,
            },
        )
        summary["status"] = "ERROR"
        summary["error_msg"] = "forced failure via SC_IDX_FORCE_FAIL=1"
        finish_run(
            run_id,
            status="ERROR",
            provider_calls_used=0,
            raw_upserts=0,
            canon_upserts=0,
            raw_ok=0,
            raw_missing=0,
            raw_error=0,
            max_provider_calls=0,
            usage_current=current_usage,
            usage_limit=plan_limit,
            usage_remaining=None,
            oracle_user=oracle_user,
            error=summary["error_msg"],
        )
        _maybe_send_alert("ERROR", summary, run_id, email_on_budget_stop)
        return 1

    trading_days_refresh_failed = False
    trading_days_warning = None
    try:
        max_attempts = _env_int(TRADING_DAYS_RETRY_ENV, 3)
        backoff_base = float(os.getenv(TRADING_DAYS_RETRY_BASE_ENV, "1"))
        updated, update_error = trading_days_module.update_trading_days_with_retry(
            auto_extend=True,
            max_attempts=max_attempts,
            backoff_base_sec=backoff_base,
            allow_cached_on_403=True,
        )
        if not updated:
            trading_days_refresh_failed = True
            summary["trading_days_update"] = "cached_calendar"
            summary["trading_days_update_error"] = update_error
            trading_days_warning = f"trading_days_update_cached:{update_error}"
            print(
                "update_trading_days: degraded to cached calendar error={error}".format(
                    error=update_error,
                ),
                file=sys.stderr,
            )
    except Exception as exc:
        summary["status"] = "ERROR"
        summary["error_msg"] = f"trading_days_update_failed:{exc}"
        finish_run(
            run_id,
            status="ERROR",
            provider_calls_used=0,
            raw_upserts=0,
            canon_upserts=0,
            raw_ok=0,
            raw_missing=0,
            raw_error=0,
            max_provider_calls=max_provider_calls,
            usage_current=current_usage,
            usage_limit=plan_limit,
            usage_remaining=None,
            oracle_user=oracle_user,
            error=summary["error_msg"],
        )
        _maybe_send_alert("ERROR", summary, run_id, email_on_budget_stop)
        return 1

    probe_symbol = os.getenv(PROBE_SYMBOL_ENV, "AAPL")
    provider_latest, trading_days, end_date = _resolve_end_date(provider, probe_symbol, today_utc)
    trading_days_max = trading_days[-1] if trading_days else None
    if trading_days_max and provider_latest and trading_days_max < provider_latest:
        if trading_days_refresh_failed:
            print(
                "warning: trading_days behind provider; using cached calendar "
                f"max_trading_day={trading_days_max.isoformat()} provider_latest={provider_latest.isoformat()}",
                file=sys.stderr,
            )
            provider_latest = trading_days_max
            end_date = compute_eligible_end_date(
                provider_latest=provider_latest,
                today_utc=today_utc,
                trading_days=trading_days,
            )
        else:
            summary["status"] = "ERROR"
            summary["error_msg"] = (
                "trading_days_behind_provider:"
                f"latest={provider_latest.isoformat()} max_trading_day={trading_days_max.isoformat()}"
            )
            finish_run(
                run_id,
                status="ERROR",
                provider_calls_used=0,
                raw_upserts=0,
                canon_upserts=0,
                raw_ok=0,
                raw_missing=0,
                raw_error=0,
                max_provider_calls=max_provider_calls,
                usage_current=current_usage,
                usage_limit=plan_limit,
                usage_remaining=None,
                oracle_user=oracle_user,
                error=summary["error_msg"],
            )
            _maybe_send_alert("ERROR", summary, run_id, email_on_budget_stop)
            return 1
    if end_date is None:
        summary["status"] = "ERROR"
        summary["error_msg"] = "no_trading_day_for_end_date"
        finish_run(
            run_id,
            status="ERROR",
            provider_calls_used=0,
            raw_upserts=0,
            canon_upserts=0,
            raw_ok=0,
            raw_missing=0,
            raw_error=0,
            max_provider_calls=max_provider_calls,
            usage_current=current_usage,
            usage_limit=plan_limit,
            usage_remaining=None,
            oracle_user=oracle_user,
            error=summary["error_msg"],
        )
        _maybe_send_alert("ERROR", summary, run_id, email_on_budget_stop)
        return 1

    max_canon_trade_date = _db_module.fetch_max_canon_trade_date()
    next_missing_trade_date = select_next_missing_trading_day(trading_days, max_canon_trade_date)
    start_date = next_missing_trade_date or end_date
    if start_date > end_date:
        start_date = end_date

    if args.dry_run:
        print(f"latest_eod_date_spy={end_date.isoformat()}")
        return 0

    usage_remaining = None
    if plan_limit is not None and current_usage is not None:
        try:
            usage_remaining = int(plan_limit) - int(current_usage)
        except Exception:
            usage_remaining = None
    summary["usage_remaining"] = usage_remaining
    summary["end_date"] = end_date
    summary["start_date"] = start_date

    start_run(
        "sc_idx_price_ingest",
        end_date=end_date,
        provider="MARKET_DATA",
        max_provider_calls=max_provider_calls,
        meta={
            "run_id": run_id,
            "start_date": start_date,
            "oracle_user": oracle_user,
            "usage_current": current_usage,
            "usage_limit": plan_limit,
            "usage_remaining": usage_remaining,
            "credit_buffer": daily_buffer,
        },
    )

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
        summary["status"] = "DAILY_BUDGET_STOP"
        finish_run(
            run_id,
            status="DAILY_BUDGET_STOP",
            provider_calls_used=0,
            raw_upserts=0,
            canon_upserts=0,
            raw_ok=0,
            raw_missing=0,
            raw_error=0,
            max_provider_calls=max_provider_calls,
            usage_current=current_usage,
            usage_limit=plan_limit,
            usage_remaining=usage_remaining,
            oracle_user=oracle_user,
            error=None,
        )
        _maybe_send_alert("DAILY_BUDGET_STOP", summary, run_id, email_on_budget_stop)
        return 0

    tickers_env = args.force_backfill_tickers or os.getenv("SC_IDX_TICKERS")
    ingest_args = argparse.Namespace(
        date=None,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        backfill=True,
        backfill_missing=False,
        tickers=tickers_env,
        debug=args.debug,
        max_provider_calls=max_provider_calls,
    )

    exit_code = 0
    error_msg = None
    ingest_summary: dict = {}
    try:
        exit_code, ingest_summary = ingest_module._run_backfill(ingest_args)
        if not isinstance(ingest_summary, dict):
            ingest_summary = {}
        if exit_code != 0:
            error_msg = f"ingest_exit_code={exit_code}"
    except Exception as exc:  # pragma: no cover - defensive
        exit_code = 1
        error_msg = str(exc)

    summary.update(ingest_summary)
    status = "OK" if exit_code == 0 else "ERROR"
    summary["status"] = status
    summary["error_msg"] = error_msg

    max_ok_trade_date = summary.get("max_ok_trade_date")
    if status == "OK":
        if max_ok_trade_date is None:
            if max_canon_trade_date is None or max_canon_trade_date < end_date:
                status = "ERROR"
                summary["status"] = status
                summary["error_msg"] = "ingest_missing_ok_prices"
        elif isinstance(max_ok_trade_date, _dt.date) and max_ok_trade_date < end_date:
            status = "ERROR"
            summary["status"] = status
            summary["error_msg"] = (
                "ingest_incomplete:"
                f"max_ok_trade_date={max_ok_trade_date.isoformat()} target={end_date.isoformat()}"
            )

    if status == "OK":
        if summary["error_msg"] is None and trading_days_warning:
            summary["error_msg"] = trading_days_warning
        try:
            missing_tickers = _db_module.fetch_missing_real_for_trade_date(end_date)
        except Exception as exc:
            missing_tickers = []
            print(f"warning: failed to check missing tickers for {end_date}: {exc}", file=sys.stderr)
        if missing_tickers:
            status = "ERROR"
            summary["status"] = status
            sample = ",".join(missing_tickers[:10])
            summary["error_msg"] = (
                "missing_prices_for_date:"
                f"date={end_date.isoformat()} missing_count={len(missing_tickers)} sample={sample}"
            )

    finish_run(
        run_id,
        status=status,
        provider_calls_used=summary.get("provider_calls_used"),
        raw_upserts=summary.get("raw_upserts"),
        canon_upserts=summary.get("canon_upserts"),
        raw_ok=summary.get("raw_ok"),
        raw_missing=summary.get("raw_missing"),
        raw_error=summary.get("raw_error"),
        max_provider_calls=max_provider_calls,
        usage_current=current_usage,
        usage_limit=plan_limit,
        usage_remaining=usage_remaining,
        oracle_user=oracle_user,
        error=summary.get("error_msg"),
    )

    _maybe_send_alert(status, summary, run_id, email_on_budget_stop)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
