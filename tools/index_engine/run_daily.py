from __future__ import annotations

import argparse
import datetime as _dt
import importlib.util
import os
import sys
import subprocess
import uuid
from pathlib import Path

APP_PATH = Path(__file__).resolve().parents[2] / "app"
if str(APP_PATH) not in sys.path:
    sys.path.insert(0, str(APP_PATH))

from index_engine.alerts import send_email
from index_engine.db import fetch_latest_trading_day_on_or_before
from index_engine.run_log import fetch_calls_used_today, finish_run, start_run
from index_engine.run_report import format_run_report
from tools.index_engine.run_lock import run_lock
from tools.oracle.env_bootstrap import load_env_files

DEFAULT_START = _dt.date(2025, 1, 2)
DEFAULT_BUFFER = 25
DEFAULT_DAILY_LIMIT = 800
BUFFER_ENV = "SC_IDX_TWELVEDATA_CREDIT_BUFFER"
DAILY_LIMIT_ENV = "SC_IDX_TWELVEDATA_DAILY_LIMIT"
DAILY_BUFFER_ENV = "SC_IDX_TWELVEDATA_DAILY_BUFFER"
PROBE_SYMBOL_ENV = "SC_IDX_PROBE_SYMBOL"
EMAIL_ON_BUDGET_STOP_ENV = "SC_IDX_EMAIL_ON_BUDGET_STOP"


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


def _load_trading_days_module():
    module_path = Path(__file__).resolve().parent / "update_trading_days.py"
    spec = importlib.util.spec_from_file_location("index_engine_trading_days", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load update_trading_days module")
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


def _select_end_date(provider, probe_symbol: str, today_utc: _dt.date) -> _dt.date | None:
    try:
        provider_latest = provider.fetch_latest_eod_date(probe_symbol)
    except Exception as exc:
        print(
            f"warning: falling back to yesterday; could not fetch latest EOD for {probe_symbol}: {exc}",
            file=sys.stderr,
        )
        provider_latest = today_utc - _dt.timedelta(days=1)

    guard_date = min(provider_latest, today_utc - _dt.timedelta(days=1))
    return fetch_latest_trading_day_on_or_before(guard_date)


def main() -> int:
    load_env_files(
        paths=(
            "/etc/sustainacore/db.env",
            "/etc/sustainacore-ai/app.env",
            "/etc/sustainacore-ai/secrets.env",
        )
    )
    run_id = str(uuid.uuid4())
    provider = _load_provider_module()
    ingest_module = _load_ingest_module()
    trading_days_module = _load_trading_days_module()

    email_on_budget_stop = os.getenv(EMAIL_ON_BUDGET_STOP_ENV) == "1"
    today_utc = _dt.datetime.now(_dt.timezone.utc).date()

    with run_lock():
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

        calls_used_today = fetch_calls_used_today("TWELVEDATA")
        daily_limit = _env_int(DAILY_LIMIT_ENV, DEFAULT_DAILY_LIMIT)
        daily_buffer = _env_int(DAILY_BUFFER_ENV, _env_int(BUFFER_ENV, DEFAULT_BUFFER))
        remaining_daily, max_provider_calls = _compute_daily_budget(daily_limit, daily_buffer, calls_used_today)

        try:
            trading_days_module.update_trading_days(auto_extend=True)
        except Exception as exc:
            summary = {
                "run_id": run_id,
                "status": "ERROR",
                "error_msg": f"trading_days_update_failed:{exc}",
                "provider": "TWELVEDATA",
                "end_date": None,
                "max_provider_calls": max_provider_calls,
                "provider_calls_used": 0,
                "raw_upserts": 0,
                "canon_upserts": 0,
                "raw_ok": 0,
                "raw_missing": 0,
                "raw_error": 0,
                "max_ok_trade_date": None,
                "oracle_user": None,
                "usage_current": current_usage,
                "usage_limit": plan_limit,
                "usage_remaining": None,
            }
            finish_run(run_id, summary)
            _maybe_send_alert("ERROR", summary, run_id, email_on_budget_stop)
            return 1

        probe_symbol = os.getenv(PROBE_SYMBOL_ENV, "AAPL")
        end_date = _select_end_date(provider, probe_symbol, today_utc)
        if end_date is None:
            summary = {
                "run_id": run_id,
                "status": "ERROR",
                "error_msg": "no_trading_day_for_end_date",
                "provider": "TWELVEDATA",
                "end_date": None,
                "max_provider_calls": max_provider_calls,
                "provider_calls_used": 0,
                "raw_upserts": 0,
                "canon_upserts": 0,
                "raw_ok": 0,
                "raw_missing": 0,
                "raw_error": 0,
                "max_ok_trade_date": None,
                "oracle_user": None,
                "usage_current": current_usage,
                "usage_limit": plan_limit,
                "usage_remaining": None,
            }
            finish_run(run_id, summary)
            _maybe_send_alert("ERROR", summary, run_id, email_on_budget_stop)
            return 1

        usage_remaining = None
        if plan_limit is not None and current_usage is not None:
            try:
                usage_remaining = int(plan_limit) - int(current_usage)
            except Exception:
                usage_remaining = None

        run_log.start_run(
            run_id,
            job_name="sc_idx_price_ingest",
            provider="TWELVEDATA",
            start_date=DEFAULT_START,
            end_date=end_date,
            usage_current=current_usage,
            usage_limit=plan_limit,
            usage_remaining=usage_remaining,
            credit_buffer=daily_buffer,
            max_provider_calls=max_provider_calls,
            oracle_user=None,
        )

        summary = {
            "run_id": run_id,
            "status": "STARTED",
            "error_msg": None,
            "provider": "TWELVEDATA",
            "end_date": end_date,
            "max_provider_calls": max_provider_calls,
            "provider_calls_used": 0,
            "raw_upserts": 0,
            "canon_upserts": 0,
            "raw_ok": 0,
            "raw_missing": 0,
            "raw_error": 0,
            "max_ok_trade_date": None,
            "oracle_user": None,
            "usage_current": current_usage,
            "usage_limit": plan_limit,
            "usage_remaining": usage_remaining,
        }

        if os.getenv("SC_IDX_FORCE_FAIL") == "1":
            status = "ERROR"
            error_msg = "forced failure via SC_IDX_FORCE_FAIL=1"
            summary["status"] = status
            summary["error_msg"] = error_msg
            finish_run(run_id, summary)
            _maybe_send_alert(status, summary, run_id, email_on_budget_stop)
            return 1

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

        status = "OK"
        error_msg = None

        if max_provider_calls <= 0:
            print(
                "daily_budget_stop: provider_calls_used=0 max_provider_calls=0 "
                f"calls_used_today={calls_used_today} daily_limit={daily_limit} daily_buffer={daily_buffer}"
            )
            status = "DAILY_BUDGET_STOP"
            summary["status"] = status
            summary["provider_calls_used"] = 0
            finish_run(run_id, summary)
            _maybe_send_alert(status, summary, run_id, email_on_budget_stop)
            return 0

        tickers_env = os.getenv("SC_IDX_TICKERS")
        ingest_args = argparse.Namespace(
            date=None,
            start=DEFAULT_START.isoformat(),
            end=end_date.isoformat(),
            backfill=True,
            backfill_missing=False,
            tickers=tickers_env,
            debug=False,
            max_provider_calls=max_provider_calls,
        )

        exit_code = 0
        try:
            exit_code, ingest_summary = ingest_module._run_backfill(ingest_args)
            summary.update(ingest_summary or {})
            if exit_code != 0:
                status = "ERROR"
                error_msg = f"ingest_exit_code={exit_code}"
            if summary.get("raw_ok", 0) == 0 and summary.get("raw_missing", 0) > 0:
                status = "ERROR"
                error_msg = "ingest_missing_payloads"
        except Exception as exc:  # pragma: no cover - defensive
            status = "ERROR"
            error_msg = str(exc)
        summary["status"] = status
        summary["error_msg"] = error_msg
        finish_run(run_id, summary)

        _maybe_send_alert(status, summary, run_id, email_on_budget_stop)

        return exit_code


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


def _maybe_send_alert(status: str, summary: dict, run_id: str, email_on_budget_stop: bool) -> None:
    if status == "ERROR":
        tail_log = _safe_journal_tail()
        body = format_run_report(run_id, summary, tail_log)
        send_email(f"SC_IDX ingest ERROR on VM1 (run_id={run_id})", body)
    elif status == "DAILY_BUDGET_STOP" and email_on_budget_stop:
        body = format_run_report(run_id, summary, None)
        send_email(f"SC_IDX ingest DAILY_BUDGET_STOP on VM1 (run_id={run_id})", body)


if __name__ == "__main__":
    sys.exit(main())
