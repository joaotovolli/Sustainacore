from __future__ import annotations

import datetime as _dt
import http.client  # preload stdlib http to avoid app.http shadowing
import importlib.util
import os
import sys
import subprocess
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.index_engine.env_loader import load_default_env

load_default_env()

from tools.index_engine.oracle_preflight import (
    collect_wallet_diagnostics,
    format_wallet_diagnostics,
    probe_oracle_user,
)

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


def _load_alerts_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "index_engine" / "alerts.py"
    spec = importlib.util.spec_from_file_location("index_engine_alerts", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load alerts module")
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


_alerts_module = _load_alerts_module()
_run_log_module = _load_run_log_module()
_run_report_module = _load_run_report_module()

send_email = _alerts_module.send_email
fetch_calls_used_today = _run_log_module.fetch_calls_used_today
start_run = _run_log_module.start_run
finish_run = _run_log_module.finish_run
format_run_report = _run_report_module.format_run_report


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

def _oracle_preflight_or_exit(*, run_id: str, today_utc: _dt.date, email_on_budget_stop: bool) -> str | None:
    """
    Ensure Oracle connectivity/wallet is working before provider calls.

    On failure:
      - prints wallet diagnostics
      - writes a SC_IDX_JOB_RUNS row (best-effort) with status ERROR and error token oracle_preflight_failed
      - sends an email alert (best-effort)
      - returns None so caller can exit with code 2
    """

    try:
        oracle_user = probe_oracle_user()
        return oracle_user or "UNKNOWN"
    except Exception as exc:
        print("oracle_preflight_failed:", str(exc), file=sys.stderr)
        print(format_wallet_diagnostics(collect_wallet_diagnostics()), file=sys.stderr)

        try:
            start_run(
                "sc_idx_price_ingest",
                end_date=today_utc,
                provider="TWELVEDATA",
                max_provider_calls=0,
                meta={"run_id": run_id, "start_date": DEFAULT_START, "oracle_user": None},
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
            # DB may be unavailable; logging is best-effort.
            pass

        try:
            _maybe_send_alert("ERROR", {"status": "ERROR", "error_msg": "oracle_preflight_failed"}, run_id, email_on_budget_stop)
        except Exception:
            pass
        return None


def main() -> int:
    run_id = str(uuid.uuid4())
    status = "STARTED"
    error_msg = None
    email_on_budget_stop = os.getenv(EMAIL_ON_BUDGET_STOP_ENV) == "1"
    force_fail = os.getenv("SC_IDX_FORCE_FAIL") == "1"
    today_utc = _dt.datetime.now(_dt.timezone.utc).date()

    oracle_user = _oracle_preflight_or_exit(run_id=run_id, today_utc=today_utc, email_on_budget_stop=email_on_budget_stop)
    if oracle_user is None:
        return 2

    summary: dict = {
        "run_id": run_id,
        "status": status,
        "error_msg": None,
        "provider": "TWELVEDATA",
        "end_date": today_utc,
        "max_provider_calls": 0,
        "provider_calls_used": 0,
        "raw_upserts": 0,
        "canon_upserts": 0,
        "raw_ok": 0,
        "raw_missing": 0,
        "raw_error": 0,
        "max_ok_trade_date": None,
        "oracle_user": oracle_user,
        "usage_current": None,
        "usage_limit": None,
        "usage_remaining": None,
    }

    if force_fail:
        start_run(
            "sc_idx_price_ingest",
            end_date=today_utc,
            provider="TWELVEDATA",
            max_provider_calls=0,
            meta={
                "run_id": run_id,
                "start_date": DEFAULT_START,
                "usage_current": None,
                "usage_limit": None,
                "usage_remaining": None,
                "credit_buffer": None,
                "oracle_user": oracle_user,
            },
        )
        status = "ERROR"
        error_msg = "forced failure via SC_IDX_FORCE_FAIL=1"
        summary["status"] = status
        summary["error_msg"] = error_msg
        finish_run(
            run_id,
            status=status,
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
            oracle_user=oracle_user,
            error=error_msg,
        )
        _maybe_send_alert(status, summary, run_id, email_on_budget_stop)
        return 1

    provider = _load_provider_module()
    ingest_module = _load_ingest_module()

    try:
        usage = provider.fetch_api_usage()
        summary["usage_current"] = usage.get("current_usage")
        summary["usage_limit"] = usage.get("plan_limit")
    except Exception as exc:
        usage = {}
        print(f"warning: unable to fetch Twelve Data usage (per-minute probe only): {exc}", file=sys.stderr)

    current_usage = summary.get("usage_current")
    plan_limit = summary.get("usage_limit")

    calls_used_today = fetch_calls_used_today("TWELVEDATA")
    daily_limit = _env_int(DAILY_LIMIT_ENV, DEFAULT_DAILY_LIMIT)
    daily_buffer = _env_int(DAILY_BUFFER_ENV, _env_int(BUFFER_ENV, DEFAULT_BUFFER))
    remaining_daily, max_provider_calls = _compute_daily_budget(daily_limit, daily_buffer, calls_used_today)

    probe_symbol = os.getenv(PROBE_SYMBOL_ENV, "AAPL")
    end_date = _select_end_date(provider, probe_symbol, today_utc)
    summary["end_date"] = end_date
    summary["max_provider_calls"] = max_provider_calls

    usage_remaining = None
    if plan_limit is not None and current_usage is not None:
        try:
            usage_remaining = int(plan_limit) - int(current_usage)
        except Exception:
            usage_remaining = None
    summary["usage_remaining"] = usage_remaining

    start_run(
        "sc_idx_price_ingest",
        end_date=end_date,
        provider="TWELVEDATA",
        max_provider_calls=max_provider_calls,
        meta={
            "run_id": run_id,
            "start_date": DEFAULT_START,
            "usage_current": current_usage,
            "usage_limit": plan_limit,
            "usage_remaining": usage_remaining,
            "credit_buffer": daily_buffer,
            "oracle_user": oracle_user,
        },
    )

    if os.getenv("SC_IDX_FORCE_FAIL") == "1":
        status = "ERROR"
        error_msg = "forced failure via SC_IDX_FORCE_FAIL=1"
        summary["status"] = status
        summary["error_msg"] = error_msg
        finish_run(
            run_id,
            status=status,
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
            error=error_msg,
        )
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

    exit_code = 0

    if max_provider_calls <= 0:
        print(
            "daily_budget_stop: provider_calls_used=0 max_provider_calls=0 "
            f"calls_used_today={calls_used_today} daily_limit={daily_limit} daily_buffer={daily_buffer}"
        )
        status = "DAILY_BUDGET_STOP"
        summary["status"] = status
        finish_run(
            run_id,
            status=status,
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
        _maybe_send_alert(status, summary, run_id, email_on_budget_stop)
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

    ingest_summary: dict = {}
    try:
        exit_code, ingest_summary = ingest_module.run_ingest(ingest_args)
        if not isinstance(ingest_summary, dict):
            ingest_summary = {}
        status = "OK" if exit_code == 0 else "ERROR"
        if exit_code != 0:
            error_msg = f"ingest_exit_code={exit_code}"
    except Exception as exc:  # pragma: no cover - defensive
        status = "ERROR"
        error_msg = str(exc)
        exit_code = 1

    summary.update(ingest_summary)
    summary["status"] = status
    summary["error_msg"] = error_msg

    finish_run(
        run_id,
        status=status,
        provider_calls_used=ingest_summary.get("provider_calls_used"),
        raw_upserts=ingest_summary.get("raw_upserts"),
        canon_upserts=ingest_summary.get("canon_upserts"),
        raw_ok=ingest_summary.get("raw_ok"),
        raw_missing=ingest_summary.get("raw_missing"),
        raw_error=ingest_summary.get("raw_error"),
        max_provider_calls=max_provider_calls,
        usage_current=current_usage,
        usage_limit=plan_limit,
        usage_remaining=usage_remaining,
        oracle_user=oracle_user,
        error=error_msg,
    )

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
    try:
        if status == "ERROR":
            tail_log = _safe_journal_tail()
            body = format_run_report(run_id, summary, tail_log)
            send_email(f"SC_IDX ingest ERROR on VM1 (run_id={run_id})", body)
        elif status == "DAILY_BUDGET_STOP" and email_on_budget_stop:
            body = format_run_report(run_id, summary, None)
            send_email(f"SC_IDX ingest DAILY_BUDGET_STOP on VM1 (run_id={run_id})", body)
    except Exception as exc:  # pragma: no cover - defensive
        print(f"warning: failed to send alert: {exc}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
