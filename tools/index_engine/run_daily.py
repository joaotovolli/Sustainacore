from __future__ import annotations

import argparse
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
EMAIL_ON_BUDGET_STOP_ENV = "SC_IDX_EMAIL_ON_BUDGET_STOP"
IMPUTATION_REPLACEMENT_DAYS_ENV = "SC_IDX_IMPUTED_REPLACEMENT_DAYS"
IMPUTATION_REPLACEMENT_LIMIT_ENV = "SC_IDX_IMPUTED_REPLACEMENT_LIMIT"
DIGEST_ALWAYS_ENV = "SC_IDX_DAILY_DIGEST_ALWAYS"


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


_alerts_module = _load_alerts_module()
_run_log_module = _load_run_log_module()
_run_report_module = _load_run_report_module()
_alert_state_module = _load_alert_state_module()
_db_module = _load_db_module()

send_email = _alerts_module.send_email
fetch_calls_used_today = _run_log_module.fetch_calls_used_today
start_run = _run_log_module.start_run
finish_run = _run_log_module.finish_run
format_run_report = _run_report_module.format_run_report
should_send_alert_once_per_day = _alert_state_module.should_send_alert_once_per_day
fetch_trading_days = _db_module.fetch_trading_days
fetch_latest_trading_day_on_or_before = _db_module.fetch_latest_trading_day_on_or_before
fetch_impacted_tickers_for_trade_date = _db_module.fetch_impacted_tickers_for_trade_date
fetch_imputed_rows = _db_module.fetch_imputed_rows
fetch_imputations = _db_module.fetch_imputations


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


def _load_completeness_module():
    module_path = Path(__file__).resolve().parent / "check_price_completeness.py"
    spec = importlib.util.spec_from_file_location("index_engine_completeness", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load check_price_completeness module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _load_impute_module():
    module_path = Path(__file__).resolve().parent / "impute_missing_prices.py"
    spec = importlib.util.spec_from_file_location("index_engine_impute", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load impute_missing_prices module")
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


def select_effective_end_date(provider_latest: _dt.date, trading_days: list[_dt.date]) -> _dt.date | None:
    """Pick latest trading day on or before provider_latest."""
    if not trading_days:
        return None
    for day in reversed(trading_days):
        if day <= provider_latest:
            return day
    return None


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


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SC_IDX daily ingest runner")
    parser.add_argument("--debug", action="store_true", help="Enable verbose ingest diagnostics")
    parser.add_argument("--dry-run", action="store_true", help="Print latest EOD date and exit")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
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
    trading_days_module = _load_trading_days_module()
    completeness_module = _load_completeness_module()
    impute_module = _load_impute_module()

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

    try:
        provider_latest = provider.fetch_latest_eod_date("SPY")
    except Exception as exc:
        status = "ERROR"
        error_msg = f"latest_eod_probe_failed:{exc}"
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

    trading_days = fetch_trading_days(DEFAULT_START, provider_latest)
    effective_end_date = select_effective_end_date(provider_latest, trading_days)
    if effective_end_date is None:
        status = "ERROR"
        error_msg = "latest_trading_day_unavailable"
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

    end_date = effective_end_date
    print(f"latest_eod_date_spy={provider_latest.isoformat()} effective_end_date={end_date.isoformat()}")
    summary["end_date"] = end_date
    summary["max_provider_calls"] = max_provider_calls

    if args.dry_run:
        return 0

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

    try:
        trading_days_module.update_trading_days()
    except Exception as exc:
        status = "ERROR"
        error_msg = f"trading_days_update_failed:{exc}"
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
    if args.debug:
        ingest_args.append("--debug")

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

    completeness_result = completeness_module.run_check(
        start_date=DEFAULT_START,
        end_date=end_date,
        min_daily_coverage=1.0,
        max_bad_days=0,
        provider="CANON",
        allow_canon_close=False,
        allow_imputation=False,
        email_on_fail=False,
    )
    completeness_status = str(completeness_result.get("status"))
    if completeness_status == "FAIL":
        summary_text = str(completeness_result.get("summary") or "")
        _send_once_per_day(
            "sc_idx_completeness_fail",
            "SC_IDX completeness FAIL",
            summary_text,
            "FAIL",
        )
    impute_summary = {}
    if completeness_status == "FAIL":
        enable_imputation = os.getenv("SC_IDX_ENABLE_IMPUTATION", "1") != "0"
        if enable_imputation:
            try:
                impute_summary = impute_module.impute_missing_prices(
                    start_date=DEFAULT_START,
                    end_date=end_date,
                    allow_canon_close=True,
                    email_on_impute=True,
                )
                if impute_summary.get("missing_without_prior"):
                    status = "ERROR"
                    error_msg = "imputation_missing_prior"
                    summary["status"] = status
                    summary["error_msg"] = error_msg
            except Exception as exc:
                status = "ERROR"
                error_msg = f"imputation_failed:{exc}"
                summary["status"] = status
                summary["error_msg"] = error_msg

    missing_without_prior = int(impute_summary.get("missing_without_prior") or 0)
    imputed_total = int(impute_summary.get("total_imputed") or 0)
    if missing_without_prior > 0:
        body = f"missing_without_prior={missing_without_prior} end_date={end_date.isoformat()}"
        _send_once_per_day("sc_idx_missing_without_prior", "SC_IDX missing prior prices", body, "ERROR")

    replacement_days = _env_int(IMPUTATION_REPLACEMENT_DAYS_ENV, 30)
    replacement_limit = _env_int(IMPUTATION_REPLACEMENT_LIMIT_ENV, 10)
    trading_days = fetch_trading_days(DEFAULT_START, end_date)
    replacement_range = trading_days[-replacement_days:] if trading_days else []
    replacement_used = 0

    if replacement_range and replacement_limit > 0 and max_provider_calls is not None:
        remaining_calls = max(0, max_provider_calls - int(ingest_summary.get("provider_calls_used") or 0))
        replacement_limit = min(replacement_limit, remaining_calls)

    if replacement_range and replacement_limit > 0:
        imputed_rows = fetch_imputed_rows(replacement_range[0], replacement_range[-1])
        impacted_cache: dict[_dt.date, list[str]] = {}
        impacted_by_date = {
            trade_date: fetch_impacted_tickers_for_trade_date(trade_date, cache=impacted_cache)
            for trade_date in replacement_range
        }
        tickers_to_refresh = impute_module.select_replacement_tickers(
            imputed_rows,
            impacted_by_date,
            replacement_limit,
        )

        if tickers_to_refresh:
            try:
                provider_rows = provider.fetch_eod_prices(
                    tickers_to_refresh,
                    replacement_range[0].isoformat(),
                    replacement_range[-1].isoformat(),
                )
                raw_rows = ingest_module._build_raw_rows_from_provider(provider_rows)
                if raw_rows:
                    ingest_module.upsert_prices_raw(raw_rows)
                canon_rows = ingest_module.compute_canonical_rows(raw_rows)
                if canon_rows:
                    ingest_module.upsert_prices_canon(canon_rows)
                replacement_used = len(tickers_to_refresh)
            except Exception as exc:
                print(f"warning: replacement fetch failed: {exc}", file=sys.stderr)

    digest_days = trading_days[-30:] if trading_days else []
    digest_imputations = fetch_imputations(digest_days[0], digest_days[-1]) if digest_days else []
    impacted_cache: dict[_dt.date, list[str]] = {}
    impacted_by_date = {d: fetch_impacted_tickers_for_trade_date(d, cache=impacted_cache) for d in digest_days}
    imputed_by_ticker: dict[str, int] = {}
    for ticker, trade_date in digest_imputations:
        impacted = impacted_by_date.get(trade_date, [])
        if ticker not in impacted:
            continue
        imputed_by_ticker[ticker] = imputed_by_ticker.get(ticker, 0) + 1

    impacted_count = len(impacted_by_date.get(end_date, []))
    digest_imputed_total = sum(imputed_by_ticker.values())
    digest_body = (
        f"end_date={end_date.isoformat()}\n"
        f"impacted_constituents={impacted_count}\n"
        f"missing_without_prior={missing_without_prior}\n"
        f"imputed_count={digest_imputed_total}\n\n"
        "imputed_by_ticker_last_30d:\n"
        + "\n".join(
            f"- {ticker} count={count}"
            for ticker, count in sorted(imputed_by_ticker.items(), key=lambda item: (-item[1], item[0]))
        )
    )
    digest_always = os.getenv(DIGEST_ALWAYS_ENV) == "1"
    if digest_always or digest_imputed_total > 0 or missing_without_prior > 0:
        _send_once_per_day("sc_idx_daily_digest", "SC_IDX daily digest", digest_body, "OK")

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


def _send_once_per_day(alert_name: str, subject: str, body: str, status: str) -> None:
    if should_send_alert_once_per_day(alert_name, detail=body, status=status):
        send_email(subject, body)


def _maybe_send_alert(status: str, summary: dict, run_id: str, email_on_budget_stop: bool) -> None:
    try:
        if status == "ERROR":
            tail_log = _safe_journal_tail()
            body = format_run_report(run_id, summary, tail_log)
            _send_once_per_day("sc_idx_ingest_error", f"SC_IDX ingest ERROR on VM1 (run_id={run_id})", body, "ERROR")
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


if __name__ == "__main__":
    sys.exit(main())
