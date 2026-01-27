from __future__ import annotations

import argparse
import datetime as _dt
import inspect
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.index_engine.env_loader import load_default_env
from tools.index_engine.pipeline_health import collect_health_snapshot, format_health_summary, write_health_artifact
from tools.index_engine.pipeline_state import PipelineStateStore

from index_engine import db as engine_db
from index_engine import db_index_calc
from index_engine.run_log import finish_run, start_run


PIPELINE_NAME = "sc_idx_pipeline"
BASE_DATE = _dt.date(2025, 1, 2)
TRANSIENT_ORA_CODES = {"ORA-12545", "ORA-29002"}


@dataclass
class StageResult:
    name: str
    status: str
    code: int
    error: str | None
    duration_sec: float


def _parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SC_IDX daily pipeline")
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume from the first incomplete stage (default: true)",
    )
    parser.add_argument("--restart", action="store_true", help="Force a fresh run (ignore resume state)")
    return parser.parse_args(argv)


def _git_head() -> str:
    try:
        proc = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            return proc.stdout.strip()
    except Exception:
        return "unknown"
    return "unknown"


def _select_next_missing_trading_day(
    trading_days: list[_dt.date],
    max_level_date: _dt.date | None,
) -> _dt.date | None:
    for day in trading_days:
        if max_level_date is None or day > max_level_date:
            return day
    return None


def _compute_impute_window(
    *,
    max_level_date: _dt.date | None,
    max_impute_date: _dt.date | None,
    lookback_days: int,
) -> tuple[_dt.date, _dt.date] | None:
    if max_level_date is None:
        return None
    start_candidates: list[_dt.date] = []
    if max_impute_date:
        start_candidates.append(max_impute_date + _dt.timedelta(days=1))
    if lookback_days > 0:
        start_candidates.append(max_level_date - _dt.timedelta(days=lookback_days))
    if not start_candidates:
        start = max_level_date
    else:
        start = max(start_candidates)
    if start > max_level_date:
        return None
    if start < BASE_DATE:
        start = BASE_DATE
    return start, max_level_date


def _is_oracle_transient_error(exc: Exception) -> bool:
    message = str(exc)
    if any(code in message for code in TRANSIENT_ORA_CODES):
        return True
    if exc.args:
        first = exc.args[0]
        code = getattr(first, "code", None)
        if isinstance(code, str) and code in TRANSIENT_ORA_CODES:
            return True
    return False


def _write_oracle_health_artifact(stage_name: str, error_text: str) -> Path:
    ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    path = REPO_ROOT / "tools" / "audit" / "output" / f"oracle_health_{ts}.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(
        [
            f"stage={stage_name}",
            f"timestamp_utc={ts}",
            f"error={error_text}",
            "hint=check DNS/wallet/SSL connectivity for ORA-12545/ORA-29002",
        ]
    )
    path.write_text(content + "\n", encoding="utf-8")
    return path


def _invoke_stage(name: str, func: Callable[[List[str]], int], args: List[str]) -> tuple[int, str | None]:
    sig = inspect.signature(func)
    detail = None
    if len(sig.parameters) == 0:
        original_argv = sys.argv[:]
        sys.argv = [f"run_pipeline_{name}"] + list(args)
        try:
            result = func()
        finally:
            sys.argv = original_argv
    else:
        result = func(args)

    if isinstance(result, tuple) and len(result) == 2:
        code, detail = result
    else:
        code = result
    if code is None:
        code = 0
    return int(code), detail


def _run_stage_with_retries(
    *,
    name: str,
    func: Callable[[List[str]], int],
    args: List[str],
    state_store: PipelineStateStore,
    run_id: str,
    max_attempts: int,
    backoff_base_sec: float,
) -> StageResult:
    attempt = 0
    while True:
        attempt += 1
        state_store.record_stage_start(run_id, name, details=f"attempt={attempt}/{max_attempts}")
        start_time = time.monotonic()
        error = None
        try:
            code, detail = _invoke_stage(name, func, args)
            if code != 0:
                error = detail or f"exit_code={code}"
        except Exception as exc:
            code = 1
            error = str(exc) or repr(exc)
            detail = None
            is_transient = _is_oracle_transient_error(exc)
            duration = time.monotonic() - start_time
            print(
                f"[pipeline] end {name} exit={code} duration_sec={duration:.1f} error={error}",
                flush=True,
            )
            if is_transient and attempt < max_attempts:
                time.sleep(backoff_base_sec * (2 ** (attempt - 1)))
                continue
            if is_transient:
                _write_oracle_health_artifact(name, error)
            state_store.record_stage_end(run_id, name, "FAILED", details=error)
            return StageResult(name=name, status="FAILED", code=code, error=error, duration_sec=duration)

        duration = time.monotonic() - start_time
        if error:
            print(
                f"[pipeline] end {name} exit={code} duration_sec={duration:.1f} error={error}",
                flush=True,
            )
        else:
            print(f"[pipeline] end {name} exit={code} duration_sec={duration:.1f}", flush=True)
        if code == 0:
            state_store.record_stage_end(run_id, name, "OK", details=detail)
            return StageResult(name=name, status="OK", code=code, error=None, duration_sec=duration)
        is_transient = False
        if error and any(code in error for code in TRANSIENT_ORA_CODES):
            is_transient = True
        if is_transient and attempt < max_attempts:
            time.sleep(backoff_base_sec * (2 ** (attempt - 1)))
            continue
        if is_transient:
            _write_oracle_health_artifact(name, error or "oracle_error")
        state_store.record_stage_end(run_id, name, "FAILED", details=error)
        return StageResult(name=name, status="FAILED", code=code, error=error, duration_sec=duration)


def _stage_durations_from_state(stage_records: dict) -> dict[str, float]:
    durations: dict[str, float] = {}
    for name, record in stage_records.items():
        started_at = record.started_at
        ended_at = record.ended_at
        if started_at and ended_at:
            durations[name] = max(0.0, (ended_at - started_at).total_seconds())
    return durations


def main(argv: List[str] | None = None) -> int:
    args = _parse_args(argv)
    load_default_env()
    skip_ingest = os.getenv("SC_IDX_PIPELINE_SKIP_INGEST") == "1"
    provider_base = os.getenv("MARKET_DATA_API_BASE_URL")
    provider_key = os.getenv("SC_MARKET_DATA_API_KEY") or os.getenv("MARKET_DATA_API_KEY")
    provider_ready = bool(provider_base and provider_key)

    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    head_commit = _git_head()
    print(
        "sc_idx_pipeline_run: started_at_utc={started} head={head} skip_ingest={skip} resume={resume}".format(
            started=started_at,
            head=head_commit,
            skip=skip_ingest,
            resume=args.resume and not args.restart,
        ),
        flush=True,
    )

    state_store = PipelineStateStore(pipeline_name=PIPELINE_NAME)
    resume_enabled = args.resume and not args.restart
    run_date = _dt.datetime.now(_dt.timezone.utc).date()
    resume_run_id = state_store.fetch_resume_run_id(run_date=run_date) if resume_enabled else None
    run_id = resume_run_id or state_store.create_run_id()

    try:
        start_run(
            PIPELINE_NAME,
            end_date=run_date,
            provider="PIPELINE",
            max_provider_calls=0,
            meta={"start_date": BASE_DATE, "run_id": run_id},
        )
    except Exception:
        pass

    stage_records = state_store.fetch_stage_statuses(run_id) if resume_run_id else {}
    stage_names = [
        "update_trading_days",
        "ingest_prices",
        "completeness_check",
        "calc_index",
        "impute",
    ]

    start_index = 0
    if resume_run_id:
        for idx, name in enumerate(stage_names):
            record = stage_records.get(name)
            if record and record.status == "OK":
                continue
            start_index = idx
            break
        else:
            start_index = len(stage_names)

    max_attempts = int(os.getenv("SC_IDX_ORACLE_RETRY_ATTEMPTS", "5"))
    backoff_base_sec = float(os.getenv("SC_IDX_ORACLE_RETRY_BASE_SEC", "1"))
    last_error: str | None = None
    stage_durations: dict[str, float] = {}

    def _refresh_targets() -> dict[str, _dt.date | None]:
        calendar_max = engine_db.fetch_latest_trading_day()
        if calendar_max is None:
            raise RuntimeError("no_trading_days_in_calendar")
        trading_days = engine_db.fetch_trading_days(BASE_DATE, calendar_max)
        max_level_date = db_index_calc.fetch_max_level_date()
        target_day = _select_next_missing_trading_day(trading_days, max_level_date)
        max_canon_date = engine_db.fetch_max_canon_trade_date()
        return {
            "calendar_max": calendar_max,
            "trading_days": trading_days,
            "max_level_date": max_level_date,
            "target_day": target_day,
            "max_canon_date": max_canon_date,
        }

    def _stage_update_trading_days(_args: List[str]) -> tuple[int, str]:
        from tools.index_engine import update_trading_days

        if skip_ingest and not provider_ready:
            return 0, "calendar_skip_missing_provider_env"
        update_trading_days.update_trading_days(auto_extend=True)
        return 0, "calendar_refreshed"

    def _stage_ingest(args_list: List[str]) -> int:
        from tools.index_engine import run_daily

        return run_daily.main(args_list)

    def _stage_completeness(_args: List[str]) -> tuple[int, str]:
        from tools.index_engine import check_price_completeness
        from tools.index_engine import ingest_prices

        target_info = _refresh_targets()
        target_day = target_info["target_day"]
        if target_day is None:
            return 0, "no_missing_trading_day"
        max_canon_date = target_info["max_canon_date"]
        if max_canon_date is None or max_canon_date < target_day:
            return (
                2,
                "canon_lag:"
                f"max_canon={max_canon_date} target_day={target_day.isoformat()}",
            )
        trading_days = target_info["trading_days"]
        end_day = None
        for day in reversed(trading_days):
            if day <= max_canon_date:
                end_day = day
                break
        if end_day is None:
            return 2, "canon_lag:no_trading_day_at_or_before_canon"

        missing = engine_db.fetch_missing_real_for_trade_date(target_day)
        if missing and not skip_ingest:
            retry_calls = min(10, max(1, len(missing)))
            ingest_args = argparse.Namespace(
                date=None,
                start=target_day.isoformat(),
                end=target_day.isoformat(),
                backfill=False,
                backfill_missing=True,
                tickers=",".join(sorted(set(missing))),
                debug=False,
                max_provider_calls=retry_calls,
            )
            ingest_prices._run_backfill_missing(ingest_args)
            missing = engine_db.fetch_missing_real_for_trade_date(target_day)

        if missing:
            sample = ",".join(missing[:10])
            action = "run_backfill_missing" if skip_ingest else "provider_missing"
            return (
                2,
                "missing_prices_for_date:"
                f"date={target_day.isoformat()} missing_count={len(missing)} "
                f"sample={sample} action={action}",
            )

        result = check_price_completeness.run_check(
            start_date=target_day,
            end_date=end_day,
            min_daily_coverage=1.0,
            max_bad_days=0,
            provider="CANON",
            allow_canon_close=True,
            allow_imputation=True,
            email_on_fail=False,
        )
        status = str(result.get("status"))
        summary_text = str(result.get("summary_text", ""))
        if status != "PASS":
            detail = f"canon_incomplete:{summary_text}" if summary_text else "canon_incomplete"
            return 2, detail
        return 0, f"coverage_ok window={target_day.isoformat()}..{end_day.isoformat()}"

    def _stage_calc_index(_args: List[str]) -> tuple[int, str]:
        from tools.index_engine import calc_index

        target_info = _refresh_targets()
        target_day = target_info["target_day"]
        if target_day is None:
            return 0, "no_missing_trading_day"
        max_canon_date = target_info["max_canon_date"]
        if max_canon_date is None or max_canon_date < target_day:
            return 2, f"canon_lag:max_canon={max_canon_date} target_day={target_day.isoformat()}"
        trading_days = target_info["trading_days"]
        end_day = None
        for day in reversed(trading_days):
            if day <= max_canon_date:
                end_day = day
                break
        if end_day is None:
            return 2, "canon_lag:no_trading_day_at_or_before_canon"

        calc_args = [
            "--start",
            target_day.isoformat(),
            "--end",
            end_day.isoformat(),
            "--strict",
            "--debug",
            "--no-preflight-self-heal",
        ]
        code, detail = _invoke_stage("index_calc", calc_index.main, calc_args)
        if code != 0:
            return code, detail or f"exit_code={code}"
        max_level_after = db_index_calc.fetch_max_level_date()
        if max_level_after is None or max_level_after < end_day:
            return (
                2,
                "levels_not_advanced:"
                f"target_day={target_day.isoformat()} end_day={end_day.isoformat()} "
                f"max_level={max_level_after}",
            )
        return 0, f"levels_advanced_to={max_level_after.isoformat()}"

    def _stage_impute(_args: List[str]) -> tuple[int, str]:
        from tools.index_engine import impute_missing_prices

        target_info = _refresh_targets()
        max_level_date = target_info["max_level_date"]
        lookback_days = int(os.getenv("SC_IDX_IMPUTE_LOOKBACK_DAYS", "30"))
        max_impute_date = engine_db.fetch_max_imputation_date()
        window = _compute_impute_window(
            max_level_date=max_level_date,
            max_impute_date=max_impute_date,
            lookback_days=lookback_days,
        )
        if window is None:
            return 0, "impute_up_to_date"
        start_date, end_date = window
        timeout_sec = os.getenv("SC_IDX_IMPUTE_TIMEOUT_SEC", "300")
        impute_args = [
            "--start",
            start_date.isoformat(),
            "--end",
            end_date.isoformat(),
            "--allow-canon-close",
            "--max-runtime-sec",
            timeout_sec,
        ]
        code, detail = _invoke_stage("impute", impute_missing_prices.main, impute_args)
        if code != 0:
            return code, detail or f"exit_code={code}"
        return 0, f"impute_window={start_date.isoformat()}..{end_date.isoformat()}"

    stages: list[tuple[str, Callable[[List[str]], int], List[str]]] = [
        ("update_trading_days", _stage_update_trading_days, []),
        ("ingest_prices", _stage_ingest, ["--debug"]),
        ("completeness_check", _stage_completeness, []),
        ("calc_index", _stage_calc_index, []),
        ("impute", _stage_impute, []),
    ]

    exit_code = 0
    try:
        for idx, (name, func, stage_args) in enumerate(stages):
            if idx < start_index and resume_run_id:
                print(f"[pipeline] skip {name} (resume)", flush=True)
                continue
            if name == "ingest_prices" and skip_ingest:
                print("[pipeline] skip ingest stage via SC_IDX_PIPELINE_SKIP_INGEST", flush=True)
                state_store.record_stage_start(run_id, name, details="skip_ingest")
                state_store.record_stage_end(run_id, name, "OK", details="skip_ingest")
                continue
            print(f"[pipeline] start {name} args={stage_args}", flush=True)
            result = _run_stage_with_retries(
                name=name,
                func=func,
                args=stage_args,
                state_store=state_store,
                run_id=run_id,
                max_attempts=max_attempts,
                backoff_base_sec=backoff_base_sec,
            )
            stage_durations[name] = result.duration_sec
            if result.status != "OK":
                last_error = result.error or f"{name}_failed"
                exit_code = result.code or 1
                break
    finally:
        try:
            stage_records = state_store.fetch_stage_statuses(run_id)
            durations = _stage_durations_from_state(stage_records)
            stage_durations.update(durations)
            health = collect_health_snapshot(
                stage_durations=stage_durations,
                last_error=last_error,
            )
        except Exception as exc:
            health = {
                "calendar_max_date": None,
                "canon_max_date": None,
                "canon_count_latest_day": None,
                "levels_max_date": None,
                "level_latest": None,
                "stats_max_date": None,
                "ret_1d_latest": None,
                "contrib_max_date": None,
                "contrib_count_latest_day": None,
                "next_missing_trading_day": None,
                "oracle_error_counts_24h": None,
                "stage_durations_sec": stage_durations,
                "last_error": f"health_query_failed:{exc}",
            }
        summary_text = format_health_summary(health)
        if len(summary_text) > 3800:
            summary_text = summary_text[:3797] + "..."
        try:
            write_health_artifact(health)
        except Exception:
            pass
        try:
            finish_run(
                run_id,
                status="OK" if exit_code == 0 else "ERROR",
                error=summary_text,
            )
        except Exception:
            pass

    if exit_code == 0:
        print("[pipeline] DONE", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
