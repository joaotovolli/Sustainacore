from __future__ import annotations

import os
import sys
import time
from pathlib import Path
import inspect
import fcntl
from typing import Callable, List
from contextlib import contextmanager

from tools.index_engine.env_loader import load_default_env

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_PIPELINE_LOCK_PATH = "/tmp/sc_idx_pipeline.lock"


@contextmanager
def _pipeline_lock():
    handle = open(_PIPELINE_LOCK_PATH, "a+")
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("sc_idx_pipeline_lock: busy", file=sys.stderr)
            sys.exit(0)
        yield
    finally:
        try:
            fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            handle.close()


def _run_stage(name: str, func: Callable[[List[str]], int], args: List[str]) -> None:
    start = time.time()
    print(f"[pipeline] start {name} args={args}", flush=True)
    sig = inspect.signature(func)
    if len(sig.parameters) == 0:
        # Stage mains that self-parse argv.
        original_argv = sys.argv[:]
        sys.argv = [f"run_pipeline_{name}"] + list(args)
        try:
            code = func()
        finally:
            sys.argv = original_argv
    else:
        code = func(args)
    duration = time.time() - start
    print(f"[pipeline] end {name} exit={code} duration_sec={duration:.1f}", flush=True)
    if code != 0:
        sys.exit(code)


def main() -> int:
    load_default_env()
    with _pipeline_lock():
        skip_ingest = os.getenv("SC_IDX_PIPELINE_SKIP_INGEST") == "1"
        started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        head_commit = os.popen("cd /opt/sustainacore-ai && git rev-parse --short HEAD").read().strip()
        print(
            f"sc_idx_pipeline_run: started_at_utc={started_at} head={head_commit} skip_ingest={skip_ingest}",
            flush=True,
        )

        from tools.index_engine import run_daily
        from tools.index_engine import check_price_completeness
        from tools.index_engine import impute_missing_prices
        from tools.index_engine import calc_index
        if skip_ingest:
            print("[pipeline] skip ingest stage via SC_IDX_PIPELINE_SKIP_INGEST", flush=True)
        else:
            _run_stage("ingest", run_daily.main, ["--debug"])
        _run_stage(
            "completeness",
            check_price_completeness.main,
            ["--since-base", "--strict", "--allow-imputation", "--email-on-fail"],
        )
        _run_stage(
            "impute",
            impute_missing_prices.main,
            ["--since-base", "--allow-canon-close", "--email"],
        )
        _run_stage(
            "index_calc",
            calc_index.main,
            ["--since-base", "--strict", "--debug", "--no-preflight-self-heal"],
        )
        print("[pipeline] DONE", flush=True)
        return 0


if __name__ == "__main__":
    sys.exit(main())
