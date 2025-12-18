from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Callable, List

from tools.index_engine.env_loader import load_default_env

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _run_stage(name: str, func: Callable[[List[str]], int], args: List[str]) -> None:
    start = time.time()
    print(f"[pipeline] start {name} args={args}", flush=True)
    code = func(args)
    duration = time.time() - start
    print(f"[pipeline] end {name} exit={code} duration_sec={duration:.1f}", flush=True)
    if code != 0:
        sys.exit(code)


def main() -> int:
    load_default_env()

    from tools.index_engine import run_daily
    from tools.index_engine import check_price_completeness
    from tools.index_engine import impute_missing_prices
    from tools.index_engine import calc_index

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
        ["--since-base", "--strict", "--debug"],
    )
    print("[pipeline] DONE", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
