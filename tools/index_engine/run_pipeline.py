from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from index_engine.orchestration import PipelineArgs, run_pipeline


def _env_flag(name: str) -> bool:
    value = (os.getenv(name) or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SC_IDX LangGraph pipeline orchestrator")
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume from persisted completed nodes when possible (default: true)",
    )
    parser.add_argument("--restart", action="store_true", help="Ignore persisted state and start a new run")
    parser.add_argument(
        "--report-dir",
        type=Path,
        help="Optional directory for JSON/text run reports (default: tools/audit/output/pipeline_runs)",
    )
    parser.add_argument("--smoke", action="store_true", help="Run a no-provider, no-Oracle smoke orchestration")
    parser.add_argument(
        "--smoke-scenario",
        choices=["success", "degraded", "clean_skip", "failed", "blocked"],
        default="success",
        help="Scenario to simulate when --smoke is set",
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip the provider ingest node and continue with downstream DB-only stages when possible",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    pipeline_args = PipelineArgs(
        resume=args.resume,
        restart=args.restart,
        report_dir=args.report_dir,
        smoke=args.smoke,
        smoke_scenario=args.smoke_scenario,
        skip_ingest=args.skip_ingest or _env_flag("SC_IDX_PIPELINE_SKIP_INGEST"),
    )
    exit_code, _state = run_pipeline(pipeline_args)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
