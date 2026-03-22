from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from index_engine.alerts import send_email_to_result
from index_engine.run_report import (
    build_pipeline_daily_summary,
    format_pipeline_daily_report,
    write_pipeline_daily_artifacts,
)
from tools.index_engine.env_loader import load_default_env
from tools.index_engine.pipeline_health import collect_health_snapshot

DEFAULT_OUTPUT_DIR = REPO_ROOT / "tools" / "audit" / "output" / "pipeline_daily"
DEFAULT_REPORT_JSON = REPO_ROOT / "tools" / "audit" / "output" / "pipeline_runs" / "sc_idx_pipeline_latest.json"
DEFAULT_TELEMETRY_JSON = (
    REPO_ROOT / "tools" / "audit" / "output" / "pipeline_telemetry" / "sc_idx_pipeline_latest.json"
)


def _csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def _resolve_recipients() -> list[str]:
    return (
        _csv_list(os.getenv("SC_IDX_DAILY_REPORT_RECIPIENTS"))
        or _csv_list(os.getenv("TELEMETRY_REPORT_RECIPIENTS"))
        or _csv_list(os.getenv("MAIL_TO"))
    )


def _health_snapshot(latest_telemetry: dict[str, Any], *, skip_db: bool) -> dict[str, Any]:
    if skip_db:
        return {"db_snapshot_skipped": True}
    stage_durations = {
        stage_name: float((result or {}).get("duration_sec") or 0.0)
        for stage_name, result in (latest_telemetry.get("stage_results") or {}).items()
        if isinstance(result, dict)
    }
    try:
        return collect_health_snapshot(
            stage_durations=stage_durations,
            last_error=(latest_telemetry.get("root_cause") or None),
        )
    except Exception as exc:
        return {
            "db_snapshot_error": f"{type(exc).__name__}:{exc}",
            "stage_durations_sec": stage_durations,
        }


def _send_daily_report(subject: str, body: str) -> dict[str, Any]:
    recipients = _resolve_recipients()
    if not recipients:
        return {
            "attempted": False,
            "ok": False,
            "delivery_state": "missing_recipients",
            "missing_env": ["SC_IDX_DAILY_REPORT_RECIPIENTS", "TELEMETRY_REPORT_RECIPIENTS", "MAIL_TO"],
            "results": [],
        }

    results = [send_email_to_result(recipient, subject, body) for recipient in recipients]
    return {
        "attempted": True,
        "ok": all(bool(item.get("ok")) for item in results),
        "delivery_state": "sent" if all(bool(item.get("ok")) for item in results) else "partial_failure",
        "results": results,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the SC_IDX daily telemetry report")
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON, help="Latest pipeline report JSON path")
    parser.add_argument(
        "--telemetry-json",
        type=Path,
        default=DEFAULT_TELEMETRY_JSON,
        help="Latest pipeline telemetry JSON path",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for daily report artifacts",
    )
    parser.add_argument("--skip-db", action="store_true", help="Skip Oracle freshness queries and use artifacts only")
    parser.add_argument("--send", action="store_true", help="Send the daily report email")
    parser.add_argument("--dry-run", action="store_true", help="Print the rendered report to stdout")
    parser.add_argument("--json-out", type=Path, help="Optional additional JSON output path")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_default_env()
    args = _parse_args(argv)

    latest_summary = _read_json(args.report_json)
    latest_telemetry = _read_json(args.telemetry_json)
    if not latest_summary and not latest_telemetry:
        print(
            "sc_idx_daily_report_error missing_artifacts=report_json,telemetry_json",
            file=sys.stderr,
        )
        return 1

    health = _health_snapshot(latest_telemetry, skip_db=args.skip_db)
    payload = build_pipeline_daily_summary(
        latest_summary=latest_summary,
        latest_telemetry=latest_telemetry,
        health_snapshot=health,
    )
    report_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
    subject = (
        f"SC_IDX daily telemetry report [{(payload.get('headline') or {}).get('overall_health', 'Unknown')}] "
        f"{report_date}"
    )

    report_text = format_pipeline_daily_report(payload)
    artifact_paths = write_pipeline_daily_artifacts(
        report_date=report_date,
        payload=payload,
        report_text=report_text,
        output_dir=args.output_dir,
    )
    payload["daily_artifact_paths"] = artifact_paths
    report_text = format_pipeline_daily_report(payload)

    delivery = {"attempted": False, "ok": False, "delivery_state": "not_requested", "results": []}
    if args.send:
        delivery = _send_daily_report(subject, report_text)

    payload["email_delivery"] = delivery
    report_text = format_pipeline_daily_report(payload)
    write_pipeline_daily_artifacts(report_date=report_date, payload=payload, report_text=report_text, output_dir=args.output_dir)

    if args.json_out:
        args.json_out.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")

    if args.dry_run or not args.send:
        print(report_text)

    return 0 if not args.send or delivery.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
