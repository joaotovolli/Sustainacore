"""Scheduled research generator entrypoint."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from . import config
from .budget_manager import BudgetDecision, choose_profile
from .analysis import (
    build_anomaly_inputs,
    build_company_spotlight_bundle,
    build_period_close_inputs,
    build_rebalance_bundle,
    build_weekly_inputs,
)
from .docx_builder import build_docx
from .data_integrity import run_integrity
from .codex_cli_runner import log_startup_config
from .codex_usage import compute_usage_delta, get_usage_snapshot
from .doctor_codex import run_doctor
from .validators import quality_gate_strict
from .oracle import (
    ResearchRequest,
    claim_request,
    count_pending_approvals,
    current_schema,
    ensure_proc_reports,
    fetch_pending_requests,
    fetch_rebalance_rows,
    get_connection,
    init_env,
    insert_approval,
    insert_alert,
    insert_research_request,
    set_report_value,
    update_request_status,
)
from .agent_pipeline import build_pipeline
from .detectors import detect_anomaly, detect_period_close, detect_rebalance, detect_weekly

LOGGER = logging.getLogger("research_generator")


def _is_retryable_issue(issues: List[str]) -> bool:
    for issue in issues:
        lower = str(issue).lower()
        if "codex_cli_nonzero" in lower or "codex_cli_timeout" in lower:
            return True
    return False


def _next_retry(retry_count: int) -> dt.datetime:
    steps = [dt.timedelta(minutes=2), dt.timedelta(minutes=5), dt.timedelta(minutes=15), dt.timedelta(minutes=30)]
    if retry_count < len(steps):
        return dt.datetime.utcnow() + steps[retry_count]
    return dt.datetime.utcnow() + dt.timedelta(minutes=60)


def _preview_table_markdown(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    columns = list(rows[0].keys())
    header = "|" + "|".join(columns) + "|"
    separator = "|" + "|".join(["---"] * len(columns)) + "|"
    lines = [header, separator]
    for row in rows:
        lines.append("|" + "|".join(str(row.get(col, "")) for col in columns) + "|")
    return "\n".join(lines)


def _build_details(
    bundle: Dict[str, Any],
    draft: Dict[str, Any],
    chart_name: str,
    *,
    regenerated_from: Optional[int] = None,
    llm_issues: Optional[List[str]] = None,
    generation_meta: Optional[Dict[str, Any]] = None,
) -> str:
    details = {
        "report_type": bundle.get("report_type"),
        "window": bundle.get("window"),
        "captions": {
            "table": draft.get("table_caption"),
            "chart": draft.get("chart_caption"),
        },
        "chart_filename": chart_name,
        "preview_table": _preview_table_markdown(bundle.get("table_rows", [])),
        "provenance": [
            "TECH11_AI_GOV_ETH_INDEX",
            "SC_IDX_LEVELS",
            "SC_IDX_STATS_DAILY",
            "SC_IDX_CONTRIBUTION_DAILY",
        ],
        "draft_mode": "template" if draft.get("template_mode") else "llm",
        "llm_issues": llm_issues or [],
        "generation_meta": generation_meta or {},
    }
    if regenerated_from:
        details["regenerated_from_approval_id"] = regenerated_from
    return json.dumps(details, sort_keys=True)


def _summary_from_draft(draft: Dict[str, Any]) -> str:
    paragraphs = draft.get("paragraphs") or []
    if not paragraphs:
        return "Research draft ready for review."
    text = str(paragraphs[0])
    return text[:240]


def _store_report_state(report_type: str, value: str) -> None:
    with get_connection() as conn:
        if report_type == "REBALANCE":
            set_report_value(conn, "rebalance_port_date", value)
        elif report_type == "ANOMALY":
            set_report_value(conn, "anomaly_last_date", value)
        elif report_type == "WEEKLY":
            set_report_value(conn, "weekly_last_date", value)
        elif report_type == "PERIOD_CLOSE":
            if value.startswith("Year"):
                set_report_value(conn, "period_close_year", value.split()[-1])
            elif value.startswith("Quarter"):
                set_report_value(conn, "period_close_quarter", value.split()[-1])
            elif value.startswith("Month"):
                set_report_value(conn, "period_close_month", value.split()[-1])


def _build_rebalance_bundle(conn, latest_date: dt.date, previous_date: Optional[dt.date]):
    latest_rows = fetch_rebalance_rows(conn, latest_date)
    prev_rows = fetch_rebalance_rows(conn, previous_date) if previous_date else []
    return build_rebalance_bundle(latest_date, previous_date, latest_rows, prev_rows)


def _determine_trigger(now: dt.datetime, force: Optional[str]) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
    with get_connection() as conn:
        ensure_proc_reports(conn)
        rebalance_trigger, latest_date, previous_date = detect_rebalance(conn)
        anomaly_trigger, anomaly_date = detect_anomaly(conn)
        weekly_trigger = detect_weekly(conn, now)
        period_trigger, period_label = detect_period_close(conn)

    if force:
        return force.upper(), None, {}

    if rebalance_trigger:
        return "REBALANCE", latest_date.strftime("%Y-%m-%d") if latest_date else None, {}
    if anomaly_trigger:
        return "ANOMALY", anomaly_date, {}
    if weekly_trigger:
        return "WEEKLY", now.strftime("%Y-%m-%d"), {}
    if period_trigger:
        return "PERIOD_CLOSE", period_label, {}
    return None, None, {}


def _build_bundle(
    report_type: str,
    label: Optional[str],
    *,
    company_ticker: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        with get_connection() as conn:
            if report_type == "REBALANCE":
                _, latest_date, previous_date = detect_rebalance(conn)
                if not latest_date:
                    return None, "Missing rebalance data"
                bundle = _build_rebalance_bundle(conn, latest_date, previous_date)
                return bundle.to_dict(), None
            if report_type == "ANOMALY":
                bundle, err = build_anomaly_inputs(conn)
                return bundle.to_dict() if bundle else None, err
            if report_type == "WEEKLY":
                bundle, err = build_weekly_inputs(conn)
                return bundle.to_dict() if bundle else None, err
            if report_type == "PERIOD_CLOSE":
                bundle, err = build_period_close_inputs(conn, label or "Period close")
                return bundle.to_dict() if bundle else None, err
            if report_type == "COMPANY_SPOTLIGHT":
                if not company_ticker:
                    return None, "Missing company_ticker"
                _, latest_date, _ = detect_rebalance(conn)
                if not latest_date:
                    return None, "Missing rebalance data"
                latest_rows = fetch_rebalance_rows(conn, latest_date)
                bundle = build_company_spotlight_bundle(latest_date, latest_rows, company_ticker)
                return bundle.to_dict(), None
    except Exception as exc:
        return None, f"Data fetch failed: {exc}"[:200]
    return None, "Unsupported report type"


def _create_approval(
    bundle: Dict[str, Any],
    draft: Dict[str, Any],
    report_key: str,
    *,
    regenerated_from: Optional[int] = None,
    llm_issues: Optional[List[str]] = None,
    generation_meta: Optional[Dict[str, Any]] = None,
) -> int:
    output_dir = config.DEFAULT_OUTPUT_DIR
    docx_payload = build_docx(draft, bundle, report_key, output_dir)
    summary = _summary_from_draft(draft)
    details = _build_details(
        bundle,
        draft,
        os.path.basename(docx_payload["chart_path"]),
        regenerated_from=regenerated_from,
        llm_issues=llm_issues,
        generation_meta=generation_meta,
    )

    payload = {
        "source_job_id": None,
        "request_type": config.REQUEST_TYPE,
        "title": draft.get("headline") or f"Research Draft: {bundle.get('report_type')}",
        "proposed_text": summary,
        "details": details,
        "file_name": docx_payload["docx_name"],
        "file_mime": config.FILE_MIME_DOCX,
        "file_blob": docx_payload["docx_bytes"],
        "status": "PENDING",
    }

    with get_connection() as conn:
        approval_id = insert_approval(conn, payload)
    LOGGER.info("Created approval approval_id=%s", approval_id)
    return approval_id


def run_once(force: Optional[str], dry_run: bool) -> int:
    init_env()
    now = dt.datetime.utcnow()
    started_at = now
    usage_before = get_usage_snapshot()
    budget: BudgetDecision = choose_profile(usage_before)
    LOGGER.info("usage_source=%s usage_available=%s", usage_before.get("source"), usage_before.get("available"))
    with get_connection() as conn:
        schema = current_schema(conn)
        ensure_proc_reports(conn)
        pending_approvals = count_pending_approvals(conn)
    LOGGER.info("utc_now=%s schema=%s", now.strftime("%Y-%m-%dT%H:%M:%SZ"), schema)
    LOGGER.info("pending_approvals=%s", pending_approvals)

    report_type, label, _ = _determine_trigger(now, force)
    if not report_type:
        LOGGER.info("No triggers fired.")
        return 0

    bundle, err = _build_bundle(report_type, label)
    if err or not bundle:
        LOGGER.error("Bundle build failed: %s", err)
        return 1

    report_key = f"{report_type.lower()}_{now.strftime('%Y%m%d_%H%M%S')}"

    if dry_run:
        LOGGER.info("Dry-run: trigger=%s label=%s", report_type, label)
        return 0

    if not usage_before.get("available"):
        with get_connection() as conn:
            insert_alert(
                conn,
                request_id=None,
                approval_id=None,
                severity="WARN",
                title="Codex usage unavailable",
                details=json.dumps(usage_before, sort_keys=True),
            )

    if budget.profile.name == "SKIPPED_BUDGET":
        with get_connection() as conn:
            insert_alert(
                conn,
                request_id=None,
                approval_id=None,
                severity="WARN",
                title="Skipped generation to protect allowance",
                details=json.dumps(
                    {"profile_selected": budget.profile.name, "usage_before": usage_before},
                    sort_keys=True,
                ),
            )
        LOGGER.warning("Skipped generation due to budget threshold.")
        return 0

    bundle["profile"] = {
        "name": budget.profile.name,
        "max_angles": budget.profile.max_angles,
        "max_candidate_metrics": budget.profile.max_candidate_metrics,
        "max_charts": budget.profile.max_charts,
        "max_tables": budget.profile.max_tables,
        "max_iterations": budget.profile.max_iterations,
        "time_budget_minutes": budget.profile.time_budget_minutes,
    }
    integrity = run_integrity(bundle)
    bundle["integrity"] = integrity
    if integrity.get("issues"):
        with get_connection() as conn:
            insert_alert(
                conn,
                request_id=None,
                approval_id=None,
                severity="CRITICAL",
                title="Data integrity failure",
                details=json.dumps(integrity, sort_keys=True),
            )
        LOGGER.error("Integrity check failed: %s", "; ".join(integrity.get("issues") or []))
        return 1
    if integrity.get("warnings"):
        with get_connection() as conn:
            insert_alert(
                conn,
                request_id=None,
                approval_id=None,
                severity="WARN",
                title="Data integrity warning",
                details=json.dumps(integrity, sort_keys=True),
            )

    template_only = not run_doctor()
    if template_only:
        LOGGER.warning("Codex doctor failed; using template-only draft.")
    draft, issues = build_pipeline(
        bundle,
        output_dir=config.DEFAULT_OUTPUT_DIR,
        template_only=template_only,
        profile=budget.profile,
    )
    if not draft:
        LOGGER.error("Drafting failed: %s", "; ".join(issues))
        return 1

    ok, gate_issues = quality_gate_strict(bundle, draft)
    if not ok:
        LOGGER.error("Quality gate failed: %s", "; ".join(gate_issues))
        return 1

    finished_at = dt.datetime.utcnow()
    usage_after = get_usage_snapshot()
    usage_delta = compute_usage_delta(usage_before, usage_after)
    duration_seconds = int((finished_at - started_at).total_seconds())
    stop_reason = budget.stop_reason
    if duration_seconds > budget.profile.time_budget_minutes * 60:
        stop_reason = "time_budget_hit"
    generation_meta = {
        "started_at": started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_at": finished_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_seconds": duration_seconds,
        "profile_selected": budget.profile.name,
        "usage_before": usage_before,
        "usage_after": usage_after,
        "usage_delta": usage_delta,
        "stop_reason": stop_reason,
        "model_plan": "codex_cli",
    }

    approval_id = _create_approval(
        bundle,
        draft,
        report_key,
        llm_issues=issues,
        generation_meta=generation_meta,
    )
    if approval_id:
        store_value = label or (bundle.get("window") or {}).get("end") or now.strftime("%Y-%m-%d")
        _store_report_state(report_type, store_value)
    return 0


def _process_request(request: ResearchRequest, *, dry_run: bool) -> Tuple[Optional[int], List[str], bool, Dict[str, Any]]:
    report_type = (request.request_type or "").upper()
    started_at = dt.datetime.utcnow()
    usage_before = get_usage_snapshot()
    budget: BudgetDecision = choose_profile(usage_before)
    LOGGER.info("usage_source=%s usage_available=%s", usage_before.get("source"), usage_before.get("available"))
    bundle, err = _build_bundle(
        report_type,
        None,
        company_ticker=request.company_ticker,
    )
    if err or not bundle:
        raise RuntimeError(err or "bundle_error")

    bundle["profile"] = {
        "name": budget.profile.name,
        "max_angles": budget.profile.max_angles,
        "max_candidate_metrics": budget.profile.max_candidate_metrics,
        "max_charts": budget.profile.max_charts,
        "max_tables": budget.profile.max_tables,
        "max_iterations": budget.profile.max_iterations,
        "time_budget_minutes": budget.profile.time_budget_minutes,
    }
    integrity = run_integrity(bundle)
    bundle["integrity"] = integrity
    if integrity.get("issues"):
        with get_connection() as conn:
            insert_alert(
                conn,
                request_id=request.request_id,
                approval_id=None,
                severity="CRITICAL",
                title="Data integrity failure",
                details=json.dumps(integrity, sort_keys=True),
            )
        raise RuntimeError("integrity_failed: " + "; ".join(integrity.get("issues") or []))
    if integrity.get("warnings"):
        with get_connection() as conn:
            insert_alert(
                conn,
                request_id=request.request_id,
                approval_id=None,
                severity="WARN",
                title="Data integrity warning",
                details=json.dumps(integrity, sort_keys=True),
            )

    if not usage_before.get("available"):
        with get_connection() as conn:
            insert_alert(
                conn,
                request_id=request.request_id,
                approval_id=None,
                severity="WARN",
                title="Codex usage unavailable",
                details=json.dumps(usage_before, sort_keys=True),
            )

    if budget.profile.name == "SKIPPED_BUDGET":
        with get_connection() as conn:
            insert_alert(
                conn,
                request_id=request.request_id,
                approval_id=None,
                severity="WARN",
                title="Skipped generation to protect allowance",
                details=json.dumps(
                    {"profile_selected": budget.profile.name, "usage_before": usage_before},
                    sort_keys=True,
                ),
            )
        raise RuntimeError("skipped_budget")

    template_only = not run_doctor()
    if template_only:
        LOGGER.warning("Codex doctor failed; using template-only draft.")
    draft, issues = build_pipeline(
        bundle,
        output_dir=config.DEFAULT_OUTPUT_DIR,
        editor_notes=request.editor_notes,
        template_only=template_only,
        profile=budget.profile,
    )
    if not draft:
        raise RuntimeError("draft_failed: " + "; ".join(issues))

    report_key = f"manual_{report_type.lower()}_{request.request_id}"
    if dry_run:
        return None, issues, False, {}

    ok, gate_issues = quality_gate_strict(bundle, draft)
    if not ok:
        raise RuntimeError("quality_gate_failed: " + "; ".join(gate_issues))

    finished_at = dt.datetime.utcnow()
    usage_after = get_usage_snapshot()
    usage_delta = compute_usage_delta(usage_before, usage_after)
    duration_seconds = int((finished_at - started_at).total_seconds())
    stop_reason = budget.stop_reason
    if duration_seconds > budget.profile.time_budget_minutes * 60:
        stop_reason = "time_budget_hit"
    generation_meta = {
        "started_at": started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_at": finished_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_seconds": duration_seconds,
        "profile_selected": budget.profile.name,
        "usage_before": usage_before,
        "usage_after": usage_after,
        "usage_delta": usage_delta,
        "stop_reason": stop_reason,
        "model_plan": "codex_cli",
    }

    approval_id = _create_approval(
        bundle,
        draft,
        report_key,
        regenerated_from=request.source_approval_id,
        llm_issues=issues,
        generation_meta=generation_meta,
    )
    return approval_id, issues, bool(draft.get("template_mode")), generation_meta


def process_pending_manual_requests(limit: int, *, dry_run: bool, request_id: Optional[int]) -> int:
    init_env()
    processed = 0
    with get_connection() as conn:
        requests = fetch_pending_requests(conn, limit=limit)
    if request_id:
        requests = [req for req in requests if req.request_id == request_id]
    if not requests:
        LOGGER.info("No pending manual requests.")
        return 0

    now = dt.datetime.utcnow()
    for request in requests:
        if request.updated_at and (now - request.updated_at).total_seconds() < 3600 and request.retry_count:
            LOGGER.info("Skip retry request_id=%s within 1h guard", request.request_id)
            continue
        with get_connection() as conn:
            claimed = claim_request(conn, request.request_id)
        if not claimed:
            continue
        try:
            approval_id, issues, template_mode, generation_meta = _process_request(request, dry_run=dry_run)
            if dry_run:
                with get_connection() as conn:
                    update_request_status(
                        conn,
                        request.request_id,
                        "DONE",
                        result_text="Dry-run complete; no approval created.",
                    )
            else:
                suffix = ""
                if template_mode:
                    suffix = " TEMPLATE_DRAFT"
                elif issues:
                    suffix = " DRAFT_LOW_CONFIDENCE"
                usage_delta = generation_meta.get("usage_delta") or {}
                delta_5h = usage_delta.get("five_hour_used_pct_delta")
                delta_week = usage_delta.get("weekly_used_pct_delta")
                duration_seconds = generation_meta.get("duration_seconds")
                duration_text = ""
                if duration_seconds is not None:
                    minutes = int(duration_seconds // 60)
                    seconds = int(duration_seconds % 60)
                    duration_text = f" duration={minutes}m{seconds:02d}s"
                usage_text = ""
                if usage_delta.get("available") is False:
                    usage_text = f" usage=n/a(reason={usage_delta.get('reason')})"
                else:
                    if delta_5h is not None or delta_week is not None:
                        usage_text = f" usage_delta_5h={delta_5h:+.2f}pp usage_delta_week={delta_week:+.2f}pp"
                with get_connection() as conn:
                    update_request_status(
                        conn,
                        request.request_id,
                        "DONE",
                        result_text=f"done: approval_id={approval_id}{duration_text}{usage_text}{suffix}",
                    )
            processed += 1
        except Exception as exc:
            message = str(exc)
            retryable = message.lower().startswith("retryable") or _is_retryable_issue([message])
            if "429" in message or "rate limit" in message.lower():
                retryable = True
            if retryable:
                current_retry = request.retry_count or 0
                next_retry_at = _next_retry(current_retry)
                new_retry = current_retry + 1
                with get_connection() as conn:
                    update_request_status(
                        conn,
                        request.request_id,
                        "PENDING",
                        result_text=f"retryable: {message[:400]}",
                        retry_count=new_retry,
                        next_retry_at=next_retry_at,
                    )
                LOGGER.warning("Manual request rate-limited request_id=%s", request.request_id)
                continue
            if "skipped_budget" in message:
                with get_connection() as conn:
                    update_request_status(
                        conn,
                        request.request_id,
                        "SKIPPED_BUDGET",
                        result_text="skipped_budget: allowance protection",
                    )
                LOGGER.warning("Manual request skipped for budget request_id=%s", request.request_id)
                continue
            with get_connection() as conn:
                update_request_status(
                    conn,
                    request.request_id,
                    "FAILED",
                    result_text=message[:400],
                )
    LOGGER.info("Manual requests processed=%s", processed)
    return 0


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scheduled Research Generator")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--dry-run", action="store_true", help="Skip approval creation")
    parser.add_argument(
        "--force",
        choices=["rebalance", "weekly", "period", "anomaly"],
        help="Force a specific report type",
    )
    parser.add_argument(
        "--process-manual",
        action="store_true",
        help="Process pending manual research requests",
    )
    parser.add_argument("--request-id", type=int, help="Process a single request id")
    parser.add_argument(
        "--seed-request",
        action="store_true",
        help="Insert a sample manual request (for verification)",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    log_startup_config()

    force = None
    if args.force:
        if args.force == "rebalance":
            force = "REBALANCE"
        elif args.force == "weekly":
            force = "WEEKLY"
        elif args.force == "period":
            force = "PERIOD_CLOSE"
        elif args.force == "anomaly":
            force = "ANOMALY"

    if args.seed_request:
        init_env()
        with get_connection() as conn:
            request_id = insert_research_request(
                conn,
                "REBALANCE",
                created_by="system",
                editor_notes="Manual request seed for verification.",
            )
        LOGGER.info("Seeded manual request request_id=%s", request_id)
        return 0

    if args.process_manual:
        return process_pending_manual_requests(5, dry_run=args.dry_run, request_id=args.request_id)

    return run_once(force=force, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
