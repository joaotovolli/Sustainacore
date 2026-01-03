"""Scheduled research generator entrypoint."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
from typing import Any, Dict, Optional, Tuple

from . import config
from .analysis import (
    build_anomaly_inputs,
    build_company_spotlight_bundle,
    build_core_vs_coverage_gap_bundle,
    build_period_close_inputs,
    build_rebalance_bundle,
    build_top25_movers_bundle,
    build_weekly_inputs,
)
from .docx_builder import build_docx
from .insight_miner import mine_insights
from .fact_check import run_fact_check
from .alerting import send_email
from .gemini_cli import log_startup_config
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
    insert_report_insights,
    fetch_last_report_insights,
    insert_research_request,
    set_report_value,
    update_request_status,
)
from .ping_pong import draft_with_ping_pong
from .validators import quality_gate_strict
from .detectors import detect_anomaly, detect_period_close, detect_rebalance, detect_weekly

LOGGER = logging.getLogger("research_generator")


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


def _retry_delay_seconds(retry_count: int) -> int:
    if retry_count <= 0:
        return 120
    if retry_count == 1:
        return 300
    if retry_count == 2:
        return 900
    if retry_count == 3:
        return 1800
    return 3600


def _next_retry_at(retry_count: int) -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(seconds=_retry_delay_seconds(retry_count))


def _build_details(
    bundle: Dict[str, Any],
    draft: Dict[str, Any],
    chart_name: str,
    *,
    regenerated_from: Optional[int] = None,
    analysis_notes: Optional[list[str]] = None,
    fact_warnings: Optional[list[str]] = None,
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
        "metrics": bundle.get("metrics"),
        "analysis_notes": analysis_notes or [],
        "csv_extracts": bundle.get("csv_extracts"),
        "table_callouts": {t.get("title"): t.get("callouts") for t in (bundle.get("docx_tables") or [])},
        "figure_callouts": {c.get("title"): c.get("callouts") for c in (bundle.get("docx_charts") or [])},
        "fact_check_warnings": fact_warnings or [],
        "provenance": [
            "TECH11_AI_GOV_ETH_INDEX",
            "SC_IDX_LEVELS",
            "SC_IDX_STATS_DAILY",
            "SC_IDX_CONTRIBUTION_DAILY",
        ],
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
            if report_type == "CORE_VS_COVERAGE_GAP":
                _, latest_date, previous_date = detect_rebalance(conn)
                if not latest_date:
                    return None, "Missing rebalance data"
                latest_rows = fetch_rebalance_rows(conn, latest_date)
                prev_rows = fetch_rebalance_rows(conn, previous_date) if previous_date else []
                bundle = build_core_vs_coverage_gap_bundle(latest_date, previous_date, latest_rows, prev_rows)
                return bundle.to_dict(), None
            if report_type == "TOP25_MOVERS_ONLY":
                _, latest_date, previous_date = detect_rebalance(conn)
                if not latest_date:
                    return None, "Missing rebalance data"
                latest_rows = fetch_rebalance_rows(conn, latest_date)
                prev_rows = fetch_rebalance_rows(conn, previous_date) if previous_date else []
                bundle = build_top25_movers_bundle(latest_date, previous_date, latest_rows, prev_rows)
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


def _build_docx_payload(
    bundle: Dict[str, Any],
    draft: Dict[str, Any],
    report_key: str,
) -> Dict[str, Any]:
    output_dir = config.DEFAULT_OUTPUT_DIR
    return build_docx(draft, bundle, report_key, output_dir)


def _ensure_outline(draft: Dict[str, Any], bundle: Dict[str, Any]) -> None:
    figures = len(bundle.get("docx_charts") or [])
    tables = len(bundle.get("docx_tables") or [])
    outline = draft.get("outline") or []

    def _valid_id(item: Dict[str, Any], max_id: int) -> bool:
        try:
            value = int(item.get("id") or 0)
        except (TypeError, ValueError):
            return False
        return 1 <= value <= max_id

    has_fig = any(item.get("type") == "figure" and _valid_id(item, figures) for item in outline)
    has_tbl = any(item.get("type") == "table" and _valid_id(item, tables) for item in outline)
    if (figures and not has_fig) or (tables and not has_tbl) or not outline:
        paragraphs = draft.get("paragraphs") or []
        new_outline: list[dict[str, Any]] = []
        fig_id = 1
        tbl_id = 1
        for idx, para in enumerate(paragraphs):
            new_outline.append({"type": "paragraph", "text": para})
            if idx == 1 and fig_id <= figures:
                new_outline.append({"type": "figure", "id": fig_id})
                fig_id += 1
            if idx == 2 and tbl_id <= tables:
                new_outline.append({"type": "table", "id": tbl_id})
                tbl_id += 1
            if idx == 3 and tbl_id <= tables:
                new_outline.append({"type": "table", "id": tbl_id})
                tbl_id += 1
        while fig_id <= figures:
            new_outline.append({"type": "figure", "id": fig_id})
            fig_id += 1
        while tbl_id <= tables:
            new_outline.append({"type": "table", "id": tbl_id})
            tbl_id += 1
        outline = new_outline

    # Ensure each artifact is referenced by an adjacent paragraph.
    enhanced: list[dict[str, Any]] = []
    for idx, item in enumerate(outline):
        enhanced.append(item)
        if item.get("type") not in ("figure", "table"):
            continue
        ref = "Figure" if item.get("type") == "figure" else "Table"
        ident = item.get("id") or 1
        prev_text = ""
        next_text = ""
        if idx > 0 and outline[idx - 1].get("type") == "paragraph":
            prev_text = str(outline[idx - 1].get("text") or "")
        if idx + 1 < len(outline) and outline[idx + 1].get("type") == "paragraph":
            next_text = str(outline[idx + 1].get("text") or "")
        if f"{ref} {ident}" not in f"{prev_text} {next_text}":
            enhanced.append(
                {
                    "type": "paragraph",
                    "text": f"{ref} {ident} provides the detailed breakdown for this section.",
                }
            )
    draft["outline"] = enhanced


def _create_approval(
    bundle: Dict[str, Any],
    draft: Dict[str, Any],
    docx_payload: Dict[str, Any],
    *,
    regenerated_from: Optional[int] = None,
    analysis_notes: Optional[list[str]] = None,
    fact_warnings: Optional[list[str]] = None,
) -> int:
    summary = _summary_from_draft(draft)
    details = _build_details(
        bundle,
        draft,
        os.path.basename(docx_payload["chart_path"]),
        regenerated_from=regenerated_from,
        analysis_notes=analysis_notes,
        fact_warnings=fact_warnings,
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


def _emit_alerts(
    *,
    request_id: Optional[int],
    approval_id: Optional[int],
    report_type: str,
    critical: list[str],
    warnings: list[str],
    details: list[str],
) -> None:
    summary = []
    if critical:
        summary.append("CRITICAL issues:")
        summary.extend(f"- {item}" for item in critical)
    if warnings:
        summary.append("WARNINGS:")
        summary.extend(f"- {item}" for item in warnings)
    if details:
        summary.append("DETAILS:")
        summary.extend(f"- {item}" for item in details[:10])
    body = "\n".join(summary)
    subject = f"Research data quality report ({report_type})"

    with get_connection() as conn:
        if critical:
            insert_alert(
                conn,
                request_id=request_id,
                approval_id=approval_id,
                severity="CRITICAL",
                title=f"Fact check failed: {report_type}",
                details=body[:2000],
            )
        if warnings:
            insert_alert(
                conn,
                request_id=request_id,
                approval_id=approval_id,
                severity="WARN",
                title=f"Fact check warnings: {report_type}",
                details=body[:2000],
            )

    send_email(subject, body)


def run_once(force: Optional[str], dry_run: bool) -> int:
    init_env()
    now = dt.datetime.utcnow()
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

    bundle["insight_candidates"] = mine_insights(bundle)

    fact = run_fact_check(bundle)
    if not fact.ok():
        _emit_alerts(
            request_id=None,
            approval_id=None,
            report_type=bundle.get("report_type", "UNKNOWN"),
            critical=fact.critical,
            warnings=fact.warnings,
            details=fact.details,
        )
        LOGGER.error("Fact check failed: %s", "; ".join(fact.critical))
        return 1

    report_key = f"{report_type.lower()}_{now.strftime('%Y%m%d_%H%M%S')}"

    if dry_run:
        LOGGER.info("Dry-run: trigger=%s label=%s", report_type, label)
        return 0

    previous_insights = None
    with get_connection() as conn:
        previous_insights = fetch_last_report_insights(conn, bundle.get("report_type", ""))
    if previous_insights:
        try:
            previous_list = json.loads(previous_insights)
        except json.JSONDecodeError:
            previous_list = []
    else:
        previous_list = []

    draft, issues, compute = draft_with_ping_pong(bundle, previous_insights=previous_list)
    if not draft:
        LOGGER.error("Drafting failed: %s", "; ".join(issues))
        return 1

    _ensure_outline(draft, bundle)
    docx_payload = _build_docx_payload(bundle, draft, report_key)
    ok, gate_issues = quality_gate_strict(bundle, draft, compute or {}, docx_payload)
    if not ok:
        LOGGER.error("Quality gate failed: %s", "; ".join(gate_issues))
        return 1

    approval_id = _create_approval(
        bundle,
        draft,
        docx_payload,
        analysis_notes=(compute or {}).get("analysis_notes"),
        fact_warnings=fact.warnings,
    )
    if approval_id:
        with get_connection() as conn:
            insights_payload = json.dumps((compute or {}).get("selected_insights") or [], sort_keys=True)
            insert_report_insights(conn, bundle.get("report_type", ""), insights_payload)
        store_value = label or (bundle.get("window") or {}).get("end") or now.strftime("%Y-%m-%d")
        _store_report_state(report_type, store_value)
    return 0


def _process_request(request: ResearchRequest, *, dry_run: bool) -> Optional[int]:
    report_type = (request.request_type or "").upper()
    bundle, err = _build_bundle(
        report_type,
        None,
        company_ticker=request.company_ticker,
    )
    if err or not bundle:
        raise RuntimeError(err or "bundle_error")

    bundle["insight_candidates"] = mine_insights(bundle)
    fact = run_fact_check(bundle)
    if not fact.ok():
        _emit_alerts(
            request_id=request.request_id,
            approval_id=None,
            report_type=bundle.get("report_type", "UNKNOWN"),
            critical=fact.critical,
            warnings=fact.warnings,
            details=fact.details,
        )
        raise RuntimeError("fact_check_failed: " + "; ".join(fact.critical))

    previous_insights = None
    with get_connection() as conn:
        previous_insights = fetch_last_report_insights(conn, bundle.get("report_type", ""))
    if previous_insights:
        try:
            previous_list = json.loads(previous_insights)
        except json.JSONDecodeError:
            previous_list = []
    else:
        previous_list = []

    draft, issues, compute = draft_with_ping_pong(
        bundle,
        editor_notes=request.editor_notes,
        previous_insights=previous_list,
    )
    if not draft:
        raise RuntimeError("draft_failed: " + "; ".join(issues))

    report_key = f"manual_{report_type.lower()}_{request.request_id}"
    if dry_run:
        return None

    _ensure_outline(draft, bundle)
    docx_payload = _build_docx_payload(bundle, draft, report_key)
    ok, gate_issues = quality_gate_strict(bundle, draft, compute or {}, docx_payload)
    if not ok:
        raise RuntimeError("quality_gate_failed: " + "; ".join(gate_issues))

    approval_id = _create_approval(
        bundle,
        draft,
        docx_payload,
        regenerated_from=request.source_approval_id,
        analysis_notes=(compute or {}).get("analysis_notes"),
        fact_warnings=fact.warnings,
    )
    with get_connection() as conn:
        insights_payload = json.dumps((compute or {}).get("selected_insights") or [], sort_keys=True)
        insert_report_insights(conn, bundle.get("report_type", ""), insights_payload)
    return approval_id


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

    for request in requests:
        if request.next_retry_at and request.next_retry_at > dt.datetime.utcnow():
            LOGGER.info(
                "cooldown_active request_id=%s next_retry_at=%s",
                request.request_id,
                request.next_retry_at,
            )
            continue
        with get_connection() as conn:
            claimed = claim_request(conn, request.request_id)
        if not claimed:
            continue
        try:
            approval_id = _process_request(request, dry_run=dry_run)
            if dry_run:
                with get_connection() as conn:
                    update_request_status(
                        conn,
                        request.request_id,
                        "DONE",
                        result_text="Dry-run complete; no approval created.",
                    )
            else:
                with get_connection() as conn:
                    update_request_status(
                        conn,
                        request.request_id,
                        "DONE",
                        result_text=f"Approval created: {approval_id}",
                    )
            processed += 1
        except Exception as exc:
            message = str(exc)
            if "429" in message or "rate limit" in message.lower():
                current_retry = int(request.retry_count or 0)
                new_retry = current_retry + 1
                next_retry = _next_retry_at(new_retry)
                with get_connection() as conn:
                    update_request_status(
                        conn,
                        request.request_id,
                        "PENDING",
                        result_text=(
                            f"retryable: rate_limited next_retry_at={next_retry.isoformat()} "
                            f"retry_count={new_retry} {message[:200]}"
                        )[:400],
                        retry_count=new_retry,
                        next_retry_at=next_retry,
                    )
                LOGGER.warning("Manual request rate-limited request_id=%s", request.request_id)
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
        choices=[
            "rebalance",
            "weekly",
            "period",
            "anomaly",
            "core_vs_coverage_gap",
            "top25_movers_only",
        ],
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
        elif args.force == "core_vs_coverage_gap":
            force = "CORE_VS_COVERAGE_GAP"
        elif args.force == "top25_movers_only":
            force = "TOP25_MOVERS_ONLY"

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
