from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

import oracledb

import db_helper
from app.index_engine.alerts import send_email_to
from tools.telemetry.report_filters import DEFAULT_BOT_UA_REGEX, DEFAULT_PROBE_PATH_REGEX

PRIVATE_IP_REGEX = r"^(127\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|0\.|fc|fd|fe80)"
BOT_PAYLOAD_REGEX = r"\"bot\"\s*:\s*true"

WINDOWS = {
    "24h": dt.timedelta(days=1),
    "7d": dt.timedelta(days=7),
    "30d": dt.timedelta(days=30),
}

PAGE_EXCLUDE_PREFIXES = (
    "/static/",
    "/media/",
    "/api/",
    "/ask2/api/",
    "/telemetry/",
    "/admin/",
)


def _csv_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _window_range(label: str, now: dt.datetime) -> Tuple[dt.datetime, dt.datetime]:
    delta = WINDOWS[label]
    return now - delta, now


def _bind_list(prefix: str, values: Iterable[str], binds: Dict[str, Any]) -> str:
    placeholders = []
    for idx, value in enumerate(values):
        key = f"{prefix}_{idx}"
        binds[key] = value
        placeholders.append(f":{key}")
    return ", ".join(placeholders)


def _build_exclusions(binds: Dict[str, Any]) -> List[str]:
    clauses: List[str] = []
    ip_hashes = _csv_list(os.getenv("TELEMETRY_EXCLUDE_IP_HASHES"))
    if ip_hashes:
        placeholder = _bind_list("ip_hash", ip_hashes, binds)
        clauses.append(f"(ip_hash IS NULL OR ip_hash NOT IN ({placeholder}))")
    session_keys = _csv_list(os.getenv("TELEMETRY_EXCLUDE_SESSION_KEYS"))
    if session_keys:
        placeholder = _bind_list("session_key", session_keys, binds)
        clauses.append(f"(session_key IS NULL OR session_key NOT IN ({placeholder}))")
    user_ids = _csv_list(os.getenv("TELEMETRY_EXCLUDE_USER_IDS"))
    if user_ids:
        placeholder = _bind_list("user_id", user_ids, binds)
        clauses.append(f"(user_id IS NULL OR user_id NOT IN ({placeholder}))")
    ua_substrings = _csv_list(os.getenv("TELEMETRY_EXCLUDE_UA_SUBSTRINGS"))
    if ua_substrings:
        regex = "|".join(re.escape(token.lower()) for token in ua_substrings if token)
        if regex:
            binds["exclude_ua_regex"] = regex
            clauses.append("NOT REGEXP_LIKE(LOWER(NVL(user_agent,'')), :exclude_ua_regex)")
    return clauses


def _bot_regex() -> str:
    extra = os.getenv("TELEMETRY_BOT_UA_REGEX")
    if extra:
        return f"({DEFAULT_BOT_UA_REGEX})|({extra})"
    return DEFAULT_BOT_UA_REGEX


def _probe_regex() -> str:
    extra = os.getenv("TELEMETRY_PROBE_PATH_REGEX")
    if extra:
        return f"({DEFAULT_PROBE_PATH_REGEX})|({extra})"
    return DEFAULT_PROBE_PATH_REGEX


def _base_where() -> str:
    return "event_ts >= :start_ts AND event_ts < :end_ts"


def _real_traffic_where(binds: Dict[str, Any]) -> str:
    clauses = [_base_where()]
    clauses.append("NOT REGEXP_LIKE(LOWER(NVL(ip_trunc,'')), :private_ip_regex)")
    clauses.append(
        "NOT (REGEXP_LIKE(LOWER(NVL(user_agent,'')), :bot_ua_regex) "
        "OR REGEXP_LIKE(NVL(payload_json,''), :bot_payload_regex, 'i'))"
    )
    clauses.append("NOT REGEXP_LIKE(NVL(path,''), :probe_path_regex, 'i')")
    clauses.extend(_build_exclusions(binds))
    return " AND ".join(clauses)


def _bot_where() -> str:
    clauses = [_base_where()]
    clauses.append(
        "(REGEXP_LIKE(LOWER(NVL(user_agent,'')), :bot_ua_regex) "
        "OR REGEXP_LIKE(NVL(payload_json,''), :bot_payload_regex, 'i'))"
    )
    return " AND ".join(clauses)


def _probe_where() -> str:
    clauses = [_base_where()]
    clauses.append("REGEXP_LIKE(NVL(path,''), :probe_path_regex, 'i')")
    return " AND ".join(clauses)


def _select_binds(sql: str, binds: Dict[str, Any]) -> Dict[str, Any]:
    keys = set(re.findall(r":([a-zA-Z_][a-zA-Z0-9_]*)", sql))
    return {key: value for key, value in binds.items() if key in keys}


def _query_scalar(conn, sql: str, binds: Dict[str, Any]) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, _select_binds(sql, binds))
        row = cur.fetchone()
        return int((row or [0])[0] or 0)


def _query_rows(conn, sql: str, binds: Dict[str, Any]) -> List[Tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(sql, _select_binds(sql, binds))
        return list(cur.fetchall() or [])


def _fmt_kv(rows: Iterable[Tuple[Any, Any]], limit: int = 10) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for key, value in list(rows)[:limit]:
        out.append({"key": key, "count": int(value or 0)})
    return out


def _referrer_hosts(rows: Iterable[Tuple[Any, Any]]) -> List[Dict[str, Any]]:
    from urllib.parse import urlparse

    counter: Dict[str, int] = {}
    for ref, count in rows:
        if not ref:
            continue
        host = urlparse(str(ref)).netloc or "(none)"
        counter[host] = counter.get(host, 0) + int(count or 0)
    items = sorted(counter.items(), key=lambda x: x[1], reverse=True)
    return [{"key": host, "count": count} for host, count in items[:10]]


def _safe_ratio(numer: int, denom: int) -> float:
    if denom <= 0:
        return 0.0
    return round(float(numer) / float(denom), 4)


def _page_exclusion_clause() -> str:
    clauses = ["path IS NOT NULL"]
    for prefix in PAGE_EXCLUDE_PREFIXES:
        clauses.append(f"path NOT LIKE '{prefix}%'")
    return " AND ".join(clauses)


def _response_stats(conn, base_where: str, binds: Dict[str, Any]) -> Dict[str, Optional[float]]:
    sql = (
        "SELECT "
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY response_ms) AS p50, "
        "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_ms) AS p95 "
        "FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE "
        f"{base_where} AND response_ms IS NOT NULL"
    )
    with conn.cursor() as cur:
        cur.execute(sql, _select_binds(sql, binds))
        row = cur.fetchone() or [None, None]
        return {"p50_ms": row[0], "p95_ms": row[1]}


def build_window_report(label: str, start_ts: dt.datetime, end_ts: dt.datetime) -> Dict[str, Any]:
    binds: Dict[str, Any] = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "private_ip_regex": PRIVATE_IP_REGEX,
        "bot_ua_regex": _bot_regex(),
        "bot_payload_regex": BOT_PAYLOAD_REGEX,
        "probe_path_regex": _probe_regex(),
    }

    report: Dict[str, Any] = {
        "window": label,
        "start": start_ts.isoformat(),
        "end": end_ts.isoformat(),
    }

    real_where = _real_traffic_where(binds)
    consented_where = f"{real_where} AND consent_analytics_effective='Y'"
    nonconsented_where = f"{real_where} AND consent_analytics_effective='N'"

    with db_helper.get_connection() as conn:
        totals = {
            "events": _query_scalar(
                conn,
                f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {real_where}",
                binds,
            ),
            "page_views": _query_scalar(
                conn,
                f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {real_where} AND event_type='page_view'",
                binds,
            ),
            "api_calls": _query_scalar(
                conn,
                f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {real_where} AND event_type='api_call'",
                binds,
            ),
            "error_events": _query_scalar(
                conn,
                f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {real_where} AND NVL(status_code,0) >= 400",
                binds,
            ),
        }
        totals["error_rate"] = _safe_ratio(totals["error_events"], totals["events"])

        uniques = {
            "sessions": _query_scalar(
                conn,
                f"SELECT COUNT(DISTINCT session_key) FROM WKSP_ESGAPEX.W_WEB_EVENT "
                f"WHERE {consented_where} AND session_key IS NOT NULL",
                binds,
            ),
            "visitors": _query_scalar(
                conn,
                f"SELECT COUNT(DISTINCT ip_hash) FROM WKSP_ESGAPEX.W_WEB_EVENT "
                f"WHERE {consented_where} AND ip_hash IS NOT NULL",
                binds,
            ),
        }

        consent = {
            "consented_events": _query_scalar(
                conn,
                f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {consented_where}",
                binds,
            ),
            "nonconsented_events": _query_scalar(
                conn,
                f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {nonconsented_where}",
                binds,
            ),
        }

        countries = _query_rows(
            conn,
            f"SELECT NVL(country_code,'unknown') AS country, COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT "
            f"WHERE {consented_where} GROUP BY NVL(country_code,'unknown') ORDER BY COUNT(*) DESC",
            binds,
        )
        unknown_country = _query_scalar(
            conn,
            f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {consented_where} AND country_code IS NULL",
            binds,
        )

        top_pages = _query_rows(
            conn,
            f"SELECT path, COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT "
            f"WHERE {consented_where} AND event_type='page_view' AND {_page_exclusion_clause()} "
            "GROUP BY path ORDER BY COUNT(*) DESC",
            binds,
        )

        referrers = _query_rows(
            conn,
            f"SELECT referrer, COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT "
            f"WHERE {consented_where} AND referrer IS NOT NULL GROUP BY referrer ORDER BY COUNT(*) DESC",
            binds,
        )

        bots = {
            "bot_events": _query_scalar(
                conn,
                f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {_bot_where()}",
                binds,
            ),
            "top_user_agents": _fmt_kv(
                _query_rows(
                    conn,
                    f"SELECT user_agent, COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT "
                    f"WHERE {_bot_where()} GROUP BY user_agent ORDER BY COUNT(*) DESC",
                    binds,
                ),
                limit=5,
            ),
        }

        probes = {
            "probe_events": _query_scalar(
                conn,
                f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {_probe_where()}",
                binds,
            ),
            "top_paths": _fmt_kv(
                _query_rows(
                    conn,
                    f"SELECT path, COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT "
                    f"WHERE {_probe_where()} GROUP BY path ORDER BY COUNT(*) DESC",
                    binds,
                ),
                limit=10,
            ),
        }

        response_stats = _response_stats(conn, real_where, binds)

        new_verified = None
        try:
            new_verified = _query_scalar(
                conn,
                "SELECT COUNT(DISTINCT user_email) FROM WKSP_ESGAPEX.U_UX_EVENTS "
                "WHERE event_ts >= :start_ts AND event_ts < :end_ts AND event_type='auth_verify_ok'",
                binds,
            )
        except Exception:
            new_verified = None

    report.update(
        {
            "totals": totals,
            "uniques": uniques,
            "consent": consent,
            "countries": _fmt_kv(countries, limit=10),
            "unknown_country_count": unknown_country,
            "top_pages": _fmt_kv(top_pages, limit=10),
            "referrers": _referrer_hosts(referrers),
            "bots": bots,
            "probes": probes,
            "response_ms": response_stats,
            "new_verified_users": new_verified,
        }
    )
    return report


def build_report(windows: Iterable[str]) -> Dict[str, Any]:
    now = _utc_now()
    call_timeout_ms = int(os.getenv("TELEMETRY_REPORT_CALL_TIMEOUT_MS", "30000"))
    oracledb.defaults.call_timeout = max(1000, call_timeout_ms)

    report: Dict[str, Any] = {
        "generated_at": now.isoformat(),
        "windows": [],
    }
    for label in windows:
        start_ts, end_ts = _window_range(label, now)
        report["windows"].append(build_window_report(label, start_ts, end_ts))
    return report


def _fmt_table(rows: List[Tuple[str, Any]], width: int = 40) -> List[str]:
    lines: List[str] = []
    for key, value in rows:
        label = str(key)[:width].ljust(width)
        lines.append(f"- {label} {value}")
    return lines


def render_text(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"Telemetry usage report (generated {report['generated_at']})")
    lines.append("")

    for window in report.get("windows", []):
        lines.append(f"=== Window: {window['window']} ({window['start']} → {window['end']}) ===")
        totals = window["totals"]
        lines.append("Totals (filtered real traffic):")
        lines.append(f"- events: {totals['events']}")
        lines.append(f"- page views: {totals['page_views']}")
        lines.append(f"- api calls: {totals['api_calls']}")
        lines.append(f"- error events: {totals['error_events']} (rate={totals['error_rate']})")
        resp = window["response_ms"]
        lines.append(f"- response ms p50: {resp['p50_ms']} p95: {resp['p95_ms']}")
        lines.append("")

        lines.append("Uniques (consented analytics only):")
        lines.append(f"- sessions: {window['uniques']['sessions']}")
        lines.append(f"- visitors (ip_hash): {window['uniques']['visitors']}")
        lines.append("")

        lines.append("Consent split (real traffic):")
        lines.append(f"- consented events: {window['consent']['consented_events']}")
        lines.append(f"- non-consented events: {window['consent']['nonconsented_events']}")
        lines.append("")

        lines.append("Top countries (consented analytics only):")
        lines.extend(_fmt_table([(row["key"], row["count"]) for row in window.get("countries", [])]))
        lines.append(f"- unknown: {window['unknown_country_count']}")
        lines.append("")

        lines.append("Top pages (consented analytics only):")
        lines.extend(_fmt_table([(row["key"], row["count"]) for row in window.get("top_pages", [])]))
        lines.append("")

        lines.append("Top referrers (consented analytics only):")
        lines.extend(_fmt_table([(row["key"], row["count"]) for row in window.get("referrers", [])]))
        lines.append("")

        lines.append("Bots:")
        lines.append(f"- bot events: {window['bots']['bot_events']}")
        lines.extend(_fmt_table([(row["key"], row["count"]) for row in window['bots'].get('top_user_agents', [])]))
        lines.append("")

        lines.append("Probes:")
        lines.append(f"- probe events: {window['probes']['probe_events']}")
        lines.extend(_fmt_table([(row["key"], row["count"]) for row in window['probes'].get('top_paths', [])]))
        lines.append("")

        new_verified = window.get("new_verified_users")
        if new_verified is None:
            lines.append("New verified users: unavailable")
        else:
            lines.append(f"New verified users: {new_verified}")
        lines.append("")

    return "\n".join(lines)


def render_html(report: Dict[str, Any]) -> str:
    parts: List[str] = []
    parts.append(f"<h2>Telemetry usage report</h2><p>Generated {report['generated_at']}</p>")
    for window in report.get("windows", []):
        parts.append(f"<h3>Window: {window['window']} ({window['start']} → {window['end']})</h3>")
        totals = window["totals"]
        parts.append("<h4>Totals (filtered real traffic)</h4><ul>")
        parts.append(f"<li>events: {totals['events']}</li>")
        parts.append(f"<li>page views: {totals['page_views']}</li>")
        parts.append(f"<li>api calls: {totals['api_calls']}</li>")
        parts.append(f"<li>error events: {totals['error_events']} (rate={totals['error_rate']})</li>")
        resp = window["response_ms"]
        parts.append(f"<li>response ms p50: {resp['p50_ms']} p95: {resp['p95_ms']}</li>")
        parts.append("</ul>")

        parts.append("<h4>Uniques (consented analytics only)</h4><ul>")
        parts.append(f"<li>sessions: {window['uniques']['sessions']}</li>")
        parts.append(f"<li>visitors (ip_hash): {window['uniques']['visitors']}</li>")
        parts.append("</ul>")

        parts.append("<h4>Consent split (real traffic)</h4><ul>")
        parts.append(f"<li>consented events: {window['consent']['consented_events']}</li>")
        parts.append(f"<li>non-consented events: {window['consent']['nonconsented_events']}</li>")
        parts.append("</ul>")

        def _list_items(rows: List[Dict[str, Any]]) -> str:
            return "".join(f"<li>{row['key']}: {row['count']}</li>" for row in rows)

        parts.append("<h4>Top countries (consented analytics only)</h4><ul>")
        parts.append(_list_items(window.get("countries", [])))
        parts.append(f"<li>unknown: {window['unknown_country_count']}</li>")
        parts.append("</ul>")

        parts.append("<h4>Top pages (consented analytics only)</h4><ul>")
        parts.append(_list_items(window.get("top_pages", [])))
        parts.append("</ul>")

        parts.append("<h4>Top referrers (consented analytics only)</h4><ul>")
        parts.append(_list_items(window.get("referrers", [])))
        parts.append("</ul>")

        parts.append("<h4>Bots</h4><ul>")
        parts.append(f"<li>bot events: {window['bots']['bot_events']}</li>")
        parts.append(_list_items(window['bots'].get('top_user_agents', [])))
        parts.append("</ul>")

        parts.append("<h4>Probes</h4><ul>")
        parts.append(f"<li>probe events: {window['probes']['probe_events']}</li>")
        parts.append(_list_items(window['probes'].get('top_paths', [])))
        parts.append("</ul>")

        new_verified = window.get("new_verified_users")
        parts.append("<h4>New verified users</h4>")
        if new_verified is None:
            parts.append("<p>unavailable</p>")
        else:
            parts.append(f"<p>{new_verified}</p>")

    return "\n".join(parts)


def _resolve_windows(args) -> List[str]:
    if args.all:
        return ["24h", "7d", "30d"]
    if args.window:
        if args.window not in WINDOWS:
            raise ValueError("invalid_window")
        return [args.window]
    return ["24h"]


def _send_email(report: Dict[str, Any], subject: str, to_override: Optional[str]) -> bool:
    recipients = _csv_list(os.getenv("TELEMETRY_REPORT_RECIPIENTS"))
    if to_override:
        recipients = [to_override]
    if not recipients:
        mail_to = os.getenv("MAIL_TO")
        if mail_to:
            recipients = [mail_to]
    if not recipients:
        raise RuntimeError("telemetry_report_missing_recipients")

    text_body = render_text(report)
    html_body = render_html(report)
    results = []
    for recipient in recipients:
        try:
            results.append(send_email_to(recipient, subject, text_body, html_body=html_body))
        except TypeError:
            results.append(send_email_to(recipient, subject, text_body))
    return all(results)


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("app.email").setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description="Telemetry usage report from W_WEB_EVENT")
    parser.add_argument("--window", help="Window: 24h, 7d, or 30d")
    parser.add_argument("--all", action="store_true", help="Include 24h, 7d, 30d sections")
    parser.add_argument("--send", action="store_true", help="Send report email")
    parser.add_argument("--dry-run", action="store_true", help="Print report to stdout")
    parser.add_argument("--to", help="Override recipient for testing")
    parser.add_argument("--json-out", help="Write JSON output to this path")
    args = parser.parse_args(argv)

    windows = _resolve_windows(args)
    report = build_report(windows)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True)

    if args.send:
        subject = f"Telemetry usage report ({', '.join(windows)})"
        sent = _send_email(report, subject, args.to)
        if not sent:
            logging.error("telemetry_report_send_failed")
            raise RuntimeError("telemetry_report_send_failed")
        print("SMTP_ACCEPTED")

    if args.dry_run or not args.send:
        print(render_text(report))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
