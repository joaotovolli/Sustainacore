from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

import db_helper
import oracledb

from tools.telemetry.report_filters import DEFAULT_BOT_UA_REGEX, DEFAULT_PROBE_PATH_REGEX


PRIVATE_IP_REGEX = r"^(127\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|0\.|fc|fd|fe80)"
BOT_PAYLOAD_REGEX = r"\"bot\"\s*:\s*true"


def _csv_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _utc_date(value: Optional[str]) -> dt.date:
    if value:
        return dt.date.fromisoformat(value)
    return (dt.datetime.utcnow().date() - dt.timedelta(days=1))


def _date_range(day: dt.date) -> Tuple[dt.datetime, dt.datetime]:
    start = dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(days=1)
    return start, end


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
    clauses = ["event_ts >= :start_ts", "event_ts < :end_ts"]
    return " AND ".join(clauses)


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


def _bot_where(binds: Dict[str, Any]) -> str:
    clauses = [_base_where()]
    clauses.append(
        "(REGEXP_LIKE(LOWER(NVL(user_agent,'')), :bot_ua_regex) "
        "OR REGEXP_LIKE(NVL(payload_json,''), :bot_payload_regex, 'i'))"
    )
    return " AND ".join(clauses)


def _probe_where(binds: Dict[str, Any]) -> str:
    clauses = [_base_where()]
    clauses.append("REGEXP_LIKE(NVL(path,''), :probe_path_regex, 'i')")
    return " AND ".join(clauses)


def _query_scalar(conn, sql: str, binds: Dict[str, Any]) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, _select_binds(sql, binds))
        row = cur.fetchone()
        return int((row or [0])[0] or 0)


def _query_rows(conn, sql: str, binds: Dict[str, Any]) -> List[Tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(sql, _select_binds(sql, binds))
        return list(cur.fetchall() or [])


def _select_binds(sql: str, binds: Dict[str, Any]) -> Dict[str, Any]:
    import re

    keys = set(re.findall(r":([a-zA-Z_][a-zA-Z0-9_]*)", sql))
    return {key: value for key, value in binds.items() if key in keys}


def _fmt_kv(rows: Iterable[Tuple[Any, Any]], *, limit: int = 10) -> List[Dict[str, Any]]:
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


def build_report(day: dt.date) -> Dict[str, Any]:
    start_ts, end_ts = _date_range(day)
    call_timeout_ms = int(os.getenv("TELEMETRY_REPORT_CALL_TIMEOUT_MS", "30000"))
    oracledb.defaults.call_timeout = max(1000, call_timeout_ms)
    binds: Dict[str, Any] = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "private_ip_regex": PRIVATE_IP_REGEX,
        "bot_ua_regex": _bot_regex(),
        "bot_payload_regex": BOT_PAYLOAD_REGEX,
        "probe_path_regex": _probe_regex(),
    }

    report: Dict[str, Any] = {
        "date": day.isoformat(),
        "utc_window": {"start": start_ts.isoformat(), "end": end_ts.isoformat()},
    }

    with db_helper.get_connection() as conn:
        real_where = _real_traffic_where(binds)
        base_where = _base_where()
        bot_where = _bot_where(binds)
        probe_where = _probe_where(binds)

        total_events = _query_scalar(
            conn,
            f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {real_where}",
            binds,
        )
        total_page_views = _query_scalar(
            conn,
            f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {real_where} AND event_type='page_view'",
            binds,
        )
        total_api_calls = _query_scalar(
            conn,
            f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {real_where} AND event_type='api_call'",
            binds,
        )
        error_events = _query_scalar(
            conn,
            f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {real_where} AND NVL(status_code,0) >= 400",
            binds,
        )

        consented_where = f"{real_where} AND consent_analytics_effective='Y'"
        nonconsented_where = f"{real_where} AND consent_analytics_effective='N'"

        unique_sessions = _query_scalar(
            conn,
            f"SELECT COUNT(DISTINCT session_key) FROM WKSP_ESGAPEX.W_WEB_EVENT "
            f"WHERE {consented_where} AND session_key IS NOT NULL",
            binds,
        )
        unique_visitors = _query_scalar(
            conn,
            f"SELECT COUNT(DISTINCT ip_hash) FROM WKSP_ESGAPEX.W_WEB_EVENT "
            f"WHERE {consented_where} AND ip_hash IS NOT NULL",
            binds,
        )
        nonconsented_events = _query_scalar(
            conn,
            f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {nonconsented_where}",
            binds,
        )

        countries = _query_rows(
            conn,
            f"SELECT NVL(country_code,'unknown') AS country, COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT "
            f"WHERE {consented_where} GROUP BY NVL(country_code,'unknown') ORDER BY COUNT(*) DESC",
            binds,
        )
        top_pages = _query_rows(
            conn,
            f"SELECT path, COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT "
            f"WHERE {consented_where} AND event_type='page_view' GROUP BY path ORDER BY COUNT(*) DESC",
            binds,
        )
        referrers = _query_rows(
            conn,
            f"SELECT referrer, COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT "
            f"WHERE {consented_where} AND referrer IS NOT NULL GROUP BY referrer ORDER BY COUNT(*) DESC",
            binds,
        )
        bot_count = _query_scalar(
            conn,
            f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {bot_where}",
            binds,
        )
        bot_uas = _query_rows(
            conn,
            f"SELECT user_agent, COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT "
            f"WHERE {bot_where} GROUP BY user_agent ORDER BY COUNT(*) DESC",
            binds,
        )
        probe_count = _query_scalar(
            conn,
            f"SELECT COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {probe_where}",
            binds,
        )
        probe_paths = _query_rows(
            conn,
            f"SELECT path, COUNT(*) FROM WKSP_ESGAPEX.W_WEB_EVENT "
            f"WHERE {probe_where} GROUP BY path ORDER BY COUNT(*) DESC",
            binds,
        )

        new_verified = None
        try:
            new_verified = _query_scalar(
                conn,
                "SELECT COUNT(DISTINCT user_email) FROM WKSP_ESGAPEX.U_UX_EVENTS "
                f"WHERE event_ts >= :start_ts AND event_ts < :end_ts AND event_type='auth_verify_ok'",
                binds,
            )
        except Exception:
            new_verified = None

    report.update(
        {
            "totals": {
                "events": total_events,
                "page_views": total_page_views,
                "api_calls": total_api_calls,
                "error_events": error_events,
                "error_rate": _safe_ratio(error_events, total_events),
            },
            "uniques": {
                "sessions": unique_sessions,
                "visitors": unique_visitors,
            },
            "consent": {
                "consented_events": total_events - nonconsented_events,
                "nonconsented_events": nonconsented_events,
            },
            "countries": _fmt_kv(countries, limit=10),
            "top_pages": _fmt_kv(top_pages, limit=10),
            "referrers": _referrer_hosts(referrers),
            "bots": {
                "bot_events": bot_count,
                "top_user_agents": _fmt_kv(bot_uas, limit=5),
            },
            "probes": {
                "probe_events": probe_count,
                "top_paths": _fmt_kv(probe_paths, limit=10),
            },
            "new_verified_users": new_verified,
        }
    )
    return report


def render_report(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"Telemetry report for {report['date']} (UTC)")
    lines.append("")
    totals = report["totals"]
    lines.append("Totals (filtered real traffic):")
    lines.append(f"- events: {totals['events']}")
    lines.append(f"- page views: {totals['page_views']}")
    lines.append(f"- api calls: {totals['api_calls']}")
    lines.append(f"- error events: {totals['error_events']} (rate={totals['error_rate']})")
    lines.append("")
    lines.append("Uniques (consented analytics only):")
    lines.append(f"- sessions: {report['uniques']['sessions']}")
    lines.append(f"- visitors (ip_hash): {report['uniques']['visitors']}")
    lines.append("")
    lines.append("Consent split (real traffic):")
    lines.append(f"- consented events: {report['consent']['consented_events']}")
    lines.append(f"- non-consented events: {report['consent']['nonconsented_events']}")
    lines.append("")
    lines.append("Top countries (consented analytics only):")
    for row in report.get("countries", []):
        lines.append(f"- {row['key']}: {row['count']}")
    lines.append("")
    lines.append("Top pages (consented analytics only):")
    for row in report.get("top_pages", []):
        lines.append(f"- {row['key']}: {row['count']}")
    lines.append("")
    lines.append("Top referrers (consented analytics only):")
    for row in report.get("referrers", []):
        lines.append(f"- {row['key']}: {row['count']}")
    lines.append("")
    lines.append("Bots:")
    lines.append(f"- bot events: {report['bots']['bot_events']}")
    for row in report.get("bots", {}).get("top_user_agents", []):
        lines.append(f"- {row['key']}: {row['count']}")
    lines.append("")
    lines.append("Probes:")
    lines.append(f"- probe events: {report['probes']['probe_events']}")
    for row in report.get("probes", {}).get("top_paths", []):
        lines.append(f"- {row['key']}: {row['count']}")
    lines.append("")
    new_verified = report.get("new_verified_users")
    if new_verified is None:
        lines.append("New verified users: unavailable")
    else:
        lines.append(f"New verified users: {new_verified}")
    return "\n".join(lines)


def _send_email(subject: str, body: str) -> None:
    from app.index_engine.alerts import send_email_to

    recipients = _csv_list(os.getenv("TELEMETRY_REPORT_RECIPIENTS"))
    if not recipients:
        mail_to = os.getenv("MAIL_TO")
        if mail_to:
            recipients = [mail_to]
    if not recipients:
        raise RuntimeError("telemetry_report_missing_recipients")
    for recipient in recipients:
        send_email_to(recipient, subject, body)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Daily telemetry report from W_WEB_EVENT")
    parser.add_argument("--date", help="UTC date (YYYY-MM-DD), defaults to yesterday")
    parser.add_argument("--dry-run", action="store_true", help="Print report to stdout only")
    parser.add_argument("--send", action="store_true", help="Send report email")
    parser.add_argument("--json-out", help="Write JSON output to this path")
    args = parser.parse_args(argv)

    day = _utc_date(args.date)
    report = build_report(day)
    report_text = render_report(report)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True)

    if args.send:
        subject = f"Telemetry daily report ({day.isoformat()})"
        _send_email(subject, report_text)

    if args.dry_run or not args.send:
        print(report_text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
