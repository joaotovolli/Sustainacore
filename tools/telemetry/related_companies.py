from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

import db_helper
from tools.telemetry.report_filters import DEFAULT_BOT_UA_REGEX, DEFAULT_PROBE_PATH_REGEX


LOGGER = logging.getLogger("telemetry.related")
PRIVATE_IP_REGEX = r"^(127\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|0\.|fc|fd|fe80)"
BOT_PAYLOAD_REGEX = r"\"bot\"\s*:\s*true"


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _csv_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _bind_list(prefix: str, values: Iterable[str], binds: Dict[str, Any]) -> str:
    placeholders = []
    for idx, value in enumerate(values):
        key = f"{prefix}_{idx}"
        binds[key] = value
        placeholders.append(f":{key}")
    return ", ".join(placeholders)


def _bot_regex() -> str:
    extra = os.getenv("RELATED_BOT_UA_REGEX")
    if extra:
        return f"({DEFAULT_BOT_UA_REGEX})|({extra})"
    return DEFAULT_BOT_UA_REGEX


def _probe_regex() -> str:
    extra = os.getenv("RELATED_PROBE_PATH_REGEX")
    if extra:
        return f"({DEFAULT_PROBE_PATH_REGEX})|({extra})"
    return DEFAULT_PROBE_PATH_REGEX


def _build_exclusions(binds: Dict[str, Any]) -> List[str]:
    clauses: List[str] = []
    ip_hashes = _csv_list(os.getenv("RELATED_EXCLUDE_IP_HASHES"))
    if ip_hashes:
        placeholder = _bind_list("ip_hash", ip_hashes, binds)
        clauses.append(f"(ip_hash IS NULL OR ip_hash NOT IN ({placeholder}))")
    user_ids = _csv_list(os.getenv("RELATED_EXCLUDE_USER_IDS"))
    if user_ids:
        placeholder = _bind_list("user_id", user_ids, binds)
        clauses.append(f"(user_id IS NULL OR user_id NOT IN ({placeholder}))")
    ua_substrings = _csv_list(os.getenv("RELATED_EXCLUDE_UA_SUBSTRINGS"))
    if ua_substrings:
        regex = "|".join(re.escape(token.lower()) for token in ua_substrings if token)
        if regex:
            binds["exclude_ua_regex"] = regex
            clauses.append("NOT REGEXP_LIKE(LOWER(NVL(user_agent,'')), :exclude_ua_regex)")
    return clauses


def _company_ticker_from_path(path: Optional[str]) -> Optional[str]:
    value = (path or "").strip()
    if not value:
        return None
    match = re.match(r"^/tech100/company/([A-Za-z0-9]+)(/)?$", value)
    if not match:
        return None
    return match.group(1).upper()


def _is_google_referrer(referrer: Optional[str]) -> bool:
    value = (referrer or "").lower()
    if not value:
        return False
    if "google." in value:
        return True
    if "://www.google" in value or "://google" in value:
        return True
    if "/url?" in value and "google" in value:
        return True
    return False


def _load_universe(cursor) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    sql = (
        "SELECT ticker, company_name, gics_sector, rank_index "
        "FROM ("
        "SELECT ticker, company_name, gics_sector, rank_index, "
        "ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY port_date DESC) AS rn "
        "FROM tech11_ai_gov_eth_index "
        "WHERE ticker IS NOT NULL"
        ") "
        "WHERE rn = 1 "
        "ORDER BY NVL(rank_index, 9999), ticker"
    )
    cursor.execute(sql)
    rows = cursor.fetchall()
    universe = []
    by_ticker = {}
    for ticker, company_name, sector, rank_index in rows:
        ticker_val = str(ticker).strip().upper()
        if not ticker_val:
            continue
        entry = {
            "ticker": ticker_val,
            "company_name": company_name,
            "sector": sector,
            "rank_index": rank_index,
        }
        universe.append(entry)
        by_ticker[ticker_val] = entry
    return universe, by_ticker


def _load_events(
    cursor, start_ts: dt.datetime, end_ts: dt.datetime, max_events: Optional[int] = None
) -> List[Tuple]:
    binds: Dict[str, Any] = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "private_ip_regex": PRIVATE_IP_REGEX,
        "bot_ua_regex": _bot_regex(),
        "bot_payload_regex": BOT_PAYLOAD_REGEX,
        "probe_path_regex": _probe_regex(),
    }
    clauses = [
        "event_ts >= :start_ts",
        "event_ts < :end_ts",
        "consent_analytics_effective = 'Y'",
        "event_type = 'page_view'",
        "path LIKE '/tech100/company/%'",
        "NOT REGEXP_LIKE(NVL(referrer,''), 'preview\\.|localhost|127\\.0\\.0\\.1', 'i')",
        "NOT REGEXP_LIKE(LOWER(NVL(ip_trunc,'')), :private_ip_regex)",
        "NOT (REGEXP_LIKE(LOWER(NVL(user_agent,'')), :bot_ua_regex) "
        "OR REGEXP_LIKE(NVL(payload_json,''), :bot_payload_regex, 'i'))",
        "NOT REGEXP_LIKE(NVL(path,''), :probe_path_regex, 'i')",
    ]
    clauses.extend(_build_exclusions(binds))
    where = " AND ".join(clauses)
    base_sql = (
        "SELECT event_ts, session_key, path, referrer, user_agent, user_id, ip_hash "
        f"FROM WKSP_ESGAPEX.W_WEB_EVENT WHERE {where} "
        "ORDER BY session_key, event_ts"
    )
    if max_events:
        binds["max_events"] = max_events
        sql = f"SELECT * FROM ({base_sql}) WHERE ROWNUM <= :max_events"
    else:
        sql = base_sql
    cursor.execute(sql, binds)
    return cursor.fetchall()


def _build_transition_scores(
    rows: List[Tuple],
    google_boost_weight: float,
    boost_window_minutes: int = 30,
) -> Tuple[Dict[str, Dict[str, float]], Dict[Tuple[str, str], str]]:
    scores: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    reasons: Dict[Tuple[str, str], str] = {}
    last_seen: Dict[str, Tuple[str, dt.datetime]] = {}
    boost_window = dt.timedelta(minutes=boost_window_minutes)
    for event_ts, session_key, path, referrer, _ua, _user_id, _ip_hash in rows:
        if not session_key:
            continue
        ticker = _company_ticker_from_path(path)
        if not ticker:
            continue
        prev = last_seen.get(session_key)
        if prev:
            prev_ticker, prev_ts = prev
            if prev_ticker and prev_ticker != ticker:
                scores[prev_ticker][ticker] += 1.0
                reasons[(prev_ticker, ticker)] = "co_view"
                if _is_google_referrer(referrer) and (event_ts - prev_ts) <= boost_window:
                    scores[prev_ticker][ticker] += google_boost_weight
                    reasons[(prev_ticker, ticker)] = "google_boost"
        last_seen[session_key] = (ticker, event_ts)
    return scores, reasons


def _rank_neighbors(universe: List[Dict[str, Any]], ticker: str, limit: int) -> List[Dict[str, Any]]:
    tickers = [row["ticker"] for row in universe]
    try:
        idx = tickers.index(ticker)
    except ValueError:
        return []
    neighbors = []
    for offset in range(1, len(universe)):
        for candidate_idx in (idx - offset, idx + offset):
            if 0 <= candidate_idx < len(universe):
                candidate = universe[candidate_idx]
                if candidate["ticker"] != ticker:
                    neighbors.append(candidate)
            if len(neighbors) >= limit:
                break
        if len(neighbors) >= limit:
            break
    return neighbors[:limit]


def _sector_neighbors(
    universe: List[Dict[str, Any]],
    ticker: str,
    sector: Optional[str],
    limit: int,
) -> List[Dict[str, Any]]:
    if not sector:
        return []
    sector_list = [row for row in universe if (row.get("sector") or "").strip().lower() == sector]
    if not sector_list:
        return []
    sector_list = sorted(
        sector_list,
        key=lambda row: (row.get("rank_index") is None, row.get("rank_index") or 9999, row["ticker"]),
    )
    tickers = [row["ticker"] for row in sector_list]
    try:
        idx = tickers.index(ticker)
    except ValueError:
        return sector_list[:limit]
    neighbors = []
    for offset in range(1, len(sector_list)):
        for candidate_idx in (idx - offset, idx + offset):
            if 0 <= candidate_idx < len(sector_list):
                candidate = sector_list[candidate_idx]
                if candidate["ticker"] != ticker:
                    neighbors.append(candidate)
            if len(neighbors) >= limit:
                break
        if len(neighbors) >= limit:
            break
    return neighbors[:limit]


def _fill_related(
    ticker: str,
    scores: Dict[str, Dict[str, float]],
    reasons: Dict[Tuple[str, str], str],
    universe: List[Dict[str, Any]],
    by_ticker: Dict[str, Dict[str, Any]],
    top_k: int,
) -> List[Tuple[str, float, str]]:
    related = []
    telemetry_items = scores.get(ticker, {})
    for related_ticker, score in sorted(telemetry_items.items(), key=lambda item: item[1], reverse=True):
        if related_ticker == ticker or related_ticker not in by_ticker:
            continue
        reason = reasons.get((ticker, related_ticker), "co_view")
        related.append((related_ticker, score, reason))
        if len(related) >= top_k:
            return related
    sector = (by_ticker.get(ticker, {}).get("sector") or "").strip().lower()
    if len(related) < top_k and sector:
        for entry in _sector_neighbors(universe, ticker, sector, top_k):
            if len(related) >= top_k:
                break
            related_ticker = entry["ticker"]
            if related_ticker == ticker or any(r[0] == related_ticker for r in related):
                continue
            related.append((related_ticker, 0.0, "sector_fallback"))
    if len(related) < top_k:
        for entry in _rank_neighbors(universe, ticker, top_k):
            if len(related) >= top_k:
                break
            related_ticker = entry["ticker"]
            if related_ticker == ticker or any(r[0] == related_ticker for r in related):
                continue
            related.append((related_ticker, 0.0, "rank_fallback"))
    return related[:top_k]


def _ensure_table(cursor) -> None:
    cursor.execute(
        "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = :owner AND TABLE_NAME = 'TECH100_RELATED_COMPANIES'",
        {"owner": os.getenv("DB_USER", "WKSP_ESGAPEX").upper()},
    )
    exists = cursor.fetchone()[0] > 0
    if exists:
        return
    cursor.execute(
        """
        CREATE TABLE TECH100_RELATED_COMPANIES (
            ticker VARCHAR2(16) NOT NULL,
            related_ticker VARCHAR2(16) NOT NULL,
            score NUMBER(18,6) DEFAULT 0 NOT NULL,
            reason VARCHAR2(32),
            window_days NUMBER(6) NOT NULL,
            computed_ts TIMESTAMP NOT NULL,
            CONSTRAINT tech100_related_companies_pk PRIMARY KEY (ticker, related_ticker, window_days)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX tech100_related_idx ON TECH100_RELATED_COMPANIES (ticker, window_days, score DESC)"
    )


def build_related_companies(
    *,
    window_days: int,
    top_k: int,
    google_boost_weight: float,
    max_events: Optional[int],
    dry_run: bool,
) -> Dict[str, Any]:
    now = _utc_now()
    start_ts = now - dt.timedelta(days=window_days)
    with db_helper.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("ALTER SESSION DISABLE PARALLEL DML")
        _ensure_table(cursor)
        universe, by_ticker = _load_universe(cursor)
        rows = _load_events(cursor, start_ts, now, max_events=max_events)
        scores, reasons = _build_transition_scores(rows, google_boost_weight)
        computed_ts = now
        payload_rows = []
        for entry in universe:
            ticker = entry["ticker"]
            related = _fill_related(ticker, scores, reasons, universe, by_ticker, top_k)
            for related_ticker, score, reason in related:
                payload_rows.append(
                    (
                        ticker,
                        related_ticker,
                        score,
                        reason,
                        window_days,
                        computed_ts,
                    )
                )
        summary = {
            "window_days": window_days,
            "events": len(rows),
            "tickers": len(universe),
            "rows": len(payload_rows),
            "computed_ts": computed_ts.isoformat(),
        }
        if dry_run:
            return summary
        cursor.execute("LOCK TABLE TECH100_RELATED_COMPANIES IN EXCLUSIVE MODE")
        cursor.execute(
            "DELETE FROM TECH100_RELATED_COMPANIES WHERE window_days = :window_days",
            {"window_days": window_days},
        )
        cursor.executemany(
            "INSERT INTO TECH100_RELATED_COMPANIES "
            "(ticker, related_ticker, score, reason, window_days, computed_ts) "
            "VALUES (:1, :2, :3, :4, :5, :6)",
            payload_rows,
        )
        conn.commit()
        return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Tech100 related companies from telemetry.")
    parser.add_argument("--window-days", type=int, default=int(os.getenv("RELATED_WINDOW_DAYS", "30")))
    parser.add_argument("--top-k", type=int, default=int(os.getenv("RELATED_TOP_K", "12")))
    parser.add_argument(
        "--google-boost-weight",
        type=float,
        default=float(os.getenv("RELATED_GOOGLE_BOOST_WEIGHT", "3.0")),
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=int(os.getenv("RELATED_MAX_EVENTS", "0")),
        help="Optional cap on events processed (0 means no cap).",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    summary = build_related_companies(
        window_days=args.window_days,
        top_k=args.top_k,
        google_boost_weight=args.google_boost_weight,
        max_events=args.max_events if args.max_events and args.max_events > 0 else None,
        dry_run=args.dry_run,
    )
    LOGGER.info(
        "related_companies_done window_days=%s events=%s tickers=%s rows=%s computed_ts=%s",
        summary["window_days"],
        summary["events"],
        summary["tickers"],
        summary["rows"],
        summary["computed_ts"],
    )


if __name__ == "__main__":
    main()
