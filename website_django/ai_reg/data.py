from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Iterable, List

import oracledb
from django.core.cache import cache

from core.oracle_db import get_connection

HEATMAP_CACHE_TTL = 600
EU_ISO_ALIASES = {"EU", "EUR"}
EU_NAME_ALIASES = {"EUROPEAN UNION", "EU"}


class AiRegDataError(RuntimeError):
    pass


def _to_plain(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, oracledb.LOB):
        return value.read()
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.decode("latin-1", errors="ignore")
    return value


def _to_date(value: Any) -> dt.date | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return None


def _normalize_jurisdiction(iso_code: str | None, name: str | None) -> tuple[str, str]:
    iso_text = str(iso_code or "").strip().upper()
    name_text = str(name or "").strip()
    name_upper = name_text.upper()
    if iso_text in EU_ISO_ALIASES or name_upper in EU_NAME_ALIASES or "EUROPEAN UNION" in name_upper:
        return "EU", "European Union"
    return iso_text, name_text


def _execute_rows(sql: str, params: dict) -> list[tuple]:
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            return cur.fetchall()
    except oracledb.Error as exc:
        raise AiRegDataError("AI regulation data is unavailable (Oracle connection failed).") from exc


def fetch_as_of_dates() -> List[str]:
    sql = """
        SELECT DISTINCT AS_OF_DATE
        FROM FACT_INSTRUMENT_SNAPSHOT
        ORDER BY AS_OF_DATE DESC
    """
    rows = _execute_rows(sql, {})
    dates: List[str] = []
    for (value,) in rows:
        as_of = _to_date(value)
        if as_of:
            dates.append(as_of.isoformat())
    return dates


def fetch_heatmap(as_of_date: dt.date) -> List[Dict[str, object]]:
    cache_key = f"ai_reg:heatmap:{as_of_date.isoformat()}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    sql = """
        SELECT j.ISO_CODE,
               j.NAME,
               COUNT(*) AS instruments_count,
               SUM(CASE WHEN s.VERIFICATION_STATUS = 'primary_verified' THEN 1 ELSE 0 END) AS primary_verified_count,
               SUM(CASE WHEN s.VERIFICATION_STATUS = 'secondary_only' THEN 1 ELSE 0 END) AS secondary_only_count,
               SUM(CASE WHEN s.VERIFICATION_STATUS = 'no_ai_specific_binding_instrument_found' THEN 1 ELSE 0 END)
                   AS no_instrument_found_count,
               SUM(CASE WHEN NOT EXISTS (
                    SELECT 1 FROM BRG_SNAPSHOT_SOURCE bs
                    WHERE bs.SNAPSHOT_SK = s.SNAPSHOT_SK
               ) THEN 1 ELSE 0 END) AS snapshots_without_source
        FROM FACT_INSTRUMENT_SNAPSHOT s
        JOIN DIM_JURISDICTION j ON s.JURISDICTION_SK = j.JURISDICTION_SK
        WHERE s.AS_OF_DATE = :as_of
        GROUP BY j.ISO_CODE, j.NAME
        ORDER BY j.NAME
    """
    rows = _execute_rows(sql, {"as_of": as_of_date})

    milestones_sql = """
        SELECT j.ISO_CODE,
               COUNT(*) AS milestones_upcoming_count
        FROM FACT_INSTRUMENT_SNAPSHOT s
        JOIN DIM_JURISDICTION j ON s.JURISDICTION_SK = j.JURISDICTION_SK
        JOIN FACT_SNAPSHOT_MILESTONE_DATE m ON s.SNAPSHOT_SK = m.SNAPSHOT_SK
        WHERE s.AS_OF_DATE = :as_of
          AND m.MILESTONE_DATE >= :as_of
          AND m.MILESTONE_DATE < ADD_MONTHS(:as_of, 24)
        GROUP BY j.ISO_CODE
    """
    milestone_rows = _execute_rows(milestones_sql, {"as_of": as_of_date})
    milestones_by_iso = {str(_to_plain(iso) or "").upper(): int(count or 0) for iso, count in milestone_rows}

    results_by_iso: Dict[str, Dict[str, object]] = {}
    for iso_code, name, total, primary, secondary, no_instrument, without_source in rows:
        iso_text, name_text = _normalize_jurisdiction(_to_plain(iso_code), _to_plain(name))
        snapshots_without_source = int(without_source or 0)
        payload = results_by_iso.get(iso_text)
        if not payload:
            payload = {
                "iso2": iso_text,
                "name": name_text,
                "instruments_count": 0,
                "instrument_count": 0,
                "primary_verified_count": 0,
                "secondary_only_count": 0,
                "no_instrument_found_count": 0,
                "milestones_upcoming_count": int(milestones_by_iso.get(iso_text, 0)),
                "data_quality": {
                    "snapshots_without_source": 0,
                    "flag": False,
                },
            }
            results_by_iso[iso_text] = payload

        payload["instruments_count"] = int(payload["instruments_count"]) + int(total or 0)
        payload["instrument_count"] = int(payload["instrument_count"]) + int(total or 0)
        payload["primary_verified_count"] = int(payload["primary_verified_count"]) + int(primary or 0)
        payload["secondary_only_count"] = int(payload["secondary_only_count"]) + int(secondary or 0)
        payload["no_instrument_found_count"] = int(payload["no_instrument_found_count"]) + int(no_instrument or 0)
        dq = payload["data_quality"]
        dq["snapshots_without_source"] = int(dq["snapshots_without_source"]) + snapshots_without_source
        dq["flag"] = dq["snapshots_without_source"] > 0

    results: List[Dict[str, object]] = list(results_by_iso.values())

    cache.set(cache_key, results, HEATMAP_CACHE_TTL)
    return results


def fetch_jurisdiction_summary(iso2: str, as_of_date: dt.date) -> Dict[str, object] | None:
    sql = """
        SELECT j.ISO_CODE,
               j.NAME
        FROM FACT_INSTRUMENT_SNAPSHOT s
        JOIN DIM_JURISDICTION j ON s.JURISDICTION_SK = j.JURISDICTION_SK
        WHERE s.AS_OF_DATE = :as_of
          AND UPPER(j.ISO_CODE) = :iso2
        GROUP BY j.ISO_CODE, j.NAME
    """
    rows = _execute_rows(sql, {"as_of": as_of_date, "iso2": iso2})
    if not rows:
        return None
    iso_code, name = rows[0]

    obligations_sql = """
        SELECT COUNT(*)
        FROM FACT_SNAPSHOT_OBLIGATION o
        JOIN FACT_INSTRUMENT_SNAPSHOT s ON o.SNAPSHOT_SK = s.SNAPSHOT_SK
        JOIN DIM_JURISDICTION j ON s.JURISDICTION_SK = j.JURISDICTION_SK
        WHERE s.AS_OF_DATE = :as_of
          AND UPPER(j.ISO_CODE) = :iso2
    """
    obligations_rows = _execute_rows(obligations_sql, {"as_of": as_of_date, "iso2": iso2})
    obligations_count = int(obligations_rows[0][0] or 0) if obligations_rows else 0

    without_source_sql = """
        SELECT COUNT(*)
        FROM FACT_INSTRUMENT_SNAPSHOT s
        JOIN DIM_JURISDICTION j ON s.JURISDICTION_SK = j.JURISDICTION_SK
        WHERE s.AS_OF_DATE = :as_of
          AND UPPER(j.ISO_CODE) = :iso2
          AND NOT EXISTS (
              SELECT 1 FROM BRG_SNAPSHOT_SOURCE bs
              WHERE bs.SNAPSHOT_SK = s.SNAPSHOT_SK
          )
    """
    without_source_rows = _execute_rows(without_source_sql, {"as_of": as_of_date, "iso2": iso2})
    snapshots_without_source = int(without_source_rows[0][0] or 0) if without_source_rows else 0

    return {
        "iso2": str(_to_plain(iso_code) or "").upper(),
        "name": _to_plain(name) or "",
        "obligations_count": obligations_count,
        "data_quality": {
            "snapshots_without_source": snapshots_without_source,
            "flag": snapshots_without_source > 0,
        },
    }


def fetch_jurisdiction_instruments(iso2: str, as_of_date: dt.date) -> List[Dict[str, object]]:
    sql = """
        SELECT i.TITLE_ENGLISH,
               i.TITLE_OFFICIAL,
               i.INSTRUMENT_TYPE,
               s.STATUS
        FROM FACT_INSTRUMENT_SNAPSHOT s
        JOIN DIM_JURISDICTION j ON s.JURISDICTION_SK = j.JURISDICTION_SK
        JOIN DIM_INSTRUMENT i ON s.INSTRUMENT_SK = i.INSTRUMENT_SK
        WHERE s.AS_OF_DATE = :as_of
          AND UPPER(j.ISO_CODE) = :iso2
        ORDER BY COALESCE(i.TITLE_ENGLISH, i.TITLE_OFFICIAL)
    """
    rows = _execute_rows(sql, {"as_of": as_of_date, "iso2": iso2})
    instruments: List[Dict[str, object]] = []
    for title_en, title_official, instrument_type, status in rows:
        instruments.append(
            {
                "title_english": _to_plain(title_en),
                "title_official": _to_plain(title_official),
                "instrument_type": _to_plain(instrument_type),
                "status": _to_plain(status),
            }
        )
    return instruments


def fetch_jurisdiction_timeline(iso2: str, as_of_date: dt.date) -> List[Dict[str, object]]:
    sql = """
        SELECT m.MILESTONE_TYPE,
               m.MILESTONE_DATE
        FROM FACT_SNAPSHOT_MILESTONE_DATE m
        JOIN FACT_INSTRUMENT_SNAPSHOT s ON m.SNAPSHOT_SK = s.SNAPSHOT_SK
        JOIN DIM_JURISDICTION j ON s.JURISDICTION_SK = j.JURISDICTION_SK
        WHERE s.AS_OF_DATE = :as_of
          AND UPPER(j.ISO_CODE) = :iso2
          AND m.MILESTONE_TYPE IN ('effective', 'enforcement_start')
        ORDER BY m.MILESTONE_DATE
    """
    rows = _execute_rows(sql, {"as_of": as_of_date, "iso2": iso2})
    timeline: List[Dict[str, object]] = []
    for milestone_type, milestone_date in rows:
        parsed_date = _to_date(milestone_date)
        timeline.append(
            {
                "milestone_type": _to_plain(milestone_type),
                "milestone_date": parsed_date.isoformat() if parsed_date else None,
            }
        )
    return timeline


def fetch_jurisdiction_sources(iso2: str, as_of_date: dt.date) -> List[Dict[str, object]]:
    sql = """
        SELECT DISTINCT src.TITLE,
               src.URL
        FROM FACT_INSTRUMENT_SNAPSHOT s
        JOIN DIM_JURISDICTION j ON s.JURISDICTION_SK = j.JURISDICTION_SK
        JOIN BRG_SNAPSHOT_SOURCE b ON s.SNAPSHOT_SK = b.SNAPSHOT_SK
        JOIN DIM_SOURCE src ON b.SOURCE_SK = src.SOURCE_SK
        WHERE s.AS_OF_DATE = :as_of
          AND UPPER(j.ISO_CODE) = :iso2
        ORDER BY src.TITLE
    """
    rows = _execute_rows(sql, {"as_of": as_of_date, "iso2": iso2})
    sources: List[Dict[str, object]] = []
    for title, url in rows:
        sources.append({"title": _to_plain(title), "url": _to_plain(url)})
    return sources
