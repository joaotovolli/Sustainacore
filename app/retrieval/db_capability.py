"""Database capability detection helpers for Oracle ESG_DOCS."""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple

from db_helper import get_connection
from .settings import settings

try:  # pragma: no cover - optional dependency
    import oracledb  # type: ignore
except Exception as exc:  # pragma: no cover - optional dependency
    oracledb = None  # type: ignore
    _ORACLE_IMPORT_ERROR = exc
else:  # pragma: no cover - optional dependency
    _ORACLE_IMPORT_ERROR = None


LOGGER = logging.getLogger("app.retrieval.capability")


@dataclass(frozen=True)
class Capability:
    """Summarises the Oracle features available to the retriever."""

    db_version: str = "unknown"
    vector_supported: bool = False
    vec_col: Optional[str] = None
    vec_dim: Optional[int] = None
    vector_rows: int = 0
    esg_docs_count: int = 0
    oracle_text_supported: bool = False


_CAPABILITY_LOCK = threading.Lock()
_CACHED_CAPABILITY: Optional[Capability] = None


def _safe_upper(value: Optional[str]) -> Optional[str]:
    return value.strip().upper() if isinstance(value, str) and value.strip() else None


def _is_missing_view(exc: Exception) -> bool:
    text = str(exc) if exc else ""
    if "ORA-00942" in text or "ORA-04043" in text:
        return True
    code = getattr(exc, "code", None)
    try:
        return int(code) in {942, 4043}
    except (TypeError, ValueError):
        return False


def _fetch_one(cursor, sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[Tuple[Any, ...]]:
    cursor.execute(sql, params or {})
    row = cursor.fetchone()
    return tuple(row) if row else None


def _fetch_all(cursor, sql: str, params: Optional[Dict[str, Any]] = None) -> Iterable[Tuple[Any, ...]]:
    cursor.execute(sql, params or {})
    return list(cursor.fetchall())


def _detect_version(cursor) -> str:
    queries = [
        "SELECT banner FROM v$version WHERE ROWNUM = 1",
        "SELECT version FROM product_component_version WHERE ROWNUM = 1",
    ]
    for sql in queries:
        try:
            row = _fetch_one(cursor, sql)
        except Exception:
            continue
        if row and isinstance(row[0], str) and row[0].strip():
            return row[0].strip()
    return "unknown"


def _candidate_vector_columns() -> Tuple[str, ...]:
    env = settings.oracle_embedding_column
    candidates = [env, "EMBEDDING", "VECTOR", "VEC", "EMB"]
    seen = []
    for name in candidates:
        upper = name.strip().upper() if isinstance(name, str) else ""
        if upper and upper not in seen:
            seen.append(upper)
    return tuple(seen)


def _detect_vector_from_view(cursor, table: str) -> Tuple[Optional[str], Optional[int]]:
    sql = (
        "SELECT column_name, dimension FROM user_vector_columns "
        "WHERE table_name = :table_name"
    )
    try:
        rows = _fetch_all(cursor, sql, {"table_name": table})
    except Exception as exc:
        if _is_missing_view(exc):
            return (None, None)
        LOGGER.debug("user_vector_columns probe failed: %s", exc)
        return (None, None)
    for column_name, dimension in rows:
        if isinstance(column_name, str) and column_name.strip():
            vec_dim: Optional[int]
            try:
                vec_dim = int(dimension) if dimension is not None else None
            except (TypeError, ValueError):
                vec_dim = None
            return (column_name.strip().upper(), vec_dim)
    return (None, None)


def _detect_vector_from_tab_cols(cursor, table: str) -> Tuple[Optional[str], Optional[int]]:
    sql = """
        SELECT column_name, data_type, data_length
        FROM user_tab_columns
        WHERE table_name = :table_name
    """
    try:
        rows = _fetch_all(cursor, sql, {"table_name": table})
    except Exception as exc:  # pragma: no cover - requires Oracle access
        LOGGER.debug("user_tab_columns probe failed: %s", exc)
        return (None, None)

    candidates = _candidate_vector_columns()
    winner: Optional[str] = None
    winner_dim: Optional[int] = None
    for column_name, data_type, data_length in rows:
        if not isinstance(column_name, str):
            continue
        col_upper = column_name.strip().upper()
        type_upper = data_type.strip().upper() if isinstance(data_type, str) else ""
        if col_upper in candidates or type_upper == "VECTOR":
            winner = col_upper
            try:
                winner_dim = int(data_length) if data_length is not None else None
            except (TypeError, ValueError):
                winner_dim = None
            override = os.getenv("ORACLE_VECTOR_DIMENSION")
            if override:
                try:
                    winner_dim = int(override)
                except ValueError:
                    pass
            break
    return (winner, winner_dim)


def _count_rows(cursor, table: str, column: Optional[str] = None) -> int:
    if column:
        sql = f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL"
    else:
        sql = f"SELECT COUNT(*) FROM {table}"
    try:
        row = _fetch_one(cursor, sql)
    except Exception as exc:  # pragma: no cover - requires Oracle access
        LOGGER.debug("row count query failed: %s", exc)
        return 0
    if not row:
        return 0
    try:
        return int(row[0])  # type: ignore[index]
    except (TypeError, ValueError):
        return 0


def _detect_oracle_text(cursor) -> bool:
    probes = [
        "SELECT 1 FROM all_users WHERE username = 'CTXSYS' AND ROWNUM = 1",
        "SELECT 1 FROM all_indexes WHERE ityp_owner = 'CTXSYS' AND ROWNUM = 1",
    ]
    for sql in probes:
        try:
            row = _fetch_one(cursor, sql)
        except Exception as exc:
            if _is_missing_view(exc):
                continue
            LOGGER.debug("oracle text probe failed: %s", exc)
            continue
        if row:
            return True
    return False


def detect_capability(refresh: bool = False) -> Capability:
    """Inspect Oracle metadata to understand available retrieval features."""

    global _CACHED_CAPABILITY
    if not refresh and _CACHED_CAPABILITY is not None:
        return _CACHED_CAPABILITY

    with _CAPABILITY_LOCK:
        if not refresh and _CACHED_CAPABILITY is not None:
            return _CACHED_CAPABILITY

        if oracledb is None:
            LOGGER.debug("Oracle client unavailable; assuming no vector support")
            capability = Capability()
            _CACHED_CAPABILITY = capability
            return capability

        table = _safe_upper(settings.oracle_table) or "ESG_DOCS"

        try:
            with get_connection() as conn:
                cursor = conn.cursor()
                version = _detect_version(cursor)
                vec_col, vec_dim = _detect_vector_from_view(cursor, table)
                if not vec_col:
                    vec_col, vec_dim = _detect_vector_from_tab_cols(cursor, table)
                total_rows = _count_rows(cursor, table)
                vector_rows = _count_rows(cursor, table, vec_col) if vec_col else 0
                oracle_text = _detect_oracle_text(cursor)
        except Exception as exc:  # pragma: no cover - requires Oracle access
            LOGGER.warning("capability detection failed: %s", exc)
            capability = Capability(db_version="unknown")
            _CACHED_CAPABILITY = capability
            return capability

        capability = Capability(
            db_version=version,
            vector_supported=bool(vec_col),
            vec_col=vec_col,
            vec_dim=vec_dim,
            vector_rows=vector_rows,
            esg_docs_count=total_rows,
            oracle_text_supported=oracle_text,
        )
        _CACHED_CAPABILITY = capability
        return capability


def get_capability(refresh: bool = False) -> Capability:
    """Public accessor that optionally refreshes cached capability data."""

    return detect_capability(refresh=refresh)


def capability_snapshot() -> Dict[str, Any]:
    """Return a serialisable dictionary for debug payloads."""

    capability = get_capability()
    return {
        "db_version": capability.db_version,
        "vector_supported": capability.vector_supported,
        "vec_col": capability.vec_col,
        "vec_dim": capability.vec_dim,
        "vector_rows": capability.vector_rows,
        "esg_docs_count": capability.esg_docs_count,
        "oracle_text_supported": capability.oracle_text_supported,
    }


__all__ = ["Capability", "capability_snapshot", "detect_capability", "get_capability"]
