"""Oracle helper utilities for SustainaCore."""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# --- injected helper (keeps DB_ envs + /etc/sustainacore/db.env compatible) ---
def _read_env_file_var(path: str, key: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                name, _, value = line.partition("=")
                if name.strip() == key:
                    return value.strip()
    except Exception:
        return None
    return None
# -------------------------------------------------------------------------------

try:  # pragma: no cover - dependency optional
    import oracledb  # type: ignore
except Exception as exc:  # pragma: no cover - import fallback
    oracledb = None  # type: ignore
    _ORACLE_IMPORT_ERROR = exc
else:  # pragma: no cover - dependency present
    _ORACLE_IMPORT_ERROR = None


LOGGER = logging.getLogger("sustainacore.db")
_ORACLE_STATUS_LOGGED = False

_VECTOR_INFO_LOCK = threading.Lock()
_VECTOR_INFO: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _log_oracle_issue(exc: Exception) -> None:
    """Log Oracle availability issues only once."""

    global _ORACLE_STATUS_LOGGED
    if _ORACLE_STATUS_LOGGED:
        return

    message = str(exc)
    code = None
    if exc.args:
        first = exc.args[0]
        code = getattr(first, "code", None)
        if not message and isinstance(first, str):
            message = first

    code_str = code.upper() if isinstance(code, str) else ""
    if "DPY-4001" in (message or "") or code_str == "DPY-4001":
        LOGGER.warning("Oracle unavailable (DPY-4001): %s", message or repr(exc))
    else:
        LOGGER.warning("Oracle unavailable: %s", message or repr(exc))

    _ORACLE_STATUS_LOGGED = True


if _ORACLE_IMPORT_ERROR is not None:  # pragma: no cover - import diagnostic
    _log_oracle_issue(_ORACLE_IMPORT_ERROR)


def _conn():
    if oracledb is None:
        raise RuntimeError("oracledb_unavailable")

    connect_kwargs = dict(
        user=os.getenv("DB_USER", "WKSP_ESGAPEX"),
        password=(
            os.environ.get("DB_PASSWORD")
            or os.environ.get("DB_PASS")
            or os.environ.get("DB_PWD")
            or _read_env_file_var("/etc/sustainacore/db.env", "DB_PASSWORD")
        ),
        dsn=os.getenv("DB_DSN", "dbri4x6_high"),
        config_dir=os.getenv("TNS_ADMIN", "/opt/adb_wallet"),
        wallet_location=os.getenv("TNS_ADMIN", "/opt/adb_wallet"),
    )
    wallet_pwd = os.environ.get("WALLET_PWD")
    if wallet_pwd:
        connect_kwargs["wallet_password"] = wallet_pwd
    try:
        return oracledb.connect(**connect_kwargs)  # type: ignore[union-attr]
    except Exception as exc:
        _log_oracle_issue(exc)
        raise


def get_connection():
    """Return a new Oracle connection using configured credentials."""

    return _conn()


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------

def _candidate_embedding_columns() -> List[str]:
    env_col = os.getenv("DB_EMBED_COL") or os.getenv("DB_EMBED_COLUMN")
    candidates: List[str] = []
    if env_col:
        candidates.append(env_col)
    candidates.extend(["EMBEDDING", "EMBED", "VECTOR"])
    seen: List[str] = []
    for name in candidates:
        upper = name.strip().upper()
        if upper and upper not in seen:
            seen.append(upper)
    return seen


def _target_table_name() -> str:
    env_table = (
        os.getenv("DB_DOC_TABLE")
        or os.getenv("DB_TABLE")
        or os.getenv("DB_VECTOR_TABLE")
        or "ESG_DOCS"
    )
    return env_table.strip().upper() or "ESG_DOCS"


def _probe_vector_column() -> Dict[str, Any]:
    """Probe ALL_TAB_COLUMNS for a usable embedding column."""

    if oracledb is None:
        raise RuntimeError("oracledb_unavailable")

    table = _target_table_name()
    owner = (
        os.getenv("DB_OWNER")
        or os.getenv("DB_SCHEMA")
        or os.getenv("DB_USER")
    )
    owner_clause: str
    params: Dict[str, Any] = {"table_name": table}
    if owner:
        owner_clause = "AND owner = :owner"
        params["owner"] = owner.strip().upper()
    else:
        owner_clause = "AND owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')"

    sql = (
        "SELECT column_name, data_length "
        "FROM all_tab_columns "
        "WHERE table_name = :table_name "
        f"{owner_clause} "
        "AND column_name = :column_name"
    )

    with _conn() as conn:
        cur = conn.cursor()
        for candidate in _candidate_embedding_columns():
            try:
                cur.execute(sql, {**params, "column_name": candidate})
                row = cur.fetchone()
            except Exception as exc:
                _log_oracle_issue(exc)
                raise
            if row:
                dimension = None
                data_length = row[1]
                if isinstance(data_length, int) and data_length > 0:
                    dimension = data_length
                return {"table": table, "column": row[0], "dimension": dimension}

    # Fallback to first candidate even if not confirmed; callers may still work if schema matches defaults.
    first = _candidate_embedding_columns()[0]
    return {"table": table, "column": first, "dimension": None}


def _get_vector_info() -> Dict[str, Any]:
    global _VECTOR_INFO
    if _VECTOR_INFO is not None:
        return _VECTOR_INFO
    with _VECTOR_INFO_LOCK:
        if _VECTOR_INFO is not None:
            return _VECTOR_INFO
        _VECTOR_INFO = _probe_vector_column()
        return _VECTOR_INFO


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def _to_plain(value: Any) -> Any:
    # Convert Oracle LOBs (CLOB) to Python str and vectors to Python lists.
    if hasattr(value, "read"):
        try:
            return value.read()
        except Exception:
            return str(value)
    if oracledb is not None:
        vector_type = getattr(oracledb, "Vector", None)
        if vector_type and isinstance(value, vector_type):  # type: ignore[arg-type]
            return list(value)
    return value


def _normalize_iterable(value: Any) -> List[str]:
    if isinstance(value, (list, tuple, set)):
        iterable: Iterable[Any] = value
    else:
        iterable = [value]
    normalized: List[str] = []
    for item in iterable:
        if item is None:
            continue
        text = str(item).strip()
        if text:
            normalized.append(text)
    return normalized


def _build_filter_clause(filters: Optional[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    if not filters:
        return "", {}

    safe_filters = {
        "doc_id": "DOC_ID",
        "source_id": "SOURCE_ID",
        "source_type": "SOURCE_TYPE",
        "sector": "SECTOR",
        "ticker": "TICKER",
        "namespace": "NAMESPACE",
    }
    like_filters = {"title": "TITLE"}
    range_filters = {
        "date_from": ("PUBLISHED_AT", ">="),
        "date_to": ("PUBLISHED_AT", "<="),
    }

    clauses: List[str] = []
    binds: Dict[str, Any] = {}
    normalized_items = {
        (str(key).lower() if isinstance(key, str) else key): value
        for key, value in filters.items()
    }

    for key, column in safe_filters.items():
        values = _normalize_iterable(normalized_items.get(key))
        if not values:
            continue
        placeholders: List[str] = []
        for idx, val in enumerate(values):
            bind_key = f"{key}_{idx}"
            placeholders.append(f":{bind_key}")
            binds[bind_key] = val
        if len(placeholders) == 1:
            clauses.append(f"{column} = {placeholders[0]}")
        else:
            clauses.append(f"{column} IN ({', '.join(placeholders)})")

    for key, column in like_filters.items():
        raw_value = normalized_items.get(key)
        if not isinstance(raw_value, str):
            continue
        text = raw_value.strip()
        if not text:
            continue
        bind_key = f"{key}_like"
        binds[bind_key] = f"%{text}%"
        clauses.append(f"LOWER({column}) LIKE LOWER(:{bind_key})")

    for key, (column, op) in range_filters.items():
        raw_value = normalized_items.get(key)
        if raw_value is None:
            continue
        text = str(raw_value).strip()
        if not text:
            continue
        bind_key = f"{key}"
        binds[bind_key] = text
        clauses.append(f"{column} {op} :{bind_key}")

    if not clauses:
        return "", {}

    return " WHERE " + " AND ".join(clauses), binds


def _compose_knn_sql(table: str, column: str, where_clause: str, k: int) -> str:
    column = column.strip().upper()
    table = table.strip().upper()
    return (
        "SELECT doc_id, title, source_url, chunk_text, "
        "VECTOR_DISTANCE(:v, {col}) AS dist "
        "FROM {table}{where_clause} "
        "ORDER BY dist "
        "FETCH FIRST {k} ROWS ONLY"
    ).format(col=column, table=table, where_clause=where_clause, k=k)


def top_k_by_vector(vec: Sequence[float], k: int = 5, *, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Return the top-k rows from Oracle using a client-supplied vector."""

    if oracledb is None:
        raise RuntimeError("oracledb_unavailable")

    try:
        k_int = int(k)
    except (TypeError, ValueError):
        k_int = 5
    if k_int < 1:
        k_int = 1

    info = _get_vector_info()
    where_clause, binds = _build_filter_clause(filters or {})
    sql = _compose_knn_sql(info["table"], info["column"], where_clause, k_int)

    try:
        with _conn() as conn:
            cur = conn.cursor()
            vector_type = getattr(oracledb, "DB_TYPE_VECTOR", None)
            if vector_type is not None:
                try:
                    cur.setinputsizes(v=vector_type)
                except Exception:
                    # Older drivers may not support DB_TYPE_VECTOR; ignore.
                    pass
            params = {"v": vec}
            params.update(binds)
            cur.execute(sql, params)
            columns = [desc[0].lower() for desc in cur.description]
            rows: List[Dict[str, Any]] = []
            for result in cur.fetchall():
                rows.append({col: _to_plain(val) for col, val in zip(columns, result)})
            return rows
    except Exception as exc:
        _log_oracle_issue(exc)
        raise


# ---------------------------------------------------------------------------
# Metadata lookups ----------------------------------------------------------
# ---------------------------------------------------------------------------

def fetch_vector_metadata(table: str = "ESG_DOCS", column: str = "EMBEDDING") -> Tuple[Optional[int], Dict[str, Any]]:
    """Return (dimension, metadata) for the configured vector column."""

    if oracledb is None:
        return None, {}

    table_name = table.strip().upper() or "ESG_DOCS"
    column_name = column.strip().upper() or "EMBEDDING"
    dimension: Optional[int] = None
    metadata: Dict[str, Any] = {}

    try:
        with _conn() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT dimension
                    FROM user_vector_columns
                    WHERE table_name = :table_name AND column_name = :column_name
                    """,
                    {"table_name": table_name, "column_name": column_name},
                )
                row = cur.fetchone()
                if row and row[0] is not None:
                    dimension = int(row[0])
            except Exception:
                # Fall back to the probe result if the view is unavailable.
                info = _get_vector_info()
                if info.get("dimension"):
                    dimension = int(info["dimension"])

            try:
                cur.execute(
                    """
                    SELECT model_name, model_hash
                    FROM sc_corpus_meta
                    WHERE ROWNUM = 1
                    """
                )
                meta_row = cur.fetchone()
                if meta_row:
                    metadata = {
                        "model_name": _to_plain(meta_row[0]),
                        "model_hash": _to_plain(meta_row[1]),
                    }
            except Exception:
                metadata = {}
    except Exception as exc:
        _log_oracle_issue(exc)
        raise

    return dimension, metadata


__all__ = [
    "get_connection",
    "top_k_by_vector",
    "fetch_vector_metadata",
]
