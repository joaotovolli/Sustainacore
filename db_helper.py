
# --- injected helper (keeps DB_ envs + /etc/sustainacore/db.env compatible) ---
def _read_env_file_var(path, key):
    try:
        with open(path) as f:
            for line in f:
                line=line.strip()
                if not line or line.startswith('#'):
                    continue
                k, _, v = line.partition('=')
                if k.strip()==key:
                    return v.strip()
    except Exception:
        return None
# -------------------------------------------------------------------------------
import logging
import os
from functools import lru_cache

try:  # pragma: no cover - dependency optional
    import oracledb  # type: ignore
except Exception as exc:  # pragma: no cover - import fallback
    oracledb = None  # type: ignore
    _ORACLE_IMPORT_ERROR = exc
else:  # pragma: no cover - dependency present
    _ORACLE_IMPORT_ERROR = None

# expects env set in your shell:
#   TNS_ADMIN=/opt/adb_wallet
#   DB_USER=WKSP_ESGAPEX
#   DB_PASS=9o0p(O)P9o0p
#   DB_DSN=dbri4x6_high
#   WALLET_PWD=9o0p(O)P9o0p


LOGGER = logging.getLogger("sustainacore.db")
_ORACLE_STATUS_LOGGED = False


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

    KW = dict(
        user=os.getenv("DB_USER","WKSP_ESGAPEX"),
        password=(os.environ.get("DB_PASSWORD")
                  or os.environ.get("DB_PASS")
                  or os.environ.get("DB_PWD")
                  or _read_env_file_var("/etc/sustainacore/db.env","DB_PASSWORD")),
        dsn=os.getenv("DB_DSN","dbri4x6_high"),
        config_dir=os.getenv("TNS_ADMIN","/opt/adb_wallet"),
        wallet_location=os.getenv("TNS_ADMIN","/opt/adb_wallet"),
    )
    WP = os.environ.get("WALLET_PWD")
    if WP:
        KW["wallet_password"] = WP
    try:
        return oracledb.connect(**KW)  # type: ignore[union-attr]
    except Exception as exc:
        _log_oracle_issue(exc)
        raise


@lru_cache(maxsize=None)
def get_vector_column(table: str = "ESG_DOCS") -> dict:
    """Return vector column metadata for the given table."""

    if oracledb is None:
        raise RuntimeError("oracledb_unavailable")

    table_name = (table or "ESG_DOCS").upper()
    column = os.getenv("ORACLE_VECTOR_COLUMN", "EMBEDDING").upper()
    dimension_env = os.getenv("ORACLE_VECTOR_DIMENSION")
    try:
        dimension = int(dimension_env) if dimension_env else None
    except (TypeError, ValueError):
        dimension = None
    return {"table": table_name, "column": column, "dimension": dimension}



def get_connection():
    """Return a new Oracle connection using configured credentials."""

    return _conn()

def _to_plain(v):
    # Convert Oracle LOBs (CLOB) to Python str
    if hasattr(v, "read"):
        try:
            return v.read()
        except Exception:
            return str(v)
    if oracledb is not None:
        vector_type = getattr(oracledb, "Vector", None)
        if vector_type and isinstance(v, vector_type):  # type: ignore[arg-type]
            return list(v)
    return v

def _build_filter_clause(filters):
    if not filters:
        return "", {}

    binds = {}
    clauses = []
    counter = 0

    def _bind(value):
        nonlocal counter
        key = f"p{counter}"
        counter += 1
        binds[key] = value
        return f":{key}"

    for key, value in (filters or {}).items():
        if value in (None, ""):
            continue
        key_u = str(key).upper()
        if key_u == "DOC_ID":
            clause = f"DOC_ID = {_bind(str(value))}"
            clauses.append(clause)
        elif key_u == "SOURCE_ID":
            clause = f"UPPER(SOURCE_ID) = {_bind(str(value).upper())}"
            clauses.append(clause)
        elif key_u == "SOURCE_TYPE":
            values = value if isinstance(value, list) else [value]
            tokens = [str(v).strip() for v in values if str(v).strip()]
            if tokens:
                binds_list = ", ".join(_bind(tok) for tok in tokens)
                clauses.append(f"UPPER(SOURCE_TYPE) IN ({binds_list})")
        elif key_u == "TITLE_LIKE":
            clause = f"TITLE LIKE {_bind(str(value))}"
            clauses.append(clause)

    if not clauses:
        return "", {}

    return " WHERE " + " AND ".join(clauses), binds


def top_k_by_vector(vec, k=5, *, filters=None):
    if oracledb is None:
        raise RuntimeError("oracledb_unavailable")

    info = get_vector_column()
    table_name = info.get("table") or "ESG_DOCS"
    column_name = info.get("column") or "EMBEDDING"
    expected_dim = info.get("dimension")

    try:
        query_vector = [float(x) for x in vec]
    except Exception as exc:
        raise TypeError("vec must be an iterable of floats") from exc

    if expected_dim and len(query_vector) != expected_dim:
        LOGGER.warning(
            "Query vector dimension mismatch: expected=%s got=%s",
            expected_dim,
            len(query_vector),
        )

    k = max(1, int(k or 1))
    where_clause, binds = _build_filter_clause(filters or {})
    sql = (
        "SELECT doc_id, chunk_ix, title, source_url, source_type, source_id, chunk_text, "
        f"VECTOR_DISTANCE(:v, {column_name}) AS dist FROM {table_name}{where_clause} "
        f"ORDER BY VECTOR_DISTANCE(:v, {column_name}) FETCH FIRST {k} ROWS ONLY"
    )

    with _conn() as conn:
        cur = conn.cursor()
        vector_type = getattr(oracledb, "DB_TYPE_VECTOR", None)
        if vector_type is not None:
            cur.setinputsizes(v=vector_type)  # type: ignore[arg-type]
        params = dict(binds)
        params.update({"v": query_vector})
        try:
            cur.execute(sql, params)
        except Exception as exc:
            LOGGER.error("Oracle vector query failed: %s", exc)
            raise
        columns = [d[0].lower() for d in cur.description]
        rows = []
        for row in cur.fetchall():
            rows.append({key: _to_plain(val) for key, val in zip(columns, row)})
    return rows


def fetch_vector_metadata(table="ESG_DOCS", column="EMBEDDING"):
    """Return (dimension, metadata) for the configured vector column."""

    if oracledb is None:
        return None, {}

    table_name = table.upper()
    column_name = column.upper()
    try:
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT dimension
                FROM   user_vector_columns
                WHERE  table_name = :table_name AND column_name = :column_name
            """,
                {"table_name": table_name, "column_name": column_name},
            )
            row = cur.fetchone()
            dimension = int(row[0]) if row and row[0] is not None else None

            metadata = {}
            try:
                cur.execute(
                    """
                    SELECT model_name, model_hash
                    FROM   sc_corpus_meta
                    WHERE  ROWNUM = 1
                """
                )
                meta_row = cur.fetchone()
                if meta_row:
                    metadata = {
                        "model_name": _to_plain(meta_row[0]),
                        "model_hash": _to_plain(meta_row[1]),
                    }
            except Exception as meta_exc:  # pragma: no cover - optional table
                if oracledb is not None and isinstance(
                    meta_exc, getattr(oracledb, "DatabaseError", Exception)
                ):
                    error = meta_exc.args[0] if meta_exc.args else None
                    if getattr(error, "code", None) != 942:
                        LOGGER.debug("Corpus metadata lookup failed", exc_info=meta_exc)
                else:
                    LOGGER.debug("Corpus metadata lookup failed", exc_info=meta_exc)

            return dimension, metadata
    except Exception as exc:
        _log_oracle_issue(exc)
        return None, {}
