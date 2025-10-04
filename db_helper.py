
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

    def _add_in(column: str, values, prefix: str):
        if isinstance(values, str):
            iterable = [values]
        else:
            iterable = list(values or [])
        items = []
        for idx, value in enumerate(iterable):
            if value in (None, ""):
                continue
            key = f"{prefix}{idx}"
            items.append(f":{key}")
            binds[key] = value
        if items:
            clauses.append(f"{column} IN ({', '.join(items)})")

    if "source_type" in filters:
        _add_in("source_type", filters["source_type"], "st")

    if "source_id" in filters:
        values = [str(v).upper() for v in filters["source_id"]]
        column = "UPPER(source_id)"
        _add_in(column, values, "sid")

    if not clauses:
        return "", {}

    return " WHERE " + " AND ".join(clauses), binds


def top_k_by_vector(vec, k=5, *, filters=None):
    if oracledb is None:
        return []

    k = max(1, int(k))
    where_clause, binds = _build_filter_clause(filters)
    sql = f"""
        SELECT doc_id,
               chunk_ix,
               title,
               source_url,
               source_type,
               source_id,
               chunk_text,
               VECTOR_DISTANCE(embedding, :v) AS dist
        FROM   esg_docs{where_clause}
        ORDER  BY VECTOR_DISTANCE(embedding, :v)
        FETCH  FIRST {k} ROWS ONLY
    """
    try:
        with _conn() as conn:
            cur = conn.cursor()
            vector_type = getattr(oracledb, "DB_TYPE_VECTOR", None)
            if vector_type is not None:
                try:
                    cur.setinputsizes(v=vector_type)
                except Exception:  # pragma: no cover - older driver fallback
                    cur.setinputsizes(v=vector_type)
            params = {"v": vec}
            params.update(binds)
            cur.execute(sql, params)
            cols = [d[0].lower() for d in cur.description]
            out = []
            for row in cur.fetchall():
                out.append({key: _to_plain(val) for key, val in zip(cols, row)})
            return out
    except Exception as exc:
        _log_oracle_issue(exc)
        return []


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
