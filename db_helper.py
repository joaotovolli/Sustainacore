
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
import os, oracledb

# expects env set in your shell:
#   TNS_ADMIN=/opt/adb_wallet
#   DB_USER=WKSP_ESGAPEX
#   DB_PASS=9o0p(O)P9o0p
#   DB_DSN=dbri4x6_high
#   WALLET_PWD=9o0p(O)P9o0p


LOGGER = logging.getLogger("sustainacore.db")


def _conn():
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
    return oracledb.connect(**KW)


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
    with _conn() as conn:
        cur = conn.cursor()
        cur.setinputsizes(v=oracledb.DB_TYPE_VECTOR)
        params = {"v": vec}
        params.update(binds)
        cur.execute(sql, params)
        cols = [d[0].lower() for d in cur.description]
        out = []
        for row in cur.fetchall():
            out.append({k: _to_plain(v) for k, v in zip(cols, row)})
        return out


def fetch_vector_metadata(table="ESG_DOCS", column="EMBEDDING"):
    """Return (dimension, metadata) for the configured vector column."""

    table_name = table.upper()
    column_name = column.upper()
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
        except oracledb.DatabaseError as exc:  # pragma: no cover - optional table
            error = exc.args[0] if exc.args else None
            if getattr(error, "code", None) != 942:
                LOGGER.debug("Corpus metadata lookup failed", exc_info=exc)

        return dimension, metadata
