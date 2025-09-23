
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
import os, oracledb

# expects env set in your shell:
#   TNS_ADMIN=/opt/adb_wallet
#   DB_USER=WKSP_ESGAPEX
#   DB_PASS=9o0p(O)P9o0p
#   DB_DSN=dbri4x6_high
#   WALLET_PWD=9o0p(O)P9o0p


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

def _to_plain(v):
    # Convert Oracle LOBs (CLOB) to Python str
    if hasattr(v, "read"):
        try:
            return v.read()
        except Exception:
            return str(v)
    return v

def top_k_by_vector(vec, k=5):
    k = max(1, int(k))
    sql = f"""
        SELECT doc_id, chunk_ix, title, source_url, chunk_text,
               VECTOR_DISTANCE(embedding, :v) AS dist
        FROM   esg_docs
        ORDER  BY dist
        FETCH  FIRST {k} ROWS ONLY
    """
    with _conn() as conn:
        cur = conn.cursor()
        cur.setinputsizes(v=oracledb.DB_TYPE_VECTOR)
        cur.execute(sql, {"v": vec})
        cols = [d[0].lower() for d in cur.description]
        out = []
        for row in cur.fetchall():
            out.append({k: _to_plain(v) for k, v in zip(cols, row)})
        return out
