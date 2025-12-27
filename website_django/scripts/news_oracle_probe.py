from __future__ import annotations

import os
import sys
from pathlib import Path

NEWS_ID = os.getenv("NEWS_ID", "ESG_NEWS:738")


def _load_env():
    env_path = Path("/etc/sustainacore/db.env")
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        if not line or line.strip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))
    _load_env()
    os.environ.setdefault("TNS_ADMIN", "/opt/adb_wallet_tp")
    os.environ.setdefault("ORACLE_CLIENT_LIB_DIR", "/opt/oracle/instantclient_23")

    from core.news_data import _get_sources, _qualify_name
    from core.oracle_db import get_connection

    sources = _get_sources()
    detail_source = sources["detail"]
    mapping = detail_source["mapping"]
    table_name = _qualify_name(detail_source["name"], sources["schema"])

    candidates = [
        "full_text",
        "content",
        "article_text",
        "body",
        "text",
        "summary",
    ]
    cols = {key: mapping.get(key) for key in candidates}
    cols = {key: col for key, col in cols.items() if col}

    item_id = NEWS_ID.split(":", 1)[-1]
    binds = {"item_id": int(item_id) if item_id.isdigit() else item_id}

    select_cols = [f"{col} AS {key}" for key, col in cols.items()]
    if not select_cols:
        raise SystemExit("No candidate columns found in mapping.")

    sql = (
        "SELECT "
        + ", ".join(select_cols)
        + f" FROM {table_name} WHERE {mapping['item_id']} = :item_id"
    )

    values = {}
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        row = cur.fetchone()
        if not row:
            raise SystemExit("No row found for id")
        for idx, key in enumerate(cols.keys()):
            value = row[idx]
            if value is None:
                values[key] = ""
                continue
            if hasattr(value, "read"):
                values[key] = value.read()
            else:
                values[key] = str(value)

    print("news_id", NEWS_ID)
    for key in candidates:
        text = values.get(key, "")
        print(f"{key}_len", len(text))
    chosen_key = max(values, key=lambda k: len(values[k]))
    snippet = values[chosen_key][:120].replace("\n", " ")
    print("chosen_field", chosen_key)
    print("chosen_preview", snippet)
