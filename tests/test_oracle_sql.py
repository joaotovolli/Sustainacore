import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db_helper


def test_knn_sql_structure():
    sql = db_helper._compose_knn_sql("ESG_DOCS", "EMBEDDING", "", 7)
    normalized = " ".join(sql.split())
    assert "AI_VECTOR.EMBED_TEXT" not in normalized.upper()
    assert "FETCH FIRST :" not in normalized.upper()
    assert "FETCH FIRST 7 ROWS ONLY" in normalized.upper()


def test_filter_clause_whitelist():
    where, binds = db_helper._build_filter_clause(
        {
            "doc_id": ["DOC-1", "DOC-2"],
            "title": "climate",
            "unknown": "ignored",
        }
    )
    assert "UNKNOWN" not in where.upper()
    assert "DOC_ID" in where.upper()
    assert "TITLE" in where.upper()
    assert sorted(binds.keys()) == ["doc_id_0", "doc_id_1", "title_like"]


def test_filter_clause_empty():
    where, binds = db_helper._build_filter_clause(None)
    assert where == ""
    assert binds == {}
