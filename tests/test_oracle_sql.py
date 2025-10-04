import types

import pytest

import db_helper
from app.retrieval import oracle_retriever


class _DummyCursor:
    def __init__(self, description):
        self.description = description
        self.executed_sql = None
        self.executed_params = None

    def setinputsizes(self, **kwargs):  # pragma: no cover - interface only
        self.inputsizes = kwargs

    def execute(self, sql, params):
        self.executed_sql = sql
        self.executed_params = params

    def fetchall(self):
        return [(1, 0, "Doc", "SRC", "Source name", "https://example", "Snippet", 0.12)]


class _DummyConn:
    def __init__(self, description):
        self._cursor = _DummyCursor(description)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor


@pytest.fixture(autouse=True)
def _clear_vector_cache():
    db_helper.get_vector_column.cache_clear()
    yield
    db_helper.get_vector_column.cache_clear()


def test_top_k_by_vector_sql_contract(monkeypatch):
    description = [
        ("DOC_ID",),
        ("CHUNK_IX",),
        ("TITLE",),
        ("SOURCE_URL",),
        ("SOURCE_TYPE",),
        ("SOURCE_ID",),
        ("CHUNK_TEXT",),
        ("DIST",),
    ]
    holder = {}

    def fake_conn():
        conn = _DummyConn(description)
        holder["cursor"] = conn.cursor()
        return conn

    monkeypatch.setattr(db_helper, "oracledb", types.SimpleNamespace(DB_TYPE_VECTOR=object()))
    monkeypatch.setattr(db_helper, "_conn", fake_conn)
    monkeypatch.setattr(
        db_helper,
        "get_vector_column",
        lambda table="ESG_DOCS": {"table": "ESG_DOCS", "column": "EMBEDDING", "dimension": 3},
    )

    result = db_helper.top_k_by_vector([0.1, 0.2, 0.3], k=5)

    cursor = holder["cursor"]
    assert cursor.executed_sql.count("FETCH FIRST 5 ROWS ONLY") == 1
    assert "VECTOR_DISTANCE(:v, EMBEDDING)" in cursor.executed_sql
    assert all(banned not in cursor.executed_sql for banned in ["AI_VECTOR", "UTL_HTTP", "APEX_WEB_SERVICE", "DBMS_CLOUD"])
    assert "k" not in cursor.executed_params
    assert result[0]["doc_id"] == 1


def test_oracle_retriever_vector_sql(monkeypatch):
    description = [
        ("DOC_ID",),
        ("CHUNK_IX",),
        ("SOURCE_ID",),
        ("SOURCE_NAME",),
        ("TITLE",),
        ("SOURCE_URL",),
        ("CHUNK_TEXT",),
        ("DIST",),
    ]
    holder = {}

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            cursor = _DummyCursor(description)
            holder["cursor"] = cursor
            return cursor

    monkeypatch.setattr(oracle_retriever, "oracledb", types.SimpleNamespace(DB_TYPE_VECTOR=object()))
    monkeypatch.setattr(oracle_retriever, "_ORACLE_AVAILABLE", True)

    retr = oracle_retriever.OracleRetriever()
    retr._metadata_ready = True
    retr._available_columns = {
        retr.doc_id_column,
        retr.chunk_ix_column,
        retr.source_id_column,
        retr.source_column,
        retr.title_column,
        retr.url_column,
        retr.text_column,
        retr.embedding_column,
    }

    rows = retr._vector_query(DummyConn(), [0.1, 0.2, 0.3], filters={}, k=4)

    cursor = holder["cursor"]
    assert "VECTOR_DISTANCE(:vec, EMBEDDING" in cursor.executed_sql
    assert "FETCH FIRST 4 ROWS ONLY" in cursor.executed_sql
    assert ":k" not in cursor.executed_sql
    assert all(banned not in cursor.executed_sql for banned in ["AI_VECTOR", "UTL_HTTP", "APEX_WEB_SERVICE", "DBMS_CLOUD"])
    assert rows and rows[0]["doc_id"] == 1
