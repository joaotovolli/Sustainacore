import types

import pytest

import db_helper


def _make_conn(rows=None, execute_exc=None, cursor_description=None, fetch_rows=None):
    class DummyCursor:
        def __init__(self):
            self.description = cursor_description or [
                ("DOC_ID",),
                ("CHUNK_IX",),
                ("TITLE",),
                ("SOURCE_URL",),
                ("SOURCE_TYPE",),
                ("SOURCE_ID",),
                ("CHUNK_TEXT",),
                ("DIST",),
            ]
            self.executed_sql = None
            self.executed_params = None
            self.inputsizes = None

        def setinputsizes(self, **kwargs):
            self.inputsizes = kwargs

        def execute(self, sql, params):
            self.executed_sql = sql
            self.executed_params = params
            if execute_exc is not None:
                raise execute_exc

        def fetchall(self):
            if rows is not None:
                return rows
            return fetch_rows or []

        def fetchone(self):
            fetched = self.fetchall()
            return fetched[0] if fetched else None
    class DummyConn:
        def __init__(self):
            self.cursor_obj = DummyCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

        def cursor(self):
            return self.cursor_obj

    return DummyConn()


def _stub_oracledb(monkeypatch):
    monkeypatch.setattr(db_helper, "oracledb", types.SimpleNamespace(DB_TYPE_VECTOR=object()))


def test_get_vector_column_prefers_embedding(monkeypatch):
    db_helper.get_vector_column.cache_clear()
    _stub_oracledb(monkeypatch)
    rows = [("OTHER", 256), ("EMBEDDING", 384)]
    info = db_helper.get_vector_column("esg_docs")

    assert info == {"table": "ESG_DOCS", "column": "EMBEDDING", "dimension": None}


def test_get_vector_column_no_rows(monkeypatch):
    db_helper.get_vector_column.cache_clear()
    _stub_oracledb(monkeypatch)
    monkeypatch.setattr(db_helper, "_conn", lambda: _make_conn(rows=[]))

    info = db_helper.get_vector_column("missing")
    assert info["column"] == "EMBEDDING"


def test_top_k_by_vector_executes_query(monkeypatch):
    db_helper.get_vector_column.cache_clear()
    _stub_oracledb(monkeypatch)

    def fake_get_vector_column(table="ESG_DOCS"):
        return {"table": "ESG_DOCS", "column": "EMBEDDING", "dimension": 3}

    monkeypatch.setattr(db_helper, "get_vector_column", fake_get_vector_column)
    monkeypatch.setattr(db_helper, "_get_vector_info", lambda: {"table": "ESG_DOCS", "column": "EMBEDDING"})

    cursor_holder = {}

    def fake_conn():
        conn = _make_conn(fetch_rows=[(1, 0, "Title", "https://example", "DOC", "SRC", "Snippet", 0.25)])
        cursor_holder["cursor"] = conn.cursor_obj
        return conn

    monkeypatch.setattr(db_helper, "_conn", fake_conn)

    result = db_helper.top_k_by_vector([0.1, 0.2, 0.3], k=2)

    cursor = cursor_holder["cursor"]
    assert "VECTOR_DISTANCE(:v, EMBEDDING)" in cursor.executed_sql
    assert "AI_VECTOR" not in cursor.executed_sql
    assert "FETCH FIRST 2 ROWS ONLY" in cursor.executed_sql
    assert result[0]["doc_id"] == 1


def test_top_k_by_vector_raises_on_oracle_error(monkeypatch):
    db_helper.get_vector_column.cache_clear()
    _stub_oracledb(monkeypatch)

    monkeypatch.setattr(db_helper, "get_vector_column", lambda table="ESG_DOCS": {"table": "ESG_DOCS", "column": "EMBEDDING", "dimension": 3})
    monkeypatch.setattr(db_helper, "_get_vector_info", lambda: {"table": "ESG_DOCS", "column": "EMBEDDING"})

    error = RuntimeError("ORA-29273: HTTP request failed")

    def failing_conn():
        return _make_conn(execute_exc=error)

    monkeypatch.setattr(db_helper, "_conn", failing_conn)

    with pytest.raises(RuntimeError) as excinfo:
        db_helper.top_k_by_vector([0.1, 0.2, 0.3])

    assert "ORA-29273" in str(excinfo.value)
