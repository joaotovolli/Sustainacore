import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.retrieval import oracle_retriever
from app.retrieval.db_capability import Capability


class _VectorCursor:
    def __init__(self):
        self.description = [
            ("DOC_ID",),
            ("TITLE",),
            ("SOURCE_NAME",),
            ("SOURCE_URL",),
            ("CHUNK_TEXT",),
            ("DIST",),
        ]
        self.executed_sql = None
        self.executed_params = None

    def setinputsizes(self, **kwargs):
        self.inputsizes = kwargs

    def execute(self, sql, params=None):
        self.executed_sql = sql
        self.executed_params = params or {}

    def fetchall(self):
        return [("1", "Doc", "Source", "https://example", "Snippet", 0.2)]


class _VectorConnection:
    def cursor(self):
        return _VectorCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _TextCursor:
    def __init__(self):
        self.description = [
            ("DOC_ID",),
            ("TITLE",),
            ("SOURCE_NAME",),
            ("SOURCE_URL",),
            ("CHUNK_TEXT",),
            ("SCORE",),
        ]
        self.executed_sql = None

    def execute(self, sql, params=None):
        self.executed_sql = sql

    def fetchall(self):
        return [("2", "Fallback", "Source", "https://fallback", "Body", 90)]


class _TextConnection:
    def cursor(self):
        return _TextCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@pytest.fixture(autouse=True)
def reset_capability(monkeypatch):
    monkeypatch.setattr(oracle_retriever, "embed_text", lambda text, timeout=5.0: [0.1, 0.2, 0.3, 0.4])


def test_vector_mode(monkeypatch):
    capability = Capability(
        db_version="23ai",
        vector_supported=True,
        vec_col="EMBEDDING",
        vec_dim=4,
        vector_rows=5,
        esg_docs_count=5,
        oracle_text_supported=True,
    )
    monkeypatch.setattr(oracle_retriever, "get_capability", lambda refresh=False: capability)
    monkeypatch.setattr(oracle_retriever, "get_connection", lambda: _VectorConnection())
    monkeypatch.setattr(oracle_retriever, "oracledb", object(), raising=False)

    result = oracle_retriever.retriever.retrieve("How many reports?", 3)
    assert result.mode == "vector"
    assert result.contexts
    assert result.contexts[0]["score"] == pytest.approx(0.8, rel=1e-2)


def test_like_fallback(monkeypatch):
    capability = Capability(
        db_version="21c",
        vector_supported=False,
        vec_col=None,
        vec_dim=None,
        vector_rows=0,
        esg_docs_count=10,
        oracle_text_supported=False,
    )
    monkeypatch.setattr(oracle_retriever, "get_capability", lambda refresh=False: capability)
    monkeypatch.setattr(oracle_retriever, "get_connection", lambda: _TextConnection())

    result = oracle_retriever.retriever.retrieve("fallback query", 2)
    assert result.mode == "like"
    assert result.contexts
    assert result.contexts[0]["title"] == "Fallback"
    assert 0 < result.contexts[0]["score"] <= 1
