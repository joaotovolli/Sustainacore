import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.retrieval import db_capability


class _StubCursor:
    def __init__(self, mode: str = "view") -> None:
        self._mode = mode
        self._data = []
        self.description = []

    def execute(self, sql: str, params=None) -> None:  # pragma: no cover - trivial
        sql_lower = sql.lower()
        if "v$version" in sql_lower:
            self._data = [("Oracle Database 23ai",)]
        elif "user_vector_columns" in sql_lower:
            if self._mode == "view_fail":
                raise Exception("ORA-00942")
            self._data = [("EMBEDDING", 384)]
        elif "user_tab_columns" in sql_lower:
            self._data = [("VECTOR", "VECTOR", 768)]
        elif "count(*) from esg_docs where" in sql_lower:
            self._data = [(5,)]
        elif "count(*) from esg_docs" in sql_lower:
            self._data = [(10,)]
        elif "all_users" in sql_lower:
            self._data = [(1,)]
        elif "all_indexes" in sql_lower:
            self._data = [(1,)]
        else:
            self._data = []

    def fetchone(self):  # pragma: no cover - exercised via tests
        return self._data[0] if self._data else None

    def fetchall(self):  # pragma: no cover - exercised via tests
        return list(self._data)


class _StubConnection:
    def __init__(self, mode: str = "view") -> None:
        self._mode = mode

    def cursor(self):
        return _StubCursor(self._mode)

    def __enter__(self):  # pragma: no cover - context helper
        return self

    def __exit__(self, exc_type, exc, tb):  # pragma: no cover - context helper
        return False


@pytest.fixture(autouse=True)
def _reset_cache(monkeypatch):
    monkeypatch.setattr(db_capability, "_CACHED_CAPABILITY", None)
    monkeypatch.setattr(db_capability, "oracledb", object())


def test_detects_vector_from_view(monkeypatch):
    monkeypatch.setattr(db_capability, "get_connection", lambda: _StubConnection("view"))
    capability = db_capability.detect_capability(refresh=True)
    assert capability.vector_supported is True
    assert capability.vec_col == "EMBEDDING"
    assert capability.vec_dim == 384
    assert capability.vector_rows == 5
    assert capability.esg_docs_count == 10
    assert capability.oracle_text_supported is True


def test_falls_back_to_tab_columns(monkeypatch):
    monkeypatch.setattr(db_capability, "get_connection", lambda: _StubConnection("view_fail"))
    capability = db_capability.detect_capability(refresh=True)
    assert capability.vector_supported is True
    assert capability.vec_col == "VECTOR"
    assert capability.vec_dim == 768
