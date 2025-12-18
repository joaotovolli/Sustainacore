import datetime as dt
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from index_engine import db_index_calc


class _FakeCursor:
    def __init__(self, calls: list[str], results: list[list[tuple]]) -> None:
        self._calls = calls
        self._results = results
        self._idx = -1

    def execute(self, sql: str, *args, **kwargs) -> None:
        self._calls.append(sql)
        self._idx += 1

    def fetchall(self) -> list[tuple]:
        if 0 <= self._idx < len(self._results):
            return self._results[self._idx]
        return []


class _FakeConn:
    def __init__(self, calls: list[str], results: list[list[tuple]]) -> None:
        self._calls = calls
        self._results = results

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._calls, self._results)

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_diagnose_missing_canon_sql_executes_queries(monkeypatch) -> None:
    calls: list[str] = []
    results = [
        [(dt.date(2025, 1, 2), 1, 25, 0)],
        [("AAA", 1)],
        [(dt.date(2025, 1, 2), "AAA", "no_canon_price")],
    ]

    def _fake_get_connection():
        return _FakeConn(calls, results)

    monkeypatch.setattr(db_index_calc, "get_connection", _fake_get_connection)

    output = db_index_calc.diagnose_missing_canon_sql(
        start=dt.date(2025, 1, 2),
        end=dt.date(2025, 1, 3),
        max_dates=5,
        max_tickers=5,
        max_samples=5,
    )

    assert len(calls) == 3
    assert output["missing_by_date"][0][1] == 1
