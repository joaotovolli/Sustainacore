import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from index_engine import db_index_calc


class _FakeCursor:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    def execute(self, sql: str, *args, **kwargs) -> None:
        self._calls.append(sql)

    def executemany(self, sql: str, *args, **kwargs) -> None:
        self._calls.append(sql)


class _FakeConn:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._calls)

    def commit(self) -> None:
        self._calls.append("COMMIT")

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_upsert_constituent_daily_disables_parallel_dml(monkeypatch) -> None:
    calls: list[str] = []

    def _fake_get_connection():
        return _FakeConn(calls)

    monkeypatch.setattr(db_index_calc, "get_connection", _fake_get_connection)

    db_index_calc.upsert_constituent_daily(
        [
            {
                "trade_date": "2025-01-02",
                "ticker": "AAA",
                "rebalance_date": "2025-01-02",
                "shares": 1.0,
                "price_used": 100.0,
                "market_value": 100.0,
                "weight": 1.0,
                "price_quality": "REAL",
            }
        ]
    )

    assert any("ALTER SESSION DISABLE PARALLEL DML" in call for call in calls)
