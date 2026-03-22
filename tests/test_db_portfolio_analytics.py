import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.index_engine import db_portfolio_analytics


class _MissingObjectCursor:
    def execute(self, sql, params=None):
        raise Exception("ORA-00942: table or view does not exist")

    def fetchone(self):  # pragma: no cover - execute always raises
        return None


class _MissingObjectConnection:
    def cursor(self):
        return _MissingObjectCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_fetch_portfolio_analytics_max_date_returns_none_when_table_missing(monkeypatch):
    monkeypatch.setattr(
        db_portfolio_analytics,
        "get_connection",
        lambda: _MissingObjectConnection(),
    )
    assert db_portfolio_analytics.fetch_portfolio_analytics_max_date() is None
