import sys
import datetime as dt
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
    assert db_portfolio_analytics.fetch_portfolio_position_max_date() is None


def test_fetch_portfolio_completion_max_date_uses_daily_floor_when_opt_inputs_cover_latest_rebalance(monkeypatch):
    monkeypatch.setattr(
        db_portfolio_analytics,
        "fetch_portfolio_analytics_max_date",
        lambda: dt.date(2026, 4, 2),
    )
    monkeypatch.setattr(
        db_portfolio_analytics,
        "fetch_portfolio_position_max_date",
        lambda: dt.date(2026, 4, 2),
    )
    monkeypatch.setattr(
        db_portfolio_analytics,
        "fetch_portfolio_opt_inputs_max_date",
        lambda: dt.date(2026, 4, 1),
    )
    monkeypatch.setattr(
        db_portfolio_analytics,
        "fetch_latest_required_portfolio_opt_inputs_date",
        lambda end_date: dt.date(2026, 4, 1),
    )

    assert db_portfolio_analytics.fetch_portfolio_completion_max_date() == dt.date(2026, 4, 2)


def test_fetch_portfolio_completion_max_date_stays_back_when_latest_rebalance_inputs_missing(monkeypatch):
    monkeypatch.setattr(
        db_portfolio_analytics,
        "fetch_portfolio_analytics_max_date",
        lambda: dt.date(2026, 4, 2),
    )
    monkeypatch.setattr(
        db_portfolio_analytics,
        "fetch_portfolio_position_max_date",
        lambda: dt.date(2026, 4, 2),
    )
    monkeypatch.setattr(
        db_portfolio_analytics,
        "fetch_portfolio_opt_inputs_max_date",
        lambda: dt.date(2026, 4, 1),
    )
    monkeypatch.setattr(
        db_portfolio_analytics,
        "fetch_latest_required_portfolio_opt_inputs_date",
        lambda end_date: dt.date(2026, 4, 2),
    )

    assert db_portfolio_analytics.fetch_portfolio_completion_max_date() == dt.date(2026, 4, 1)
