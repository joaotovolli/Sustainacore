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


def test_model_output_persistence_uses_bounded_batches_and_fresh_connections(monkeypatch):
    batch_lengths = []
    connections = []

    class Cursor:
        def executemany(self, _sql, rows):
            batch_lengths.append(len(rows))

    class Connection:
        call_timeout = None

        def __init__(self):
            self.commits = 0

        def cursor(self):
            return Cursor()

        def commit(self):
            self.commits += 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def connection():
        value = Connection()
        connections.append(value)
        return value

    monkeypatch.setenv("SC_IDX_PORTFOLIO_WRITE_BATCH_SIZE", "250")
    monkeypatch.setattr(db_portfolio_analytics, "get_connection", connection)
    analytics = [{} for _ in range(601)]
    positions = [{} for _ in range(601)]

    assert db_portfolio_analytics.persist_model_output_batch(
        analytics_rows=analytics,
        position_rows=positions,
    ) == (601, 601)
    assert batch_lengths == [250, 250, 101, 250, 250, 101]
    assert len(connections) == 2
    assert [conn.commits for conn in connections] == [1, 1]


def test_static_constraints_match_without_any_constraint_dml(monkeypatch):
    expected = [
        {
            "model_code": "TECH100",
            "constraint_key": "LONG_ONLY",
            "constraint_type": "BOOLEAN",
            "constraint_value": "TRUE",
        }
    ]
    monkeypatch.setattr(
        db_portfolio_analytics,
        "fetch_constraint_rows",
        lambda: [("TECH100", "LONG_ONLY", "BOOLEAN", "TRUE")],
    )
    db_portfolio_analytics.validate_static_constraints(expected)


def test_static_constraint_mismatch_fails_closed(monkeypatch):
    expected = [
        {
            "model_code": "TECH100",
            "constraint_key": "LONG_ONLY",
            "constraint_type": "BOOLEAN",
            "constraint_value": "TRUE",
        }
    ]
    monkeypatch.setattr(db_portfolio_analytics, "fetch_constraint_rows", lambda: [])
    import pytest

    with pytest.raises(RuntimeError, match="model_portfolio_constraints_mismatch"):
        db_portfolio_analytics.validate_static_constraints(expected)


def test_reset_output_window_never_mutates_static_constraints(monkeypatch):
    executed = []

    class Cursor:
        def execute(self, sql, _binds=None):
            executed.append(" ".join(sql.upper().split()))

    class Connection:
        call_timeout = None

        def __init__(self):
            self.commits = 0

        def cursor(self):
            return Cursor()

        def commit(self):
            self.commits += 1

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    connection = Connection()
    monkeypatch.setattr(db_portfolio_analytics, "get_connection", lambda: connection)
    db_portfolio_analytics.reset_output_window(
        start_date=dt.date(2025, 1, 2),
        end_date=dt.date(2026, 7, 10),
    )
    assert connection.commits == 1
    assert len(executed) == 3
    assert all("SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS" not in sql for sql in executed)


def test_optimizer_persistence_validates_but_never_rewrites_constraints(monkeypatch):
    executed = []
    validations = []

    class Cursor:
        def executemany(self, sql, rows):
            executed.append((" ".join(sql.upper().split()), len(rows)))

    class Connection:
        call_timeout = None

        def cursor(self):
            return Cursor()

        def commit(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(db_portfolio_analytics, "get_connection", Connection)
    monkeypatch.setattr(
        db_portfolio_analytics,
        "validate_static_constraints",
        lambda rows: validations.append(rows),
    )
    constraints = [{"model_code": "TECH100", "constraint_key": "LONG_ONLY"}]
    assert db_portfolio_analytics.persist_optimizer_with_static_constraints(
        optimizer_rows=[{}],
        constraint_rows=constraints,
    ) == 1
    assert validations == [constraints]
    assert all("SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS" not in sql for sql, _ in executed)
