import datetime as dt
import subprocess
from types import SimpleNamespace

import pytest

from tools.db_migrations import repair_sc_idx_corporate_actions as repair
from tools.index_engine import reconstruction_readiness as readiness


class FakeConnection:
    def __init__(self):
        self.rollbacks = 0

    def cursor(self):
        return object()

    def rollback(self):
        self.rollbacks += 1


class RecordingCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, binds=None):
        self.calls.append((sql, binds))


def test_select_only_cursor_rejects_oracle_writes():
    cursor = RecordingCursor()
    guarded = readiness.SelectOnlyCursor(cursor)
    guarded.execute("SELECT 1 FROM dual")
    with pytest.raises(RuntimeError, match="readiness_non_select_sql_rejected"):
        guarded.execute("UPDATE SC_IDX_LEVELS SET level_tr=0")
    assert len(cursor.calls) == 1


def args(*, end=dt.date(2026, 7, 2)):
    return SimpleNamespace(
        start=dt.date(2025, 1, 2),
        end=end,
        ticker="CRWD",
        adjusted_price_csv=None,
        probe_missing_anchors=True,
        require_quiescent=False,
        rehearse_portfolio=False,
        max_abs_constituent_return=0.20,
    )


def universe(prefix: str) -> list[str]:
    return [f"{prefix}{index:02d}" for index in range(25)]


def install_history(monkeypatch, *, days, universes, prices, confirmed=()):
    monkeypatch.setattr(readiness, "_schema_blockers", lambda *_: [])
    monkeypatch.setattr(readiness, "_trading_days", lambda *_: days)
    monkeypatch.setattr(readiness, "_universes", lambda *_: universes)
    def price_days(*_args, trading_days, planned=None, planned_ticker=None, **_kwargs):
        for day in trading_days:
            daily = {
                ticker: value
                for (price_day, ticker), value in prices.items()
                if price_day == day
            }
            if planned and planned_ticker and day in planned:
                daily[planned_ticker] = (float(planned[day]), "PLANNED_REPAIR")
            yield day, daily

    monkeypatch.setattr(readiness, "_price_maps_for_trading_days", price_days)
    monkeypatch.setattr(readiness, "_confirmed_actions", lambda *_: set(confirmed))


def test_readiness_aggregates_all_anchor_blockers_and_probes_each(monkeypatch):
    days = [
        dt.date(2025, 1, 2),
        dt.date(2025, 1, 3),
        dt.date(2025, 4, 1),
        dt.date(2025, 4, 2),
        dt.date(2025, 7, 1),
    ]
    base = universe("A")
    april = base[:-1] + ["NEW1"]
    july = april[:-3] + ["NEW2", "STALE1", "SUB1"]
    universes = {days[0]: base, days[2]: april, days[4]: july}
    tickers = set(base + april + july)
    prices = {(day, ticker): (100.0, "REAL") for day in days for ticker in tickers}
    prices.pop((days[1], "NEW1"))
    prices.pop((days[3], "NEW2"))
    prices[(days[3], "STALE1")] = (100.0, "STALE")
    prices[(days[3], "SUB1")] = (100.0, "CURRENT")
    install_history(monkeypatch, days=days, universes=universes, prices=prices)
    calls = []

    def probe(ticker, day):
        calls.append((ticker, day))
        return ticker == "NEW1"

    conn = FakeConnection()
    report = readiness.collect_readiness(conn, args(end=days[-1]), anchor_probe=probe)

    assert report.rebalance_count == 3
    assert report.missing_exact_anchor_count == 2
    assert report.unrecoverable_anchor_count == 1
    assert report.stale_anchor_count == 1
    assert report.substitute_anchor_count == 1
    assert calls == [("NEW1", days[1]), ("NEW2", days[3])]
    assert not report.passed
    assert conn.rollbacks == 1


def test_provider_failure_is_bounded_and_reported(monkeypatch):
    days = [dt.date(2025, 1, 2), dt.date(2025, 4, 1)]
    base = universe("A")
    april = base[:-1] + ["NEW1"]
    prices = {(day, ticker): (100.0, "REAL") for day in days for ticker in set(base + april)}
    prices.pop((days[0], "NEW1"))
    install_history(monkeypatch, days=days, universes={days[0]: base, days[1]: april}, prices=prices)
    calls = []

    def failing_probe(ticker, day):
        calls.append((ticker, day))
        raise RuntimeError("bounded_provider_failure")

    report = readiness.collect_readiness(FakeConnection(), args(end=days[-1]), anchor_probe=failing_probe)
    assert calls == [("NEW1", days[0])]
    assert report.unrecoverable_anchor_count == 1
    assert not report.passed


def test_recovered_anchor_price_is_used_for_split_classification(monkeypatch):
    days = [dt.date(2025, 1, 2), dt.date(2025, 4, 1)]
    base = universe("A")
    april = base[:-1] + ["NEW1"]
    prices = {(day, ticker): (100.0, "REAL") for day in days for ticker in set(base + april)}
    prices.pop((days[0], "NEW1"))
    prices[(days[1], "NEW1")] = (25.0, "REAL")
    install_history(monkeypatch, days=days, universes={days[0]: base, days[1]: april}, prices=prices)

    report = readiness.collect_readiness(
        FakeConnection(),
        args(end=days[-1]),
        anchor_probe=lambda _ticker, _day: 100.0,
    )
    assert report.unrecoverable_anchor_count == 0
    assert [(move.ticker, move.classification) for move in report.unresolved_splits] == [
        ("NEW1", "UNRESOLVED_SPLIT_CANDIDATE")
    ]
    assert not report.passed


def test_readiness_classifies_all_large_moves_and_unresolved_split(monkeypatch):
    days = [dt.date(2025, 1, 2), dt.date(2025, 1, 3)]
    tickers = universe("A")
    prices = {(day, ticker): (100.0, "REAL") for day in days for ticker in tickers}
    prices[(days[1], tickers[0])] = (130.0, "REAL")
    prices[(days[1], tickers[1])] = (25.0, "REAL")
    install_history(monkeypatch, days=days, universes={days[0]: tickers}, prices=prices)

    report = readiness.collect_readiness(FakeConnection(), args(end=days[-1]))
    classes = {move.ticker: move.classification for move in report.large_moves}
    assert classes[tickers[0]] == "AUDITED_NON_SPLIT_MOVE"
    assert classes[tickers[1]] == "UNRESOLVED_SPLIT_CANDIDATE"
    assert len(report.unresolved_splits) == 1
    assert not report.passed


def test_readiness_output_declares_zero_writes_and_aggregated_failure(capsys):
    report = readiness.ReadinessReport(dt.date(2025, 1, 2), dt.date(2026, 7, 10))
    report.schema_blockers.extend(("one", "two"))
    report.partial_source_dates.update((dt.date(2025, 2, 1), dt.date(2025, 2, 2)))
    readiness.print_report(report)
    output = capsys.readouterr().out
    assert "mode=RECONSTRUCTION_READINESS" in output
    assert "oracle_writes=0" in output
    assert "schema_blocker_count=2" in output
    assert "partial_source_date_count=2" in output
    assert "overall_status=FAIL" in output


def test_failed_readiness_prevents_reconstruction(monkeypatch):
    command = []

    def failed_run(argv, **kwargs):
        command.extend(argv)
        raise subprocess.CalledProcessError(2, argv)

    monkeypatch.setattr(repair.subprocess, "run", failed_run)
    repair_args = SimpleNamespace(
        start=dt.date(2025, 1, 2),
        ticker="CRWD",
        adjusted_price_csv=None,
    )
    with pytest.raises(subprocess.CalledProcessError):
        repair.run_reconstruction_readiness(repair_args, dt.date(2026, 7, 10))
    assert "--probe-missing-anchors" in command
    assert "--require-quiescent" in command
    assert "--rehearse-portfolio" in command


def test_price_stream_retains_only_one_date_and_uses_fetchmany():
    class Cursor:
        def __init__(self, rows):
            self.rows = rows
            self.offset = 0
            self.arraysize = 100
            self.fetch_sizes = []

        def execute(self, _sql, _binds):
            self.offset = 0

        def fetchmany(self, size):
            self.fetch_sizes.append(size)
            batch = self.rows[self.offset : self.offset + size]
            self.offset += len(batch)
            return batch

    days = [dt.date(2025, 1, 2), dt.date(2025, 1, 3), dt.date(2025, 1, 6)]
    tickers = [f"T{index:03d}" for index in range(400)]
    rows = [
        (day, ticker, 100.0 + index, "REAL")
        for day in days
        for index, ticker in enumerate(tickers)
    ]
    raw = Cursor(rows)
    streamed = list(
        readiness._iter_prices_by_day(
            readiness.SelectOnlyCursor(raw),
            start=days[0],
            end=days[-1],
            tickers=tickers,
            fetch_size=127,
        )
    )

    assert [day for day, _prices in streamed] == days
    assert [len(prices) for _day, prices in streamed] == [400, 400, 400]
    assert set(raw.fetch_sizes) == {127}
