import datetime as dt

from tools.index_engine import run_daily


def test_select_next_missing_trading_day_when_none():
    trading_days = [
        dt.date(2026, 1, 2),
        dt.date(2026, 1, 5),
        dt.date(2026, 1, 6),
    ]

    assert run_daily.select_next_missing_trading_day(trading_days, None) == dt.date(2026, 1, 2)


def test_select_next_missing_trading_day_progresses():
    trading_days = [
        dt.date(2026, 1, 2),
        dt.date(2026, 1, 5),
        dt.date(2026, 1, 6),
    ]

    assert (
        run_daily.select_next_missing_trading_day(trading_days, dt.date(2026, 1, 2))
        == dt.date(2026, 1, 5)
    )
    assert run_daily.select_next_missing_trading_day(trading_days, dt.date(2026, 1, 6)) is None
