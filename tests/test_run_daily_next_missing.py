import datetime as dt

from tools.index_engine.run_daily import select_next_missing_trading_day


def test_select_next_missing_trading_day_year_boundary():
    trading_days = [dt.date(2025, 12, 31), dt.date(2026, 1, 2)]
    assert select_next_missing_trading_day(trading_days, dt.date(2025, 12, 31)) == dt.date(2026, 1, 2)


def test_select_next_missing_trading_day_when_up_to_date():
    trading_days = [dt.date(2025, 12, 31), dt.date(2026, 1, 2)]
    assert select_next_missing_trading_day(trading_days, dt.date(2026, 1, 2)) is None
