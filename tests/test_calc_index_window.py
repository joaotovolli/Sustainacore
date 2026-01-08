import datetime as dt

from tools.index_engine import calc_index


def test_select_calc_window_advances_start_date():
    trading_days = [
        dt.date(2026, 1, 2),
        dt.date(2026, 1, 5),
        dt.date(2026, 1, 6),
        dt.date(2026, 1, 7),
    ]
    start_date = dt.date(2025, 1, 2)
    end_date = dt.date(2026, 1, 7)
    max_level_date = dt.date(2026, 1, 2)

    new_start, filtered, status = calc_index._select_calc_window(
        trading_days=trading_days,
        start_date=start_date,
        end_date=end_date,
        max_level_date=max_level_date,
        rebuild=False,
    )

    assert status is None
    assert new_start == dt.date(2026, 1, 5)
    assert filtered == [dt.date(2026, 1, 5), dt.date(2026, 1, 6), dt.date(2026, 1, 7)]


def test_select_calc_window_noop_when_up_to_date():
    trading_days = [dt.date(2026, 1, 2)]
    start_date = dt.date(2025, 1, 2)
    end_date = dt.date(2026, 1, 2)
    max_level_date = dt.date(2026, 1, 2)

    new_start, filtered, status = calc_index._select_calc_window(
        trading_days=trading_days,
        start_date=start_date,
        end_date=end_date,
        max_level_date=max_level_date,
        rebuild=False,
    )

    assert status == "up_to_date"
    assert filtered == []
    assert new_start == start_date
