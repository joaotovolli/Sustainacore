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
    max_complete_date = dt.date(2026, 1, 2)

    new_start, filtered, status = calc_index._select_calc_window(
        trading_days=trading_days,
        start_date=start_date,
        end_date=end_date,
        max_complete_date=max_complete_date,
        rebuild=False,
    )

    assert status is None
    assert new_start == dt.date(2026, 1, 5)
    assert filtered == [dt.date(2026, 1, 5), dt.date(2026, 1, 6), dt.date(2026, 1, 7)]


def test_select_calc_window_noop_when_up_to_date():
    trading_days = [dt.date(2026, 1, 2)]
    start_date = dt.date(2025, 1, 2)
    end_date = dt.date(2026, 1, 2)
    max_complete_date = dt.date(2026, 1, 2)

    new_start, filtered, status = calc_index._select_calc_window(
        trading_days=trading_days,
        start_date=start_date,
        end_date=end_date,
        max_complete_date=max_complete_date,
        rebuild=False,
    )

    assert status == "up_to_date"
    assert filtered == []
    assert new_start == start_date


def test_select_calc_window_reprocesses_when_stats_lag_levels():
    trading_days = [
        dt.date(2026, 3, 31),
        dt.date(2026, 4, 1),
    ]
    start_date = dt.date(2026, 4, 1)
    end_date = dt.date(2026, 4, 1)
    max_complete_date = dt.date(2026, 3, 31)

    new_start, filtered, status = calc_index._select_calc_window(
        trading_days=trading_days,
        start_date=start_date,
        end_date=end_date,
        max_complete_date=max_complete_date,
        rebuild=False,
    )

    assert status is None
    assert new_start == dt.date(2026, 4, 1)
    assert filtered == [dt.date(2026, 4, 1)]
