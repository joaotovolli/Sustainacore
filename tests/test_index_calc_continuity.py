import datetime as dt

import pytest

from app.index_engine.index_calc_v1 import compute_stats
from tools.index_engine import calc_index as ci


def test_seed_prior_state_uses_previous_level(monkeypatch):
    start = dt.date(2026, 1, 2)
    rebalance = dt.date(2025, 11, 21)
    prev_day = dt.date(2025, 12, 31)

    monkeypatch.setattr(ci.db, "fetch_last_level_before", lambda date: (prev_day, 1079.0))
    monkeypatch.setattr(ci.db, "fetch_latest_rebalance_date", lambda date: rebalance)
    monkeypatch.setattr(ci.db, "fetch_holdings_for_rebalance", lambda date: {"AAA": 2.0})
    monkeypatch.setattr(ci.db, "fetch_divisor_for_date", lambda date: 3.0)
    monkeypatch.setattr(ci.db, "fetch_universe", lambda date: (rebalance, ["AAA"]))

    prev_trade, current_reb, prev_port_date, holdings_by_reb, divisors_by_reb, levels = ci._seed_prior_state(
        start_date=start,
        allow_close=True,
    )

    assert prev_trade == prev_day
    assert current_reb == rebalance
    assert prev_port_date == rebalance
    assert holdings_by_reb[rebalance]["AAA"] == 2.0
    assert divisors_by_reb[rebalance] == 3.0
    assert levels[prev_day] == 1079.0


def test_stats_ret_1d_uses_lookback_level():
    prev_day = dt.date(2025, 12, 31)
    day = dt.date(2026, 1, 2)
    levels = {prev_day: 1100.0, day: 1122.0}
    ordered = [prev_day, day]
    returns = ci._compute_returns_1d_from_levels(ordered, levels)
    stats = compute_stats(
        trading_days=ordered,
        levels=levels,
        weights_by_date={},
        returns_1d=returns,
    )
    assert stats[day]["ret_1d"] == pytest.approx(1122.0 / 1100.0 - 1.0)
