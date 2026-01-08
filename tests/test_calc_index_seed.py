import datetime as dt

import pytest

from tools.index_engine import calc_index


def test_seed_prior_state_prefers_constituent_shares(monkeypatch):
    prev_trade = dt.date(2026, 1, 2)
    prev_level = 1234.0
    shares_prev = {"AAA": 10.0, "BBB": 20.0}

    monkeypatch.setattr(calc_index.db, "fetch_last_level_before", lambda start: (prev_trade, prev_level))
    monkeypatch.setattr(calc_index.db, "fetch_constituent_shares", lambda trade: shares_prev)
    monkeypatch.setattr(
        calc_index.db,
        "fetch_prices",
        lambda trade, tickers, allow_close: {
            "AAA": {"price": 5.0, "quality": "REAL"},
            "BBB": {"price": 10.0, "quality": "REAL"},
        },
    )
    monkeypatch.setattr(calc_index.db, "fetch_universe", lambda trade: (dt.date(2026, 1, 1), list(shares_prev)))
    monkeypatch.setattr(calc_index.db, "fetch_latest_rebalance_date", lambda start: (_ for _ in ()).throw(RuntimeError()))
    monkeypatch.setattr(calc_index.db, "fetch_holdings_for_rebalance", lambda date: (_ for _ in ()).throw(RuntimeError()))
    monkeypatch.setattr(calc_index.db, "fetch_divisor_for_date", lambda date: (_ for _ in ()).throw(RuntimeError()))
    monkeypatch.setattr(calc_index.db, "fetch_level_for_date", lambda date: (_ for _ in ()).throw(RuntimeError()))

    prev_trade_out, current_reb, prev_port_date, holdings_by_reb, divisors_by_reb, levels = (
        calc_index._seed_prior_state(start_date=dt.date(2026, 1, 5), allow_close=False)
    )

    assert prev_trade_out == prev_trade
    assert current_reb == prev_trade
    assert prev_port_date == dt.date(2026, 1, 1)
    assert holdings_by_reb[prev_trade] == shares_prev
    assert pytest.approx(divisors_by_reb[prev_trade], rel=1e-9) == 250.0 / prev_level
    assert levels[prev_trade] == prev_level
