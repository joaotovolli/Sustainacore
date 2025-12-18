import datetime as dt
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from index_engine.index_calc_v1 import (
    compute_constituent_daily,
    compute_contributions,
    compute_holdings_at_rebalance,
    compute_levels,
    compute_stats,
)


def test_two_ticker_levels_and_contributions() -> None:
    trading_days = [
        dt.date(2025, 1, 2),
        dt.date(2025, 1, 3),
        dt.date(2025, 1, 6),
    ]
    tickers = ["AAA", "BBB"]
    prices_day1 = {"AAA": 100.0, "BBB": 200.0}
    shares, divisor = compute_holdings_at_rebalance(
        tickers=tickers,
        prices_prev=prices_day1,
        level_prev=1000.0,
        divisor_prev=1.0,
    )
    assert divisor == 1.0
    assert shares["AAA"] == 5.0
    assert shares["BBB"] == 2.5

    prices_by_date = {
        trading_days[0]: prices_day1,
        trading_days[1]: {"AAA": 110.0, "BBB": 190.0},
        trading_days[2]: {"AAA": 120.0, "BBB": 210.0},
    }
    levels = compute_levels(
        trading_days=trading_days,
        holdings_by_rebalance={trading_days[0]: shares},
        divisors_by_rebalance={trading_days[0]: divisor},
        prices_by_date=prices_by_date,
    )
    assert levels[trading_days[0]] == 1000.0
    assert levels[trading_days[1]] == 1025.0
    assert levels[trading_days[2]] == 1125.0

    weights_by_date = compute_constituent_daily(
        trading_days=trading_days,
        holdings_by_rebalance={trading_days[0]: shares},
        prices_by_date=prices_by_date,
    )
    contributions = compute_contributions(
        trading_days=trading_days,
        weights_by_date=weights_by_date,
        prices_by_date=prices_by_date,
    )
    day2 = trading_days[1]
    day3 = trading_days[2]
    contrib_sum_day2 = sum(contributions[day2].values())
    contrib_sum_day3 = sum(contributions[day3].values())
    assert round(contrib_sum_day2, 6) == round(levels[day2] / levels[trading_days[0]] - 1.0, 6)
    assert round(contrib_sum_day3, 6) == round(levels[day3] / levels[day2] - 1.0, 6)


def test_stats_top5_and_herfindahl() -> None:
    trading_days = [dt.date(2025, 1, 2), dt.date(2025, 1, 3)]
    levels = {trading_days[0]: 1000.0, trading_days[1]: 1025.0}
    returns_1d = {trading_days[1]: levels[trading_days[1]] / levels[trading_days[0]] - 1.0}
    weights_by_date = {
        trading_days[0]: {"AAA": 0.5, "BBB": 0.5},
        trading_days[1]: {"AAA": 0.5365853659, "BBB": 0.4634146341},
    }
    stats = compute_stats(
        trading_days=trading_days,
        levels=levels,
        weights_by_date=weights_by_date,
        returns_1d=returns_1d,
    )
    day1 = stats[trading_days[0]]
    assert round(day1["top5_weight"], 6) == 1.0
    assert round(day1["herfindahl"], 6) == 0.5
    day2 = stats[trading_days[1]]
    assert round(day2["top5_weight"], 6) == 1.0
    assert round(day2["herfindahl"], 6) == round(0.5365853659**2 + 0.4634146341**2, 6)
