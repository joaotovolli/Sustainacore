import pathlib
import sys

# Allow importing pure index_engine modules without pulling the broken top-level app.py
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "app"))

from index_engine.divisor import compute_divisor_for_continuity
from index_engine.index_calc import compute_index_level


def test_divisor_ensures_level_continuity():
    old_level = 1000.0
    new_holdings = {"AAA": 50.0, "BBB": 75.0, "CCC": 120.0}
    new_prices = {"AAA": 200.0, "BBB": 150.0, "CCC": 80.0}

    divisor = compute_divisor_for_continuity(old_level, new_holdings, new_prices)
    new_level = compute_index_level(new_holdings, new_prices, divisor)

    assert new_level is not None
    assert abs(new_level - old_level) < 1e-9
