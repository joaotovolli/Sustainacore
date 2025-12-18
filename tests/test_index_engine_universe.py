import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "app" / "index_engine" / "universe.py"
spec = importlib.util.spec_from_file_location("universe_test", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("unable to load universe module for tests")
universe = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = universe
spec.loader.exec_module(universe)  # type: ignore[arg-type]


def test_select_top_weighted_tickers_filters_and_orders():
    rows = [
        {"ticker": "AAA", "port_weight": 0.2, "rank_index": 2},
        {"ticker": "BBB", "port_weight": 0.5, "rank_index": 1},
        {"ticker": "CCC", "port_weight": -0.1, "rank_index": 3},
        {"ticker": "DDD", "port_weight": 0.3, "rank_index": 4},
    ]
    result = universe.select_top_weighted_tickers(rows, limit=2)
    assert result == ["BBB", "DDD"]
