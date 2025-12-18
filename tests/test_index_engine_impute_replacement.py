import datetime as _dt
import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "index_engine" / "impute_missing_prices.py"
spec = importlib.util.spec_from_file_location("impute_tool_test", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("unable to load impute_missing_prices module for tests")
impute_tool = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = impute_tool
spec.loader.exec_module(impute_tool)  # type: ignore[arg-type]


def test_select_replacement_tickers_limits_and_filters():
    imputed_rows = [
        ("AAA", _dt.date(2025, 1, 2)),
        ("BBB", _dt.date(2025, 1, 2)),
        ("CCC", _dt.date(2025, 1, 3)),
    ]
    impacted_by_date = {
        _dt.date(2025, 1, 2): ["AAA"],
        _dt.date(2025, 1, 3): ["CCC"],
    }
    result = impute_tool.select_replacement_tickers(imputed_rows, impacted_by_date, limit=1)
    assert result == ["AAA"]
