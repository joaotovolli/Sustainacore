import importlib.util
from pathlib import Path


def _load_calc_index_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "tools" / "index_engine" / "calc_index.py"
    spec = importlib.util.spec_from_file_location("calc_index", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable_to_load_calc_index")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def test_missing_diagnostics_render() -> None:
    calc_index = _load_calc_index_module()
    diagnostics = {
        "missing_by_date": [
            (calc_index._parse_date("2025-01-02"), 3, 25, 5),
        ],
        "missing_by_ticker": [("AAA", 2)],
        "sample_missing": [
            (calc_index._parse_date("2025-01-02"), "AAA", "no_canon_price"),
        ],
    }

    output = calc_index._render_missing_diagnostics(
        start_date=calc_index._parse_date("2025-01-02"),
        end_date=calc_index._parse_date("2025-01-03"),
        diagnostics=diagnostics,
    )

    assert "index_calc_missing_diagnostics" in output
    assert "top_missing_dates" in output
    assert "top_missing_tickers" in output
    assert "AAA missing_days=2" in output


def test_print_missing_diagnostics_header(capsys, monkeypatch) -> None:
    calc_index = _load_calc_index_module()

    def _fake_diagnose(**_kwargs):
        return {
            "missing_by_date": [],
            "missing_by_ticker": [],
            "sample_missing": [],
        }

    monkeypatch.setattr(calc_index.db, "diagnose_missing_canon_sql", _fake_diagnose)

    calc_index._print_missing_diagnostics(
        start_date=calc_index._parse_date("2025-01-02"),
        end_date=calc_index._parse_date("2025-01-03"),
        max_dates=5,
        max_tickers=5,
        max_samples=5,
    )
    captured = capsys.readouterr()
    assert "starting diagnostics..." in captured.out
