import datetime as dt

from tools.research_generator import analysis


def _row(company, ticker, weight, sector, aiges):
    return {
        "company": company,
        "ticker": ticker,
        "weight": weight,
        "sector": sector,
        "aiges": aiges,
        "summary": "",
    }


def test_equal_weight_delta_check():
    assert analysis._check_equal_weight_delta(4.0, 25)
    assert not analysis._check_equal_weight_delta(5.0, 25)


def test_core_vs_coverage_summary_exists():
    latest = [
        _row("Alpha", "AAA", 0.2, "Tech", 80),
        _row("Beta", "BBB", 0.1, "Tech", 70),
        _row("Gamma", "CCC", 0.0, "Health", 60),
    ]
    prev = [
        _row("Alpha", "AAA", 0.15, "Tech", 78),
        _row("Beta", "BBB", 0.05, "Tech", 72),
        _row("Gamma", "CCC", 0.0, "Health", 61),
    ]
    bundle = analysis.build_rebalance_bundle(dt.date(2025, 1, 1), dt.date(2024, 12, 1), latest, prev)
    metrics = bundle.metrics
    assert metrics["core"]["n"] == 2
    assert metrics["coverage"]["n"] == 3
