import datetime as dt

from tools.research_generator import analysis
from tools.research_generator.validators import quality_gate_strict


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


def test_quality_gate_requires_coverage_mean():
    latest = [_row("Alpha", "AAA", 0.2, "Tech", 80)]
    prev = [_row("Alpha", "AAA", 0.2, "Tech", 79)]
    bundle = analysis.build_rebalance_bundle(dt.date(2025, 1, 1), dt.date(2024, 12, 1), latest, prev)
    bundle.metrics["coverage"]["mean_aiges"] = None
    writer = {
        "headline": "Core and coverage metrics in AI governance",
        "paragraphs": [
            "The chart above shows core and coverage metrics. The table below highlights IQR, HHI, turnover, and breadth with values 1 2 3 4 5 6.",
        ],
        "table_caption": "Summary",
        "chart_caption": "Chart",
        "compliance_checklist": {"no_prices": True, "no_advice": True, "tone_ok": True},
    }
    ok, issues = quality_gate_strict(bundle.to_dict(), writer, {"validation_flags": {}}, {"table_style_applied": True})
    assert not ok
    assert "coverage_mean_missing" in issues
