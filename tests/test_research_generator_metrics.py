import datetime as dt

from tools.research_generator import analysis
from tools.research_generator import ping_pong
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


def test_weight_delta_pp_units():
    latest = [
        _row("Alpha", "AAA", 0.02, "Tech", 80),
    ]
    prev = [
        _row("Alpha", "AAA", 0.04, "Tech", 78),
    ]
    movers = analysis._build_movers(latest, prev)
    incumbent = movers["incumbent_weight"][0]
    delta_pp = round(incumbent["delta_weight"] * 100, 2)
    assert delta_pp == -2.0


def test_sanitize_removes_needs_review():
    assert "needs review" not in ping_pong._sanitize_text("Needs review: automated checks flagged")


def test_remove_external_claims():
    text = "Industry reports indicate progress. Core metrics show change."
    cleaned = ping_pong._remove_external_claims(text)
    assert "Industry reports" not in cleaned
