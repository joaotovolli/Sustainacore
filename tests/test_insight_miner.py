from tools.research_generator.insight_miner import mine_insights


def test_insight_miner_returns_candidates():
    bundle = {
        "metrics": {
            "core": {"std_aiges": 5.0, "membership_turnover_pct": 12.0, "breadth_pct": 70.0, "top_quintile_share_pct": 60.0},
            "coverage": {"std_aiges": 10.0},
            "gaps": {"mean_gap_core_vs_coverage": 10.0},
            "sector_exposure": {
                "notable_moves": [("Tech", 4.0)],
                "sector_turnover": [{"Sector": "Tech", "Net Change": 2}],
            },
            "top_movers": {"entrants": [{"ticker": "AAA", "aiges_new": 70.0}]},
        }
    }
    insights = mine_insights(bundle)
    assert len(insights) >= 3
    titles = [item["title"] for item in insights]
    assert "Core vs coverage gap" in titles
