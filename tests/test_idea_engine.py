from tools.research_generator.idea_engine import build_metric_pool, ensure_angle_count, rank_angles


def test_metric_pool_min_size():
    bundle = {
        "core_latest_rows": [{"aiges": 80, "weight": 0.04}],
        "rest_latest_rows": [{"aiges": 60, "weight": 0.0}],
        "core_previous_rows": [{"aiges": 78, "weight": 0.04, "ticker": "AAA"}],
    }
    pool = build_metric_pool(bundle, max_metrics=25)
    assert len(pool) <= 25


def test_ranking_returns_one():
    angles = [
        {"angle_title": "Angle A", "callouts": [1, 2, 3], "metrics_used": ["m1"]},
        {"angle_title": "Angle B", "callouts": [1], "metrics_used": ["m2"]},
    ]
    ranked = rank_angles(angles, report_type="REBALANCE", conn=None, prior_fingerprints=[])
    assert ranked[0]["score"] >= ranked[1]["score"]


def test_candidate_generation_count():
    angles = ensure_angle_count([], minimum=3)
    assert len(angles) == 3


def test_novelty_penalty_basic():
    angles = [
        {"angle_title": "Angle A", "callouts": [1, 2, 3], "metrics_used": ["m1"]},
    ]
    first = rank_angles(angles, report_type="REBALANCE", conn=None, prior_fingerprints=[])
    base_score = first[0]["score"]
    prior = [first[0]["fingerprint"]]
    second = rank_angles(angles, report_type="REBALANCE", conn=None, prior_fingerprints=prior)
    assert second[0]["score"] < base_score
