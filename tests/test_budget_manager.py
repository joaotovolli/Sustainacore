from tools.research_generator.budget_manager import choose_profile


def test_budget_profile_medium_safe_when_unavailable():
    snapshot = {"available": False, "reason": "usage_unavailable"}
    decision = choose_profile(snapshot)
    assert decision.profile.name == "MEDIUM_SAFE_NO_USAGE"


def test_budget_profile_low():
    snapshot = {"available": True, "weekly": {"used_pct": 72}, "five_hour": {"used_pct": 10}}
    decision = choose_profile(snapshot)
    assert decision.profile.name == "LOW"


def test_budget_profile_minimal():
    snapshot = {"available": True, "weekly": {"used_pct": 10}, "five_hour": {"used_pct": 90}}
    decision = choose_profile(snapshot)
    assert decision.profile.name == "MINIMAL"


def test_budget_profile_skipped():
    snapshot = {"available": True, "weekly": {"used_pct": 96}, "five_hour": {"used_pct": 10}}
    decision = choose_profile(snapshot)
    assert decision.profile.name == "SKIPPED_BUDGET"
