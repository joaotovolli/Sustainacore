from tools.research_generator.budget_manager import choose_profile


def test_budget_profile_minimal_by_context():
    settings = {"max_context_pct": 4, "saver_mode": "MEDIUM"}
    decision = choose_profile(settings)
    assert decision.profile.name == "MINIMAL"


def test_budget_profile_low_by_context():
    settings = {"max_context_pct": 7, "saver_mode": "MEDIUM"}
    decision = choose_profile(settings)
    assert decision.profile.name == "LOW"


def test_budget_profile_saver_mode():
    settings = {"max_context_pct": 20, "saver_mode": "LOW"}
    decision = choose_profile(settings)
    assert decision.profile.name == "LOW"


def test_budget_profile_medium_default():
    settings = {"max_context_pct": 20, "saver_mode": "MEDIUM"}
    decision = choose_profile(settings)
    assert decision.profile.name == "MEDIUM"
