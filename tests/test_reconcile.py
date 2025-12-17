import math
import pathlib
import sys

# Allow importing pure index_engine modules without pulling the broken top-level app.py
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "app"))

from index_engine.reconcile import reconcile_canonical


def test_two_providers_within_threshold_high_quality():
    provider_adj_closes = {"TWELVEDATA": 100.0, "OTHER": 100.4}
    provider_closes = {"TWELVEDATA": 101.0, "OTHER": 101.4}

    result = reconcile_canonical(provider_adj_closes, provider_closes)

    assert result["providers_ok"] == 2
    assert result["chosen_provider"] == "MEDIAN"
    assert result["quality"] == "HIGH"
    assert math.isclose(result["canon_adj_close"], 100.2)
    assert math.isclose(result["canon_close"], 101.2)
    assert result["divergence_pct"] <= 0.5


def test_two_providers_above_threshold_conflict_prefers_twelvedata():
    provider_adj_closes = {"TWELVEDATA": 100.0, "OTHER": 101.0}
    provider_closes = {"TWELVEDATA": 99.5, "OTHER": 101.2}

    result = reconcile_canonical(provider_adj_closes, provider_closes)

    assert result["providers_ok"] == 2
    assert result["chosen_provider"] == "TWELVEDATA"
    assert result["quality"] == "CONFLICT"
    assert result["canon_adj_close"] == 100.0
    assert result["canon_close"] == 99.5
    assert result["divergence_pct"] > 0.5


def test_single_provider_low_quality():
    provider_adj_closes = {"TWELVEDATA": 50.0}
    provider_closes = {"TWELVEDATA": 49.8}

    result = reconcile_canonical(provider_adj_closes, provider_closes)

    assert result["providers_ok"] == 1
    assert result["chosen_provider"] == "TWELVEDATA"
    assert result["quality"] == "LOW"
    assert result["canon_adj_close"] == 50.0
    assert result["canon_close"] == 49.8
    assert result["divergence_pct"] is None
