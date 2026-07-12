import datetime as dt

import pytest

from app.index_engine.index_integrity import (
    audit_rebalance,
    contribution_residual,
    market_value_residual,
    maximum_check,
    rebalance_bridge_residual,
)


def test_contribution_sum_reconciles() -> None:
    assert contribution_residual(0.03, {"A": 0.01, "B": 0.02}) == pytest.approx(0)


def test_market_value_identity() -> None:
    assert market_value_residual(shares=4, price=25, market_value=100) == pytest.approx(0)


def test_rebalance_bridge_reconciles() -> None:
    assert rebalance_bridge_residual(
        previous_level=1000,
        divisor=1,
        shares={"A": 5, "B": 2.5},
        prices={"A": 100, "B": 200},
    ) == pytest.approx(0)


def test_excessive_index_return_fails_verification() -> None:
    check = maximum_check(
        "maximum_unexplained_index_return",
        -0.25,
        0.20,
        date=dt.date(2026, 7, 2),
    )
    assert not check.passed


def test_rebalance_bridge_mismatch_fails_verification() -> None:
    audit = audit_rebalance(
        previous_level=1000,
        previous_divisor=1,
        new_divisor=0.9,
        target_weights={"A": 0.5, "B": 0.5},
        shares={"A": 5, "B": 2.5},
        exact_previous_prices={"A": 100, "B": 200},
        anchor_relative_tolerance=1e-8,
    )
    assert abs(audit.bridge_residual) > 1e-6


def test_stale_rebalance_anchor_fails_verification() -> None:
    audit = audit_rebalance(
        previous_level=1000,
        previous_divisor=1,
        new_divisor=1,
        target_weights={"A": 1.0},
        shares={"A": 5.0},
        exact_previous_prices={"A": 100.0},
        anchor_relative_tolerance=1e-8,
    )
    assert audit.stale_anchor_count == 1
    assert audit.maximum_anchor_residual == pytest.approx(1.0)


def test_rebalance_bridge_requires_all_prices() -> None:
    with pytest.raises(ValueError, match="missing_prices"):
        rebalance_bridge_residual(previous_level=1000, divisor=1, shares={"A": 1}, prices={})
