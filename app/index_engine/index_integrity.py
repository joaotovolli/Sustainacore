"""Pure quantitative invariants for TECH100 reconstruction verification."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class IntegrityCheck:
    name: str
    value: float | int | str
    tolerance: float | int | str
    passed: bool
    date: dt.date | None = None
    ticker: str | None = None
    detail: str = ""


@dataclass(frozen=True)
class RebalanceAudit:
    bridge_residual: float
    missing_price_count: int
    stale_anchor_count: int
    maximum_anchor_residual: float


def contribution_residual(index_return: float, contributions: Mapping[str, float]) -> float:
    return float(index_return) - sum(float(value) for value in contributions.values())


def market_value_residual(*, shares: float, price: float, market_value: float) -> float:
    return float(market_value) - float(shares) * float(price)


def rebalance_bridge_residual(
    *,
    previous_level: float,
    divisor: float,
    shares: Mapping[str, float],
    prices: Mapping[str, float],
) -> float:
    if divisor <= 0:
        raise ValueError("invalid_divisor")
    missing = set(shares) - set(prices)
    if missing:
        raise ValueError(f"missing_prices:{len(missing)}")
    bridge = sum(float(shares[ticker]) * float(prices[ticker]) for ticker in shares) / float(divisor)
    return bridge - float(previous_level)


def audit_rebalance(
    *,
    previous_level: float,
    previous_divisor: float,
    new_divisor: float,
    target_weights: Mapping[str, float],
    shares: Mapping[str, float],
    exact_previous_prices: Mapping[str, float],
    anchor_relative_tolerance: float,
) -> RebalanceAudit:
    missing = set(shares) - set(exact_previous_prices)
    bridge_residual = float("inf")
    if not missing:
        bridge_residual = rebalance_bridge_residual(
            previous_level=previous_level,
            divisor=new_divisor,
            shares=shares,
            prices=exact_previous_prices,
        )

    stale_count = 0
    maximum_anchor_residual = 0.0
    for ticker, share_count in shares.items():
        price = exact_previous_prices.get(ticker)
        weight = target_weights.get(ticker)
        if price is None or weight is None or share_count <= 0 or price <= 0:
            continue
        implied_anchor = float(weight) * float(previous_level) * float(previous_divisor) / float(share_count)
        relative_residual = abs(implied_anchor / float(price) - 1.0)
        maximum_anchor_residual = max(maximum_anchor_residual, relative_residual)
        if relative_residual > anchor_relative_tolerance:
            stale_count += 1
    return RebalanceAudit(bridge_residual, len(missing), stale_count, maximum_anchor_residual)


def maximum_check(
    name: str,
    value: float,
    tolerance: float,
    *,
    date: dt.date | None = None,
    ticker: str | None = None,
    detail: str = "",
) -> IntegrityCheck:
    return IntegrityCheck(name, value, tolerance, abs(float(value)) <= tolerance, date, ticker, detail)


__all__ = [
    "IntegrityCheck",
    "RebalanceAudit",
    "audit_rebalance",
    "contribution_residual",
    "market_value_residual",
    "maximum_check",
    "rebalance_bridge_residual",
]
