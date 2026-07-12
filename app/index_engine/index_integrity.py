"""Pure quantitative invariants for TECH100 reconstruction verification."""

from __future__ import annotations

from typing import Mapping


def contribution_residual(index_return: float, contributions: Mapping[str, float]) -> float:
    return float(index_return) - sum(float(value) for value in contributions.values())


def market_value_residual(*, shares: float, price: float, market_value: float) -> float:
    return float(market_value) - float(shares) * float(price)


def rebalance_bridge_residual(
    *, previous_level: float, divisor: float, shares: Mapping[str, float], prices: Mapping[str, float]
) -> float:
    if divisor <= 0:
        raise ValueError("invalid_divisor")
    missing=set(shares)-set(prices)
    if missing:
        raise ValueError(f"missing_prices:{len(missing)}")
    bridge=sum(float(shares[t])*float(prices[t]) for t in shares)/float(divisor)
    return bridge-float(previous_level)


__all__=["contribution_residual","market_value_residual","rebalance_bridge_residual"]
