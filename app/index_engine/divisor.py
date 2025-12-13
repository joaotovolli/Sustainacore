"""Divisor continuity utilities for SC_IDX indexes."""


def compute_divisor_for_continuity(
    old_level: float,
    new_holdings: dict[str, float],
    prices: dict[str, float],
) -> float:
    """
    Compute the divisor that keeps the index level continuous across a rebalance.

    Args:
        old_level: Previous official index level.
        new_holdings: Mapping of ticker -> shares for the new basket.
        prices: Mapping of ticker -> adjusted close prices on the rebalance date.

    Returns:
        The divisor that ensures sum(shares * price) / divisor == old_level.
    """

    market_value = 0.0
    for ticker, shares in new_holdings.items():
        price = prices.get(ticker)
        if price is None:
            raise ValueError(f"Missing price for {ticker} while computing divisor")
        market_value += shares * price

    if old_level == 0:
        raise ValueError("old_level must be non-zero to compute divisor")

    return market_value / old_level
