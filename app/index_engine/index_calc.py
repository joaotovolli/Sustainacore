"""Index level calculation utilities for SC_IDX indexes."""


def compute_index_level(
    holdings: dict[str, float],
    prices: dict[str, float],
    divisor: float,
    min_coverage: float = 0.90,
) -> float | None:
    """
    Compute an index level from holdings and prices.

    Args:
        holdings: Mapping of ticker -> shares.
        prices: Mapping of ticker -> adjusted close prices for the calculation date.
        divisor: Current index divisor.
        min_coverage: Minimum fraction of holdings that must have prices.

    Returns:
        The computed level, or ``None`` if coverage is insufficient.
    """

    if not holdings or divisor == 0:
        return None

    available = {
        ticker: price
        for ticker, price in prices.items()
        if ticker in holdings and price is not None
    }

    coverage = len(available) / len(holdings)
    if coverage < min_coverage:
        return None

    market_value = sum(holdings[ticker] * price for ticker, price in available.items())
    return market_value / divisor
